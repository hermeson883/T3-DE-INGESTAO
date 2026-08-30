"""Load: landing (JSON Lines) -> Bronze (Delta).

Dois motores (config `autoloader.engine`):

  batch  (PADRAO) -> spark.read dos arquivos da landing. Sem streaming, sem
                     checkpoint, sem cold start. Rapido e previsivel para os
                     volumes do sample_mflix (~80k linhas no total).
  autoloader      -> readStream `cloudFiles` + checkpoint + schemaLocation
                     persistido + trigger(availableNow). Bonus +5 (ingestao
                     orientada a arquivos). Use no `jobs/bronze_job` para
                     reprocessamento / demonstracao.

Idempotencia (R3), igual nos dois motores:
  - colecoes full         -> MERGE por _source_id (upsert)
  - colecoes incrementais -> append (nao-duplicacao vem da watermark no extract;
                             o batch le so o arquivo DA execucao, o autoloader
                             usa o checkpoint)
  - modo reprocesso (bronze_job, le a landing inteira) -> sempre MERGE
"""

from __future__ import annotations

import datetime as _dt
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import AutoloaderConfig, BronzeConfig, CollectionSpec, TargetConfig
from .control import bronze_ddl
from .utils import get_logger

_log = get_logger("mflix_ingest.load")

# Ordem canonica das colunas da Bronze (a mesma do DDL em control.bronze_ddl).
BRONZE_COLUMNS = [
    "_source_id",
    "body_variant",
    "body_json",
    "_rescued_data",
    "_source_hash",
    "_source_file",
    "_ingestion_id",
    "_ingestion_timestamp",
    "_source_path",
    "_load_type",
    "_ingestion_date",
]


@dataclass
class LoadResult:
    collection: str
    rows_written: int
    rows_quarantined: int
    batches: int
    checkpoint: str = ""
    schema_location: str = ""


class BronzeLoader:
    def __init__(
        self,
        spark: SparkSession,
        target: TargetConfig,
        autoloader: AutoloaderConfig,
        bronze: BronzeConfig,
    ):
        self.spark = spark
        self.target = target
        self.al = autoloader
        self.bronze = bronze

    # ------------------------------------------------------------------ #
    def load(
        self,
        spec: CollectionSpec,
        run_id: str,
        ingestion_ts: _dt.datetime,
        source_path_tag: str,
        files: list[str] | None = None,
    ) -> LoadResult:
        """`files` = arquivos exatos desta execucao (extractor). None -> le a
        landing inteira (reprocesso)."""
        collection = spec.collection
        target_table = self.target.bronze_table(collection)
        quarantine_table = self.target.bronze_quarantine_table(collection)
        landing = self.target.landing_path(collection)
        part_col = self.bronze.partition_by

        os.makedirs(landing, exist_ok=True)
        self.spark.sql(bronze_ddl(target_table, part_col, self.bronze.table_properties))
        self.spark.sql(bronze_ddl(quarantine_table, part_col, {}))

        if self.al.engine == "autoloader":
            return self._load_autoloader(
                spec, target_table, quarantine_table, landing,
                run_id, ingestion_ts, source_path_tag,
            )
        return self._load_batch(
            spec, target_table, quarantine_table, landing, files,
            run_id, ingestion_ts, source_path_tag,
        )

    # ------------------------------------------------------------------ #
    # Motor BATCH (padrao)
    # ------------------------------------------------------------------ #
    def _load_batch(
        self,
        spec: CollectionSpec,
        target_table: str,
        quarantine_table: str,
        landing: str,
        files: list[str] | None,
        run_id: str,
        ingestion_ts: _dt.datetime,
        source_path_tag: str,
    ) -> LoadResult:
        collection = spec.collection
        ingest_mode = spec.ingest_mode or self.al.ingest_mode
        reprocess = files is None

        if reprocess:
            try:
                files = sorted(
                    os.path.join(landing, f) for f in os.listdir(landing)
                    if f.endswith(".jsonl")
                )
            except FileNotFoundError:
                files = []
        if not files:
            _log.info("[%s] landing sem arquivos — nada a carregar", collection)
            return LoadResult(collection, 0, 0, 0)

        use_merge = reprocess or self._write_mode(spec) == "merge"
        _log.info("[%s] batch: %d arquivo(s) | ingest_mode=%s | %s",
                  collection, len(files), ingest_mode, "MERGE" if use_merge else "append")

        raw = self._read_files(files, ingest_mode)
        enriched = self._add_lineage(
            raw, run_id, ingestion_ts, source_path_tag, spec.load_mode,
        ).persist()
        try:
            good = enriched.where(F.col("_source_id").isNotNull() & F.col("body_variant").isNotNull())
            bad = enriched.where(F.col("_source_id").isNull() | F.col("body_variant").isNull())
            n_bad = bad.count()

            if use_merge:
                self._merge(good, target_table)
            else:
                self._append(good, target_table)
            if n_bad:
                self._append(bad, quarantine_table)
                _log.warning("[%s] %d registro(s) -> quarentena", collection, n_bad)
        finally:
            enriched.unpersist()

        written = (
            self.spark.table(target_table)
            .where(F.col("_ingestion_id") == run_id).count()
        )
        _log.info("[%s] bronze (batch): %d gravados / %d quarentena", collection, written, n_bad)
        return LoadResult(collection, written, n_bad, 1)

    def _read_files(self, files: list[str], ingest_mode: str) -> DataFrame:
        """Le os .jsonl e devolve DF com body_json + body_variant + _rescued_data + _source_file."""
        if ingest_mode == "single_variant":
            # 1 linha = 1 documento; body_json byte-a-byte como veio da origem
            df = (
                self.spark.read.text(files)
                .withColumnRenamed("value", "body_json")
                .withColumn("body_variant", F.expr("try_parse_json(body_json)"))
                .withColumn("_rescued_data", F.lit(None).cast("string"))
            )
        else:
            j = (
                self.spark.read
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_rescued_data")
                .json(files)
            )
            if "_rescued_data" not in j.columns:
                j = j.withColumn("_rescued_data", F.lit(None).cast("string"))
            payload = [c for c in j.columns if c != "_rescued_data"]
            df = (
                j.withColumn("body_json", F.to_json(F.struct(*[F.col(c) for c in payload])))
                .withColumn("body_variant", F.expr("try_parse_json(body_json)"))
            )
        return df.withColumn("_source_file", F.col("_metadata.file_path"))

    # ------------------------------------------------------------------ #
    # Motor AUTO LOADER (bonus +5)
    # ------------------------------------------------------------------ #
    def _load_autoloader(
        self,
        spec: CollectionSpec,
        target_table: str,
        quarantine_table: str,
        landing: str,
        run_id: str,
        ingestion_ts: _dt.datetime,
        source_path_tag: str,
    ) -> LoadResult:
        collection = spec.collection
        checkpoint = self.target.checkpoint_path(collection)
        schema_loc = self.target.schema_path(collection)
        ingest_mode = spec.ingest_mode or self.al.ingest_mode
        write_mode = self._write_mode(spec)

        reader = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", self.al.cloud_files_format)
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.maxFilesPerTrigger", self.al.max_files_per_trigger)
            .option("cloudFiles.includeExistingFiles", "true")
        )
        if ingest_mode == "single_variant":
            reader = reader.option("singleVariantColumn", "body_variant") \
                           .option("cloudFiles.schemaEvolutionMode", "none")
        else:
            reader = (
                reader.option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaEvolutionMode", self.al.schema_evolution_mode)
                .option("rescuedDataColumn", self.al.rescued_data_column)
            )
            if spec.schema_hints:
                reader = reader.option("cloudFiles.schemaHints", spec.schema_hints)

        raw = reader.load(landing).withColumn("_source_file", F.col("_metadata.file_path"))
        stats = {"batches": 0}

        def process_batch(batch_df: DataFrame, batch_id: int) -> None:
            enriched = self._shape_autoloader(batch_df, ingest_mode)
            enriched = self._add_lineage(
                enriched, run_id, ingestion_ts, source_path_tag, spec.load_mode,
            ).persist()
            try:
                good = enriched.where(F.col("_source_id").isNotNull() & F.col("body_variant").isNotNull())
                bad = enriched.where(F.col("_source_id").isNull() | F.col("body_variant").isNull())
                if write_mode == "merge":
                    self._merge(good, target_table)
                else:
                    self._append(good, target_table)
                if bad.count():
                    self._append(bad, quarantine_table)
                stats["batches"] += 1
            finally:
                enriched.unpersist()

        query = (
            raw.writeStream
            .option("checkpointLocation", checkpoint)
            .foreachBatch(process_batch)
            .trigger(availableNow=True)
            .start()
        )
        try:
            query.awaitTermination()
        finally:
            if query.isActive:
                query.stop()

        written = self.spark.table(target_table).where(F.col("_ingestion_id") == run_id).count()
        quar = self.spark.table(quarantine_table).where(F.col("_ingestion_id") == run_id).count()
        _log.info("[%s] bronze (autoloader): %d gravados / %d quarentena (%d batches)",
                  collection, written, quar, stats["batches"])
        return LoadResult(collection, written, quar, stats["batches"], checkpoint, schema_loc)

    def _shape_autoloader(self, df: DataFrame, ingest_mode: str) -> DataFrame:
        rescued_col = self.al.rescued_data_column
        if ingest_mode == "single_variant":
            return (
                df.withColumn("body_json", F.expr("to_json(body_variant)"))
                .withColumn("_rescued_data", F.lit(None).cast("string"))
            )
        control_like = {rescued_col, "_source_file", "_metadata", "_rescued_data"}
        payload = [c for c in df.columns if c not in control_like]
        out = (
            df.withColumn("body_json", F.to_json(F.struct(*[F.col(c) for c in payload])))
            .withColumn("body_variant", F.expr("try_parse_json(body_json)"))
        )
        if rescued_col in df.columns and rescued_col != "_rescued_data":
            out = out.withColumnRenamed(rescued_col, "_rescued_data")
        elif "_rescued_data" not in out.columns:
            out = out.withColumn("_rescued_data", F.lit(None).cast("string"))
        return out

    # ------------------------------------------------------------------ #
    # Comum
    # ------------------------------------------------------------------ #
    def _write_mode(self, spec: CollectionSpec) -> str:
        return (
            self.bronze.write_mode_full if spec.load_mode == "full"
            else self.bronze.write_mode_incremental
        )

    def _add_lineage(
        self,
        df: DataFrame,
        run_id: str,
        ingestion_ts: _dt.datetime,
        source_path_tag: str,
        load_type: str,
    ) -> DataFrame:
        out = (
            df
            .withColumn("_source_id", F.expr("try_cast(body_variant:_id as string)"))
            .withColumn(
                "_source_id",
                F.coalesce(F.col("_source_id"),
                           F.expr("try_cast(body_variant['_id']['$oid'] as string)")),
            )
            .withColumn("_source_hash", F.sha2(F.coalesce(F.col("body_json"), F.lit("")), 256))
            .withColumn("_ingestion_id", F.lit(run_id))
            .withColumn("_ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
            .withColumn("_source_path", F.lit(source_path_tag))
            .withColumn("_load_type", F.lit(load_type))
            .withColumn("_ingestion_date", F.lit(ingestion_ts.date()).cast("date"))
        )
        for col in ("_rescued_data", "_source_file"):
            if col not in out.columns:
                out = out.withColumn(col, F.lit(None).cast("string"))
        return out.select(*BRONZE_COLUMNS)

    def _append(self, df: DataFrame, table: str) -> None:
        (df.write.format("delta").mode("append")
         .partitionBy(self.bronze.partition_by)
         .saveAsTable(table))

    def _merge(self, df: DataFrame, table: str) -> None:
        from delta.tables import DeltaTable

        (
            DeltaTable.forName(self.spark, table).alias("t")
            .merge(df.alias("s"), "t._source_id = s._source_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
