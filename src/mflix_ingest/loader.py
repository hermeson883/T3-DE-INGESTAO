"""Load: landing (JSON Lines) -> Bronze (Delta).

Motores (config `autoloader.engine`):
  batch  (PADRAO) -> spark.read.json dos arquivos DA execucao. Sem streaming.
  autoloader      -> readStream cloudFiles + checkpoint + schemaLocation (bonus +5).

Bronze guarda o documento como STRING JSON (`body_json`) — fidelidade total, zero
risco de parsing. A tipagem acontece na Silver (`from_json`). `_source_id` vem da
coluna `_id` (o extractor ja serializa como string), sem extrair de tipo complexo.

Idempotencia (R3):
  - full         -> MERGE por _source_id (fallback: overwrite snapshot)
  - incremental  -> append (nao-duplicacao: watermark no extract + arquivo por run)
  - reprocesso   -> MERGE (bronze_job le a landing inteira)
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

# Ordem canonica das colunas da Bronze (igual ao DDL em control.bronze_ddl).
BRONZE_COLUMNS = [
    "_source_id",
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

_NON_PAYLOAD = {"_rescued_data", "_source_file", "_metadata", "_corrupt_record"}


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
        _log.info("[%s] batch: %d arquivo(s) | %s",
                  collection, len(files), "MERGE" if use_merge else "append")

        raw = (
            self.spark.read
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("multiLine", "false")
            .json(files)
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )
        enriched = self._shape(raw, run_id, ingestion_ts, source_path_tag, spec.load_mode).persist()
        try:
            good = enriched.where(F.col("_source_id").isNotNull())
            bad = enriched.where(F.col("_source_id").isNull())
            n_bad = bad.count()

            if use_merge:
                self._merge(good, target_table)
            else:
                self._append(good, target_table)
            if n_bad:
                self._append(bad, quarantine_table)
                _log.warning("[%s] %d registro(s) sem _id -> quarentena", collection, n_bad)
        finally:
            enriched.unpersist()

        written = self.spark.table(target_table).where(F.col("_ingestion_id") == run_id).count()
        _log.info("[%s] bronze (batch): %d gravados / %d quarentena", collection, written, n_bad)
        return LoadResult(collection, written, n_bad, 1)

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
        write_mode = self._write_mode(spec)

        raw = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("rescuedDataColumn", "_rescued_data")
            .option("cloudFiles.maxFilesPerTrigger", self.al.max_files_per_trigger)
            .load(landing)
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )
        stats = {"batches": 0}

        def process_batch(bdf: DataFrame, batch_id: int) -> None:
            enriched = self._shape(bdf, run_id, ingestion_ts, source_path_tag, spec.load_mode).persist()
            try:
                good = enriched.where(F.col("_source_id").isNotNull())
                bad = enriched.where(F.col("_source_id").isNull())
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

    # ------------------------------------------------------------------ #
    # Comum — nenhuma extracao de tipo complexo / VARIANT
    # ------------------------------------------------------------------ #
    def _shape(
        self,
        df: DataFrame,
        run_id: str,
        ingestion_ts: _dt.datetime,
        source_path_tag: str,
        load_type: str,
    ) -> DataFrame:
        cols = set(df.columns)
        payload = [c for c in df.columns if c not in _NON_PAYLOAD]

        body_json = F.to_json(F.struct(*[F.col(c) for c in payload])) if payload else F.lit(None).cast("string")
        rescued = F.col("_rescued_data").cast("string") if "_rescued_data" in cols else F.lit(None).cast("string")
        if "_corrupt_record" in cols:
            rescued = F.coalesce(rescued, F.col("_corrupt_record").cast("string"))
        source_id = F.col("_id").cast("string") if "_id" in cols else F.lit(None).cast("string")
        source_file = F.col("_source_file").cast("string") if "_source_file" in cols else F.lit(None).cast("string")

        out = (
            df
            .withColumn("body_json", body_json)
            .withColumn("_rescued_data", rescued)
            .withColumn("_source_id", source_id)
            .withColumn("_source_hash", F.sha2(F.coalesce(F.col("body_json"), F.lit("")), 256))
            .withColumn("_source_file", source_file)
            .withColumn("_ingestion_id", F.lit(run_id))
            .withColumn("_ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
            .withColumn("_source_path", F.lit(source_path_tag))
            .withColumn("_load_type", F.lit(load_type))
            .withColumn("_ingestion_date", F.lit(ingestion_ts.date()).cast("date"))
        )
        return out.select(*BRONZE_COLUMNS)

    def _write_mode(self, spec: CollectionSpec) -> str:
        return (
            self.bronze.write_mode_full if spec.load_mode == "full"
            else self.bronze.write_mode_incremental
        )

    def _append(self, df: DataFrame, table: str) -> None:
        df.write.format("delta").mode("append").saveAsTable(table)

    def _merge(self, df: DataFrame, table: str) -> None:
        from delta.tables import DeltaTable

        try:
            (
                DeltaTable.forName(self.spark, table).alias("t")
                .merge(df.dropDuplicates(["_source_id"]).alias("s"), "t._source_id = s._source_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception as exc:  # noqa: BLE001
            _log.warning("MERGE em %s falhou (%s) — fallback: overwrite snapshot", table, exc)
            (
                df.dropDuplicates(["_source_id"])
                .write.format("delta").mode("overwrite")
                .option("overwriteSchema", "false")
                .saveAsTable(table)
            )
