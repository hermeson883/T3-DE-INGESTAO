"""Load: landing (JSON Lines) -> Bronze (Delta) via Auto Loader.

Bonus +5 (ingestao orientada a arquivos):
  - readStream `cloudFiles` com checkpoint  -> cada arquivo processado exatamente 1x
  - schemaLocation                          -> schema inferido PERSISTIDO entre execucoes
  - schemaEvolutionMode=rescue / singleVariantColumn -> schema drift nunca quebra o stream (R7)
  - trigger(availableNow=True)              -> job em lote, nao streaming continuo

Idempotencia (R3):
  - colecoes full        -> MERGE por _source_id (upsert, 1 linha por documento)
  - colecoes incrementais -> append (a nao-duplicacao vem da watermark no extract
                             + do checkpoint do Auto Loader)
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
    checkpoint: str
    schema_location: str


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
    ) -> LoadResult:
        collection = spec.collection
        target_table = self.target.bronze_table(collection)
        quarantine_table = self.target.bronze_quarantine_table(collection)
        checkpoint = self.target.checkpoint_path(collection)
        schema_loc = self.target.schema_path(collection)
        landing = self.target.landing_path(collection)
        ingest_mode = spec.ingest_mode or self.al.ingest_mode
        load_type = spec.load_mode
        part_col = self.bronze.partition_by
        write_mode = (
            self.bronze.write_mode_full if load_type == "full"
            else self.bronze.write_mode_incremental
        )

        os.makedirs(landing, exist_ok=True)  # Auto Loader falha se o path nao existe
        self.spark.sql(bronze_ddl(target_table, part_col, self.bronze.table_properties))
        self.spark.sql(bronze_ddl(quarantine_table, part_col, {}))

        reader = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", self.al.cloud_files_format)
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.maxFilesPerTrigger", self.al.max_files_per_trigger)
            .option("cloudFiles.includeExistingFiles", "true")
        )
        if ingest_mode == "single_variant":
            reader = (
                reader
                .option("singleVariantColumn", "body_variant")
                .option("cloudFiles.schemaEvolutionMode", "none")
            )
        else:
            reader = (
                reader
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaEvolutionMode", self.al.schema_evolution_mode)
                .option("rescuedDataColumn", self.al.rescued_data_column)
            )
            if spec.schema_hints:
                reader = reader.option("cloudFiles.schemaHints", spec.schema_hints)

        raw = reader.load(landing).withColumn("_source_file", F.col("_metadata.file_path"))

        stats = {"batches": 0}

        def process_batch(batch_df: DataFrame, batch_id: int) -> None:
            enriched = self._enrich(batch_df, ingest_mode, run_id, ingestion_ts,
                                    source_path_tag, load_type).persist()
            try:
                good = enriched.where(F.col("_source_id").isNotNull() & F.col("body_variant").isNotNull())
                bad = enriched.where(F.col("_source_id").isNull() | F.col("body_variant").isNull())

                if write_mode == "merge":
                    self._merge(good, target_table)
                else:
                    self._append(good, target_table)

                bad_count = bad.count()
                if bad_count:
                    # preserva o payload para inspecao — nao anula body_variant
                    self._append(bad, quarantine_table)
                    _log.warning("[%s] batch %d: %d registro(s) para quarentena",
                                 collection, batch_id, bad_count)
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
        query.awaitTermination()

        rows_written = (
            self.spark.table(target_table)
            .where(F.col("_ingestion_id") == run_id)
            .count()
        )
        rows_quar = (
            self.spark.table(quarantine_table)
            .where(F.col("_ingestion_id") == run_id)
            .count()
        )
        _log.info("[%s] bronze: %d gravados / %d quarentena (%s, %d batches)",
                  collection, rows_written, rows_quar, write_mode, stats["batches"])
        return LoadResult(collection, rows_written, rows_quar, stats["batches"], checkpoint, schema_loc)

    # ------------------------------------------------------------------ #
    def _enrich(
        self,
        df: DataFrame,
        ingest_mode: str,
        run_id: str,
        ingestion_ts: _dt.datetime,
        source_path_tag: str,
        load_type: str,
    ) -> DataFrame:
        rescued_col = self.al.rescued_data_column

        if ingest_mode == "single_variant":
            out = df.withColumn("body_json", F.expr("to_json(body_variant)"))
            if rescued_col in df.columns:
                out = out.withColumnRenamed(rescued_col, "_rescued_data")
            else:
                out = out.withColumn("_rescued_data", F.lit(None).cast("string"))
        else:
            control_like = {rescued_col, "_source_file", "_metadata", "_rescued_data"}
            payload_cols = [c for c in df.columns if c not in control_like]
            struct_json = F.to_json(F.struct(*[F.col(c) for c in payload_cols]))
            # body_json vem de to_json(struct(...)) -> sempre JSON valido, parse_json e seguro
            out = (
                df.withColumn("body_json", struct_json)
                .withColumn("body_variant", F.expr("parse_json(body_json)"))
            )
            if rescued_col in df.columns and rescued_col != "_rescued_data":
                out = out.withColumnRenamed(rescued_col, "_rescued_data")
            elif "_rescued_data" not in out.columns:
                out = out.withColumn("_rescued_data", F.lit(None).cast("string"))

        out = (
            out
            .withColumn("_source_id", F.expr("try_cast(body_variant:_id as string)"))
            .withColumn(
                "_source_id",
                F.coalesce(F.col("_source_id"), F.expr("try_cast(body_variant['_id']['$oid'] as string)")),
            )
            .withColumn("_source_hash", F.sha2(F.coalesce(F.col("body_json"), F.lit("")), 256))
            .withColumn("_ingestion_id", F.lit(run_id))
            .withColumn("_ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
            .withColumn("_source_path", F.lit(source_path_tag))
            .withColumn("_load_type", F.lit(load_type))
            .withColumn("_ingestion_date", F.lit(ingestion_ts.date()).cast("date"))
        )
        if "_source_file" not in out.columns:
            out = out.withColumn("_source_file", F.lit(None).cast("string"))
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
