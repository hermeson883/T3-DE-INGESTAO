"""Control: watermark persistida + tabela de controle de execucoes (R4/R5).

- bronze.ingestion_watermark      -> 1 linha por colecao (MERGE upsert)
- bronze.control_ingestion_log    -> 1 linha por execucao por colecao (append-only)
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import TargetConfig
from .utils import get_logger, utc_now

_log = get_logger("mflix_ingest.control")


# --------------------------------------------------------------------------- #
# DDL
# --------------------------------------------------------------------------- #
def bronze_ddl(table_fqn: str, partition_col: str = "_ingestion_date",
               props: dict[str, str] | None = None) -> str:
    """DDL da tabela Bronze — colunas de rastreabilidade (R4) garantidas.

    body_json    : documento inteiro como veio da origem (STRING JSON) — fidelidade
                   total, zero parsing na Bronze. A tipagem acontece na Silver.
    _rescued_data: campos/registros que o reader nao conseguiu interpretar (R7) —
                   nunca descartados.
    """
    tblprops = ""
    if props:
        pairs = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        tblprops = f"\nTBLPROPERTIES ({pairs})"
    return f"""
CREATE TABLE IF NOT EXISTS {table_fqn} (
  _source_id            STRING,
  body_json             STRING,
  _rescued_data         STRING,
  _source_hash          STRING,
  _source_file          STRING,
  _ingestion_id         STRING,
  _ingestion_timestamp  TIMESTAMP,
  _source_path          STRING,
  _load_type            STRING,
  _ingestion_date       DATE
)
USING DELTA
PARTITIONED BY ({partition_col}){tblprops}
""".strip()


CONTROL_SCHEMA = T.StructType([
    T.StructField("_ingestion_id", T.StringType()),
    T.StructField("collection", T.StringType()),
    T.StructField("load_type", T.StringType()),
    T.StructField("watermark_inicial", T.StringType()),
    T.StructField("watermark_final", T.StringType()),
    T.StructField("qtd_lida_origem", T.LongType()),
    T.StructField("qtd_gravada_destino", T.LongType()),
    T.StructField("start_time", T.TimestampType()),
    T.StructField("end_time", T.TimestampType()),
    T.StructField("duracao_seg", T.DoubleType()),
    T.StructField("status", T.StringType()),
    T.StructField("mensagem_erro", T.StringType()),
    # colunas extras (o enunciado pede "pelo menos" as de cima)
    T.StructField("qtd_quarentena", T.LongType()),
    T.StructField("qtd_duplicada_lote", T.LongType()),
    T.StructField("divergencia_pct", T.DoubleType()),
    T.StructField("contrato_ok", T.BooleanType()),
    T.StructField("landing_files", T.ArrayType(T.StringType())),
    T.StructField("ingest_mode", T.StringType()),
    T.StructField("pipeline_version", T.StringType()),
])

WATERMARK_SCHEMA = T.StructType([
    T.StructField("collection", T.StringType()),
    T.StructField("watermark_field", T.StringType()),
    T.StructField("watermark_value", T.StringType()),
    T.StructField("watermark_type", T.StringType()),
    T.StructField("_ingestion_id", T.StringType()),
    T.StructField("updated_at", T.TimestampType()),
])

VIOLATIONS_SCHEMA = T.StructType([
    T.StructField("_ingestion_id", T.StringType()),
    T.StructField("collection", T.StringType()),
    T.StructField("field", T.StringType()),
    T.StructField("kind", T.StringType()),
    T.StructField("detail", T.StringType()),
    T.StructField("violation_pct", T.DoubleType()),
    T.StructField("detected_at", T.TimestampType()),
])


# --------------------------------------------------------------------------- #
# Registro de execucao (uma linha do control_ingestion_log)
# --------------------------------------------------------------------------- #
@dataclass
class ControlRecord:
    ingestion_id: str
    collection: str
    load_type: str
    start_time: _dt.datetime = field(default_factory=utc_now)
    watermark_inicial: str | None = None
    watermark_final: str | None = None
    qtd_lida_origem: int = 0
    qtd_gravada_destino: int = 0
    end_time: _dt.datetime | None = None
    status: str = "FAILED"
    mensagem_erro: str | None = None
    qtd_quarentena: int = 0
    qtd_duplicada_lote: int = 0
    divergencia_pct: float = 0.0
    contrato_ok: bool = True
    landing_files: list[str] = field(default_factory=list)
    ingest_mode: str = "single_variant"
    pipeline_version: str = ""

    def finish(self, status: str, **updates: Any) -> "ControlRecord":
        for key, value in updates.items():
            setattr(self, key, value)
        self.status = status
        self.end_time = utc_now()
        return self

    @property
    def duracao_seg(self) -> float:
        end = self.end_time or utc_now()
        return round((end - self.start_time).total_seconds(), 3)

    def as_dict(self) -> dict:
        """dict com as MESMAS chaves de CONTROL_SCHEMA (mapeamento por nome)."""
        return dict(
            _ingestion_id=self.ingestion_id,
            collection=self.collection,
            load_type=self.load_type,
            watermark_inicial=self.watermark_inicial,
            watermark_final=self.watermark_final,
            qtd_lida_origem=int(self.qtd_lida_origem),
            qtd_gravada_destino=int(self.qtd_gravada_destino),
            start_time=self.start_time,
            end_time=self.end_time or utc_now(),
            duracao_seg=float(self.duracao_seg),
            status=self.status,
            mensagem_erro=(self.mensagem_erro or "")[:4000] or None,
            qtd_quarentena=int(self.qtd_quarentena),
            qtd_duplicada_lote=int(self.qtd_duplicada_lote),
            divergencia_pct=float(self.divergencia_pct),
            contrato_ok=bool(self.contrato_ok),
            landing_files=list(self.landing_files),
            ingest_mode=self.ingest_mode,
            pipeline_version=self.pipeline_version,
        )


@dataclass
class WatermarkRecord:
    collection: str
    watermark_field: str | None
    value: str | None
    watermark_type: str
    updated_at: _dt.datetime | None = None


# --------------------------------------------------------------------------- #
class ControlManager:
    def __init__(self, spark: SparkSession, target: TargetConfig):
        self.spark = spark
        self.target = target

    # ---- setup ----
    def ensure_tables(self) -> None:
        c = self.target
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {c.catalog}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c.catalog}.{c.bronze_schema}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c.catalog}.{c.silver_schema}")
        self._ensure(c.control_table_fqn, CONTROL_SCHEMA, partition_by=None)
        self._ensure(c.watermark_table_fqn, WATERMARK_SCHEMA, partition_by=None)
        self._ensure(c.contract_violations_fqn, VIOLATIONS_SCHEMA, partition_by=None)

    def _ensure(self, fqn: str, schema: T.StructType, partition_by: str | None) -> None:
        if self.spark.catalog.tableExists(fqn):
            return
        writer = (
            self.spark.createDataFrame([], schema)
            .write.format("delta").mode("ignore")
        )
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.saveAsTable(fqn)
        _log.info("tabela de controle criada: %s", fqn)

    # ---- watermark ----
    def get_watermark(self, collection: str) -> WatermarkRecord | None:
        fqn = self.target.watermark_table_fqn
        if not self.spark.catalog.tableExists(fqn):
            return None
        row = (
            self.spark.table(fqn)
            .where(F.col("collection") == collection)
            .orderBy(F.col("updated_at").desc())
            .limit(1)
            .collect()
        )
        if not row:
            return None
        r = row[0]
        return WatermarkRecord(
            collection=r["collection"],
            watermark_field=r["watermark_field"],
            value=r["watermark_value"],
            watermark_type=r["watermark_type"] or "timestamp",
            updated_at=r["updated_at"],
        )

    def set_watermark(
        self,
        collection: str,
        watermark_field: str,
        value: str,
        watermark_type: str,
        ingestion_id: str,
    ) -> None:
        fqn = self.target.watermark_table_fqn
        src = self.spark.createDataFrame(
            [dict(
                collection=collection,
                watermark_field=watermark_field,
                watermark_value=value,
                watermark_type=watermark_type,
                _ingestion_id=ingestion_id,
                updated_at=utc_now(),
            )],
            schema=WATERMARK_SCHEMA,
        )
        from delta.tables import DeltaTable

        (
            DeltaTable.forName(self.spark, fqn).alias("t")
            .merge(src.alias("s"), "t.collection = s.collection")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        _log.info("watermark[%s] = %s", collection, value)

    # ---- control log ----
    def log_run(self, record: ControlRecord) -> None:
        df = self.spark.createDataFrame([record.as_dict()], schema=CONTROL_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(self.target.control_table_fqn)
        _log.info(
            "control_ingestion_log <- [%s] %s | lida=%d gravada=%d | %.1fs",
            record.collection, record.status, record.qtd_lida_origem,
            record.qtd_gravada_destino, record.duracao_seg,
        )

    def log_violations(self, ingestion_id: str, collection: str, violations: list) -> None:
        if not violations:
            return
        now = utc_now()
        rows = [
            dict(
                _ingestion_id=ingestion_id,
                collection=collection,
                field=v.field,
                kind=v.kind,
                detail=v.detail,
                violation_pct=float(getattr(v, "sample_pct", 0.0)),
                detected_at=now,
            )
            for v in violations
        ]
        (
            self.spark.createDataFrame(rows, schema=VIOLATIONS_SCHEMA)
            .write.format("delta").mode("append")
            .saveAsTable(self.target.contract_violations_fqn)
        )

    # ---- reconciliacao acumulada ----
    def accumulated_written(self, collection: str) -> int:
        fqn = self.target.control_table_fqn
        if not self.spark.catalog.tableExists(fqn):
            return 0
        agg = (
            self.spark.table(fqn)
            .where((F.col("collection") == collection) & (F.col("status") != "FAILED"))
            .agg(F.sum("qtd_gravada_destino").alias("t"))
            .collect()[0]["t"]
        )
        return int(agg or 0)