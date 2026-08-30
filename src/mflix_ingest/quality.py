"""Reconciliacao e qualidade da Bronze (R8).

A cada execucao valida e registra:
  - contagem origem x destino (por execucao e acumulada)
  - % de nulos na chave (_source_id nunca nulo)
  - duplicidade de _source_id dentro do lote da execucao
  - decide SUCCESS / PARTIAL / FAILED conforme limiares documentados
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import CollectionSpec, PipelineConfig
from .control import ControlManager
from .rules import ReconciliationInput, ReconciliationOutcome, decide_status
from .utils import get_logger

_log = get_logger("mflix_ingest.quality")


@dataclass
class QualityReport:
    collection: str
    source_count: int
    written_count: int
    null_key_pct: float
    batch_duplicates: int
    accumulated_written: int
    accumulated_bronze: int
    accumulated_ok: bool
    outcome: ReconciliationOutcome

    @property
    def status(self) -> str:
        return self.outcome.status

    @property
    def message(self) -> str:
        acc = "ok" if self.accumulated_ok else (
            f"acumulado divergente (control={self.accumulated_written} "
            f"bronze={self.accumulated_bronze})"
        )
        return f"{self.outcome.message}; reconciliacao acumulada {acc}"


class Reconciler:
    def __init__(self, spark: SparkSession, cfg: PipelineConfig, control: ControlManager):
        self.spark = spark
        self.cfg = cfg
        self.control = control

    def evaluate(
        self,
        spec: CollectionSpec,
        run_id: str,
        source_count: int,
        written_count: int,
        contract_ok: bool = True,
        force_full: bool = False,
    ) -> QualityReport:
        table = self.cfg.target.bronze_table(spec.collection)
        batch = self.spark.table(table).where(F.col("_ingestion_id") == run_id)

        agg = batch.agg(
            F.count(F.lit(1)).alias("n"),
            F.sum(F.when(F.col("_source_id").isNull(), 1).otherwise(0)).alias("null_keys"),
        ).collect()[0]
        n_batch = int(agg["n"] or 0)
        null_keys = int(agg["null_keys"] or 0)
        null_key_pct = (null_keys / n_batch * 100.0) if n_batch else 0.0

        dups = (
            batch.groupBy("_source_id")
            .count()
            .where((F.col("count") > 1) & F.col("_source_id").isNotNull())
            .count()
        )

        rc = self.cfg.reconciliation
        outcome = decide_status(ReconciliationInput(
            source_count=source_count,
            written_count=written_count,
            null_key_pct=null_key_pct,
            batch_duplicates=int(dups),
            contract_ok=contract_ok,
            threshold_pct=spec.threshold_pct(rc.default_threshold_pct),
            hard_fail_pct=rc.hard_fail_pct,
            null_key_is_fatal=rc.null_key_is_fatal,
        ))

        acc_written = self.control.accumulated_written(spec.collection) + written_count
        acc_bronze = self.spark.table(table).count()
        # append: devem bater; merge (full, ou incremental sob force_full): bronze <= soma dos lotes (upsert)
        write_mode_full = self.cfg.bronze.write_mode_full == "merge" and (
            force_full or spec.load_mode == "full"
        )
        accumulated_ok = acc_bronze >= acc_written if write_mode_full else acc_bronze == acc_written

        report = QualityReport(
            collection=spec.collection,
            source_count=source_count,
            written_count=written_count,
            null_key_pct=round(null_key_pct, 4),
            batch_duplicates=int(dups),
            accumulated_written=acc_written,
            accumulated_bronze=acc_bronze,
            accumulated_ok=bool(accumulated_ok),
            outcome=outcome,
        )
        _log.info(
            "[%s] reconciliacao -> %s | origem=%d destino=%d divergencia=%.3f%% "
            "nulos=%.3f%% dups=%d",
            spec.collection, report.status, source_count, written_count,
            outcome.divergence_pct, null_key_pct, dups,
        )
        return report