"""Orquestrador ponta a ponta (R1) — extract -> load -> reconcile -> watermark -> log.

Uso tipico (notebook / job):

    from mflix_ingest.pipeline import run_pipeline
    summary = run_pipeline(spark, dbutils, config_path="config/pipeline_config.yaml",
                           collections_path="config/collections.json",
                           collections="all")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import __version__
from .config import PipelineConfig
from .contract import ContractResult, load_contract, validate_contract
from .control import ControlManager, ControlRecord
from .extractor import LandingExtractor
from .loader import BronzeLoader
from .mongo_source import MongoSource
from .quality import Reconciler
from .rules import STATUS_FAILED, STATUS_SUCCESS
from .utils import get_logger, new_run_id, utc_now

_log = get_logger("mflix_ingest.pipeline")


@dataclass
class RunSummary:
    run_id: str
    started_at: Any
    records: list[ControlRecord] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(r.status != STATUS_FAILED for r in self.records)

    def to_rows(self) -> list[dict]:
        rows = []
        for r in self.records:
            rows.append(dict(
                run_id=self.run_id, collection=r.collection, load_type=r.load_type,
                status=r.status, qtd_lida_origem=r.qtd_lida_origem,
                qtd_gravada_destino=r.qtd_gravada_destino,
                watermark_inicial=r.watermark_inicial, watermark_final=r.watermark_final,
                divergencia_pct=r.divergencia_pct, qtd_quarentena=r.qtd_quarentena,
                duracao_seg=r.duracao_seg, mensagem_erro=r.mensagem_erro,
            ))
        return rows


def build_config(
    config_path: str = "config/pipeline_config.yaml",
    collections_path: str = "config/collections.json",
    overrides: dict[str, Any] | None = None,
) -> PipelineConfig:
    return PipelineConfig.load(config_path, collections_path, overrides=overrides)


def run_pipeline(
    spark: Any,
    dbutils: Any,
    *,
    config: PipelineConfig | None = None,
    config_path: str = "config/pipeline_config.yaml",
    collections_path: str = "config/collections.json",
    collections: list[str] | str | None = "all",
    run_id: str | None = None,
    force_full: bool = False,
    overrides: dict[str, Any] | None = None,
) -> RunSummary:
    cfg = config or build_config(config_path, collections_path, overrides)
    run_id = run_id or new_run_id()
    ingestion_ts = utc_now()
    specs = cfg.resolve_collections(collections)

    _log.info("=== RUN %s | %d colecao(oes) | force_full=%s ===",
              run_id, len(specs), force_full)

    def uri_provider() -> str:
        return dbutils.secrets.get(scope=cfg.source.secret_scope, key=cfg.source.secret_key)

    control = ControlManager(spark, cfg.target)
    control.ensure_tables()
    reconciler = Reconciler(spark, cfg, control)
    contract = _safe_load_contract(cfg.contract_path)

    summary = RunSummary(run_id=run_id, started_at=ingestion_ts)

    with MongoSource(cfg.source, uri_provider) as source:
        extractor = LandingExtractor(source, cfg)
        loader = BronzeLoader(spark, cfg.target, cfg.autoloader, cfg.bronze)

        for spec in specs:
            rec = ControlRecord(
                ingestion_id=run_id,
                collection=spec.collection,
                load_type=spec.effective_load_type(force_full),
                ingest_mode=spec.ingest_mode or cfg.autoloader.ingest_mode,
                pipeline_version=__version__,
            )
            try:
                wm = None if force_full else control.get_watermark(spec.collection)
                wm_value = wm.value if wm else None

                # ---- contract (amostra da origem) ----
                cres = _validate(source, spec, cfg, contract)
                if not cres.ok:
                    control.log_violations(run_id, spec.collection, cres.violations)
                    _log.warning("[%s] %s", spec.collection, cres.summary())

                # ---- extract -> landing ----
                ext = extractor.extract(spec, wm_value, run_id, force_full=force_full)
                rec.watermark_inicial = ext.watermark_initial
                rec.watermark_final = ext.watermark_final
                rec.landing_files = ext.files
                rec.contrato_ok = cres.ok

                if ext.empty:
                    rec.finish(
                        STATUS_SUCCESS,
                        qtd_lida_origem=0,
                        qtd_gravada_destino=0,
                        mensagem_erro=None,
                        divergencia_pct=0.0,
                    )
                    _log.info("[%s] %s -> SUCCESS sem gravacao", spec.collection, ext.skipped_reason)
                else:
                    # ---- load landing -> bronze ----
                    ld = loader.load(spec, run_id, ingestion_ts, cfg.source.source_path_tag)

                    # ---- reconcile (R8) ----
                    rep = reconciler.evaluate(
                        spec, run_id, ext.source_count, ld.rows_written, contract_ok=cres.ok,
                    )
                    rec.finish(
                        rep.status,
                        qtd_lida_origem=ext.source_count,
                        qtd_gravada_destino=ld.rows_written,
                        qtd_quarentena=ld.rows_quarantined,
                        qtd_duplicada_lote=rep.batch_duplicates,
                        divergencia_pct=rep.outcome.divergence_pct,
                        mensagem_erro=rep.message if rep.status != STATUS_SUCCESS else None,
                    )

                    # ---- advance watermark (so quando seguro) ----
                    if (
                        spec.is_incremental and not force_full
                        and ext.watermark_final
                        and rep.outcome.safe_to_advance_watermark
                    ):
                        control.set_watermark(
                            spec.collection, spec.watermark_field,
                            ext.watermark_final, spec.watermark_type, run_id,
                        )
                    elif spec.is_incremental and not rep.outcome.safe_to_advance_watermark:
                        _log.warning(
                            "[%s] watermark NAO avancada (status=%s) — proxima execucao "
                            "reprocessa a janela", spec.collection, rep.status,
                        )

            except Exception as exc:  # noqa: BLE001 — registra e segue para a proxima colecao
                _log.exception("[%s] FALHA", spec.collection)
                rec.finish(STATUS_FAILED, mensagem_erro=f"{type(exc).__name__}: {exc}")

            control.log_run(rec)
            summary.records.append(rec)

    _log.info("=== RUN %s concluida | ok=%s ===", run_id, summary.ok)
    return summary


# --------------------------------------------------------------------------- #
def _safe_load_contract(path: str) -> dict | None:
    try:
        return load_contract(path)
    except FileNotFoundError:
        _log.warning("data contract nao encontrado em %s — validacao desativada", path)
        return None


def _validate(source, spec, cfg, contract) -> ContractResult:
    if not contract:
        return ContractResult(spec.collection, 0, ok=True)
    try:
        sample = source.sample(spec.collection, cfg.contract_sample_size, spec.mongo_projection())
        return validate_contract(spec.collection, sample, contract)
    except Exception as exc:  # noqa: BLE001
        _log.warning("[%s] falha ao validar contrato: %s", spec.collection, exc)
        return ContractResult(spec.collection, 0, ok=True)