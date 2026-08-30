"""Configuracao da pipeline — dataclasses + loader de YAML/JSON (R1).

Toda a configuracao e externalizada (config/pipeline_config.yaml + config/collections.json).
Nada de parametro de negocio hardcoded no codigo.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .rules import mongo_projection
from .utils import coerce_scalar_list

_VALID_MODES = {"full", "incremental"}
_VALID_INGEST = {"single_variant", "inferred"}


# --------------------------------------------------------------------------- #
# Blocos de configuracao global
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 4
    base_delay_seconds: float = 2.0
    max_delay_seconds: float = 30.0


@dataclass(frozen=True)
class SourceConfig:
    type: str = "mongodb"
    database: str = "sample_mflix"
    source_path_tag: str = "mongodb_atlas"
    secret_scope: str = "conn-db"
    secret_key: str = "cnn-mongodb-sampleflix"
    app_name: str = "databricks-mflix-ingest"
    server_selection_timeout_ms: int = 15000
    connect_timeout_ms: int = 20000
    socket_timeout_ms: int = 300000
    max_pool_size: int = 20
    retry: RetryConfig = field(default_factory=RetryConfig)


@dataclass(frozen=True)
class TargetConfig:
    catalog: str = "mflix"
    landing_schema: str = "landing"
    landing_volume: str = "mflix_raw"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    control_table: str = "control_ingestion_log"
    watermark_table: str = "ingestion_watermark"
    contract_violations_table: str = "data_contract_violations"
    checkpoints_dir: str = "_checkpoints"
    schemas_dir: str = "_schemas"
    badrecords_dir: str = "_badrecords"

    # ---- helpers de nomenclatura (R6 — padrao unico documentado) ----
    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.landing_volume}"

    def landing_path(self, collection: str) -> str:
        return f"{self.volume_root}/{collection}"

    def checkpoint_path(self, collection: str) -> str:
        return f"{self.volume_root}/{self.checkpoints_dir}/{collection}"

    def schema_path(self, collection: str) -> str:
        return f"{self.volume_root}/{self.schemas_dir}/{collection}"

    def badrecords_path(self, collection: str) -> str:
        return f"{self.volume_root}/{self.badrecords_dir}/{collection}"

    def bronze_table(self, collection: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{collection}"

    def bronze_quarantine_table(self, collection: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{collection}_quarentena"

    def silver_table(self, name: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{name}"

    @property
    def control_table_fqn(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.control_table}"

    @property
    def watermark_table_fqn(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.watermark_table}"

    @property
    def contract_violations_fqn(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.contract_violations_table}"


@dataclass(frozen=True)
class AutoloaderConfig:
    engine: str = "batch"               # "batch" (padrao) | "autoloader"
    ingest_mode: str = "single_variant"
    cloud_files_format: str = "json"
    schema_evolution_mode: str = "rescue"
    rescued_data_column: str = "_rescued_data"
    max_files_per_trigger: int = 200
    trigger: str = "availableNow"


@dataclass(frozen=True)
class BronzeConfig:
    write_mode_full: str = "merge"
    write_mode_incremental: str = "append"
    partition_by: str = "_ingestion_date"
    table_properties: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ReconciliationConfig:
    default_threshold_pct: float = 1.0
    hard_fail_pct: float = 5.0
    null_key_is_fatal: bool = True
    fail_job_on_partial: bool = False


@dataclass(frozen=True)
class OrchestrationConfig:
    job_name: str = "mflix-ingestao-moderna"
    max_retries: int = 2
    min_retry_interval_millis: int = 60000
    retry_on_timeout: bool = True
    timeout_seconds: int = 7200
    on_failure_emails: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Parametros por colecao (R1 — database, collection, modo_carga, watermark, destino)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CollectionSpec:
    collection: str
    load_mode: str = "full"
    watermark_field: str | None = None
    watermark_type: str = "timestamp"
    projection_exclude: list[str] = field(default_factory=list)
    batch_size: int = 5000
    expected_count_min: int = 0
    allow_empty: bool = False
    reconciliation_threshold_pct: float | None = None
    schema_hints: str | None = None
    ingest_mode: str | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        if self.load_mode not in _VALID_MODES:
            raise ValueError(
                f"[{self.collection}] modo_carga invalido: {self.load_mode!r} "
                f"(use {sorted(_VALID_MODES)})"
            )
        if self.load_mode == "incremental" and not self.watermark_field:
            raise ValueError(
                f"[{self.collection}] modo_carga=incremental exige campo_watermark"
            )
        if self.ingest_mode is not None and self.ingest_mode not in _VALID_INGEST:
            raise ValueError(
                f"[{self.collection}] ingest_mode invalido: {self.ingest_mode!r}"
            )
        if self.batch_size <= 0:
            raise ValueError(f"[{self.collection}] batch_size deve ser > 0")

    @property
    def is_incremental(self) -> bool:
        return self.load_mode == "incremental"

    def mongo_projection(self) -> dict | None:
        return mongo_projection(self.projection_exclude)

    def effective_load_type(self, force_full: bool = False) -> str:
        return "full" if force_full else self.load_mode

    def threshold_pct(self, default: float) -> float:
        return self.reconciliation_threshold_pct if self.reconciliation_threshold_pct is not None else default

    @staticmethod
    def from_dict(name: str, raw: dict[str, Any]) -> "CollectionSpec":
        return CollectionSpec(
            collection=raw.get("collection", name),
            load_mode=raw.get("modo_carga", raw.get("load_mode", "full")),
            watermark_field=raw.get("campo_watermark", raw.get("watermark_field")),
            watermark_type=raw.get("watermark_type", "timestamp"),
            projection_exclude=coerce_scalar_list(raw.get("projection_exclude")),
            batch_size=int(raw.get("batch_size", 5000)),
            expected_count_min=int(raw.get("expected_count_min", 0)),
            allow_empty=bool(raw.get("allow_empty", False)),
            reconciliation_threshold_pct=_opt_float(raw.get("reconciliation_threshold_pct")),
            schema_hints=raw.get("schema_hints"),
            ingest_mode=raw.get("ingest_mode"),
            notes=raw.get("notes", ""),
        )


# --------------------------------------------------------------------------- #
# Configuracao completa
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PipelineConfig:
    source: SourceConfig
    target: TargetConfig
    autoloader: AutoloaderConfig
    bronze: BronzeConfig
    reconciliation: ReconciliationConfig
    orchestration: OrchestrationConfig
    collections: dict[str, CollectionSpec]
    silver_enabled: bool = True
    silver_movies_arrays: list[str] = field(default_factory=list)
    landing_file_format: str = "jsonl"
    contract_path: str = "config/data_contract.yaml"
    contract_sample_size: int = 80

    def resolve_collections(self, names: list[str] | str | None) -> list[CollectionSpec]:
        """'all' / None -> todas. Lista -> subconjunto, na ordem informada."""
        wanted = coerce_scalar_list(names)
        if not wanted or wanted == ["all"]:
            return list(self.collections.values())
        missing = [n for n in wanted if n not in self.collections]
        if missing:
            raise KeyError(f"colecao(oes) desconhecida(s) em collections.json: {missing}")
        return [self.collections[n] for n in wanted]

    # ---- loader ----
    @staticmethod
    def load(
        pipeline_config_path: str | Path,
        collections_path: str | Path,
        overrides: dict[str, Any] | None = None,
    ) -> "PipelineConfig":
        cfg = _read_structured(pipeline_config_path)
        cols_raw = _read_structured(collections_path)
        overrides = overrides or {}

        src = cfg.get("source", {})
        retry_raw = src.get("retry", {})
        source = SourceConfig(
            type=src.get("type", "mongodb"),
            database=src.get("database", "sample_mflix"),
            source_path_tag=src.get("source_path_tag", "mongodb_atlas"),
            secret_scope=src.get("secret_scope", "conn-db"),
            secret_key=src.get("secret_key", "cnn-mongodb-sampleflix"),
            app_name=src.get("app_name", "databricks-mflix-ingest"),
            server_selection_timeout_ms=int(src.get("server_selection_timeout_ms", 15000)),
            connect_timeout_ms=int(src.get("connect_timeout_ms", 20000)),
            socket_timeout_ms=int(src.get("socket_timeout_ms", 300000)),
            max_pool_size=int(src.get("max_pool_size", 20)),
            retry=RetryConfig(
                max_attempts=int(retry_raw.get("max_attempts", 4)),
                base_delay_seconds=float(retry_raw.get("base_delay_seconds", 2.0)),
                max_delay_seconds=float(retry_raw.get("max_delay_seconds", 30.0)),
            ),
        )

        tgt = cfg.get("target", {})
        catalog = overrides.get("catalog") or tgt.get("catalog", "mflix")
        target = TargetConfig(
            catalog=catalog,
            landing_schema=tgt.get("landing_schema", "landing"),
            landing_volume=tgt.get("landing_volume", "mflix_raw"),
            bronze_schema=tgt.get("bronze_schema", "bronze"),
            silver_schema=tgt.get("silver_schema", "silver"),
            control_table=tgt.get("control_table", "control_ingestion_log"),
            watermark_table=tgt.get("watermark_table", "ingestion_watermark"),
            contract_violations_table=tgt.get("contract_violations_table", "data_contract_violations"),
            checkpoints_dir=tgt.get("checkpoints_dir", "_checkpoints"),
            schemas_dir=tgt.get("schemas_dir", "_schemas"),
            badrecords_dir=tgt.get("badrecords_dir", "_badrecords"),
        )

        al = cfg.get("autoloader", {})
        autoloader = AutoloaderConfig(
            engine=overrides.get("engine") or al.get("engine", "batch"),
            ingest_mode=overrides.get("ingest_mode") or al.get("ingest_mode", "single_variant"),
            cloud_files_format=al.get("cloud_files_format", "json"),
            schema_evolution_mode=al.get("schema_evolution_mode", "rescue"),
            rescued_data_column=al.get("rescued_data_column", "_rescued_data"),
            max_files_per_trigger=int(al.get("max_files_per_trigger", 200)),
            trigger=al.get("trigger", "availableNow"),
        )

        br = cfg.get("bronze", {})
        bronze = BronzeConfig(
            write_mode_full=br.get("write_mode_full", "merge"),
            write_mode_incremental=br.get("write_mode_incremental", "append"),
            partition_by=br.get("partition_by", "_ingestion_date"),
            table_properties=dict(br.get("table_properties", {})),
        )

        rc = cfg.get("reconciliation", {})
        reconciliation = ReconciliationConfig(
            default_threshold_pct=float(rc.get("default_threshold_pct", 1.0)),
            hard_fail_pct=float(rc.get("hard_fail_pct", 5.0)),
            null_key_is_fatal=bool(rc.get("null_key_is_fatal", True)),
            fail_job_on_partial=bool(rc.get("fail_job_on_partial", False)),
        )

        orc = cfg.get("orchestration", {})
        orchestration = OrchestrationConfig(
            job_name=orc.get("job_name", "mflix-ingestao-moderna"),
            max_retries=int(orc.get("max_retries", 2)),
            min_retry_interval_millis=int(orc.get("min_retry_interval_millis", 60000)),
            retry_on_timeout=bool(orc.get("retry_on_timeout", True)),
            timeout_seconds=int(orc.get("timeout_seconds", 7200)),
            on_failure_emails=coerce_scalar_list(
                overrides.get("on_failure_emails") or orc.get("on_failure_emails")
            ),
        )

        collections = {
            name: CollectionSpec.from_dict(name, raw)
            for name, raw in cols_raw.items()
            if not name.startswith("_")
        }
        if not collections:
            raise ValueError(f"nenhuma colecao valida em {collections_path}")

        silver = cfg.get("silver", {})
        landing = cfg.get("landing", {})
        contract = cfg.get("contract", {})

        return PipelineConfig(
            source=source,
            target=target,
            autoloader=autoloader,
            bronze=bronze,
            reconciliation=reconciliation,
            orchestration=orchestration,
            collections=collections,
            silver_enabled=bool(silver.get("enabled", True)),
            silver_movies_arrays=coerce_scalar_list(silver.get("movies_arrays")),
            landing_file_format=landing.get("file_format", "jsonl"),
            contract_path=contract.get("path", "config/data_contract.yaml"),
            contract_sample_size=int(contract.get("sample_size", 80)),
        )


# --------------------------------------------------------------------------- #
# leitura de YAML ou JSON
# --------------------------------------------------------------------------- #
def _read_structured(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"arquivo de configuracao nao encontrado: {p}")
    text = p.read_text(encoding="utf-8")
    if p.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise ModuleNotFoundError(
                "pyyaml nao instalado — rode `%pip install pyyaml` no notebook "
                "ou converta o arquivo para .json"
            ) from exc
        return yaml.safe_load(text) or {}
    return json.loads(text)


def _opt_float(value: Any) -> float | None:
    return None if value is None else float(value)
