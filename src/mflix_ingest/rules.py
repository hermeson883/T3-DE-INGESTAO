"""Regras puras da pipeline: filtro incremental, projection e reconciliacao (R2/R3/R8).

Nenhum import de Spark/PyMongo aqui — este modulo e coberto por testes unitarios.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Any

# Status possiveis do control_ingestion_log (R5)
STATUS_SUCCESS = "SUCCESS"
STATUS_PARTIAL = "PARTIAL"
STATUS_FAILED = "FAILED"


# --------------------------------------------------------------------------- #
# Extract — filtro de carga incremental (R3) e projection/pushdown (R2)
# --------------------------------------------------------------------------- #
def build_incremental_query(
    watermark_field: str | None,
    watermark_value: Any,
    watermark_type: str = "timestamp",
) -> dict:
    """Monta o filtro do `find()` para a carga incremental.

    - watermark_value None  -> {}                 (primeira carga = full)
    - watermark_field None  -> {}                 (colecao full)
    - caso contrario        -> {campo: {"$gt": v, "$exists": True}}

    `$gt` (e nao `$gte`) garante que o ultimo registro ja gravado nao volte.
    `$exists: True` evita trazer documentos sem o campo de watermark em cargas
    incrementais (eles sao capturados apenas na primeira carga).
    """
    if not watermark_field or watermark_value is None or watermark_value == "":
        return {}
    value = _coerce_watermark(watermark_value, watermark_type)
    return {watermark_field: {"$gt": value, "$exists": True}}


def parse_watermark(value: Any, watermark_type: str = "timestamp") -> Any:
    """Normaliza um valor de watermark para o tipo comparavel nativo.

    - timestamp -> datetime (tz-aware, UTC se sem tz)
    - string    -> str (comparacao lexicografica)
    - None      -> None
    """
    if value is None or value == "":
        return None
    if watermark_type == "timestamp":
        if isinstance(value, _dt.datetime):
            return value if value.tzinfo else value.replace(tzinfo=_dt.timezone.utc)
        if isinstance(value, str):
            dt = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=_dt.timezone.utc)
        return value
    return str(value)


# compat: nome antigo
_coerce_watermark = parse_watermark


def mongo_projection(exclude_fields: list[str] | None) -> dict | None:
    """Converte a lista de campos sensiveis/largos em projection de exclusao.

    MongoDB nao permite misturar inclusao e exclusao (exceto `_id`), portanto
    usamos apenas exclusao: {campo: 0}. `_id` e sempre mantido (chave de negocio).
    """
    if not exclude_fields:
        return None
    projection = {field: 0 for field in exclude_fields if field and field != "_id"}
    return projection or None


def watermark_to_string(value: Any) -> str | None:
    """Serializa o valor da watermark para persistir na tabela de controle."""
    if value is None:
        return None
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.isoformat()
    return str(value)


# --------------------------------------------------------------------------- #
# Reconciliacao / qualidade (R8)
# --------------------------------------------------------------------------- #
def compute_divergence_pct(source_count: int, written_count: int) -> float:
    """|origem - destino| / origem * 100. Origem 0 => 0% se destino 0, senao 100%."""
    if source_count <= 0:
        return 0.0 if written_count <= 0 else 100.0
    return abs(source_count - written_count) / float(source_count) * 100.0


@dataclass(frozen=True)
class ReconciliationInput:
    source_count: int
    written_count: int
    null_key_pct: float
    batch_duplicates: int
    contract_ok: bool
    threshold_pct: float
    hard_fail_pct: float
    null_key_is_fatal: bool = True


@dataclass(frozen=True)
class ReconciliationOutcome:
    status: str
    divergence_pct: float
    written_lt_source: bool
    safe_to_advance_watermark: bool
    message: str


def decide_status(inp: ReconciliationInput) -> ReconciliationOutcome:
    """Decide SUCCESS / PARTIAL / FAILED a partir das metricas de qualidade.

    FAILED  -> _source_id nulo (chave obrigatoria) OU perda sistemica de dados
               (destino < origem e divergencia > hard_fail_pct).
    PARTIAL -> divergencia > threshold, duplicidade no lote, ou contrato violado.
    SUCCESS -> dentro de todos os limiares.
    """
    divergence = compute_divergence_pct(inp.source_count, inp.written_count)
    written_lt_source = inp.written_count < inp.source_count
    reasons: list[str] = []
    status = STATUS_SUCCESS

    if inp.null_key_pct > 0.0:
        reasons.append(f"_source_id nulo em {inp.null_key_pct:.2f}% do lote")
        status = STATUS_FAILED if inp.null_key_is_fatal else STATUS_PARTIAL

    if written_lt_source and divergence > inp.hard_fail_pct:
        reasons.append(
            f"perda sistemica: destino {inp.written_count} < origem {inp.source_count} "
            f"(divergencia {divergence:.2f}% > hard_fail {inp.hard_fail_pct:.2f}%)"
        )
        status = STATUS_FAILED

    if status != STATUS_FAILED:
        if divergence > inp.threshold_pct:
            reasons.append(
                f"divergencia {divergence:.2f}% > limiar {inp.threshold_pct:.2f}%"
            )
            status = STATUS_PARTIAL
        if inp.batch_duplicates > 0:
            reasons.append(f"{inp.batch_duplicates} _source_id duplicado(s) no lote")
            status = STATUS_PARTIAL
        if not inp.contract_ok:
            reasons.append("data contract violado (ver data_contract_violations)")
            status = STATUS_PARTIAL

    # Watermark so avanca quando nao ha risco de PULAR dados.
    # Over-ingestao (duplicidade) e segura; shortfall nao e.
    safe = status == STATUS_SUCCESS or (
        status == STATUS_PARTIAL and not written_lt_source
    )

    message = "; ".join(reasons) if reasons else "origem e destino reconciliados"
    return ReconciliationOutcome(
        status=status,
        divergence_pct=round(divergence, 4),
        written_lt_source=written_lt_source,
        safe_to_advance_watermark=safe,
        message=message,
    )
