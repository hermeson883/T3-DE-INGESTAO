"""Data contract da origem — definicao formal + validacao a cada execucao (bonus +3).

Puro: valida uma amostra de documentos (list[dict]) contra config/data_contract.yaml.
"""

from __future__ import annotations

import datetime as _dt
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# mapeia "type" do contrato -> checagem sobre o valor python ja desserializado do BSON
_CHECKS: dict[str, Any] = {
    "string": lambda v: isinstance(v, str),
    "int": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "double": lambda v: isinstance(v, float),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "bool": lambda v: isinstance(v, bool),
    "datetime": lambda v: isinstance(v, (_dt.datetime, _dt.date)),
    "objectid": lambda v: _looks_like_objectid(v),
    "array": lambda v: isinstance(v, (list, tuple)),
    "object": lambda v: isinstance(v, dict),
}


def _looks_like_objectid(value: Any) -> bool:
    if type(value).__name__ == "ObjectId":
        return True
    if isinstance(value, dict) and "$oid" in value:
        return True
    if isinstance(value, str) and len(value) == 24:
        try:
            int(value, 16)
            return True
        except ValueError:
            return False
    return False


@dataclass
class FieldViolation:
    field: str
    kind: str          # "missing" | "null" | "type" | "forbidden"
    detail: str
    count: int = 0
    sample_pct: float = 0.0


@dataclass
class ContractResult:
    collection: str
    sample_size: int
    ok: bool
    violations: list[FieldViolation] = field(default_factory=list)

    def summary(self) -> str:
        if self.ok:
            return f"contrato OK ({self.sample_size} docs amostrados)"
        parts = [f"{v.field}:{v.kind}({v.sample_pct:.1f}%)" for v in self.violations]
        return "contrato VIOLADO -> " + ", ".join(parts)


def load_contract(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"data contract nao encontrado: {p}")
    text = p.read_text(encoding="utf-8")
    if p.suffix.lower() in {".yaml", ".yml"}:
        import yaml  # type: ignore

        return yaml.safe_load(text) or {}
    return json.loads(text)


def validate_contract(
    collection: str,
    sample_docs: list[dict],
    contract: dict[str, Any],
) -> ContractResult:
    """Valida uma amostra da origem contra o contrato da colecao.

    Um campo `required` que falta/e nulo em ate `max_violation_pct` dos docs
    e tolerado (schemaless). `forbidden` presente em QUALQUER doc ja e violacao
    (campo sensivel/largo que deveria ter sido removido na projection).
    """
    spec = (contract.get("collections") or {}).get(collection, {})
    defaults = contract.get("defaults") or {}
    max_violation_pct = float(spec.get("max_violation_pct", defaults.get("max_violation_pct", 5.0)))

    n = len(sample_docs)
    if n == 0:
        allow_empty = bool(spec.get("allow_empty", False))
        return ContractResult(collection, 0, ok=allow_empty,
                              violations=[] if allow_empty
                              else [FieldViolation("*", "missing", "amostra vazia e allow_empty=false")])

    fields: dict[str, dict] = spec.get("fields", {})
    forbidden: list[str] = spec.get("forbidden", []) or []
    violations: list[FieldViolation] = []

    for fname, rules in fields.items():
        required = bool(rules.get("required", False))
        nullable = bool(rules.get("nullable", not required))
        ftype = rules.get("type")
        missing = null_count = type_bad = 0
        for doc in sample_docs:
            present = fname in doc
            value = doc.get(fname)
            if not present:
                missing += 1
                continue
            if value is None:
                null_count += 1
                continue
            if ftype and ftype in _CHECKS and not _CHECKS[ftype](value):
                type_bad += 1

        if required and missing:
            pct = missing / n * 100.0
            if pct > max_violation_pct:
                violations.append(FieldViolation(fname, "missing",
                                                 f"ausente em {missing}/{n} docs", missing, pct))
        if not nullable and null_count:
            pct = null_count / n * 100.0
            if pct > max_violation_pct:
                violations.append(FieldViolation(fname, "null",
                                                 f"nulo em {null_count}/{n} docs", null_count, pct))
        if type_bad:
            pct = type_bad / n * 100.0
            if pct > max_violation_pct:
                violations.append(FieldViolation(fname, "type",
                                                 f"tipo != {ftype} em {type_bad}/{n} docs", type_bad, pct))

    for fname in forbidden:
        hits = sum(1 for doc in sample_docs if fname in doc)
        if hits:
            violations.append(FieldViolation(fname, "forbidden",
                                             f"campo proibido presente em {hits}/{n} docs",
                                             hits, hits / n * 100.0))

    return ContractResult(collection, n, ok=not violations, violations=violations)
