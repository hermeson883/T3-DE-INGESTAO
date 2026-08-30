"""Utilidades puras (sem dependencia de Spark ou PyMongo)."""

from __future__ import annotations

import datetime as _dt
import functools
import hashlib
import logging
import random
import time
import uuid
from typing import Any, Callable, Iterable, Iterator, Sequence, TypeVar

T = TypeVar("T")

_LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def get_logger(name: str = "mflix_ingest") -> logging.Logger:
    """Logger consistente para notebooks e jobs."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_LOG_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def new_run_id() -> str:
    """UUID da execucao (run id) — coluna _ingestion_id (R4/R5)."""
    return str(uuid.uuid4())


def utc_now() -> _dt.datetime:
    """Timestamp UTC *timezone-aware* do momento da chamada."""
    return _dt.datetime.now(_dt.timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def ingestion_date(ts: _dt.datetime | None = None) -> _dt.date:
    """Data (UTC) usada como coluna de particao _ingestion_date (R4/R6)."""
    return (ts or utc_now()).astimezone(_dt.timezone.utc).date()


# --------------------------------------------------------------------------- #
# Encoder BSON -> JSON (ObjectId, datetime, Decimal128, bytes, ...)
# Usado ao serializar cada documento para JSON Lines na landing zone.
# --------------------------------------------------------------------------- #
def bson_default(obj: Any) -> Any:
    """`default=` para json.dumps — converte tipos BSON em algo serializavel.

    Mantido puro: reconhece tipos BSON por *nome de classe* para nao exigir
    `import bson` (que puxa pymongo). Quando o pymongo esta presente os tipos
    reais tambem casam por heranca/atributo.
    """
    # datetime / date
    if isinstance(obj, (_dt.datetime, _dt.date)):
        return obj.isoformat()
    if isinstance(obj, _dt.timedelta):
        return obj.total_seconds()
    if isinstance(obj, (bytes, bytearray)):
        return bytes(obj).hex()
    cls = type(obj).__name__
    # bson.ObjectId, bson.Decimal128, bson.Timestamp, bson.Int64, ...
    if cls in {"ObjectId", "Decimal128", "Binary", "Regex", "Timestamp", "Int64", "Code", "MinKey", "MaxKey"}:
        return str(obj)
    if cls == "Decimal":
        return float(obj)
    # ultimo recurso — nunca deixa quebrar a serializacao do lote
    return str(obj)


# --------------------------------------------------------------------------- #
# Hashing
# --------------------------------------------------------------------------- #
def stable_hash(text: str) -> str:
    """sha256 hex de uma string — usado em _source_hash (dedup / auditoria)."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def doc_source_id(doc: dict) -> str | None:
    """Extrai a chave de negocio (_id do Mongo) como string. Nunca deve ser nula (R8)."""
    raw = doc.get("_id")
    if raw is None:
        return None
    if isinstance(raw, dict) and "$oid" in raw:  # extended JSON
        return str(raw["$oid"])
    return str(raw)


# --------------------------------------------------------------------------- #
# Retry com backoff exponencial + jitter (R2 — falhas de rede da origem)
# --------------------------------------------------------------------------- #
def retry(
    max_attempts: int = 4,
    base_delay_seconds: float = 2.0,
    max_delay_seconds: float = 30.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,
    logger: logging.Logger | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator: reexecuta a funcao em caso de excecao transitoria.

    delay(n) = min(max_delay, base_delay * 2**(n-1)) + jitter(0..base_delay)
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            log = logger or get_logger()
            attempt = 1
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:  # noqa: BLE001 - reraise abaixo
                    if attempt >= max_attempts:
                        log.error(
                            "%s falhou apos %d tentativas: %s",
                            getattr(func, "__name__", "func"), attempt, exc,
                        )
                        raise
                    delay = min(max_delay_seconds, base_delay_seconds * (2 ** (attempt - 1)))
                    delay += random.uniform(0.0, base_delay_seconds)
                    log.warning(
                        "%s tentativa %d/%d falhou (%s). Retentando em %.1fs",
                        getattr(func, "__name__", "func"), attempt, max_attempts, exc, delay,
                    )
                    sleep(delay)
                    attempt += 1

        return wrapper

    return decorator


# --------------------------------------------------------------------------- #
# Iteracao em lotes — nunca materializa o cursor inteiro (R2)
# --------------------------------------------------------------------------- #
def chunked(iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """Quebra um iteravel em listas de no maximo `size` elementos (lazy)."""
    if size <= 0:
        raise ValueError("size deve ser > 0")
    batch: list[T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def max_watermark(current: Any, candidate: Any) -> Any:
    """Maximo entre dois valores de watermark, tolerando None e tipos mistos."""
    if candidate is None:
        return current
    if current is None:
        return candidate
    try:
        return candidate if candidate > current else current
    except TypeError:
        # tipos incomparaveis (ex.: str vs datetime) — compara pela repr ISO/str
        return candidate if str(candidate) > str(current) else current


def coerce_scalar_list(value: Any) -> list[str]:
    """Normaliza um campo de config que pode vir como str, None ou lista."""
    if value is None:
        return []
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    if isinstance(value, Sequence):
        return [str(v).strip() for v in value if str(v).strip()]
    return [str(value)]
