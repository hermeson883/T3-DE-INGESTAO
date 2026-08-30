"""Acesso ao MongoDB de origem — leitura paginada, pooling e retry (R2).

Import de `pymongo` acontece aqui; os testes unitarios NAO importam este modulo.
"""

from __future__ import annotations

from typing import Any, Callable, Iterator

import pymongo
from pymongo import MongoClient
from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    ExecutionTimeout,
    NetworkTimeout,
    ServerSelectionTimeoutError,
)

from .config import SourceConfig
from .utils import get_logger, retry

_TRANSIENT = (
    AutoReconnect,
    NetworkTimeout,
    ConnectionFailure,
    ServerSelectionTimeoutError,
    ExecutionTimeout,
)

_log = get_logger("mflix_ingest.mongo")


class MongoSource:
    """Encapsula uma unica conexao (pool) reaproveitada por toda a execucao.

    R2 atendido:
      - connection pooling: 1 MongoClient para todas as colecoes (maxPoolSize).
      - leitura paginada: cursor com batch_size, iterado como *generator*.
      - sem list(cursor)/toPandas: `iter_documents` nunca materializa o cursor.
      - retry + backoff exponencial nas operacoes de rede (decorator @retry).
      - projection/pushdown: `projection` e `query` empurrados para o servidor.
    """

    def __init__(self, cfg: SourceConfig, uri_provider: Callable[[], str]):
        self._cfg = cfg
        self._uri_provider = uri_provider
        self._client: MongoClient | None = None
        r = cfg.retry
        self._retry = dict(
            max_attempts=r.max_attempts,
            base_delay_seconds=r.base_delay_seconds,
            max_delay_seconds=r.max_delay_seconds,
            exceptions=_TRANSIENT,
            logger=_log,
        )

    # ------------------------------------------------------------------ #
    @property
    def client(self) -> MongoClient:
        if self._client is None:
            uri = self._uri_provider()
            if not uri or "://" not in uri:
                raise ValueError(
                    "URI do MongoDB vazia/invalida — verifique o Databricks Secret "
                    f"{self._cfg.secret_scope}/{self._cfg.secret_key} (notebook 00)."
                )
            self._client = MongoClient(
                uri,
                appName=self._cfg.app_name,
                serverSelectionTimeoutMS=self._cfg.server_selection_timeout_ms,
                connectTimeoutMS=self._cfg.connect_timeout_ms,
                socketTimeoutMS=self._cfg.socket_timeout_ms,
                maxPoolSize=self._cfg.max_pool_size,
                retryReads=True,
                tz_aware=True,
            )
            _log.info("MongoClient conectado (db=%s, pool<=%d)",
                      self._cfg.database, self._cfg.max_pool_size)
        return self._client

    def _db(self):
        return self.client[self._cfg.database]

    # ------------------------------------------------------------------ #
    def list_collections(self) -> list[str]:
        fn = retry(**self._retry)(lambda: sorted(self._db().list_collection_names()))
        return fn()

    def count(self, collection: str, query: dict | None = None) -> int:
        # count_documents (nao estimated) para bater exatamente na reconciliacao (R8)
        def _count() -> int:
            return self._db()[collection].count_documents(query or {})
        return retry(**self._retry)(_count)()

    def max_value(self, collection: str, field: str, query: dict | None = None) -> Any:
        """max(field) server-side — usado como fallback de watermark_final."""
        pipeline: list[dict] = []
        if query:
            pipeline.append({"$match": query})
        pipeline += [
            {"$match": {field: {"$exists": True, "$ne": None}}},
            {"$group": {"_id": None, "mx": {"$max": f"${field}"}}},
        ]

        def _agg() -> Any:
            docs = list(self._db()[collection].aggregate(pipeline, allowDiskUse=True))
            return docs[0]["mx"] if docs else None
        return retry(**self._retry)(_agg)()

    def sample(self, collection: str, size: int, projection: dict | None = None) -> list[dict]:
        """Amostra PEQUENA (contrato/schema). `size` limitado — seguro materializar."""
        size = max(1, min(size, 500))

        def _sample() -> list[dict]:
            cur = self._db()[collection].find({}, projection=projection).limit(size)
            return list(cur)
        return retry(**self._retry)(_sample)()

    def iter_documents(
        self,
        collection: str,
        query: dict | None = None,
        projection: dict | None = None,
        batch_size: int = 5000,
        sort_field: str | None = "_id",
    ) -> Iterator[dict]:
        """Generator paginado. NUNCA materializa o cursor inteiro (R2).

        A reconexao transitoria no meio da iteracao e tratada reabrindo o cursor
        a partir do ultimo `_id` lido (retomada por chave, sem duplicar).
        """
        last_id: Any = None
        yielded = 0
        base_query = dict(query or {})

        while True:
            effective = dict(base_query)
            if last_id is not None:
                effective["_id"] = {"$gt": last_id}
            cursor = (
                self._db()[collection]
                .find(effective, projection=projection, batch_size=batch_size)
                .sort("_id", pymongo.ASCENDING)
            )
            try:
                for doc in cursor:
                    last_id = doc.get("_id", last_id)
                    yielded += 1
                    yield doc
                break  # cursor esgotado normalmente
            except _TRANSIENT as exc:
                _log.warning(
                    "cursor de %s caiu apos %d docs (%s) — retomando de _id > %r",
                    collection, yielded, exc, last_id,
                )
                continue
            finally:
                cursor.close()

    # ------------------------------------------------------------------ #
    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            _log.info("MongoClient encerrado")

    def __enter__(self) -> "MongoSource":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
