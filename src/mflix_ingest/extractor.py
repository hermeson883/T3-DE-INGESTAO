"""Extract: MongoDB -> landing zone (JSON Lines, 1 arquivo por colecao por execucao).

A landing zone (Volume do Unity Catalog) guarda o dado **exatamente como veio da
origem** (R6) — cada linha e o documento serializado por json.dumps + bson_default.
E a copia byte-a-byte que a Bronze reconstroi.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

from .config import CollectionSpec, PipelineConfig
from .mongo_source import MongoSource
from .rules import build_incremental_query, parse_watermark, watermark_to_string
from .utils import bson_default, get_logger, max_watermark, utc_now

_log = get_logger("mflix_ingest.extract")


@dataclass
class ExtractResult:
    collection: str
    load_type: str
    watermark_field: str | None
    watermark_initial: str | None
    watermark_final: str | None
    source_count: int
    docs_written: int
    files: list[str] = field(default_factory=list)
    empty: bool = False
    skipped_reason: str | None = None

    @property
    def has_new_data(self) -> bool:
        return self.docs_written > 0


class LandingExtractor:
    def __init__(self, source: MongoSource, cfg: PipelineConfig):
        self.source = source
        self.cfg = cfg
        self.target = cfg.target

    # ------------------------------------------------------------------ #
    def extract(
        self,
        spec: CollectionSpec,
        watermark_initial: Any,
        run_id: str,
        force_full: bool = False,
    ) -> ExtractResult:
        load_type = spec.effective_load_type(force_full)
        use_watermark = spec.is_incremental and not force_full
        # valor inicial normalizado para o tipo nativo (datetime | str | None)
        wm_initial = parse_watermark(watermark_initial, spec.watermark_type) if use_watermark else None

        query = build_incremental_query(
            spec.watermark_field if use_watermark else None,
            wm_initial,
            spec.watermark_type,
        )
        projection = spec.mongo_projection()

        source_count = self.source.count(spec.collection, query)
        _log.info(
            "[%s] load_type=%s | filtro=%s | projection_exclude=%s | origem=%d docs",
            spec.collection, load_type, query or "{}", spec.projection_exclude, source_count,
        )

        base = ExtractResult(
            collection=spec.collection,
            load_type=load_type,
            watermark_field=spec.watermark_field if use_watermark else None,
            watermark_initial=watermark_to_string(wm_initial),
            watermark_final=watermark_to_string(wm_initial),
            source_count=source_count,
            docs_written=0,
        )

        # colecao pequena/vazia ou incremental sem novidades -> nao escreve arquivo
        if source_count == 0:
            reason = "sem novidades apos a watermark" if use_watermark else "colecao vazia na origem"
            if not use_watermark and not spec.allow_empty:
                _log.warning("[%s] %s (allow_empty=false)", spec.collection, reason)
            base.empty = True
            base.skipped_reason = reason
            return base

        # ---- escrita paginada em JSON Lines ----
        ts = utc_now()
        os.makedirs(self.target.landing_path(spec.collection), exist_ok=True)
        fname = f"{spec.collection}_{run_id}_{ts.strftime('%Y%m%dT%H%M%S')}.jsonl"
        fpath = os.path.join(self.target.landing_path(spec.collection), fname)

        wm_value: Any = wm_initial
        saw_watermark_field = False
        written = 0
        with open(fpath, "w", encoding="utf-8") as fh:
            for doc in self.source.iter_documents(
                spec.collection, query, projection, spec.batch_size,
            ):
                fh.write(json.dumps(doc, default=bson_default, ensure_ascii=False))
                fh.write("\n")
                written += 1
                if use_watermark and doc.get(spec.watermark_field) is not None:
                    saw_watermark_field = True
                    wm_value = max_watermark(
                        wm_value, parse_watermark(doc[spec.watermark_field], spec.watermark_type)
                    )

        # fallback: nenhum doc trouxe o campo de watermark -> pega o max server-side
        if use_watermark and written and not saw_watermark_field:
            wm_value = parse_watermark(
                self.source.max_value(spec.collection, spec.watermark_field, query),
                spec.watermark_type,
            ) or wm_value

        base.docs_written = written
        base.files = [fpath]
        base.watermark_final = watermark_to_string(wm_value) if use_watermark else None
        _log.info("[%s] landing: %d docs -> %s", spec.collection, written, fpath)
        return base
