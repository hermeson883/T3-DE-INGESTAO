"""Testes do carregamento de configuracao (R1 — parametrizacao externalizada)."""

import json

import pytest

from mflix_ingest.config import CollectionSpec, PipelineConfig

REPO_ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
PIPELINE_YAML = REPO_ROOT / "config" / "pipeline_config.yaml"
COLLECTIONS_JSON = REPO_ROOT / "config" / "collections.json"


@pytest.fixture(scope="module")
def cfg() -> PipelineConfig:
    return PipelineConfig.load(PIPELINE_YAML, COLLECTIONS_JSON)


def test_repo_config_files_load(cfg):
    assert cfg.source.database == "sample_mflix"
    assert cfg.target.catalog == "mflix"
    assert set(cfg.collections) == {
        "movies", "comments", "users", "theaters", "sessions", "embedded_movies",
    }


def test_incremental_collections_have_watermark(cfg):
    assert cfg.collections["comments"].is_incremental
    assert cfg.collections["comments"].watermark_field == "date"
    assert cfg.collections["movies"].watermark_field == "lastupdated"
    assert cfg.collections["movies"].watermark_type == "string"


def test_full_collections_are_full(cfg):
    for name in ("users", "theaters", "sessions", "embedded_movies"):
        assert cfg.collections[name].load_mode == "full"


def test_sensitive_fields_excluded_in_projection(cfg):
    assert cfg.collections["users"].mongo_projection() == {"password": 0}
    assert cfg.collections["sessions"].mongo_projection() == {"jwt": 0}
    assert cfg.collections["embedded_movies"].mongo_projection() == {"plot_embedding": 0}


def test_resolve_collections_all_and_subset(cfg):
    assert len(cfg.resolve_collections("all")) == 6
    assert len(cfg.resolve_collections(None)) == 6
    subset = cfg.resolve_collections("comments,users")
    assert [s.collection for s in subset] == ["comments", "users"]


def test_resolve_unknown_collection_raises(cfg):
    with pytest.raises(KeyError):
        cfg.resolve_collections("naoexiste")


def test_target_naming_helpers(cfg):
    t = cfg.target
    assert t.bronze_table("comments") == "mflix.bronze.comments"
    assert t.control_table_fqn == "mflix.bronze.control_ingestion_log"
    assert t.landing_path("movies") == "/Volumes/mflix/landing/mflix_raw/movies"
    assert t.checkpoint_path("movies").endswith("_checkpoints/movies")
    assert t.silver_table("movies_cast") == "mflix.silver.movies_cast"


def test_catalog_override(cfg):
    over = PipelineConfig.load(PIPELINE_YAML, COLLECTIONS_JSON, overrides={"catalog": "dev_mflix"})
    assert over.target.catalog == "dev_mflix"
    assert over.target.bronze_table("users") == "dev_mflix.bronze.users"


def test_collectionspec_rejects_bad_mode():
    with pytest.raises(ValueError):
        CollectionSpec(collection="x", load_mode="delta")


def test_collectionspec_incremental_requires_watermark():
    with pytest.raises(ValueError):
        CollectionSpec(collection="x", load_mode="incremental")


def test_collections_json_is_valid_json():
    data = json.loads(COLLECTIONS_JSON.read_text(encoding="utf-8"))
    assert "comments" in data