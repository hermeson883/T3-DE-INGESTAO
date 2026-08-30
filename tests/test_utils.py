"""Testes das utilidades puras (bonus +3 — testes automatizados)."""

import datetime as dt

import pytest

from mflix_ingest.utils import (
    bson_default,
    chunked,
    coerce_scalar_list,
    doc_source_id,
    max_watermark,
    retry,
    stable_hash,
)


class _FakeObjectId:
    def __init__(self, v):
        self._v = v

    def __str__(self):
        return self._v


def test_bson_default_datetime_iso():
    d = dt.datetime(2016, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)
    assert bson_default(d) == d.isoformat()


def test_bson_default_objectid_by_classname():
    # a classe se chama "ObjectId" -> encoder reconhece por nome
    _FakeObjectId.__name__ = "ObjectId"
    assert bson_default(_FakeObjectId("abc123")) == "abc123"


def test_bson_default_bytes_to_hex():
    assert bson_default(b"\x00\xff") == "00ff"


def test_stable_hash_deterministic():
    assert stable_hash("x") == stable_hash("x")
    assert stable_hash("x") != stable_hash("y")
    assert len(stable_hash("x")) == 64


@pytest.mark.parametrize(
    "doc,expected",
    [
        ({"_id": "573a1390f"}, "573a1390f"),
        ({"_id": {"$oid": "573a1390f"}}, "573a1390f"),
        ({"nome": "x"}, None),
    ],
)
def test_doc_source_id(doc, expected):
    assert doc_source_id(doc) == expected


def test_chunked_lazy_and_complete():
    out = list(chunked(range(10), 3))
    assert out == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]


def test_chunked_rejects_zero():
    with pytest.raises(ValueError):
        list(chunked([1], 0))


def test_max_watermark_handles_none_and_order():
    assert max_watermark(None, 5) == 5
    assert max_watermark(5, None) == 5
    assert max_watermark(3, 7) == 7
    assert max_watermark(7, 3) == 7


def test_max_watermark_mixed_types_fallback_to_str():
    d = dt.datetime(2020, 1, 1)
    assert max_watermark("2019-01-01", d) is not None  # nao levanta


def test_coerce_scalar_list():
    assert coerce_scalar_list(None) == []
    assert coerce_scalar_list("a, b ,c") == ["a", "b", "c"]
    assert coerce_scalar_list(["x", " y "]) == ["x", "y"]


def test_retry_succeeds_after_transient_failures():
    calls = {"n": 0}

    @retry(max_attempts=5, base_delay_seconds=0, max_delay_seconds=0,
           exceptions=(ValueError,), sleep=lambda _s: None)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3


def test_retry_reraises_after_max_attempts():
    @retry(max_attempts=2, base_delay_seconds=0, max_delay_seconds=0,
           exceptions=(KeyError,), sleep=lambda _s: None)
    def always_fails():
        raise KeyError("nope")

    with pytest.raises(KeyError):
        always_fails()