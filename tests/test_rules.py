"""Testes das regras de extract e reconciliacao (R2/R3/R8)."""

import datetime as dt

import pytest

from mflix_ingest.rules import (
    STATUS_FAILED,
    STATUS_PARTIAL,
    STATUS_SUCCESS,
    ReconciliationInput,
    build_incremental_query,
    compute_divergence_pct,
    decide_status,
    mongo_projection,
    parse_watermark,
    watermark_to_string,
)


# --------------------------------------------------------------------------- #
# build_incremental_query (R3)
# --------------------------------------------------------------------------- #
def test_query_empty_when_no_watermark_value():
    assert build_incremental_query("date", None, "timestamp") == {}
    assert build_incremental_query("date", "", "timestamp") == {}


def test_query_empty_when_no_watermark_field():
    assert build_incremental_query(None, "2016-01-01", "string") == {}


def test_query_timestamp_parses_iso_with_z():
    q = build_incremental_query("date", "2016-01-01T00:00:00Z", "timestamp")
    assert q["date"]["$exists"] is True
    assert q["date"]["$gt"] == dt.datetime(2016, 1, 1, tzinfo=dt.timezone.utc)


def test_query_string_watermark_kept_lexicographic():
    q = build_incremental_query("lastupdated", "2015-08-26 00:03:50", "string")
    assert q == {"lastupdated": {"$gt": "2015-08-26 00:03:50", "$exists": True}}


def test_query_timestamp_accepts_datetime_object():
    d = dt.datetime(2016, 5, 1, tzinfo=dt.timezone.utc)
    q = build_incremental_query("date", d, "timestamp")
    assert q["date"]["$gt"] == d


def test_query_timestamp_naive_datetime_gets_utc():
    q = build_incremental_query("date", dt.datetime(2016, 5, 1), "timestamp")
    assert q["date"]["$gt"] == dt.datetime(2016, 5, 1, tzinfo=dt.timezone.utc)


# --------------------------------------------------------------------------- #
# mongo_projection (R2 — pushdown, sem misturar include/exclude)
# --------------------------------------------------------------------------- #
def test_projection_none_when_empty():
    assert mongo_projection([]) is None
    assert mongo_projection(None) is None


def test_projection_exclusion_only_and_keeps_id():
    assert mongo_projection(["password", "_id"]) == {"password": 0}


def test_projection_multiple_fields():
    assert mongo_projection(["fullplot", "poster"]) == {"fullplot": 0, "poster": 0}


# --------------------------------------------------------------------------- #
# watermark_to_string / parse_watermark
# --------------------------------------------------------------------------- #
def test_watermark_to_string():
    assert watermark_to_string(None) is None
    assert watermark_to_string("abc") == "abc"
    d = dt.datetime(2016, 1, 1, tzinfo=dt.timezone.utc)
    assert watermark_to_string(d) == d.isoformat()


def test_parse_watermark_roundtrip_timestamp():
    d = dt.datetime(2016, 3, 26, 23, 20, 16, tzinfo=dt.timezone.utc)
    s = watermark_to_string(d)
    assert parse_watermark(s, "timestamp") == d


def test_parse_watermark_adds_utc_when_naive():
    out = parse_watermark("2016-03-26T23:20:16", "timestamp")
    assert out.tzinfo is not None


def test_parse_watermark_string_type_is_lexicographic():
    assert parse_watermark("2015-08-26 00:03:50", "string") == "2015-08-26 00:03:50"


def test_parse_watermark_none():
    assert parse_watermark(None, "timestamp") is None
    assert parse_watermark("", "string") is None


# --------------------------------------------------------------------------- #
# compute_divergence_pct + decide_status (R8)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "src,dst,expected",
    [
        (100, 100, 0.0),
        (100, 99, 1.0),
        (100, 90, 10.0),
        (0, 0, 0.0),
        (0, 5, 100.0),
        (50000, 50000, 0.0),
    ],
)
def test_divergence(src, dst, expected):
    assert compute_divergence_pct(src, dst) == pytest.approx(expected)


def _inp(**kw):
    base = dict(
        source_count=1000, written_count=1000, null_key_pct=0.0, batch_duplicates=0,
        contract_ok=True, threshold_pct=1.0, hard_fail_pct=5.0, null_key_is_fatal=True,
    )
    base.update(kw)
    return ReconciliationInput(**base)


def test_status_success_when_reconciled():
    out = decide_status(_inp())
    assert out.status == STATUS_SUCCESS
    assert out.safe_to_advance_watermark is True


def test_status_success_within_threshold():
    out = decide_status(_inp(written_count=995))  # 0.5% < 1%
    assert out.status == STATUS_SUCCESS


def test_status_partial_over_threshold_but_not_data_loss_direction():
    out = decide_status(_inp(written_count=1030))  # 3% acima, over-ingestao
    assert out.status == STATUS_PARTIAL
    assert out.safe_to_advance_watermark is True  # over-ingestao e seguro


def test_status_partial_shortfall_holds_watermark():
    out = decide_status(_inp(written_count=970))  # 3% faltando, < hard_fail
    assert out.status == STATUS_PARTIAL
    assert out.written_lt_source is True
    assert out.safe_to_advance_watermark is False  # nao avanca: pode pular dados


def test_status_failed_on_systematic_loss():
    out = decide_status(_inp(written_count=800))  # 20% de perda > hard_fail 5%
    assert out.status == STATUS_FAILED
    assert out.safe_to_advance_watermark is False


def test_status_failed_on_null_key():
    out = decide_status(_inp(null_key_pct=0.5))
    assert out.status == STATUS_FAILED


def test_status_partial_on_batch_duplicates():
    out = decide_status(_inp(batch_duplicates=3))
    assert out.status == STATUS_PARTIAL


def test_status_partial_on_contract_violation():
    out = decide_status(_inp(contract_ok=False))
    assert out.status == STATUS_PARTIAL