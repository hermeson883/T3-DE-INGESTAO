"""Testes da validacao de data contract (bonus +3)."""

import datetime as dt

import pytest

from mflix_ingest.contract import load_contract, validate_contract

REPO_ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
CONTRACT = REPO_ROOT / "config" / "data_contract.yaml"


@pytest.fixture(scope="module")
def contract():
    return load_contract(CONTRACT)


def _comment(**over):
    base = dict(
        _id="5a9427648b0beebeb69579cc",
        name="Andrea Le",
        email="andrea_le@fakegmail.com",
        movie_id="573a1390f29313caabcd4135",
        text="lorem",
        date=dt.datetime(2012, 3, 26, tzinfo=dt.timezone.utc),
    )
    base.update(over)
    return base


def test_valid_sample_passes(contract):
    res = validate_contract("comments", [_comment() for _ in range(20)], contract)
    assert res.ok, res.summary()


def test_missing_required_field_fails(contract):
    docs = [_comment() for _ in range(18)] + [
        {k: v for k, v in _comment().items() if k != "email"} for _ in range(2)
    ]
    # 10% ausencia de email (required) > max_violation_pct 5% -> falha
    res = validate_contract("comments", docs, contract)
    assert not res.ok
    assert any(v.field == "email" and v.kind == "missing" for v in res.violations)


def test_small_violation_tolerated(contract):
    docs = [_comment() for _ in range(99)] + [
        {k: v for k, v in _comment().items() if k != "text"}
    ]
    # text e nullable -> nem conta; 1% de qualquer coisa < 5%
    res = validate_contract("comments", docs, contract)
    assert res.ok


def test_forbidden_field_present_fails(contract):
    docs = [dict(_id="59b99db4cfa9a34dcd7885b6", name="Ned", email="n@x.es",
                 password="$2b$12$hash")]
    res = validate_contract("users", docs, contract)
    assert not res.ok
    assert any(v.kind == "forbidden" and v.field == "password" for v in res.violations)


def test_empty_sample_allowed_only_when_flagged(contract):
    assert validate_contract("sessions", [], contract).ok       # allow_empty: true
    assert not validate_contract("comments", [], contract).ok   # sem allow_empty


def test_type_mismatch_detected(contract):
    docs = [_comment(date="2012-03-26") for _ in range(20)]  # string, deveria ser datetime
    res = validate_contract("comments", docs, contract)
    assert not res.ok
    assert any(v.field == "date" and v.kind == "type" for v in res.violations)