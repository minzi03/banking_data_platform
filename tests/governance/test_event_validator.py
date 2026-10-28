"""Tests for runtime-style event validation against the topic registry."""

import json
from pathlib import Path

import pytest

from schemas.validate_event import EventContractError, validate_event
from tests.governance.test_event_schema_contract import valid_event

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "schemas" / "topic_registry.yml"
TOPIC = "postgresql.banking.digital_banking.online_transaction"


# The topic contract declares event_id as the idempotency key.
def test_registered_event_has_idempotency_key():
    event = valid_event()
    assert event["event_id"]
    validate_event(event, TOPIC, REGISTRY)


def test_event_without_idempotency_key_is_rejected():
    event = valid_event()
    event.pop("event_id")

    with pytest.raises(EventContractError, match="schema validation"):
        validate_event(event, TOPIC, REGISTRY)


def test_registered_event_is_valid():
    validate_event(valid_event(), TOPIC, REGISTRY)


def test_unknown_topic_is_rejected():
    with pytest.raises(EventContractError, match="Unknown topic"):
        validate_event(valid_event(), "unknown.topic", REGISTRY)


def test_wrong_event_version_is_rejected_before_schema_validation():
    event = valid_event()
    event["event_version"] = 2

    with pytest.raises(EventContractError, match="version"):
        validate_event(event, TOPIC, REGISTRY)


def test_schema_violation_is_rejected():
    event = valid_event()
    event["payload"]["transaction_id"] = 0

    with pytest.raises(EventContractError, match="schema validation"):
        validate_event(event, TOPIC, REGISTRY)


def test_cli_accepts_valid_event(tmp_path, monkeypatch, capsys):
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps(valid_event()), encoding="utf-8")
    monkeypatch.setattr("sys.stdin", event_path.open(encoding="utf-8"))

    from schemas import validate_event as module

    monkeypatch.setattr(
        "sys.argv",
        ["validate_event", "--topic", TOPIC, "--registry", str(REGISTRY)],
    )
    assert module.main() == 0
    assert "Event contract valid" in capsys.readouterr().out
