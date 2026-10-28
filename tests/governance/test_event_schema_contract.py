"""Contract tests for versioned Kafka event schemas and topic ownership."""

import json
from pathlib import Path

import jsonschema
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "schemas" / "online_transaction_event_v1.json"
REGISTRY = ROOT / "schemas" / "topic_registry.yml"


def load_schema() -> dict:
    return json.loads(SCHEMA.read_text(encoding="utf-8"))


def load_registry() -> dict:
    return yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))


def valid_event() -> dict:
    return {
        "event_id": "evt-10001",
        "event_type": "ONLINE_TRANSACTION_CHANGED",
        "event_version": 1,
        "event_key": "transaction_id:10001",
        "source_system": "postgresql",
        "source_table": "digital_banking.online_transaction",
        "occurred_at": "2026-10-28T10:00:00Z",
        "ingested_at": "2026-10-28T10:00:01Z",
        "payload": {
            "transaction_id": 10001,
            "amount": 10000.0,
            "currency": "VND",
            "status": "SUCCESS",
            "merchant_id": 900001,
            "payment_channel": "QR",
            "refund_flag": 0,
            "transaction_date": "2026-10-28T10:00:00Z",
        },
    }


def test_schema_and_registry_are_aligned():
    schema = load_schema()
    topic = load_registry()["topics"][0]
    assert topic["schema"] == "schemas/online_transaction_event_v1.json"
    assert topic["event_type"] == schema["properties"]["event_type"]["const"]
    assert topic["event_version"] == schema["properties"]["event_version"]["const"]
    assert topic["key"] == "transaction_id"
    assert topic["owner"] == "data-platform"
    assert topic["compatibility"] == "BACKWARD"


def test_valid_online_transaction_event_matches_schema():
    jsonschema.Draft202012Validator.check_schema(load_schema())
    jsonschema.validate(valid_event(), load_schema())


def test_required_event_identity_and_business_key_cannot_be_removed():
    schema = load_schema()
    required = set(schema["required"])
    assert {"event_id", "event_type", "event_version", "event_key", "occurred_at", "payload"} <= required
    assert "transaction_id" in schema["properties"]["payload"]["required"]


def test_breaking_event_changes_are_rejected():
    schema = load_schema()
    invalid = valid_event()
    invalid.pop("event_key")
    with __import__("pytest").raises(jsonschema.ValidationError):
        jsonschema.validate(invalid, schema)

    invalid = valid_event()
    invalid["event_version"] = 2
    with __import__("pytest").raises(jsonschema.ValidationError):
        jsonschema.validate(invalid, schema)


def test_registry_declares_replay_and_delivery_boundaries():
    topic = load_registry()["topics"][0]
    assert topic["replayable"] is True
    assert topic["delivery_semantics"] == "at_least_once"
    assert topic["ordering_scope"]
    assert topic["bronze_table"] == "lakehouse.bronze.online_transaction_cdc"
