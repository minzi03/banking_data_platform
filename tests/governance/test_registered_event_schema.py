"""Tests for read-only comparison of registry and repository schemas."""

import json
from pathlib import Path

import pytest

from scripts.verify_registered_event_schema import canonical_schema, verify_registered_schema

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "schemas" / "topic_registry.yml"
TOPIC = "postgresql.banking.digital_banking.online_transaction"


def test_canonical_schema_handles_registry_json_string():
    schema = {"type": "object", "additionalProperties": False}

    assert canonical_schema(json.dumps(schema)) == schema
    assert canonical_schema(json.dumps(json.dumps(schema))) == schema


def test_registered_schema_matches_local_contract(monkeypatch):
    local = json.loads((ROOT / "schemas" / "online_transaction_event_v1.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(
        "scripts.verify_registered_event_schema.fetch_json",
        lambda url, path: {"schema": json.dumps(local)},
    )

    verify_registered_schema("http://registry", TOPIC, 1, REGISTRY)


def test_registered_schema_drift_is_rejected(monkeypatch):
    monkeypatch.setattr(
        "scripts.verify_registered_event_schema.fetch_json",
        lambda url, path: {"schema": json.dumps({"type": "object"})},
    )

    with pytest.raises(ValueError, match="differs"):
        verify_registered_schema("http://registry", TOPIC, 1, REGISTRY)
