"""Tests for the Confluent-compatible schema registration adapter."""

import json
from pathlib import Path

from scripts.register_event_schema import registration_payload, subject_for

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "schemas" / "online_transaction_event_v1.json"
TOPIC = "postgresql.banking.digital_banking.online_transaction"


def test_subject_uses_topic_name_strategy():
    assert subject_for(TOPIC) == f"{TOPIC}-value"


def test_registration_payload_contains_json_schema():
    payload = registration_payload(SCHEMA)

    assert payload["schemaType"] == "JSON"
    assert json.loads(payload["schema"])["properties"]["event_version"]["const"] == 1
