"""Ensure the checked-in event example remains contract-valid."""

import json
from pathlib import Path

from schemas.validate_event import validate_event

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "schemas" / "topic_registry.yml"
EXAMPLE = ROOT / "schemas" / "examples" / "online_transaction_event_v1.example.json"
TOPIC = "postgresql.banking.digital_banking.online_transaction"


def test_checked_in_online_transaction_example_is_valid():
    event = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    validate_event(event, TOPIC, REGISTRY)
