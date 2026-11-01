"""Tests for persistent processed-event replay tracking."""

import json
from pathlib import Path

import pytest

from scripts.processed_event_ledger import EventLedgerConflict, ProcessedEventLedger, event_fingerprint

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "schemas" / "examples" / "online_transaction_event_v1.example.json"


def _event() -> dict:
    return json.loads(EXAMPLE.read_text(encoding="utf-8"))


def test_fingerprint_is_stable_for_mapping_order():
    assert event_fingerprint({"b": 2, "a": 1}) == event_fingerprint({"a": 1, "b": 2})


def test_first_event_is_recorded_and_identical_replay_is_skipped(tmp_path):
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    try:
        event = _event()
        assert ledger.record("topic", event) is True
        assert ledger.record("topic", json.loads(json.dumps(event))) is False
    finally:
        ledger.close()


def test_same_event_id_with_different_payload_is_rejected(tmp_path):
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    try:
        first = _event()
        second = json.loads(json.dumps(first))
        second["payload"]["amount"] = 20000.0
        ledger.record("topic", first)
        with pytest.raises(EventLedgerConflict, match="different payload"):
            ledger.record("topic", second)
    finally:
        ledger.close()


def test_topic_is_part_of_idempotency_scope(tmp_path):
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    try:
        event = _event()
        assert ledger.record("topic-a", event) is True
        assert ledger.record("topic-b", event) is True
    finally:
        ledger.close()


def test_ledger_survives_reopen(tmp_path):
    database = tmp_path / "events.db"
    event = _event()
    ledger = ProcessedEventLedger(database)
    assert ledger.record("topic", event) is True
    ledger.close()

    reopened = ProcessedEventLedger(database)
    try:
        assert reopened.record("topic", event) is False
    finally:
        reopened.close()
