"""Tests for post-side-effect governed event consumption."""

import json
from pathlib import Path

import pytest

from scripts.governed_event_consumer import consume_batch
from scripts.processed_event_ledger import ProcessedEventLedger

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "schemas" / "topic_registry.yml"
TOPIC = "postgresql.banking.digital_banking.online_transaction"
EXAMPLE = ROOT / "schemas" / "examples" / "online_transaction_event_v1.example.json"


def _event() -> dict:
    return json.loads(EXAMPLE.read_text(encoding="utf-8"))


def test_side_effect_receives_only_valid_unique_events(tmp_path):
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    received = []
    try:
        event = _event()
        assert consume_batch([event, json.loads(json.dumps(event))], TOPIC, REGISTRY, received.extend, ledger) == 1
        assert received == [event]
        assert ledger.is_replay(TOPIC, event) is True
    finally:
        ledger.close()


def test_failed_side_effect_does_not_mark_events_processed(tmp_path):
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    try:
        event = _event()

        def fail(_events):
            raise RuntimeError("downstream unavailable")

        with pytest.raises(RuntimeError, match="downstream unavailable"):
            consume_batch([event], TOPIC, REGISTRY, fail, ledger)
        assert ledger.is_replay(TOPIC, event) is False
    finally:
        ledger.close()


def test_replay_is_not_sent_to_side_effect(tmp_path):
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    received = []
    try:
        event = _event()
        ledger.record(TOPIC, event)
        assert consume_batch([event], TOPIC, REGISTRY, received.extend, ledger) == 0
        assert received == []
    finally:
        ledger.close()


def test_ledger_records_only_after_the_side_effect_returns(tmp_path):
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    observed = []
    try:
        event = _event()

        def observe(events):
            observed.append(ledger.is_replay(TOPIC, events[0]))

        assert consume_batch([event], TOPIC, REGISTRY, observe, ledger) == 1
        assert observed == [False], "the event was already recorded before the side effect ran"
        assert ledger.is_replay(TOPIC, event) is True
    finally:
        ledger.close()


def test_side_effect_is_skipped_entirely_for_an_all_replay_batch(tmp_path):
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    calls = []
    try:
        event = _event()
        ledger.record(TOPIC, event)

        def side_effect(events):
            calls.append(list(events))

        assert consume_batch([event, event], TOPIC, REGISTRY, side_effect, ledger) == 0
        assert calls == [], "an all-replay batch must not reach the side effect at all"

    finally:
        ledger.close()


def test_duplicate_events_in_one_batch_reach_the_side_effect_once(tmp_path):
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    received = []
    try:
        event = _event()
        copy = json.loads(json.dumps(event))
        assert consume_batch([event, copy, event], TOPIC, REGISTRY, received.extend, ledger) == 1
        assert len(received) == 1
        assert received[0]["event_id"] == event["event_id"]
    finally:
        ledger.close()
