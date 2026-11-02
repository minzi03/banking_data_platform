"""End-to-end local proof for governed event validation and replay handling."""

import json
from pathlib import Path

from scripts.governed_event_consumer import consume_batch
from scripts.processed_event_ledger import ProcessedEventLedger
from scripts.publish_governed_event import main as publish_main

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "schemas" / "topic_registry.yml"
TOPIC = "postgresql.banking.digital_banking.online_transaction"
EXAMPLE = ROOT / "schemas" / "examples" / "online_transaction_event_v1.example.json"


def _event() -> dict:
    return json.loads(EXAMPLE.read_text(encoding="utf-8"))


def test_dry_run_then_consume_then_replay_is_suppressed(tmp_path, capsys):
    event = _event()
    event_path = tmp_path / "event.json"
    ledger_path = tmp_path / "processed.db"
    event_path.write_text(json.dumps(event), encoding="utf-8")

    assert publish_main(["--topic", TOPIC, "--event", str(event_path)]) == 0
    assert "Event contract valid; dry-run only" in capsys.readouterr().out

    received = []
    ledger = ProcessedEventLedger(ledger_path)
    try:
        assert consume_batch([event], TOPIC, REGISTRY, received.extend, ledger) == 1
        assert consume_batch([event], TOPIC, REGISTRY, received.extend, ledger) == 0
    finally:
        ledger.close()

    assert received == [event]


def test_replay_flow_accepts_json_equivalent_event(tmp_path):
    event = _event()
    equivalent = json.loads(json.dumps(event))
    received = []
    ledger = ProcessedEventLedger(tmp_path / "processed.db")
    try:
        assert consume_batch([event], TOPIC, REGISTRY, received.extend, ledger) == 1
        assert consume_batch([equivalent], TOPIC, REGISTRY, received.extend, ledger) == 0
    finally:
        ledger.close()

    assert len(received) == 1
