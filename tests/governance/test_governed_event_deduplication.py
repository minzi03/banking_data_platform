"""Tests for governed event validation and batch deduplication."""

import json
from pathlib import Path

from scripts.deduplicate_governed_events import deduplicate_events, main

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "schemas" / "topic_registry.yml"
TOPIC = "postgresql.banking.digital_banking.online_transaction"
EXAMPLE = ROOT / "schemas" / "examples" / "online_transaction_event_v1.example.json"


def _event() -> dict:
    return json.loads(EXAMPLE.read_text(encoding="utf-8"))


def test_duplicate_identical_event_id_is_emitted_once():
    event = _event()

    assert deduplicate_events([event, json.loads(json.dumps(event))], TOPIC, REGISTRY) == [event]


def test_conflicting_event_id_is_rejected():
    first = _event()
    second = json.loads(json.dumps(first))
    second["payload"]["amount"] = 20000.0

    try:
        deduplicate_events([first, second], TOPIC, REGISTRY)
    except ValueError as exc:
        assert "conflicts" in str(exc)
    else:
        raise AssertionError("conflicting event_id was accepted")


def test_invalid_event_is_rejected():
    event = _event()
    event["payload"]["currency"] = "USDD"

    try:
        deduplicate_events([event], TOPIC, REGISTRY)
    except ValueError as exc:
        assert "failed contract validation" in str(exc)
    else:
        raise AssertionError("invalid event was accepted")


def test_jsonl_cli_emits_unique_events(tmp_path, capsys):
    event = _event()
    events_path = tmp_path / "events.jsonl"
    events_path.write_text("\n".join(json.dumps(event) for _ in (1, 2)), encoding="utf-8")

    assert main(["--events", str(events_path), "--topic", TOPIC]) == 0
    output = capsys.readouterr()
    assert len(output.out.splitlines()) == 1
    assert "Validated 2 events; emitted 1 unique events" in output.err


def test_ledger_skips_replay_across_cli_runs(tmp_path, capsys):
    event = _event()
    events_path = tmp_path / "events.jsonl"
    ledger_path = tmp_path / "processed.db"
    events_path.write_text(json.dumps(event), encoding="utf-8")

    assert main(["--events", str(events_path), "--topic", TOPIC, "--ledger", str(ledger_path)]) == 0
    capsys.readouterr()
    assert main(["--events", str(events_path), "--topic", TOPIC, "--ledger", str(ledger_path)]) == 0
    output = capsys.readouterr()
    assert output.out == ""
    assert "Validated 1 events; emitted 0 unique events" in output.err


def test_ledger_conflict_is_reported(tmp_path, capsys):
    first = _event()
    second = json.loads(json.dumps(first))
    second["payload"]["amount"] = 20000.0
    events_path = tmp_path / "events.jsonl"
    ledger_path = tmp_path / "processed.db"
    events_path.write_text(json.dumps(first), encoding="utf-8")
    assert main(["--events", str(events_path), "--topic", TOPIC, "--ledger", str(ledger_path)]) == 0
    capsys.readouterr()

    events_path.write_text(json.dumps(second), encoding="utf-8")
    assert main(["--events", str(events_path), "--topic", TOPIC, "--ledger", str(ledger_path)]) == 1
    assert "different payload" in capsys.readouterr().err


def test_without_ledger_preserves_batch_behavior(tmp_path, capsys):
    event = _event()
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(json.dumps(event), encoding="utf-8")

    assert main(["--events", str(events_path), "--topic", TOPIC]) == 0
    assert main(["--events", str(events_path), "--topic", TOPIC]) == 0
    assert len(capsys.readouterr().out.splitlines()) == 2
