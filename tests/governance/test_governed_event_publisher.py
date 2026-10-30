"""Tests for validation and dry-run behavior of the governed Kafka publisher."""

import json
from pathlib import Path

import pytest

from scripts import publish_governed_event as publisher

ROOT = Path(__file__).resolve().parents[2]
TOPIC = "postgresql.banking.digital_banking.online_transaction"
EXAMPLE = ROOT / "schemas" / "examples" / "online_transaction_event_v1.example.json"


def test_valid_event_dry_run_does_not_connect_to_kafka(monkeypatch, capsys):
    event = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    monkeypatch.setattr(publisher, "_publish", lambda *args: pytest.fail("must not publish in dry-run"))

    assert publisher.main(["--topic", TOPIC, "--event", str(EXAMPLE)]) == 0
    output = capsys.readouterr().out
    assert "Event contract valid; dry-run only" in output
    assert event["event_key"] in output


def test_invalid_event_is_rejected_before_publish(monkeypatch, tmp_path, capsys):
    event = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    event["payload"]["currency"] = "USDD"
    event_path = tmp_path / "invalid.json"
    event_path.write_text(json.dumps(event), encoding="utf-8")
    monkeypatch.setattr(publisher, "_publish", lambda *args: pytest.fail("invalid event was published"))

    assert publisher.main(["--topic", TOPIC, "--event", str(event_path), "--apply"]) == 1
    assert "ERROR:" in capsys.readouterr().err


def test_apply_publishes_validated_event_with_registered_topic(monkeypatch, tmp_path):
    event = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps(event), encoding="utf-8")
    calls = []
    monkeypatch.setattr(publisher, "_publish", lambda *args: calls.append(args))

    assert publisher.main(["--topic", TOPIC, "--event", str(event_path), "--bootstrap", "kafka:9092", "--apply"]) == 0
    assert calls == [("kafka:9092", TOPIC, event)]


def test_non_object_json_is_rejected(tmp_path, capsys):
    event_path = tmp_path / "array.json"
    event_path.write_text("[]", encoding="utf-8")

    assert publisher.main(["--event", str(event_path)]) == 1
    assert "Event must be a JSON object" in capsys.readouterr().err
