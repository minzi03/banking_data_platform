"""Tests for the governed Kafka consumer adapter."""

import json
import sys
import types
from pathlib import Path

import pytest

from scripts.governed_kafka_consumer import decode_event, main, process_poll
from scripts.processed_event_ledger import ProcessedEventLedger

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "schemas" / "topic_registry.yml"
TOPIC = "postgresql.banking.digital_banking.online_transaction"
EXAMPLE = ROOT / "schemas" / "examples" / "online_transaction_event_v1.example.json"


def _event() -> dict:
    return json.loads(EXAMPLE.read_text(encoding="utf-8"))


def test_decode_event_accepts_bytes_and_string():
    event = _event()
    assert decode_event(json.dumps(event).encode("utf-8")) == event
    assert decode_event(json.dumps(event)) == event


def test_decode_event_rejects_non_object():
    with pytest.raises(ValueError, match="JSON object"):
        decode_event(b"[]")


def test_process_poll_runs_governed_boundary(tmp_path):
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    received = []
    try:
        records = [json.dumps(_event()).encode("utf-8")] * 2
        assert process_poll(records, TOPIC, REGISTRY, received.extend, ledger) == 1
        assert received == [_event()]
    finally:
        ledger.close()


def test_process_poll_empty_batch_is_noop():
    assert process_poll([], TOPIC, REGISTRY, lambda events: None) == 0


def test_process_poll_routes_invalid_record_to_dlq(tmp_path):
    dlq = tmp_path / "nested" / "dead.jsonl"
    record = types.SimpleNamespace(
        value=b"not-json", topic=TOPIC, partition=2, offset=17, key=b"transaction_id:10001", timestamp=123
    )
    assert process_poll([record], TOPIC, REGISTRY, lambda events: None, dlq=dlq) == 0
    payload = json.loads(dlq.read_text(encoding="utf-8").splitlines()[0])
    assert payload == {
        "topic": TOPIC,
        "partition": 2,
        "offset": 17,
        "key": "transaction_id:10001",
        "timestamp": 123,
        "record": "not-json",
        "error": payload["error"],
    }
    assert "line 1" in payload["error"]


def test_process_poll_routes_schema_violation_to_dlq(tmp_path):
    dlq = tmp_path / "dead.jsonl"
    invalid = _event()
    invalid["payload"]["currency"] = "USDD"
    record = types.SimpleNamespace(value=json.dumps(invalid).encode("utf-8"), topic=TOPIC, partition=0, offset=1)

    assert process_poll([record], TOPIC, REGISTRY, lambda events: None, dlq=dlq) == 0
    payload = json.loads(dlq.read_text(encoding="utf-8").splitlines()[0])
    assert payload["topic"] == TOPIC
    assert payload["offset"] == 1
    assert "contract validation" in payload["error"]
    assert payload["record"]["payload"]["currency"] == "USDD"


def test_process_poll_without_dlq_preserves_failure():
    with pytest.raises(json.JSONDecodeError):
        process_poll([b"not-json"], TOPIC, REGISTRY, lambda events: None)


def test_cli_fails_without_kafka_python(monkeypatch, capsys):
    monkeypatch.setitem(sys.modules, "kafka", None)
    assert main(["--group-id", "test-group"]) == 2
    assert "kafka-python" in capsys.readouterr().err


def test_cli_commits_offsets_after_successful_processing(monkeypatch, tmp_path):
    committed = []

    class FakeMessage:
        def __init__(self, value):
            self.value = value

    class FakeConsumer:
        def __init__(self, *args, **kwargs):
            self._polls = 1

        def poll(self, timeout_ms):
            if self._polls == 0:
                return {}
            self._polls -= 1
            return {object(): [FakeMessage(json.dumps(_event()).encode("utf-8"))]}

        def commit(self):
            committed.append(True)

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaConsumer=FakeConsumer))

    assert main(["--group-id", "test-group", "--ledger", str(tmp_path / "l.db"), "--max-messages", "1"]) == 0
    assert committed == [True]


def test_cli_writes_accepted_events_after_processing(monkeypatch, tmp_path):
    output = tmp_path / "accepted" / "events.jsonl"

    class FakeConsumer:
        def __init__(self, *args, **kwargs):
            self._polled = False

        def poll(self, timeout_ms):
            if self._polled:
                return {}
            self._polled = True
            return {object(): [types.SimpleNamespace(value=json.dumps(_event()).encode("utf-8"))]}

        def commit(self):
            pass

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaConsumer=FakeConsumer))

    assert main(["--group-id", "test-group", "--output", str(output), "--max-messages", "1"]) == 0
    assert [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()] == [_event()]


def test_cli_stops_after_bounded_empty_polls(monkeypatch, capsys):
    class FakeConsumer:
        def __init__(self, *args, **kwargs):
            pass

        def poll(self, timeout_ms):
            return {}

        def commit(self):
            pytest.fail("empty poll must not commit")

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaConsumer=FakeConsumer))

    assert main(["--group-id", "test-group", "--max-empty-polls", "2", "--timeout-ms", "1"]) == 0
    assert "Processed 0 governed events" in capsys.readouterr().out


def test_cli_rejects_negative_poll_bounds():
    with pytest.raises(SystemExit):
        main(["--group-id", "test-group", "--max-empty-polls", "-1"])


def test_cli_does_not_commit_when_side_effect_fails(monkeypatch, tmp_path):
    committed = []

    class FakeConsumer:
        def __init__(self, *args, **kwargs):
            self._polls = 1

        def poll(self, timeout_ms):
            if self._polls == 0:
                return {}
            self._polls -= 1
            return {object(): [types.SimpleNamespace(value=json.dumps(_event()).encode("utf-8"))]}

        def commit(self):
            committed.append(True)

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaConsumer=FakeConsumer))

    def boom(*args, **kwargs):
        raise RuntimeError("downstream failed")

    monkeypatch.setattr("scripts.governed_kafka_consumer.process_poll", boom)

    with pytest.raises(RuntimeError, match="downstream failed"):
        main(["--group-id", "test-group", "--max-messages", "1"])
    assert committed == []
