"""Tests for the governed Kafka consumer adapter."""

import json
import sys
import types
from pathlib import Path

import pytest

from scripts.governed_kafka_consumer import _append_events, decode_event, main, process_poll
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


@pytest.mark.parametrize("raw", [b"[]", b'"text"', b"42", b"null"])
def test_decode_event_rejects_every_json_non_object(raw):
    with pytest.raises(ValueError, match="JSON object"):
        decode_event(raw)


def test_decode_event_rejects_undecodable_bytes():
    with pytest.raises(UnicodeDecodeError):
        decode_event(b"\xff\xfe\x00")


def test_decode_event_reads_the_value_attribute_of_a_kafka_record():
    event = _event()
    record = types.SimpleNamespace(value=json.dumps(event).encode("utf-8"))
    assert decode_event(record) == event


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


def test_dlq_write_survives_a_bare_record_without_coordinates(tmp_path):
    dlq = tmp_path / "dead.jsonl"
    assert process_poll([b"not-json"], TOPIC, REGISTRY, lambda events: None, dlq=dlq) == 0
    payload = json.loads(dlq.read_text(encoding="utf-8").splitlines()[0])
    assert payload["partition"] is None
    assert payload["record"] == "not-json"
    assert "Expecting value" in payload["error"]


def test_process_poll_without_dlq_preserves_failure():
    with pytest.raises(json.JSONDecodeError):
        process_poll([b"not-json"], TOPIC, REGISTRY, lambda events: None)


def test_process_poll_routes_reused_event_id_to_dlq(tmp_path):
    dlq = tmp_path / "dead.jsonl"
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    original = _event()
    conflicting = _event()
    conflicting["payload"]["amount"] = 999999.99
    try:
        assert process_poll([original], TOPIC, REGISTRY, lambda events: None, ledger) == 1
        records = [
            types.SimpleNamespace(value=json.dumps(conflicting).encode("utf-8"), topic=TOPIC, partition=0, offset=5)
        ]
        assert process_poll(records, TOPIC, REGISTRY, lambda events: None, ledger, dlq) == 0
        payload = json.loads(dlq.read_text(encoding="utf-8").splitlines()[0])
        assert "different payload" in payload["error"]
        assert payload["record"]["payload"]["amount"] == 999999.99
        assert ledger.is_replay(TOPIC, original) is True
    finally:
        ledger.close()


def test_cli_commits_only_after_process_poll_returns(monkeypatch, tmp_path):
    order = []

    def fake_process_poll(*args, **kwargs):
        order.append("poll")
        return 1

    class FakeConsumer:
        def __init__(self, *args, **kwargs):
            self._polled = False

        def poll(self, timeout_ms):
            if self._polled:
                return {}
            self._polled = True
            return {object(): [types.SimpleNamespace(value=b"{}")]}

        def commit(self):
            order.append("commit")

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaConsumer=FakeConsumer))
    monkeypatch.setattr("scripts.governed_kafka_consumer.process_poll", fake_process_poll)

    assert main(["--group-id", "test-group", "--max-messages", "1"]) == 0
    assert order == ["poll", "commit"]


def test_output_sink_appends_multiple_events_as_jsonl(tmp_path):
    output = tmp_path / "nested" / "events.jsonl"
    event = _event()
    second = json.loads(json.dumps(event))
    second["event_id"] = "evt-10002"
    second["event_key"] = "transaction_id:10002"
    _append_events(output, [event, second])
    assert [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()] == [event, second]


def test_output_sink_appends_to_an_existing_jsonl_file(tmp_path):
    output = tmp_path / "events.jsonl"
    first = _event()
    second = json.loads(json.dumps(first))
    second["event_id"] = "evt-10002"
    _append_events(output, [first])
    _append_events(output, [second])
    assert [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()] == [first, second]


def test_output_sink_creates_parent_directories(tmp_path):
    output = tmp_path / "deep" / "nested" / "events.jsonl"
    _append_events(output, [_event()])
    assert output.exists()
    assert len(output.read_text(encoding="utf-8").splitlines()) == 1


def test_output_sink_preserves_unicode(tmp_path):
    output = tmp_path / "events.jsonl"
    event = _event()
    event["source_system"] = "ngân hàng"
    _append_events(output, [event])
    assert "ngân hàng" in output.read_text(encoding="utf-8")


def test_process_poll_uses_the_same_output_boundary_for_multiple_events(tmp_path):
    output = tmp_path / "events.jsonl"
    event = _event()
    second = json.loads(json.dumps(event))
    second["event_id"] = "evt-10002"
    second["event_key"] = "transaction_id:10002"
    received = []
    assert (
        process_poll(
            [json.dumps(event).encode("utf-8"), json.dumps(second).encode("utf-8")],
            TOPIC,
            REGISTRY,
            lambda events: (received.extend(events), _append_events(output, events)),
        )
        == 2
    )
    assert received == [event, second]
    assert len(output.read_text(encoding="utf-8").splitlines()) == 2


def test_process_poll_isolates_a_bad_record_from_its_valid_neighbours(tmp_path):
    dlq = tmp_path / "dead.jsonl"
    ledger = ProcessedEventLedger(tmp_path / "events.db")
    received = []
    try:
        event = _event()
        bad = types.SimpleNamespace(value=b"not-json", topic=TOPIC, partition=0, offset=2)
        good = types.SimpleNamespace(value=json.dumps(event).encode("utf-8"), topic=TOPIC, partition=0, offset=3)

        assert process_poll([bad, good], TOPIC, REGISTRY, received.extend, ledger, dlq) == 1
        assert received == [event]
        lines = dlq.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1
        assert json.loads(lines[0])["offset"] == 2
    finally:
        ledger.close()


def test_process_poll_keeps_valid_records_when_every_record_but_one_is_bad(tmp_path):
    dlq = tmp_path / "dead.jsonl"
    received = []
    event = _event()
    records = [
        b"not-json",
        types.SimpleNamespace(value=json.dumps(event).encode("utf-8"), topic=TOPIC, partition=0, offset=1),
        b"{broken",
    ]

    assert process_poll(records, TOPIC, REGISTRY, received.extend, dlq=dlq) == 1
    assert received == [event]
    assert len(dlq.read_text(encoding="utf-8").splitlines()) == 2


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


def test_cli_output_sink_and_ledger_suppress_replay(monkeypatch, tmp_path, capsys):
    output = tmp_path / "events.jsonl"
    ledger = tmp_path / "events.db"

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

    arguments = [
        "--group-id",
        "test-group",
        "--ledger",
        str(ledger),
        "--output",
        str(output),
        "--max-messages",
        "1",
    ]
    assert main(arguments) == 0
    assert main(arguments) == 0
    output_text = capsys.readouterr().out
    assert output_text.count("Processed 1 governed events") == 1
    assert "Processed 0 governed events" in output_text
    assert [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()] == [_event()]


def test_cli_without_ledger_writes_the_same_event_on_every_run(monkeypatch, tmp_path):
    output = tmp_path / "events.jsonl"

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
    arguments = ["--group-id", "test-group", "--output", str(output), "--max-messages", "1"]

    assert main(arguments) == 0
    assert main(arguments) == 0
    assert [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()] == [_event(), _event()]


def test_cli_output_failure_leaves_the_ledger_untouched(monkeypatch, tmp_path):
    ledger_path = tmp_path / "events.db"
    output = tmp_path / "events.jsonl"
    committed = []

    class FakeConsumer:
        def __init__(self, *args, **kwargs):
            self._polled = False

        def poll(self, timeout_ms):
            if self._polled:
                return {}
            self._polled = True
            return {object(): [types.SimpleNamespace(value=json.dumps(_event()).encode("utf-8"))]}

        def commit(self):
            committed.append(True)

        def close(self):
            pass

    def failing_append(path, events):
        raise OSError("sink is full")

    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaConsumer=FakeConsumer))
    monkeypatch.setattr("scripts.governed_kafka_consumer._append_events", failing_append)

    with pytest.raises(OSError, match="sink is full"):
        main(["--group-id", "test-group", "--ledger", str(ledger_path), "--output", str(output), "--max-messages", "1"])

    assert committed == []
    assert not output.exists()

    ledger = ProcessedEventLedger(ledger_path)
    try:
        assert ledger.is_replay(TOPIC, _event()) is False
    finally:
        ledger.close()


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


def test_cli_bounds_invalid_record_routed_to_dlq(monkeypatch, tmp_path, capsys):
    dlq = tmp_path / "invalid.jsonl"
    committed = []

    class FakeConsumer:
        def __init__(self, *args, **kwargs):
            self._polled = False

        def poll(self, timeout_ms):
            if self._polled:
                return {}
            self._polled = True
            return {
                object(): [
                    types.SimpleNamespace(
                        value=b"not-json",
                        topic=TOPIC,
                        partition=1,
                        offset=9,
                        key=b"bad",
                    )
                ]
            }

        def commit(self):
            committed.append(True)

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "kafka", types.SimpleNamespace(KafkaConsumer=FakeConsumer))

    assert (
        main(
            [
                "--group-id",
                "test-group",
                "--dlq",
                str(dlq),
                "--max-messages",
                "1",
            ]
        )
        == 0
    )
    assert committed == [True]
    assert "Processed 0 governed events" in capsys.readouterr().out
    payload = json.loads(dlq.read_text(encoding="utf-8").splitlines()[0])
    assert payload["topic"] == TOPIC
    assert payload["partition"] == 1
    assert payload["offset"] == 9


@pytest.mark.parametrize("option", ["--max-messages", "--max-empty-polls"])
def test_cli_rejects_negative_poll_bounds(option):
    with pytest.raises(SystemExit):
        main(["--group-id", "test-group", option, "-1"])


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
