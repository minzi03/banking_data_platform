"""Cross-component contract tests for governed event identity fields."""

import json
from pathlib import Path

from scripts.deduplicate_governed_events import deduplicate_events
from scripts.governed_event_consumer import consume_batch
from scripts.publish_governed_event import _publish

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "schemas" / "topic_registry.yml"
TOPIC = "postgresql.banking.digital_banking.online_transaction"
EXAMPLE = ROOT / "schemas" / "examples" / "online_transaction_event_v1.example.json"


def _event() -> dict:
    return json.loads(EXAMPLE.read_text(encoding="utf-8"))


def test_registry_declares_same_event_id_key_used_by_consumer():
    registry = (ROOT / "schemas" / "topic_registry.yml").read_text(encoding="utf-8")
    assert "idempotency_key: event_id" in registry


def test_batch_deduplication_uses_event_id_not_event_key():
    first = _event()
    second = json.loads(json.dumps(first))
    second["event_id"] = "evt-10002"
    second["event_key"] = first["event_key"]

    unique = deduplicate_events([first, second], TOPIC, REGISTRY)

    assert [event["event_id"] for event in unique] == ["evt-10001", "evt-10002"]


def test_consumer_side_effect_receives_event_key_for_partitioning():
    event = _event()
    received = []

    assert consume_batch([event], TOPIC, REGISTRY, received.extend) == 1
    assert received[0]["event_key"] == "transaction_id:10001"
    assert received[0]["event_id"] == "evt-10001"


def test_publisher_uses_event_key_as_kafka_key(monkeypatch):
    calls = []

    class Future:
        def get(self, timeout):
            return None

    class Producer:
        def __init__(self, **kwargs):
            pass

        def send(self, topic, **kwargs):
            calls.append((topic, kwargs))
            return Future()

        def flush(self, timeout):
            pass

        def close(self, timeout):
            pass

    monkeypatch.setitem(__import__("sys").modules, "kafka", type("Kafka", (), {"KafkaProducer": Producer}))
    event = _event()

    _publish("kafka:9092", TOPIC, event)

    assert calls == [(TOPIC, {"key": "transaction_id:10001", "value": event})]
