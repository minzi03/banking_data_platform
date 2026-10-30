"""Validate and optionally publish one governed event to Kafka."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOPIC = "postgresql.banking.digital_banking.online_transaction"
DEFAULT_REGISTRY = ROOT / "schemas" / "topic_registry.yml"


def _load_event(path: Path | None) -> dict[str, Any]:
    source = path.read_text(encoding="utf-8") if path else sys.stdin.read()
    event = json.loads(source)
    if not isinstance(event, dict):
        raise ValueError("Event must be a JSON object")
    return event


def _validate(event: dict[str, Any], topic: str, registry: Path) -> None:
    sys.path.insert(0, str(ROOT))
    from schemas.validate_event import EventContractError, validate_event

    try:
        validate_event(event, topic, registry)
    except EventContractError as exc:
        raise ValueError(str(exc)) from exc


def _publish(bootstrap: str, topic: str, event: dict[str, Any]) -> None:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError("kafka-python is required with --apply") from exc

    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3,
    )
    try:
        producer.send(topic, key=event["event_key"], value=event).get(timeout=10)
        producer.flush(timeout=10)
    finally:
        producer.close(timeout=10)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", type=Path, help="JSON event file; defaults to stdin")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--apply", action="store_true", help="Publish after validation")
    args = parser.parse_args(argv)

    try:
        event = _load_event(args.event)
        _validate(event, args.topic, args.registry)
        if args.apply:
            _publish(args.bootstrap, args.topic, event)
            print(f"Published governed event to {args.topic}")
        else:
            print(json.dumps(event, ensure_ascii=False, indent=2))
            print("Event contract valid; dry-run only")
    except (OSError, ValueError, json.JSONDecodeError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
