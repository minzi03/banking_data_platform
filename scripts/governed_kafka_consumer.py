"""Kafka consumer adapter for governed events with post-side-effect commits."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:  # running as a script puts scripts/ on sys.path, not the repo root
    sys.path.insert(0, str(ROOT))

from scripts.governed_event_consumer import consume_batch  # noqa: E402
from scripts.processed_event_ledger import ProcessedEventLedger  # noqa: E402

DEFAULT_TOPIC = "postgresql.banking.digital_banking.online_transaction"
DEFAULT_REGISTRY = ROOT / "schemas" / "topic_registry.yml"


def decode_event(record: Any) -> dict[str, Any]:
    """Decode one Kafka record into an event envelope."""
    raw = record.value if hasattr(record, "value") else record
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, dict):
        raise ValueError("Kafka record value must decode to a JSON object")
    return raw


def process_poll(
    records: Iterable[Any],
    topic: str,
    registry: Path,
    side_effect: Callable[[list[dict[str, Any]]], None],
    ledger: ProcessedEventLedger | None = None,
) -> int:
    """Decode one poll batch and run it through the governed consumer boundary."""
    events = [decode_event(record) for record in records]
    if not events:
        return 0
    return consume_batch(events, topic, registry, side_effect, ledger)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--ledger", type=Path, help="SQLite processed-event ledger")
    parser.add_argument("--max-messages", type=int, default=0, help="Stop after N messages (0 = run forever)")
    parser.add_argument("--timeout-ms", type=int, default=5000)
    args = parser.parse_args(argv)

    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("ERROR: kafka-python is required to run the Kafka consumer adapter", file=sys.stderr)
        return 2

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap.split(","),
        group_id=args.group_id,
        # Commit chỉ sau khi side effect và ledger thành công. Xem docstring.
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    ledger = ProcessedEventLedger(args.ledger) if args.ledger else None
    processed = 0
    try:
        while True:
            polled = consumer.poll(timeout_ms=args.timeout_ms)
            records = [record for _, batch in polled.items() for record in batch]
            if not records:
                if args.max_messages and processed >= args.max_messages:
                    break
                continue
            processed += process_poll(records, args.topic, args.registry, lambda events: None, ledger)
            consumer.commit()
            if args.max_messages and processed >= args.max_messages:
                break
    finally:
        consumer.close()
        if ledger is not None:
            ledger.close()

    print(f"Processed {processed} governed events from {args.topic}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
