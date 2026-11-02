"""Kafka consumer adapter for governed events with post-side-effect commits."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Iterable
from contextlib import suppress
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


def _write_dlq(path: Path, record: Any, error: Exception) -> None:
    """Append one rejected Kafka record, coordinates, and reason to JSONL."""
    raw = record.value if hasattr(record, "value") else record
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        with suppress(json.JSONDecodeError):
            raw = json.loads(raw)
    payload = {
        "topic": getattr(record, "topic", None),
        "partition": getattr(record, "partition", None),
        "offset": getattr(record, "offset", None),
        "key": getattr(record, "key", None),
        "timestamp": getattr(record, "timestamp", None),
        "record": raw,
        "error": str(error),
    }
    if isinstance(payload["key"], bytes):
        payload["key"] = payload["key"].decode("utf-8", errors="replace")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _append_events(path: Path, events: list[dict[str, Any]]) -> None:
    """Append governed events to a JSONL sink after validation and deduplication."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        for event in events:
            output.write(json.dumps(event, ensure_ascii=False) + "\n")


def process_poll(
    records: Iterable[Any],
    topic: str,
    registry: Path,
    side_effect: Callable[[list[dict[str, Any]]], None],
    ledger: ProcessedEventLedger | None = None,
    dlq: Path | None = None,
) -> int:
    """Decode one poll batch, route invalid records, and process valid records."""
    processed = 0
    for record in records:
        try:
            event = decode_event(record)
            processed += consume_batch([event], topic, registry, side_effect, ledger)
        except (ValueError, json.JSONDecodeError) as exc:
            if dlq is None:
                raise
            _write_dlq(dlq, record, exc)
    return processed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--ledger", type=Path, help="SQLite processed-event ledger")
    parser.add_argument("--max-messages", type=int, default=0, help="Stop after N messages (0 = run forever)")
    parser.add_argument("--max-empty-polls", type=int, default=0, help="Stop after N empty polls (0 = wait forever)")
    parser.add_argument("--dlq", type=Path, help="Append invalid records as JSONL")
    parser.add_argument(
        "--output",
        type=Path,
        help="Append accepted governed events as JSONL after side-effect processing",
    )
    parser.add_argument("--timeout-ms", type=int, default=5000)

    args = parser.parse_args(argv)
    if args.max_messages < 0 or args.max_empty_polls < 0:
        parser.error("--max-messages and --max-empty-polls must be non-negative")

    if args.dlq is not None:
        args.dlq.parent.mkdir(parents=True, exist_ok=True)

    def side_effect(events: list[dict[str, Any]]) -> None:
        if args.output is not None:
            _append_events(args.output, events)

    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("ERROR: kafka-python is required to run the Kafka consumer adapter", file=sys.stderr)
        return 2

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap.split(","),
        group_id=args.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    ledger = ProcessedEventLedger(args.ledger) if args.ledger else None
    processed = 0
    consumed = 0
    empty_polls = 0
    try:
        while True:
            polled = consumer.poll(timeout_ms=args.timeout_ms)
            records = [record for _, batch in polled.items() for record in batch]
            if not records:
                empty_polls += 1
                if args.max_empty_polls and empty_polls >= args.max_empty_polls:
                    break
                if args.max_messages and processed >= args.max_messages:
                    break
                continue
            empty_polls = 0
            processed += process_poll(records, args.topic, args.registry, side_effect, ledger, args.dlq)
            consumed += len(records)
            consumer.commit()
            if args.max_messages and consumed >= args.max_messages:
                break
    finally:
        consumer.close()
        if ledger is not None:
            ledger.close()

    print(f"Processed {processed} governed events from {args.topic}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
