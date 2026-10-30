"""Validate and deduplicate governed JSON events for one consumer batch."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOPIC = "postgresql.banking.digital_banking.online_transaction"
DEFAULT_REGISTRY = ROOT / "schemas" / "topic_registry.yml"


def _read_events(path: Path | None) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8") if path else sys.stdin.read()
    events: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        event = json.loads(line)
        if not isinstance(event, dict):
            raise ValueError(f"Line {line_number}: event must be a JSON object")
        events.append(event)
    return events


def deduplicate_events(events: list[dict[str, Any]], topic: str, registry: Path) -> list[dict[str, Any]]:
    sys.path.insert(0, str(ROOT))
    from schemas.validate_event import EventContractError, validate_event

    unique: dict[str, dict[str, Any]] = {}
    for position, event in enumerate(events, start=1):
        try:
            validate_event(event, topic, registry)
        except EventContractError as exc:
            raise ValueError(f"Event {position} failed contract validation: {exc}") from exc

        event_id = event["event_id"]
        previous = unique.get(event_id)
        if previous is None:
            unique[event_id] = event
        elif previous != event:
            raise ValueError(f"Event {position} conflicts with an earlier event_id: {event_id}")

    return list(unique.values())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, help="JSONL input file; defaults to stdin")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    args = parser.parse_args(argv)

    try:
        events = _read_events(args.events)
        unique = deduplicate_events(events, args.topic, args.registry)
        for event in unique:
            print(json.dumps(event, ensure_ascii=False, separators=(",", ":")))
        print(f"Validated {len(events)} events; emitted {len(unique)} unique events", file=sys.stderr)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
