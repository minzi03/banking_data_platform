"""Validate a Kafka event against the schema selected by the topic registry."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema

from schemas.validate_topic_registry import load_registry


class EventContractError(ValueError):
    """Raised when an event cannot be resolved or fails its schema contract."""


def _schema_path(registry_path: Path, schema_ref: str) -> Path:
    path = (registry_path.parent.parent / schema_ref).resolve()
    if not path.is_file():
        path = (registry_path.parent / Path(schema_ref).name).resolve()
    return path


def validate_event(event: dict[str, Any], topic: str, registry_path: Path) -> None:
    """Validate one event for a registered topic, raising on contract failure."""
    registry = load_registry(registry_path)
    topics = registry.get("topics", [])
    matches = [entry for entry in topics if isinstance(entry, dict) and entry.get("name") == topic]
    if not matches:
        raise EventContractError(f"Unknown topic: {topic}")
    if len(matches) != 1:
        raise EventContractError(f"Topic has {len(matches)} registry entries: {topic}")

    entry = matches[0]
    event_type = event.get("event_type")
    event_version = event.get("event_version")
    if event_type != entry.get("event_type"):
        raise EventContractError(f"Event type {event_type!r} does not match topic contract")
    if event_version != entry.get("event_version"):
        raise EventContractError(f"Event version {event_version!r} does not match topic contract")

    schema_ref = entry.get("schema")
    if not isinstance(schema_ref, str):
        raise EventContractError("Topic schema reference must be a string")
    schema_path = _schema_path(registry_path, schema_ref)
    if not schema_path.is_file():
        raise EventContractError(f"Schema file does not exist: {schema_ref}")

    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        jsonschema.validate(event, schema)
    except (OSError, json.JSONDecodeError, jsonschema.ValidationError) as exc:
        raise EventContractError(f"Event failed schema validation: {exc}") from exc


def main() -> int:
    """Validate one JSON event supplied on stdin."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--registry", type=Path, default=Path(__file__).with_name("topic_registry.yml"))
    args = parser.parse_args()
    try:
        event = json.load(sys.stdin)
        validate_event(event, args.topic, args.registry)
    except (json.JSONDecodeError, EventContractError) as exc:
        print(f"ERROR: {exc}")
        return 1
    print("Event contract valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
