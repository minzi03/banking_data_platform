"""Validate Kafka topic registry entries against their JSON Schemas."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema
import yaml

SUPPORTED_COMPATIBILITY = {"BACKWARD", "FORWARD", "FULL", "NONE"}
SUPPORTED_DELIVERY = {"at_least_once", "at_most_once", "exactly_once"}


def load_registry(path: Path) -> dict[str, Any]:
    registry = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(registry, dict):
        raise ValueError("Topic registry must contain a mapping")
    return registry


def _required_string(mapping: dict[str, Any], field: str, context: str) -> str:
    value = mapping.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}: {field} must be a non-empty string")
    return value


def validate_registry(registry_path: Path) -> list[str]:
    """Return validation errors; an empty list means the registry is valid."""
    registry = load_registry(registry_path)
    errors: list[str] = []
    owners = registry.get("owners")
    topics = registry.get("topics")
    if not isinstance(owners, dict):
        errors.append("owners must be a mapping")
        owners = {}
    if not isinstance(topics, list) or not topics:
        errors.append("topics must be a non-empty list")
        topics = []

    seen_topics: set[str] = set()
    seen_schema_keys: set[tuple[str, int]] = set()
    for index, topic in enumerate(topics):
        context = f"topics[{index}]"
        if not isinstance(topic, dict):
            errors.append(f"{context} must be a mapping")
            continue
        try:
            topic_name = _required_string(topic, "name", context)
            event_type = _required_string(topic, "event_type", context)
            schema_ref = _required_string(topic, "schema", context)
            owner = _required_string(topic, "owner", context)
            compatibility = _required_string(topic, "compatibility", context)
            delivery = _required_string(topic, "delivery_semantics", context)
            key = _required_string(topic, "key", context)
            idempotency_key = _required_string(topic, "idempotency_key", context)
        except ValueError as exc:
            errors.append(str(exc))
            continue

        if topic_name in seen_topics:
            errors.append(f"{context}: duplicate topic name {topic_name!r}")
        seen_topics.add(topic_name)
        if owner not in owners:
            errors.append(f"{context}: owner {owner!r} is not declared")
        if compatibility not in SUPPORTED_COMPATIBILITY:
            errors.append(f"{context}: unsupported compatibility {compatibility!r}")
        if delivery not in SUPPORTED_DELIVERY:
            errors.append(f"{context}: unsupported delivery semantics {delivery!r}")
        if topic.get("replayable") is not True:
            errors.append(f"{context}: replayable must be true for governed topics")

        version = topic.get("event_version")
        schema_key = (event_type, version) if isinstance(version, int) else None
        if schema_key is None:
            errors.append(f"{context}: event_version must be an integer")
        elif schema_key in seen_schema_keys:
            errors.append(f"{context}: duplicate event_type/event_version {schema_key!r}")
        else:
            seen_schema_keys.add(schema_key)

        schema_path = (registry_path.parent.parent / schema_ref).resolve()
        if not schema_path.is_file():
            schema_path = (registry_path.parent / Path(schema_ref).name).resolve()
        if not schema_path.is_file():
            errors.append(f"{context}: schema file does not exist: {schema_ref}")
            continue
        try:
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            jsonschema.Draft202012Validator.check_schema(schema)
        except (OSError, json.JSONDecodeError, jsonschema.SchemaError) as exc:
            errors.append(f"{context}: invalid schema {schema_ref}: {exc}")
            continue

        properties = schema.get("properties", {})
        if properties.get("event_type", {}).get("const") != event_type:
            errors.append(f"{context}: event_type does not match schema {schema_ref}")
        if properties.get("event_version", {}).get("const") != version:
            errors.append(f"{context}: event_version does not match schema {schema_ref}")
        if key not in properties.get("payload", {}).get("properties", {}):
            errors.append(f"{context}: key {key!r} is not a payload schema property")
        if idempotency_key not in properties:
            errors.append(f"{context}: idempotency_key {idempotency_key!r} is not an envelope schema property")

        required = schema.get("required", [])
        if idempotency_key not in required:
            errors.append(f"{context}: idempotency_key {idempotency_key!r} must be required by the schema")

    return errors


def main() -> int:
    registry_path = Path(__file__).with_name("topic_registry.yml")
    errors = validate_registry(registry_path)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print(f"Topic registry valid: {registry_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
