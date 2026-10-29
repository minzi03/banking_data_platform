"""Apply the repository's explicit backward-compatibility policy to JSON Schemas."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class CompatibilityError(ValueError):
    """Raised when a candidate schema breaks existing consumers."""


def _properties(schema: dict[str, Any]) -> dict[str, Any]:
    properties = schema.get("properties", {})
    result = dict(properties)
    payload = properties.get("payload", {})
    for name, definition in payload.get("properties", {}).items():
        result[f"payload.{name}"] = definition
    return result


def find_backward_breaks(previous: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    """Return breaks for consumers that validate data accepted by ``previous``."""
    errors: list[str] = []
    previous_required = set(previous.get("required", []))
    candidate_required = set(candidate.get("required", []))
    for field in sorted(previous_required - candidate_required):
        errors.append(f"required envelope field removed: {field}")
    for field in sorted(candidate_required - previous_required):
        errors.append(f"required envelope field added: {field}")

    previous_payload = previous.get("properties", {}).get("payload", {})
    candidate_payload = candidate.get("properties", {}).get("payload", {})
    previous_payload_required = set(previous_payload.get("required", []))
    candidate_payload_required = set(candidate_payload.get("required", []))
    for field in sorted(previous_payload_required - candidate_payload_required):
        errors.append(f"required payload field removed: {field}")
    for field in sorted(candidate_payload_required - previous_payload_required):
        errors.append(f"required payload field added: {field}")

    current_properties = _properties(candidate)
    for name, previous_definition in _properties(previous).items():
        candidate_definition = current_properties.get(name)
        if candidate_definition is None:
            errors.append(f"property removed: {name}")
            continue
        if name != "event_version" and candidate_definition.get("type") != previous_definition.get("type"):
            errors.append(f"property type changed: {name}")
        previous_enum = previous_definition.get("enum")
        candidate_enum = candidate_definition.get("enum")
        if previous_enum and candidate_enum and not set(previous_enum) <= set(candidate_enum):
            errors.append(f"enum values removed: {name}")
    return errors


def validate_backward(previous_path: Path, candidate_path: Path) -> None:
    previous = json.loads(previous_path.read_text(encoding="utf-8"))
    candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
    errors = find_backward_breaks(previous, candidate)
    if errors:
        raise CompatibilityError("; ".join(errors))
