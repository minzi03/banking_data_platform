"""Compare a registered subject schema with the active local topic contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from schemas.validate_topic_registry import load_registry  # noqa: E402
from scripts.register_event_schema import subject_for  # noqa: E402
from scripts.verify_schema_registry import fetch_json  # noqa: E402


def canonical_schema(schema_text: str) -> dict:
    """Parse a registry schema string into a JSON object."""
    schema = json.loads(schema_text)
    if isinstance(schema, str):
        schema = json.loads(schema)
    if not isinstance(schema, dict):
        raise ValueError("Registered schema is not a JSON object")
    return schema


def local_schema_for(topic: str, registry_path: Path) -> tuple[str, dict]:
    registry = load_registry(registry_path)
    entries = [entry for entry in registry.get("topics", []) if entry.get("name") == topic]
    if len(entries) != 1:
        raise ValueError(f"Expected one registry entry for topic {topic!r}")
    schema_ref = entries[0].get("schema")
    schema_path = (registry_path.parent.parent / schema_ref).resolve()
    if not schema_path.is_file():
        schema_path = (registry_path.parent / Path(schema_ref).name).resolve()
    return schema_ref, json.loads(schema_path.read_text(encoding="utf-8"))


def verify_registered_schema(registry_url: str, topic: str, version: int, registry_path: Path) -> None:
    schema_ref, local_schema = local_schema_for(topic, registry_path)
    subject = subject_for(topic)
    response = fetch_json(registry_url, f"/subjects/{subject}/versions/{version}")
    if not isinstance(response, dict) or "schema" not in response:
        raise ValueError(f"Subject {subject!r} version {version} has no schema payload")
    registered_schema = canonical_schema(response["schema"])
    if registered_schema != local_schema:
        raise ValueError(f"Registered schema differs from {schema_ref}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--version", type=int, default=1)
    parser.add_argument("--registry", type=Path, default=Path(__file__).parents[1] / "schemas" / "topic_registry.yml")
    args = parser.parse_args()
    try:
        verify_registered_schema(args.registry_url, args.topic, args.version, args.registry)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}")
        return 1
    print("Registered schema matches local topic contract")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
