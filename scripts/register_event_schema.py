"""Register a governed JSON Schema in a Confluent-compatible registry."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.request import Request, urlopen


def subject_for(topic: str) -> str:
    """Return the value subject used by the default TopicNameStrategy."""
    return f"{topic}-value"


def registration_payload(schema_path: Path) -> dict[str, str]:
    """Build the Confluent-compatible JSON Schema registration payload."""
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    return {"schemaType": "JSON", "schema": json.dumps(schema, separators=(",", ":"))}


def register_schema(registry_url: str, subject: str, payload: dict[str, str]) -> dict:
    """Register one schema and return the registry response."""
    request = Request(
        f"{registry_url.rstrip('/')}/subjects/{subject}/versions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:  # noqa: S310 - URL is operator supplied
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from schemas.validate_topic_registry import load_registry

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--registry", type=Path, default=Path(__file__).parents[1] / "schemas" / "topic_registry.yml")
    parser.add_argument("--apply", action="store_true", help="Register the schema; without this flag, print a dry run")
    args = parser.parse_args()

    topic_registry = load_registry(args.registry)
    entries = [entry for entry in topic_registry.get("topics", []) if entry.get("name") == args.topic]
    if len(entries) != 1:
        print(f"ERROR: expected one registry entry for topic {args.topic!r}")
        return 1

    schema_ref = entries[0]["schema"]
    schema_path = (args.registry.parent.parent / schema_ref).resolve()
    if not schema_path.is_file():
        schema_path = (args.registry.parent / Path(schema_ref).name).resolve()
    payload = registration_payload(schema_path)
    subject = subject_for(args.topic)

    if not args.apply:
        print(json.dumps({"subject": subject, "payload": payload}, indent=2))
        return 0

    try:
        response = register_schema(args.registry_url, subject, payload)
    except OSError as exc:
        print(f"ERROR: schema registration failed: {exc}")
        return 1
    print(json.dumps(response))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
