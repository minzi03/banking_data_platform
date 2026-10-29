"""Validate a candidate event schema against the active registered schema."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from schemas.check_backward_compatibility import CompatibilityError, validate_backward  # noqa: E402
from schemas.validate_topic_registry import load_registry  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--registry", type=Path, default=Path(__file__).parents[1] / "schemas" / "topic_registry.yml")
    args = parser.parse_args()

    registry = load_registry(args.registry)
    entries = [entry for entry in registry.get("topics", []) if entry.get("name") == args.topic]
    if len(entries) != 1:
        print(f"ERROR: expected one active contract for topic {args.topic!r}")
        return 1

    schema_ref = entries[0].get("schema")
    previous = (args.registry.parent.parent / schema_ref).resolve()
    if not previous.is_file():
        previous = (args.registry.parent / Path(schema_ref).name).resolve()

    try:
        validate_backward(previous, args.candidate)
    except (CompatibilityError, OSError, ValueError) as exc:
        print(f"ERROR: incompatible schema: {exc}")
        return 1
    print(f"Schema is backward-compatible with {schema_ref}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
