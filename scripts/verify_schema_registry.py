"""Verify a registered topic schema without modifying the registry."""

from __future__ import annotations

import argparse
import json
from urllib.request import Request, urlopen


def subject_for(topic: str) -> str:
    """Return the default value subject for a Kafka topic."""
    return f"{topic}-value"


def fetch_json(registry_url: str, path: str) -> object:
    request = Request(f"{registry_url.rstrip('/')}{path}", method="GET")
    with urlopen(request, timeout=10) as response:  # noqa: S310 - URL is operator supplied
        return json.loads(response.read().decode("utf-8"))


def verify_subject(registry_url: str, subject: str, expected_version: int) -> dict:
    versions = fetch_json(registry_url, f"/subjects/{subject}/versions")
    if not isinstance(versions, list) or not versions:
        raise ValueError(f"Subject has no registered versions: {subject}")
    if expected_version not in versions:
        raise ValueError(f"Expected version {expected_version} is absent from subject {subject}")

    registered = fetch_json(registry_url, f"/subjects/{subject}/versions/{expected_version}")
    if not isinstance(registered, dict):
        raise ValueError(f"Invalid registry response for subject {subject}")
    return registered


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--version", type=int, default=1)
    args = parser.parse_args()

    subject = subject_for(args.topic)
    try:
        response = verify_subject(args.registry_url, subject, args.version)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}")
        return 1
    print(json.dumps({"subject": subject, "version": args.version, "schema_id": response.get("id")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
