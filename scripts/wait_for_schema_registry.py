"""Wait for an isolated Schema Registry and optionally verify a subject."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Suốt thời gian chạy như một script, thư mục `scripts/` được đưa lên sys.path
# chứ không phải repo root; chèn root trước khi import để `scripts.*` resolve được.
from scripts.verify_schema_registry import fetch_json, subject_for, verify_subject  # noqa: E402


def wait_for_registry(registry_url: str, timeout_seconds: int) -> None:
    """Wait until the registry health endpoint responds with a subject list."""
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            subjects = fetch_json(registry_url, "/subjects")
            if isinstance(subjects, list):
                return
            last_error = ValueError("Schema Registry returned a non-list subject response")
        except (OSError, ValueError) as exc:
            last_error = exc
        time.sleep(2)
    raise TimeoutError(f"Schema Registry not ready after {timeout_seconds}s: {last_error}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--topic")
    parser.add_argument("--version", type=int, default=1)
    args = parser.parse_args()

    try:
        wait_for_registry(args.registry_url, args.timeout_seconds)
        if args.topic:
            subject = subject_for(args.topic)
            result = verify_subject(args.registry_url, subject, args.version)
            print(f"Schema Registry ready: {subject} v{result.get('version', args.version)}")
        else:
            print("Schema Registry ready")
    except (OSError, ValueError, TimeoutError) as exc:
        print(f"ERROR: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
