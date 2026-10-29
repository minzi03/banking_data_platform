"""Register the governed v1 event in the isolated E2E Schema Registry."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOPIC = "postgresql.banking.digital_banking.online_transaction"
DEFAULT_REGISTRY_URL = "http://localhost:8081"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--apply", action="store_true", help="Register after readiness verification")
    args = parser.parse_args()

    waiter = [
        sys.executable,
        str(ROOT / "scripts" / "wait_for_schema_registry.py"),
        "--registry-url",
        args.registry_url,
    ]
    ready = subprocess.run(waiter, cwd=ROOT, check=False)
    if ready.returncode != 0:
        return ready.returncode

    command = [
        sys.executable,
        str(ROOT / "scripts" / "register_event_schema.py"),
        "--registry-url",
        args.registry_url,
        "--topic",
        args.topic,
    ]
    if args.apply:
        command.append("--apply")
    result = subprocess.run(command, cwd=ROOT, check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
