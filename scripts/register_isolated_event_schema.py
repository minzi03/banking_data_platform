"""Register the governed v1 event in the isolated E2E Schema Registry."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOPIC = "postgresql.banking.digital_banking.online_transaction"
DEFAULT_REGISTRY_URL = "http://localhost:8081"


def _run(command: list[str]) -> int:
    return subprocess.run(command, cwd=ROOT, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--apply", action="store_true", help="Register after readiness verification")
    parser.add_argument("--verify", action="store_true", help="Verify the version after registration")
    parser.add_argument("--version", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args()

    if args.verify and not args.apply:
        print("ERROR: --verify requires --apply so a registered version can be checked")
        return 2

    python = sys.executable
    ready = _run(
        [
            python,
            str(ROOT / "scripts" / "wait_for_schema_registry.py"),
            "--registry-url",
            args.registry_url,
            "--timeout-seconds",
            str(args.timeout_seconds),
        ]
    )
    if ready != 0:
        return ready

    register_command = [
        python,
        str(ROOT / "scripts" / "register_event_schema.py"),
        "--registry-url",
        args.registry_url,
        "--topic",
        args.topic,
    ]
    if args.apply:
        register_command.append("--apply")
    registered = _run(register_command)
    if registered != 0 or not args.verify:
        return registered

    return _run(
        [
            python,
            str(ROOT / "scripts" / "verify_schema_registry.py"),
            "--registry-url",
            args.registry_url,
            "--topic",
            args.topic,
            "--version",
            str(args.version),
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
