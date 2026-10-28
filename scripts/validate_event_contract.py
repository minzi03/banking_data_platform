"""CLI wrapper for validating one event against the topic registry."""

import sys
from pathlib import Path


def main() -> int:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from schemas.validate_event import main as validate_main

    return validate_main()


if __name__ == "__main__":
    raise SystemExit(main())
