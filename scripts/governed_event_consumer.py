"""Run a governed event batch with post-side-effect replay tracking."""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.deduplicate_governed_events import deduplicate_events
from scripts.processed_event_ledger import ProcessedEventLedger


def consume_batch(
    events: list[dict[str, Any]],
    topic: str,
    registry: Path,
    side_effect: Callable[[list[dict[str, Any]]], None],
    ledger: ProcessedEventLedger | None = None,
) -> int:
    """Apply one validated batch and record it only after side effects succeed."""
    pending = deduplicate_events(events, topic, registry, ledger)
    if not pending:
        return 0

    side_effect(pending)
    if ledger is not None:
        ledger.record_many(topic, pending)
    return len(pending)


def main() -> int:
    """This module is a library boundary; use deduplicate_governed_events for the CLI."""
    print("Use deduplicate_governed_events.py for the command-line consumer", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
