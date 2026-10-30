"""Persistent SQLite ledger for governed consumer event processing."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any


class EventLedgerConflict(ValueError):
    """Raised when an event ID is reused for a different event payload."""


def event_fingerprint(event: dict[str, Any]) -> str:
    """Return a stable fingerprint for the complete event envelope."""
    encoded = json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class ProcessedEventLedger:
    """Record processed events and distinguish safe replays from conflicts."""

    def __init__(self, path: Path) -> None:
        self._connection = sqlite3.connect(path)
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                event_fingerprint TEXT NOT NULL,
                processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (topic, event_id)
            )
            """
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def record(self, topic: str, event: dict[str, Any]) -> bool:
        """Record an event; return False for an identical replay."""
        event_id = event["event_id"]
        fingerprint = event_fingerprint(event)
        row = self._connection.execute(
            "SELECT event_fingerprint FROM processed_events WHERE topic = ? AND event_id = ?",
            (topic, event_id),
        ).fetchone()
        if row is not None:
            if row[0] != fingerprint:
                raise EventLedgerConflict(f"Event ID reused with a different payload: {event_id}")
            return False

        self._connection.execute(
            "INSERT INTO processed_events (topic, event_id, event_fingerprint) VALUES (?, ?, ?)",
            (topic, event_id, fingerprint),
        )
        self._connection.commit()
        return True
