"""Shared reconciliation statuses and legacy gate compatibility."""

from __future__ import annotations

from typing import Any

RECONCILIATION_STATUSES = frozenset(
    {
        "MATCHED",
        "MISMATCH",
        "MISSING",
        "DUPLICATE",
        "PENDING",
        "AMBIGUOUS",
        "NOT_COMPARABLE",
    }
)

LEGACY_CHECK_STATUSES = frozenset({"PASS", "FAIL", "WARN"})

_OUTCOME_STATUS = {
    "PASS": "MATCHED",
    "GROSS_AMOUNT_MISMATCH": "MISMATCH",
    "TXN_COUNT_MISMATCH": "MISMATCH",
    "MISSING_TRANSACTION": "MISSING",
    "MISSING_SETTLEMENT": "MISSING",
    "DUPLICATE_SETTLEMENT_KEY": "DUPLICATE",
}

_STATUS_CHECK = {
    "MATCHED": "PASS",
    "MISMATCH": "FAIL",
    "MISSING": "FAIL",
    "DUPLICATE": "FAIL",
    "PENDING": "WARN",
    "AMBIGUOUS": "WARN",
    "NOT_COMPARABLE": "WARN",
}


def status_for_outcome(outcome: str) -> str:
    """Map a domain outcome to the shared business reconciliation status."""
    try:
        return _OUTCOME_STATUS[outcome]
    except KeyError as exc:
        raise ValueError(f"Unknown reconciliation outcome: {outcome}") from exc


def check_status_for(status: str) -> str:
    """Map a shared status to the existing PASS/FAIL/WARN gate status."""
    if status not in RECONCILIATION_STATUSES:
        raise ValueError(f"Unknown reconciliation status: {status}")
    return _STATUS_CHECK[status]


def with_reconciliation_status(result: dict[str, Any]) -> dict[str, Any]:
    """Add shared and legacy statuses without changing the domain outcome."""
    status = status_for_outcome(str(result["outcome"]))
    enriched = dict(result)
    enriched["status"] = status
    enriched["check_status"] = check_status_for(status)
    return enriched
