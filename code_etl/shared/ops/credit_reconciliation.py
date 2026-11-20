"""Pure credit expected-vs-paid reconciliation helpers."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from ops.reconciliation_contract import check_status_for

OUTCOMES = frozenset({"MATCHED", "MISMATCH", "MISSING", "PENDING", "NOT_COMPARABLE"})
PAYMENT_STATUSES = frozenset({"PAID", "LATE", "MISSED", "PENDING"})


def _amount(value: Any, field: str) -> Decimal:
    if value is None:
        raise ValueError(f"{field} is required")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a valid decimal") from exc
    if not amount.is_finite() or amount < 0:
        raise ValueError(f"{field} must be finite and non-negative")
    return amount


def reconcile_payment_observations(
    observations: list[dict[str, Any]],
    *,
    amount_tolerance: Decimal = Decimal("0.00"),
) -> list[dict[str, Any]]:
    """Compare one or more payment observations at loan_id × cob_dt grain.

    Absence is NOT treated as a missed payment: the source must provide a
    ``MISSED`` or ``PENDING`` observation to distinguish state from no data.
    """
    if amount_tolerance < 0 or not amount_tolerance.is_finite():
        raise ValueError("amount_tolerance must be finite and non-negative")

    grouped: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in observations:
        loan_id = row.get("loan_id")
        cob_dt = row.get("cob_dt")
        payment_id = row.get("payment_id")
        status = str(row.get("payment_status") or "").upper()
        if loan_id is None or cob_dt is None or payment_id is None:
            raise ValueError("loan_id, cob_dt, and payment_id are required for each observation")
        if status not in PAYMENT_STATUSES:
            raise ValueError(f"unknown payment_status: {status or '<empty>'}")
        _amount(row.get("scheduled_amount"), "scheduled_amount")
        _amount(row.get("amount_paid"), "amount_paid")
        grouped.setdefault((int(loan_id), str(cob_dt)), []).append(row)

    results: list[dict[str, Any]] = []
    for (loan_id, cob_dt), rows in sorted(grouped.items()):
        payment_ids = {row.get("payment_id") for row in rows}
        duplicate = len(payment_ids) != len(rows)
        statuses = {str(row["payment_status"]).upper() for row in rows}
        scheduled = sum((_amount(row["scheduled_amount"], "scheduled_amount") for row in rows), Decimal("0"))
        paid = sum((_amount(row["amount_paid"], "amount_paid") for row in rows), Decimal("0"))
        delta = paid - scheduled

        if duplicate:
            outcome = "NOT_COMPARABLE"
        elif "PENDING" in statuses:
            outcome = "PENDING"
        elif statuses == {"MISSED"}:
            outcome = "MISSING"
        elif abs(delta) <= amount_tolerance:
            outcome = "MATCHED"
        else:
            outcome = "MISMATCH"

        results.append(
            {
                "loan_id": loan_id,
                "cob_dt": cob_dt,
                "outcome": outcome,
                "scheduled_amount": scheduled,
                "amount_paid": paid,
                "delta": delta,
                "tolerance": amount_tolerance,
                "status": outcome,
                "check_status": check_status_for(outcome),
                "payment_ids": sorted(str(value) for value in payment_ids if value is not None),
            }
        )
    return results
