"""Pure credit expected-vs-paid reconciliation helpers."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

OUTCOMES = frozenset({"MATCHED", "MISMATCH", "MISSING", "PENDING", "NOT_COMPARABLE"})


def _amount(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def reconcile_payment_observations(
    observations: list[dict[str, Any]],
    *,
    amount_tolerance: Decimal = Decimal("0.00"),
) -> list[dict[str, Any]]:
    """Compare one or more payment observations at loan_id × cob_dt grain.

    Absence is NOT treated as a missed payment: the source must provide a
    ``MISSED`` or ``PENDING`` observation to distinguish state from no data.
    """
    grouped: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in observations:
        loan_id = row.get("loan_id")
        cob_dt = row.get("cob_dt")
        if loan_id is None or cob_dt is None:
            continue
        grouped.setdefault((int(loan_id), str(cob_dt)), []).append(row)

    results: list[dict[str, Any]] = []
    for (loan_id, cob_dt), rows in sorted(grouped.items()):
        payment_ids = {row.get("payment_id") for row in rows}
        duplicate = len(payment_ids) != len(rows)
        statuses = {str(row.get("payment_status") or "").upper() for row in rows}
        scheduled = sum((_amount(row.get("scheduled_amount")) for row in rows), Decimal("0"))
        paid = sum((_amount(row.get("amount_paid")) for row in rows), Decimal("0"))
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
                "payment_ids": sorted(str(value) for value in payment_ids if value is not None),
            }
        )
    return results
