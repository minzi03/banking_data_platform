"""Aggregate merchant transaction-to-settlement reconciliation.

The settlement feed is aggregate-only: compare merchant_id, settlement date and
currency rather than pretending that a transaction-level match exists.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

KEY = ("merchant_id", "settlement_date", "currency")
METRICS = ("gross_amount", "txn_count")


def _key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (int(row["merchant_id"]), str(row["settlement_date"]), str(row["currency"]).strip())


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def reconcile(
    transactions: list[dict[str, Any]],
    settlements: list[dict[str, Any]],
    *,
    amount_tolerance: Decimal = Decimal("0.00"),
    count_tolerance: int = 0,
) -> list[dict[str, Any]]:
    """Return one fail-closed result per merchant settlement key."""
    transaction_map: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in transactions:
        key = _key(row)
        transaction_map[key] = {
            "gross_amount": _decimal(row.get("gross_amount")),
            "txn_count": int(row.get("txn_count") or 0),
        }

    settlement_counts: dict[tuple[Any, ...], int] = {}
    settlement_map: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in settlements:
        key = _key(row)
        settlement_counts[key] = settlement_counts.get(key, 0) + 1
        settlement_map[key] = {
            "gross_amount": _decimal(row.get("gross_amount")),
            "txn_count": int(row.get("txn_count") or 0),
        }

    results = []
    for key in sorted(set(transaction_map) | set(settlement_map)):
        transaction = transaction_map.get(key)
        settlement = settlement_map.get(key)
        if settlement_counts.get(key, 0) > 1:
            outcome = "DUPLICATE_SETTLEMENT_KEY"
        elif transaction is None:
            outcome = "MISSING_TRANSACTION"
        elif settlement is None:
            outcome = "MISSING_SETTLEMENT"
        elif abs(transaction["gross_amount"] - settlement["gross_amount"]) > amount_tolerance:
            outcome = "GROSS_AMOUNT_MISMATCH"
        elif abs(transaction["txn_count"] - settlement["txn_count"]) > count_tolerance:
            outcome = "TXN_COUNT_MISMATCH"
        else:
            outcome = "PASS"

        results.append(
            {
                "merchant_id": key[0],
                "settlement_date": key[1],
                "currency": key[2],
                "outcome": outcome,
                "transaction": transaction,
                "settlement": settlement,
            }
        )
    return results
