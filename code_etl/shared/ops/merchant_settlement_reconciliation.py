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


def _transaction_sql(table: str, cob_dt: str) -> str:
    return f"""
        SELECT merchant_id,
               CAST(from_utc_timestamp(transaction_date, 'Asia/Ho_Chi_Minh') AS DATE) AS settlement_date,
               currency,
               COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END), 0) AS gross_amount,
               COUNT(CASE WHEN status = 'SUCCESS' THEN transaction_id END) AS txn_count
        FROM {table}
        WHERE merchant_id IS NOT NULL
          AND payment_channel IN ('QR', 'POS')
          AND CAST(from_utc_timestamp(transaction_date, 'Asia/Ho_Chi_Minh') AS DATE) = DATE '{cob_dt}'
        GROUP BY merchant_id,
                 CAST(from_utc_timestamp(transaction_date, 'Asia/Ho_Chi_Minh') AS DATE),
                 currency
    """


def _settlement_sql(table: str, cob_dt: str) -> str:
    return f"""
        SELECT merchant_id, CAST(settlement_date AS DATE) AS settlement_date, currency,
               gross_amount, txn_count
        FROM {table}
        WHERE CAST(settlement_date AS DATE) = DATE '{cob_dt}'
          AND status = 'SETTLED'
    """


def reconcile_spark(
    spark,
    cob_dt: str,
    *,
    transaction_table: str = "lakehouse.silver.fact_online_transaction",
    settlement_table: str = "lakehouse.bronze.pos_settlement",
    amount_tolerance: Decimal = Decimal("0.00"),
    count_tolerance: int = 0,
) -> list[dict[str, Any]]:
    """Read batch and SFTP Bronze aggregates, then return reconciliation rows."""
    transactions = [row.asDict() for row in spark.sql(_transaction_sql(transaction_table, cob_dt)).collect()]
    settlements = [row.asDict() for row in spark.sql(_settlement_sql(settlement_table, cob_dt)).collect()]
    return reconcile(
        transactions,
        settlements,
        amount_tolerance=amount_tolerance,
        count_tolerance=count_tolerance,
    )


def to_reconciliation_log_rows(results: list[dict[str, Any]], cob_dt: str, tolerance: Decimal) -> list[dict[str, Any]]:
    """Adapt domain outcomes to the existing opslakehouse reconciliation schema."""
    rows = []
    for result in results:
        transaction = result["transaction"] or {}
        settlement = result["settlement"] or {}
        source_amount = Decimal(str(transaction.get("gross_amount", 0)))
        settled_amount = Decimal(str(settlement.get("gross_amount", 0)))
        rows.append(
            {
                "metric_name": f"merchant_settlement.{result['outcome'].lower()}",
                "entity": f"merchant:{result['merchant_id']}:{result['settlement_date']}:{result['currency']}",
                "stream_value": float(settled_amount),
                "batch_value": float(source_amount),
                "delta": float(settled_amount - source_amount),
                "tolerance": float(tolerance),
                "check_status": "PASS" if result["outcome"] == "PASS" else "FAIL",
                "details": f"outcome={result['outcome']} transaction={transaction} settlement={settlement}",
                "cob_dt": cob_dt,
            }
        )
    return rows
