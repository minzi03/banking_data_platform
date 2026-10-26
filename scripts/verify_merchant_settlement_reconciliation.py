"""Verify the Merchant transaction-to-settlement cohort on the isolated TD-22 stack.

This verifier is read-only. It compares the batch transaction aggregate (from
lakehouse.silver.fact_online_transaction) against the settlement aggregate
(lakehouse.bronze.pos_settlement, seeded directly in the isolated stack instead
of arriving over SFTP). It exits non-zero when the aggregates disagree.

Usage::

    python scripts/verify_merchant_settlement_reconciliation.py \
        --postgres-container merchant-e2e-postgres \
        --postgres-user merchant_e2e \
        --postgres-db merchant_e2e \
        --spark-container merchant-e2e-spark-worker \
        --cob-dt 2026-10-18
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from decimal import Decimal
from typing import Any

POSTGRES_CONTAINER = "merchant-e2e-postgres"
POSTGRES_USER = "merchant_e2e"
POSTGRES_DB = "merchant_e2e"
SPARK_CONTAINER = "merchant-e2e-spark-worker"
TRANSACTION_TABLE = "lakehouse.silver.fact_online_transaction"
SETTLEMENT_TABLE = "lakehouse.bronze.pos_settlement"


class VerificationError(RuntimeError):
    """Raised when a read-only verification query cannot complete."""


def run(cmd: list[str], *, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, check=False
    )


def docker_exec(container: str, args: list[str], *, timeout: int = 300) -> str:
    result = run(["docker", "exec", container, *args], timeout=timeout)
    if result.returncode:
        raise VerificationError(result.stderr[-2000:] or f"docker exec failed: {container}")
    return result.stdout


def spark_sql(sql: str) -> str:
    return docker_exec(
        SPARK_CONTAINER,
        [
            "/opt/spark/bin/spark-sql",
            "--master",
            "spark://spark-master:7077",
            "--conf",
            "spark.ui.enabled=false",
            "-e",
            sql,
        ],
    )


def postgres_sql(sql: str, container: str, user: str, database: str) -> str:
    return docker_exec(container, ["psql", "-U", user, "-d", database, "-At", "-F", "\t", "-c", sql])


def parse_keyed_aggregate(output: str) -> dict[tuple[int, str, str], dict[str, Decimal]]:
    result: dict[tuple[int, str, str], dict[str, Decimal]] = {}
    for line in output.splitlines():
        parts = line.strip().split("\t")
        if len(parts) != 4 or not parts[0].isdigit():
            continue
        merchant_id, currency, gross_amount, txn_count = parts
        result[(int(merchant_id), "VND", currency.strip())] = {
            "gross_amount": Decimal(gross_amount or "0"),
            "txn_count": Decimal(txn_count or "0"),
        }
    return result


def transaction_query(cob_dt: str) -> str:
    return f"""
SELECT merchant_id, currency,
       COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END), 0),
       COUNT(CASE WHEN status = 'SUCCESS' THEN transaction_id END)
FROM {TRANSACTION_TABLE}
WHERE merchant_id IS NOT NULL
  AND payment_channel IN ('QR', 'POS')
  AND CAST(from_utc_timestamp(transaction_date, 'Asia/Ho_Chi_Minh') AS DATE) = DATE '{cob_dt}'
GROUP BY merchant_id, currency
ORDER BY merchant_id, currency
"""


def settlement_query(cob_dt: str) -> str:
    return f"""
SELECT merchant_id, currency, gross_amount, txn_count
FROM {SETTLEMENT_TABLE}
WHERE CAST(settlement_date AS DATE) = DATE '{cob_dt}'
  AND status = 'SETTLED'
ORDER BY merchant_id, currency
"""


def compare(
    source: dict[tuple[int, str, str], dict[str, Decimal]],
    settlement: dict[tuple[int, str, str], dict[str, Decimal]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in sorted(set(source) | set(settlement)):
        txn = source.get(key, {"gross_amount": Decimal("0"), "txn_count": Decimal("0")})
        stl = settlement.get(key, {"gross_amount": Decimal("0"), "txn_count": Decimal("0")})
        if key not in source:
            outcome = "MISSING_TRANSACTION"
        elif key not in settlement:
            outcome = "MISSING_SETTLEMENT"
        elif abs(txn["gross_amount"] - stl["gross_amount"]) > Decimal("0.00"):
            outcome = "GROSS_AMOUNT_MISMATCH"
        elif abs(txn["txn_count"] - stl["txn_count"]) > 0:
            outcome = "TXN_COUNT_MISMATCH"
        else:
            outcome = "PASS"
        rows.append(
            {
                "merchant_id": key[0],
                "currency": key[2],
                "outcome": outcome,
                "transaction_gross": txn["gross_amount"],
                "settlement_gross": stl["gross_amount"],
                "transaction_count": txn["txn_count"],
                "settlement_count": stl["txn_count"],
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--postgres-container", default=POSTGRES_CONTAINER)
    parser.add_argument("--postgres-user", default=POSTGRES_USER)
    parser.add_argument("--postgres-db", default=POSTGRES_DB)
    parser.add_argument("--spark-container", default=SPARK_CONTAINER)
    parser.add_argument("--cob-dt", required=True)
    args = parser.parse_args()

    try:
        source = parse_keyed_aggregate(spark_sql(transaction_query(args.cob_dt)))
        settlement = parse_keyed_aggregate(
            postgres_sql(
                settlement_query(args.cob_dt),
                args.postgres_container,
                args.postgres_user,
                args.postgres_db,
            )
        )
    except VerificationError as exc:
        print(f"ENV: verification failed to read isolated sources: {exc}")
        return 2

    rows = compare(source, settlement)
    all_pass = all(row["outcome"] == "PASS" for row in rows)
    for row in rows:
        print(
            f"{row['merchant_id']}\t{row['currency']}\t{row['outcome']}\t"
            f"txn_gross={row['transaction_gross']}\tstl_gross={row['settlement_gross']}\t"
            f"txn_count={row['transaction_count']}\tstl_count={row['settlement_count']}"
        )
    print(f"SETTLEMENT_RECONCILIATION_PASS={all_pass}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
