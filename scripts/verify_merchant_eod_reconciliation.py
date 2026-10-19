"""Verify a shared Merchant QR/POS EOD cohort across source, batch, and CDC current.

This verifier is intentionally read-only. It does not emit QR producer events, mutate
Postgres, create Kafka connectors, or delete stack resources. It reports ``ENV`` when
the CDC current-state table is absent or does not contain the requested cohort.

Usage::

    python scripts/verify_merchant_eod_reconciliation.py --cob-dt 2026-09-20
    python scripts/verify_merchant_eod_reconciliation.py --cob-dt 2026-09-20 --json

The cohort is merchant_id + QR/POS + ICT business date + currency. SUCCESS rows
contribute count and gross amount; refund_flag is reported as a separate count.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date
from decimal import Decimal
from typing import Any

SPARK_CONTAINER = "banking-spark-worker-1"
POSTGRES_CONTAINER = "banking-postgres"
BATCH_TABLE = "lakehouse.silver.fact_online_transaction"
DEFAULT_CURRENT_TABLE = "lakehouse.silver.fact_online_transaction_current"


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


def postgres_sql(sql: str) -> str:
    user = os.environ.get("POSTGRES_USER", "banking_admin")
    database = os.environ.get("POSTGRES_DB", "banking_db")
    return docker_exec(POSTGRES_CONTAINER, ["psql", "-U", user, "-d", database, "-At", "-c", sql])


def parse_aggregate(output: str) -> dict[tuple[int, str, str], dict[str, Decimal]]:
    result: dict[tuple[int, str, str], dict[str, Decimal]] = {}
    for line in output.splitlines():
        parts = line.strip().split("\t")
        if len(parts) != 6 or not parts[0].isdigit():
            continue
        merchant_id, channel, currency, success_count, gross_amount, refund_count = parts
        result[(int(merchant_id), channel, currency.strip())] = {
            "success_count": Decimal(success_count or "0"),
            "gross_amount": Decimal(gross_amount or "0"),
            "refund_count": Decimal(refund_count or "0"),
        }
    return result


def source_query(cob_dt: str) -> str:
    return f"""
SELECT merchant_id, payment_channel, currency,
       COUNT(*) FILTER (WHERE status = 'SUCCESS'),
       COALESCE(SUM(amount) FILTER (WHERE status = 'SUCCESS'), 0),
       COUNT(*) FILTER (WHERE status = 'SUCCESS' AND refund_flag = 1)
FROM digital_banking.online_transaction
WHERE merchant_id IS NOT NULL
  AND payment_channel IN ('QR', 'POS')
  AND (timezone('Asia/Ho_Chi_Minh', timezone('UTC', transaction_date)))::date = DATE '{cob_dt}'
GROUP BY merchant_id, payment_channel, currency
ORDER BY merchant_id, payment_channel, currency
"""


def spark_query(table: str, cob_dt: str, *, epoch_micros: bool = False) -> str:
    timestamp = (
        "CAST(from_utc_timestamp(timestamp_micros(CAST(transaction_date AS BIGINT)), 'Asia/Ho_Chi_Minh') AS DATE)"
        if epoch_micros
        else "CAST(from_utc_timestamp(transaction_date, 'Asia/Ho_Chi_Minh') AS DATE)"
    )
    return f"""
SELECT merchant_id, payment_channel, currency,
       COUNT(CASE WHEN status = 'SUCCESS' THEN transaction_id END),
       COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END), 0),
       COUNT(CASE WHEN status = 'SUCCESS' AND refund_flag = 1 THEN transaction_id END)
FROM {table}
WHERE merchant_id IS NOT NULL
  AND payment_channel IN ('QR', 'POS')
  AND {timestamp} = DATE '{cob_dt}'
GROUP BY merchant_id, payment_channel, currency
ORDER BY merchant_id, payment_channel, currency
"""


def compare(source: dict, batch: dict, current: dict) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    keys = sorted(set(source) | set(batch) | set(current))
    for key in keys:
        for metric in ("success_count", "gross_amount", "refund_count"):
            values = {
                name: data.get(key, {}).get(metric, Decimal("0"))
                for name, data in (("source", source), ("batch", batch), ("current", current))
            }
            ok = values["source"] == values["batch"] == values["current"]
            findings.append(
                {
                    "key": {"merchant_id": key[0], "payment_channel": key[1], "currency": key[2]},
                    "metric": metric,
                    "source": str(values["source"]),
                    "batch": str(values["batch"]),
                    "current": str(values["current"]),
                    "status": "PASS" if ok else "FAIL",
                }
            )
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only Merchant QR/POS EOD three-way verification")
    parser.add_argument("--cob-dt", required=True, help="ICT business date YYYY-MM-DD")
    parser.add_argument("--current-table", default=DEFAULT_CURRENT_TABLE)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    date.fromisoformat(args.cob_dt)

    result: dict[str, Any] = {
        "test": "merchant_eod_three_way_read_only",
        "cob_dt": args.cob_dt,
        "cohort": "merchant_id × payment_channel(QR/POS) × currency × ICT date",
        "checks": [],
    }
    try:
        source = parse_aggregate(postgres_sql(source_query(args.cob_dt)))
        batch = parse_aggregate(spark_sql(spark_query(BATCH_TABLE, args.cob_dt)))
        current = parse_aggregate(spark_sql(spark_query(args.current_table, args.cob_dt, epoch_micros=True)))
        result["source_groups"] = len(source)
        result["batch_groups"] = len(batch)
        result["current_groups"] = len(current)
        result["checks"] = compare(source, batch, current)
        passed = bool(result["checks"]) and all(check["status"] == "PASS" for check in result["checks"])
        result["tree"] = "PASS" if passed else "FAIL"
        code = 0 if passed else 1
    except (VerificationError, ValueError) as exc:
        result["tree"] = "ENV"
        result["error"] = str(exc)
        code = 2

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"KẾT QUẢ: {result['tree']}")
        if "error" in result:
            print(result["error"])
        for check in result.get("checks", []):
            print(check)
    return code


if __name__ == "__main__":
    sys.exit(main())
