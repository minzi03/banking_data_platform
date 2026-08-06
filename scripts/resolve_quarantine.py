#!/usr/bin/env python3
"""
Resolve Quarantine Violations — Business Rule Violations

Handles violations found in quarantine tables by:
1. Transferring balance from closed accounts to original account
2. Updating account status
3. Logging resolution in quarantine_log

Usage:
    spark-submit --master spark://spark-master:7077 \
        resolve_quarantine.py --cob_dt 2025-01-01 --dry_run

    spark-submit --master spark://spark-master:7077 \
        resolve_quarantine.py --cob_dt 2025-01-01 --execute
"""

import argparse
import os
import sys
from datetime import datetime
from logging import basicConfig, getLogger, INFO
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "code_etl", "shared"))

from spark.spark_session import get_spark_session

basicConfig(
    level=INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = getLogger("resolve_quarantine")


# ---------------------------------------------------------------------------
# Resolution Functions
# ---------------------------------------------------------------------------

def resolve_closed_accounts(spark, dry_run: bool = True) -> Dict:
    """
    Resolve closed accounts with balance.

    Strategy: Transfer balance to original account or set balance to 0
    for closed accounts.

    Args:
        spark: SparkSession
        dry_run: If True, only log actions without executing

    Returns:
        Dict with resolution results
    """
    results = {
        "action": "resolve_closed_accounts",
        "total_accounts": 0,
        "resolved_accounts": 0,
        "total_balance": 0,
        "details": []
    }

    try:
        # Get closed accounts with balance
        df = spark.table("lakehouse.quarantine.invalid_account")
        closed_df = df.filter(
            (df["violation_type"] == "closed_with_balance") &
            (df["status"] == "CLOSED") &
            (df["balance"] > 0)
        )

        total_accounts = closed_df.count()
        results["total_accounts"] = total_accounts

        if total_accounts == 0:
            log.info("No closed accounts with balance found")
            return results

        # Get total balance
        balance_df = closed_df.select("balance").collect()
        total_balance = sum(row["balance"] for row in balance_df)
        results["total_balance"] = total_balance

        log.info(f"Found {total_accounts} closed accounts with total balance: {total_balance:,.0f} VND")

        if dry_run:
            log.info("[DRY RUN] Would resolve these accounts")
            results["status"] = "DRY_RUN"
        else:
            # Strategy: Set balance to 0 for closed accounts
            # In real banking, this would transfer to original account
            log.info("Executing resolution...")

            # Update dim_account to set balance to 0
            update_sql = """
                MERGE INTO lakehouse.silver.dim_account AS target
                USING (
                    SELECT account_id
                    FROM lakehouse.quarantine.invalid_account
                    WHERE violation_type = 'closed_with_balance'
                    AND status = 'CLOSED'
                    AND balance > 0
                ) AS source
                ON target.account_id = source.account_id
                WHEN MATCHED THEN
                    UPDATE SET balance = 0, status = 'CLOSED_RESOLVED'
            """

            spark.sql(update_sql)
            results["resolved_accounts"] = total_accounts
            results["status"] = "EXECUTED"

            log.info(f"Resolved {total_accounts} closed accounts")

        return results

    except Exception as e:
        log.error(f"Error resolving closed accounts: {e}")
        results["status"] = "FAILED"
        results["error"] = str(e)
        return results


def log_resolution(spark, results: Dict, dry_run: bool = True) -> None:
    """
    Log resolution results to quarantine_log.

    Args:
        spark: SparkSession
        results: Resolution results
        dry_run: If True, only log actions without executing
    """
    try:
        from pyspark.sql import Row
        from datetime import datetime

        # Create resolution record
        resolution_record = {
            "quarantine_id": 1,
            "source_table": "lakehouse.silver.dim_account",
            "source_id": 0,
            "violation_type": "closed_with_balance",
            "violation_severity": "FAIL",
            "violation_detail": f"Resolved {results.get('total_accounts', 0)} accounts with balance {results.get('total_balance', 0):,.0f} VND",
            "detected_at": datetime.now(),
            "resolved_at": datetime.now(),
            "resolution_status": "RESOLVED",
            "resolved_by": "system"
        }

        if dry_run:
            log.info("[DRY RUN] Would log resolution to quarantine_log")
            log.info(f"Resolution details: {resolution_record}")
        else:
            # Write to quarantine_log
            df = spark.createDataFrame([Row(**resolution_record)])
            df.write.format("iceberg").mode("append").saveAsTable("lakehouse.quarantine.quarantine_log")
            log.info("Resolution logged to quarantine_log")

    except Exception as e:
        log.error(f"Error logging resolution: {e}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(results: Dict) -> None:
    """Print resolution summary."""
    log.info("=" * 60)
    log.info("RESOLUTION SUMMARY")
    log.info("=" * 60)
    log.info(f"  Action: {results.get('action', 'N/A')}")
    log.info(f"  Status: {results.get('status', 'N/A')}")
    log.info(f"  Total Accounts: {results.get('total_accounts', 0)}")
    log.info(f"  Resolved Accounts: {results.get('resolved_accounts', 0)}")
    log.info(f"  Total Balance: {results.get('total_balance', 0):,.0f} VND")

    if results.get("error"):
        log.error(f"  Error: {results['error']}")

    log.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Resolve Quarantine Violations")
    parser.add_argument("--cob_dt", required=True, help="Business date (YYYY-MM-DD)")
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Only log actions without executing"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute resolution"
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    log.info(f"Resolve Quarantine Violations — cob_dt={args.cob_dt}")
    log.info(f"Mode: {'DRY RUN' if args.dry_run else 'EXECUTE'}")

    # Create Spark session
    spark = get_spark_session("ResolveQuarantine")

    # Resolve closed accounts
    results = resolve_closed_accounts(spark, dry_run=args.dry_run)

    # Log resolution
    log_resolution(spark, results, dry_run=args.dry_run)

    # Summary
    print_summary(results)


if __name__ == "__main__":
    main()
