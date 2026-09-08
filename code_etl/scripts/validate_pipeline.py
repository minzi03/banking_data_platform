#!/usr/bin/env python3
"""Validate layer counts, snapshot alignment, and customer grain.

Run inside the Spark worker after a pipeline run:
  spark-submit code_etl/scripts/validate_pipeline.py --cob_dt 2025-12-31

The script exits non-zero on any invariant violation so it can be used by CI
or an Airflow validation task.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "shared"))

from spark.spark_session import get_spark_session


EXPECTED = {
    "lakehouse.bronze.core_customer": 10000,
    "lakehouse.bronze.core_account": 30000,
    "lakehouse.bronze.core_txn_account": 1200000,
    "lakehouse.bronze.core_card_txn": 600000,
    "lakehouse.bronze.core_online_transaction": 500000,
    "lakehouse.silver.dim_customer": 10000,
    "lakehouse.silver.dim_account": 30000,
    "lakehouse.silver.fact_txn_account": 1200000,
    "lakehouse.silver.fact_card_txn": 600000,
    "lakehouse.silver.fact_online_transaction": 500000,
    "lakehouse.gold.mart_customer_360": 10000,
    "lakehouse.gold.rfm_segment": 10000,
    "lakehouse.gold.churn_prediction": 10000,
}

PARTITIONED = {
    table for table in EXPECTED if ".bronze." in table or ".fact_" in table or ".gold." in table
}


def main():
    parser = argparse.ArgumentParser(description="Validate Banking Data Platform")
    parser.add_argument("--cob_dt", required=True)
    args = parser.parse_args()

    spark = get_spark_session("pipeline-validation")
    failures = []

    try:
        for table, expected in EXPECTED.items():
            condition = ""
            if table in PARTITIONED:
                condition = f" WHERE cob_dt = DATE '{args.cob_dt}'"
            elif table.endswith("dim_customer") or table.endswith("dim_account"):
                condition = " WHERE is_current = 1"

            try:
                row = spark.sql(f"SELECT COUNT(*) FROM {table}{condition}").first()
                actual = int(row[0])
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{table}: query failed: {exc}")
                continue

            print(f"{table}: actual={actual}, expected={expected}")
            if actual != expected:
                failures.append(f"{table}: expected {expected}, got {actual}")

        # Grain checks: one current row per customer in dimensions and Gold.
        grain_checks = [
            ("lakehouse.silver.dim_customer", "is_current = 1"),
            ("lakehouse.silver.dim_account", "is_current = 1"),
            ("lakehouse.gold.mart_customer_360", f"cob_dt = DATE '{args.cob_dt}'"),
            ("lakehouse.gold.rfm_segment", f"cob_dt = DATE '{args.cob_dt}'"),
            ("lakehouse.gold.churn_prediction", f"cob_dt = DATE '{args.cob_dt}'"),
        ]
        for table, predicate in grain_checks:
            key = "account_id" if table.endswith("dim_account") else "customer_id"
            duplicate = spark.sql(
                f"SELECT {key} FROM {table} WHERE {predicate} "
                f"GROUP BY {key} HAVING COUNT(*) > 1 LIMIT 1"
            ).take(1)
            if duplicate:
                failures.append(f"{table}: duplicate current {key}")

        if failures:
            print("VALIDATION FAILED")
            for failure in failures:
                print(f" - {failure}")
            return 1

        print("VALIDATION PASSED")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
