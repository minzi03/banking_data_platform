"""
Serving Layer Bootstrap — Populate Current Snapshots
====================================================
Replaces dbt-trino: reads Gold → writes to Iceberg serving schema.
Each serving table = 1 row per customer (latest cob_dt snapshot).

This script uses Spark SQL directly (not dbt) because the Docker
environment doesn't have dbt-trino installed.

Usage:
  spark-submit \\
    --master spark://spark-master:7077 \\
    code_etl/serving/bootstrap/run_serving.py \\
    --cob_dt 2025-12-31
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))

from spark.spark_session import get_spark_session
from utils.logger import get_logger

# Serving models: (target_table, source_gold_table)
# Each is: SELECT * FROM gold.source WHERE cob_dt = target_cob_dt
# Note: Spark catalog = "lakehouse", Trino catalog = "iceberg"
SERVING_MODELS = [
    ("lakehouse.serving.mart_customer_360_current",           "lakehouse.gold.mart_customer_360"),
    ("lakehouse.serving.rfm_segment_current",                 "lakehouse.gold.rfm_segment"),
    ("lakehouse.serving.churn_prediction_current",            "lakehouse.gold.churn_prediction"),
    ("lakehouse.serving.cross_sell_segment_current",          "lakehouse.gold.cross_sell_segment"),
    ("lakehouse.serving.campaign_target_current",             "lakehouse.gold.campaign_target"),
    ("lakehouse.serving.customer_balance_summary_current",    "lakehouse.gold.customer_balance_summary"),
    ("lakehouse.serving.customer_transaction_summary_current","lakehouse.gold.customer_transaction_summary"),
    ("lakehouse.serving.customer_product_summary_current",    "lakehouse.gold.customer_product_summary"),
    ("lakehouse.serving.customer_card_summary_current",       "lakehouse.gold.customer_card_summary"),
]


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Serving Layer Bootstrap")
    parser.add_argument("--cob_dt", required=True, help="Business date YYYY-MM-DD")
    args = parser.parse_args()

    logger = get_logger(__name__)
    cob_dt = args.cob_dt

    logger.info("=" * 60)
    logger.info("SERVING BOOTSTRAP — CURRENT SNAPSHOTS")
    logger.info(f"Business Date: {cob_dt}")
    logger.info("=" * 60)

    spark = get_spark_session("serving-bootstrap")
    results = {"success": [], "failed": []}

    for target, source in SERVING_MODELS:
        target_name = target.split(".")[-1]
        try:
            logger.info(f"Loading: {target_name} from {source}")

            # Read from Gold WHERE cob_dt = target
            df = spark.sql(f"""
                SELECT * FROM {source}
                WHERE cob_dt = DATE '{cob_dt}'
            """)

            count = df.count()
            logger.info(f"  Source rows: {count}")

            if count == 0:
                logger.warning(f"  SKIPPING {target_name}: 0 rows for cob_dt={cob_dt}")
                results["failed"].append(f"{target_name} (0 rows)")
                continue

            # Write to Iceberg serving table (overwritePartitions)
            df.writeTo(target).overwritePartitions()
            logger.info(f"  ✓ {target_name}: {count:,} rows written")
            results["success"].append(target_name)

        except Exception as e:
            logger.error(f"  ✗ {target_name} FAILED: {e}")
            results["failed"].append(f"{target_name}: {e}")

    spark.stop()

    # Summary
    logger.info("=" * 60)
    logger.info("SERVING LOAD SUMMARY")
    logger.info("=" * 60)
    for name in results["success"]:
        logger.info(f"  ✓ {name}")
    if results["failed"]:
        logger.info("")
        for name in results["failed"]:
            logger.info(f"  ✗ {name}")

    logger.info("")
    logger.info(f"Total: {len(results['success'])}/{len(SERVING_MODELS)} tables loaded")
    logger.info(f"Failed: {len(results['failed'])}")

    if results["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
