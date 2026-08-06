"""
Gold Bootstrap Job — Initial Load
===================================
Chạy tất cả Gold jobs theo thứ tự dependency:
  Phase 1: mart360 + segment (trừ campaign_target)
  Phase 2: campaign_target (phụ thuộc rfm, churn, cross_sell, mart360)

Usage:
  spark-submit \\
    --master spark://spark-master:7077 \\
    code_etl/gold/bootstrap/initial_load.py \\
    --cob_dt 2025-01-01
"""

import sys
import subprocess
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))

from utils.logger import get_logger

# Thứ tự chạy Gold jobs (dependency-ordered)
GOLD_JOB_ORDER = [
    # === Phase 1: Independent Gold jobs (parallel-safe) ===
    {
        "name": "mart_customer_360",
        "type": "mart360",
        "config": "code_etl/gold/mart360/customer_360.yml",
    },
    {
        "name": "customer_balance_summary",
        "type": "mart360",
        "config": "code_etl/gold/mart360/customer_balance_summary.yml",
    },
    {
        "name": "customer_transaction_summary",
        "type": "mart360",
        "config": "code_etl/gold/mart360/customer_transaction_summary.yml",
    },
    {
        "name": "customer_product_summary",
        "type": "mart360",
        "config": "code_etl/gold/mart360/customer_product_summary.yml",
    },
    {
        "name": "customer_card_summary",
        "type": "mart360",
        "config": "code_etl/gold/mart360/customer_card_summary.yml",
    },
    {
        "name": "rfm_segment",
        "type": "segment",
        "config": "code_etl/gold/segmentation/rfm_segment.yml",
    },
    {
        "name": "churn_prediction",
        "type": "segment",
        "config": "code_etl/gold/segmentation/churn_prediction.yml",
    },
    {
        "name": "cross_sell_segment",
        "type": "segment",
        "config": "code_etl/gold/segmentation/cross_sell_segment.yml",
    },
    {
        "name": "branch_monthly_summary",
        "type": "time_analytics",
        "config": "code_etl/gold/time_analytics/branch_monthly_summary.yml",
    },
    # === Phase 2: Depends on Phase 1 outputs ===
    {
        "name": "campaign_target",
        "type": "segment",
        "config": "code_etl/gold/segmentation/campaign_target.yml",
        "depends_on": ["rfm_segment", "churn_prediction", "cross_sell_segment", "mart_customer_360"],
    },
]

# Map job type → Python module path
JOB_TYPE_MAP = {
    "mart360":        "code_etl.gold.base_job.gold_job",
    "segment":        "code_etl.gold.base_job.gold_job",
    "time_analytics": "code_etl.gold.base_job.gold_job",
}


def parse_arguments():
    parser = argparse.ArgumentParser(description="Gold Bootstrap Initial Load")
    parser.add_argument("--cob_dt", required=True, help="Business date YYYY-MM-DD")
    parser.add_argument("--spark_submit", default="spark-submit",
                        help="Path to spark-submit command")
    return parser.parse_args()


def run_gold_job(job_def: dict, cob_dt: str, spark_submit: str, logger) -> bool:
    """Run a single Gold job via spark-submit."""
    name = job_def["name"]
    config_path = job_def["config"]
    job_type = job_def["type"]

    cmd = [
        spark_submit,
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--conf", "spark.driver.memory=512m",
        "--conf", "spark.executor.memory=768m",
        f"code_etl/gold/base_job/gold_job.py",
        "--config", config_path,
        "--cob_dt", cob_dt,
    ]

    logger.info(f"Running: {name} ({job_def['type']})")
    logger.info(f"  Config: {config_path}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            logger.info(f"  ✓ {name} completed successfully")
            return True
        else:
            logger.error(f"  ✗ {name} FAILED (exit code {result.returncode})")
            logger.error(f"  stderr: {result.stderr[-500:]}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"  ✗ {name} TIMEOUT (600s)")
        return False
    except Exception as e:
        logger.error(f"  ✗ {name} ERROR: {str(e)}")
        return False


def main():
    args = parse_arguments()
    logger = get_logger(__name__)

    logger.info("=" * 60)
    logger.info("GOLD BOOTSTRAP — INITIAL LOAD")
    logger.info(f"Business Date: {args.cob_dt}")
    logger.info("=" * 60)

    results = {"success": [], "failed": []}

    for job_def in GOLD_JOB_ORDER:
        # Check if dependencies are met
        deps = job_def.get("depends_on", [])
        if deps:
            unmet = [d for d in deps if d not in results["success"]]
            if unmet:
                logger.warning(f"Skipping {job_def['name']}: unmet dependencies {unmet}")
                results["failed"].append(f"{job_def['name']} (deps: {unmet})")
                continue

        success = run_gold_job(job_def, args.cob_dt, args.spark_submit, logger)
        if success:
            results["success"].append(job_def["name"])
        else:
            results["failed"].append(job_def["name"])

    # Summary
    logger.info("=" * 60)
    logger.info("LOAD SUMMARY")
    logger.info("=" * 60)

    for name in results["success"]:
        logger.info(f"  ✓ {name}")

    if results["failed"]:
        logger.info("")
        for name in results["failed"]:
            logger.info(f"  ✗ {name}")

    logger.info("")
    logger.info(f"Total: {len(results['success'])}/{len(GOLD_JOB_ORDER)} jobs succeeded")
    logger.info(f"Failed: {len(results['failed'])}")

    if results["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
