"""
Silver Bootstrap Job — Initial Load
====================================
Chạy tất cả Silver jobs (SCD1, SCD2, Fact) theo thứ tự dependency:
  1. Dimensions SCD1 (branch, product, employee, card, device, location)
  2. Dimensions SCD2 (customer, account)
  3. Facts (txn_account, card_txn, crm_interaction, online_transaction, support_ticket)

Thứ tự quan trọng: Facts join với Dimensions → Dimensions phải có data trước.

Usage:
  spark-submit \\
    --master spark://spark-master:7077 \\
    code_etl/silver/bootstrap/initial_load.py \\
    --cob_dt 2025-01-01
"""

import sys
import subprocess
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))

from utils.logger import get_logger

# Thứ tự chạy Silver jobs (dependency-ordered)
SILVER_JOB_ORDER = [
    # === Phase 1: Dimensions SCD1 (no dependency) ===
    {
        "name": "dim_branch",
        "type": "scd_type1",
        "config": "code_etl/silver/dims/dim_branch.yml",
    },
    {
        "name": "dim_product",
        "type": "scd_type1",
        "config": "code_etl/silver/dims/dim_product.yml",
    },
    {
        "name": "dim_employee",
        "type": "scd_type1",
        "config": "code_etl/silver/dims/dim_employee.yml",
    },
    {
        "name": "dim_card",
        "type": "scd_type1",
        "config": "code_etl/silver/dims/dim_card.yml",
    },
    {
        "name": "dim_device",
        "type": "scd_type1",
        "config": "code_etl/silver/dims/dim_device.yml",
    },
    {
        "name": "dim_location",
        "type": "scd_type1",
        "config": "code_etl/silver/dims/dim_location.yml",
    },
    # === Phase 2: Dimensions SCD2 (no dependency on other dims) ===
    {
        "name": "dim_customer",
        "type": "scd_type2",
        "config": "code_etl/silver/dims/dim_customer.yml",
    },
    {
        "name": "dim_account",
        "type": "scd_type2",
        "config": "code_etl/silver/dims/dim_account.yml",
    },
    # === Phase 3: Facts (depend on dimensions) ===
    {
        "name": "fact_txn_account",
        "type": "fact_txn",
        "config": "code_etl/silver/facts/fact_txn_account.yml",
        "depends_on": ["dim_account", "dim_customer"],
    },
    {
        "name": "fact_card_txn",
        "type": "fact_txn",
        "config": "code_etl/silver/facts/fact_card_txn.yml",
        "depends_on": ["dim_customer"],
    },
    {
        "name": "fact_crm_interaction",
        "type": "fact_txn",
        "config": "code_etl/silver/facts/fact_crm_interaction.yml",
        "depends_on": ["dim_customer"],
    },
    {
        "name": "fact_online_transaction",
        "type": "fact_txn",
        "config": "code_etl/silver/facts/fact_online_transaction.yml",
        "depends_on": ["dim_customer"],
    },
    {
        "name": "fact_support_ticket",
        "type": "fact_txn",
        "config": "code_etl/silver/facts/fact_support_ticket.yml",
        "depends_on": ["dim_customer"],
    },
]

# Map job type → Python module path
JOB_TYPE_MAP = {
    "scd_type1": "code_etl.silver.base_job.scd_type1",
    "scd_type2": "code_etl.silver.base_job.scd_type2",
    "fact_txn":  "code_etl.silver.base_job.fact_txn",
}


def parse_arguments():
    parser = argparse.ArgumentParser(description="Silver Bootstrap Initial Load")
    parser.add_argument("--cob_dt", required=True, help="Business date YYYY-MM-DD")
    parser.add_argument("--spark_submit", default="/opt/spark/bin/spark-submit",
                        help="Path to spark-submit command")
    return parser.parse_args()


def run_silver_job(job_def: dict, cob_dt: str, spark_submit: str, logger) -> bool:
    """Run a single Silver job via spark-submit."""
    name = job_def["name"]
    job_type = job_def["type"]
    config_path = job_def["config"]
    module = JOB_TYPE_MAP[job_type]

    cmd = [
        spark_submit,
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--conf", "spark.driver.memory=512m",
        "--conf", "spark.executor.memory=768m",
        f"code_etl/silver/base_job/{job_type}.py",
        "--config", config_path,
        "--cob_dt", cob_dt,
    ]

    logger.info(f"Running: {name} ({job_type})")
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
    logger.info("SILVER BOOTSTRAP — INITIAL LOAD")
    logger.info(f"Business Date: {args.cob_dt}")
    logger.info("=" * 60)

    results = {"success": [], "failed": []}

    for job_def in SILVER_JOB_ORDER:
        success = run_silver_job(job_def, args.cob_dt, args.spark_submit, logger)
        if success:
            results["success"].append(job_def["name"])
        else:
            results["failed"].append(job_def["name"])
            # Stop if a dimension fails (facts depend on it)
            if job_def["type"] != "fact_txn":
                logger.error(f"Stopping: dimension {job_def['name']} failed")
                break

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
    logger.info(f"Total: {len(results['success'])}/{len(SILVER_JOB_ORDER)} jobs succeeded")
    logger.info(f"Failed: {len(results['failed'])}")

    if results["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
