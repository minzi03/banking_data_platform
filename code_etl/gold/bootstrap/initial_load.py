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
    --cob_dt 2025-01-01 [--in-process]

--in-process: mọi job chạy trong CHÍNH app này (mỗi job một session con) thay vì một
spark-submit mỗi job — bỏ ~20 s khởi động mỗi job. Xem code_etl/shared/spark/in_process.py.
"""

import argparse
import subprocess
import sys
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
    {
        # Đọc silver.fact_loan_payment + silver.dim_loan, không phụ thuộc Gold khác.
        "name": "customer_loan_summary",
        "type": "mart360",
        "config": "code_etl/gold/mart360/customer_loan_summary.yml",
    },
    {
        "name": "loan_portfolio_risk",
        "type": "risk",
        "config": "code_etl/gold/risk/loan_portfolio_risk.yml",
    },
    {
        "name": "fraud_risk_txn",
        "type": "risk",
        "config": "code_etl/gold/risk/fraud_risk_txn.yml",
    },
    {
        "name": "aml_monitoring",
        "type": "risk",
        "config": "code_etl/gold/risk/aml_monitoring.yml",
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
    "mart360": "code_etl.gold.base_job.gold_job",
    "segment": "code_etl.gold.base_job.gold_job",
    "time_analytics": "code_etl.gold.base_job.gold_job",
    "risk": "code_etl.gold.base_job.gold_job",
}

# --in-process: mọi loại job Gold dùng chung gold_job.run_gold_job.
GOLD_JOB_FILE = Path(__file__).resolve().parent.parent / "base_job" / "gold_job.py"


def parse_arguments():
    parser = argparse.ArgumentParser(description="Gold Bootstrap Initial Load")
    parser.add_argument("--cob_dt", required=True, help="Business date YYYY-MM-DD")
    # Đường dẫn TUYỆT ĐỐI, khớp với silver/bootstrap/initial_load.py. Tên trần
    # "spark-submit" phụ thuộc PATH của tiến trình con, mà /opt/spark/bin không
    # có trong PATH khi vào container qua `docker exec` — CI đo được:
    # `[Errno 2] No such file or directory: 'spark-submit'`, 0/10 Gold job chạy,
    # trong khi Silver (vốn đã dùng đường dẫn tuyệt đối) chạy 13/13.
    parser.add_argument("--spark_submit", default="/opt/spark/bin/spark-submit", help="Path to spark-submit command")
    parser.add_argument(
        "--in-process",
        action="store_true",
        help="chạy mọi job trong app này (một session con mỗi job) thay vì spark-submit từng job",
    )
    return parser.parse_args()


def run_gold_job_in_process(job_def: dict, cob_dt: str, spark, logger) -> bool:
    """Chạy một job Gold trên session con của app hiện tại (--in-process)."""
    from spark.in_process import load_job_module, run_job_in_process

    logger.info(f"Running: {job_def['name']} ({job_def['type']}, in-process)")
    module = load_job_module(GOLD_JOB_FILE)
    ok = run_job_in_process(spark, module, "run_gold_job", job_def["config"], cob_dt, logger)
    if ok:
        logger.info(f"  ✓ {job_def['name']} completed successfully")
    return ok


def run_gold_job(job_def: dict, cob_dt: str, spark_submit: str, logger) -> bool:
    """Run a single Gold job via spark-submit."""
    name = job_def["name"]
    config_path = job_def["config"]
    job_type = job_def["type"]  # noqa: F841

    cmd = [
        spark_submit,
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--conf",
        "spark.driver.memory=512m",
        "--conf",
        "spark.executor.memory=768m",
        "code_etl/gold/base_job/gold_job.py",
        "--config",
        config_path,
        "--cob_dt",
        cob_dt,
    ]

    logger.info(f"Running: {name} ({job_def['type']})")
    logger.info(f"  Config: {config_path}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600, check=False)
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
        logger.error(f"  ✗ {name} ERROR: {e}")
        return False


def main():
    args = parse_arguments()
    logger = get_logger(__name__)

    logger.info("=" * 60)
    logger.info("GOLD BOOTSTRAP — INITIAL LOAD")
    logger.info(f"Business Date: {args.cob_dt}")
    logger.info("=" * 60)

    results = {"success": [], "failed": []}

    spark = None
    if args.in_process:
        from spark.spark_session import get_spark_session

        spark = get_spark_session("gold-bootstrap-in-process")

    for job_def in GOLD_JOB_ORDER:
        # Check if dependencies are met
        deps = job_def.get("depends_on", [])
        if deps:
            unmet = [d for d in deps if d not in results["success"]]
            if unmet:
                logger.warning(f"Skipping {job_def['name']}: unmet dependencies {unmet}")
                results["failed"].append(f"{job_def['name']} (deps: {unmet})")
                continue

        if spark is not None:
            success = run_gold_job_in_process(job_def, args.cob_dt, spark, logger)
        else:
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

    if spark is not None:
        spark.stop()

    if results["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
