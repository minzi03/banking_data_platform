"""
Iceberg Maintenance Job
-----------------------
Chạy ba thủ tục bảo trì định kỳ trên mọi bảng Iceberg:
  1. rewrite_data_files  — compact small files → giảm số file, tăng tốc query
  2. expire_snapshots    — xóa snapshot cũ hơn ngưỡng giữ lại
  3. remove_orphan_files — dọn file rác không thuộc snapshot nào

Lịch chạy (qua Airflow):
  - Weekly (Chủ nhật 02:00) cho fact table lớn
  - Daily cho mart/segment
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

from spark.spark_session import get_spark_session

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("iceberg_maintenance")

# Bảng fact lớn: compact + expire + orphan weekly
FACT_TABLES = [
    # Bronze facts (partitioned by cob_dt)
    "lakehouse.bronze.core_txn_account",
    "lakehouse.bronze.core_card_txn",
    "lakehouse.bronze.core_online_transaction",
    # Silver facts
    "lakehouse.silver.fact_txn_account",
    "lakehouse.silver.fact_card_txn",
    "lakehouse.silver.fact_crm_interaction",
]

# Bảng mart/segment: compact + expire daily
MART_TABLES = [
    "lakehouse.gold.mart_customer_360",
    "lakehouse.gold.customer_balance_summary",
    "lakehouse.gold.customer_transaction_summary",
    "lakehouse.gold.customer_product_summary",
    "lakehouse.gold.customer_card_summary",
    "lakehouse.gold.rfm_segment",
    "lakehouse.gold.churn_prediction",
    "lakehouse.gold.cross_sell_segment",
    "lakehouse.gold.campaign_target",
]

# Dimension tables: expire weekly
DIM_TABLES = [
    "lakehouse.bronze.core_branch",
    "lakehouse.bronze.core_product",
    "lakehouse.bronze.core_customer",
    "lakehouse.bronze.core_account",
    "lakehouse.bronze.core_deposit",
    "lakehouse.bronze.core_loan",
    "lakehouse.bronze.core_txn_account",
    "lakehouse.bronze.core_employee",
    "lakehouse.bronze.core_card",
    "lakehouse.bronze.core_card_txn",
    "lakehouse.bronze.core_crm_interaction",
    "lakehouse.bronze.core_device",
    "lakehouse.bronze.core_location",
    "lakehouse.bronze.core_online_transaction",
    "lakehouse.bronze.core_support_ticket",
    "lakehouse.bronze.core_mcc_code",
    "lakehouse.silver.dim_customer",
    "lakehouse.silver.dim_account",
]


def rewrite_data_files(spark, table: str) -> None:
    logger.info("rewrite_data_files: %s", table)
    spark.sql(f"""
        CALL lakehouse.system.rewrite_data_files(
            table => '{table}',
            strategy => 'binpack',
            options => map(
                'min-input-files', '5',
                'target-file-size-bytes', '134217728'
            )
        )
    """)


def expire_snapshots(spark, table: str, retain_days: int = 7, min_snapshots: int = 3) -> None:
    older_than = (datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")) - timedelta(days=retain_days)).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("expire_snapshots: %s (older than %s, keep min %d)", table, older_than, min_snapshots)
    spark.sql(f"""
        CALL lakehouse.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{older_than}',
            retain_last => {min_snapshots}
        )
    """)


def remove_orphan_files(spark, table: str, older_than_days: int = 3) -> None:
    older_than = (datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")) - timedelta(days=older_than_days)).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("remove_orphan_files: %s (older than %s)", table, older_than)
    spark.sql(f"""
        CALL lakehouse.system.remove_orphan_files(
            table => '{table}',
            older_than => TIMESTAMP '{older_than}'
        )
    """)


def run_maintenance(spark, tables: list[str], mode: str) -> None:
    """
    mode: 'full' (compact + expire + orphan) | 'expire_only'
    """
    errors = []
    for table in tables:
        try:
            if mode == "full":
                rewrite_data_files(spark, table)
                expire_snapshots(spark, table)
                remove_orphan_files(spark, table)
            else:
                expire_snapshots(spark, table)
        except Exception as exc:
            logger.error("Maintenance FAILED for %s: %s", table, exc, exc_info=True)
            errors.append(table)

    if errors:
        raise RuntimeError(f"Maintenance failed for tables: {errors}")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Iceberg Maintenance Job")
    parser.add_argument(
        "--target",
        choices=["fact", "mart", "dim", "all"],
        default="all",
        help="Nhóm bảng cần bảo trì",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "expire_only"],
        default="full",
        help="full = compact+expire+orphan | expire_only = chỉ expire snapshot",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    spark = get_spark_session("iceberg_maintenance")

    table_groups: dict[str, list[str]] = {
        "fact": FACT_TABLES,
        "mart": MART_TABLES,
        "dim": DIM_TABLES,
        "all": FACT_TABLES + MART_TABLES + DIM_TABLES,
    }
    tables = table_groups[args.target]
    logger.info("Starting maintenance — target=%s mode=%s tables=%d", args.target, args.mode, len(tables))

    run_maintenance(spark, tables, mode=args.mode)
    logger.info("Iceberg maintenance completed successfully.")


if __name__ == "__main__":
    main()
