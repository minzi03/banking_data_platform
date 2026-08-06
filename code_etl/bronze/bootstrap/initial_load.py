"""
Bronze Bootstrap Job — Initial Load
====================================
Tạo Iceberg tables + load toàn bộ data từ PostgreSQL vào Bronze layer.

Chạy 1 lần duy nhất khi khởi tạo hệ thống.
Sau khi chạy xong, các job hàng ngày (incremental) sẽ thay thế.

Usage:
  spark-submit \\
    --master spark://spark-master:7077 \\
    code_etl/bronze/bootstrap/initial_load.py \\
    --jdbc_url "jdbc:postgresql://postgres:5432/banking_db" \\
    --db_user banking_admin \\
    --db_password BankingAdmin123 \\
    --cob_dt 2025-01-01
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))
sys.path.insert(0, str(Path(__file__).parent.parent / "base_job"))

from spark.spark_session import get_spark_session
from spark.iceberg_utils import get_iceberg_table_name, write_to_iceberg
from utils.yaml_loader import load_config
from utils.sql_renderer import render_sql
from utils.logger import get_logger

# Tất cả YAML configs cho Bronze layer
BRONZE_CONFIGS = [
    # core_banking (8 tables)
    "code_etl/bronze/core_banking/branch.yml",
    "code_etl/bronze/core_banking/product.yml",
    "code_etl/bronze/core_banking/customer.yml",
    "code_etl/bronze/core_banking/account.yml",
    "code_etl/bronze/core_banking/deposit.yml",
    "code_etl/bronze/core_banking/loan.yml",
    "code_etl/bronze/core_banking/txn_account.yml",
    "code_etl/bronze/core_banking/employee.yml",
    # card_crm (3 tables)
    "code_etl/bronze/card_crm/card.yml",
    "code_etl/bronze/card_crm/card_txn.yml",
    "code_etl/bronze/card_crm/crm_interaction.yml",
    # digital_banking (5 tables)
    "code_etl/bronze/digital_banking/device.yml",
    "code_etl/bronze/digital_banking/location.yml",
    "code_etl/bronze/digital_banking/online_transaction.yml",
    "code_etl/bronze/digital_banking/support_ticket.yml",
    "code_etl/bronze/digital_banking/mcc_code.yml",
]


def parse_arguments():
    parser = argparse.ArgumentParser(description="Bronze Bootstrap Initial Load")
    parser.add_argument("--jdbc_url",    required=True, help="JDBC URL to source PostgreSQL")
    parser.add_argument("--db_user",     required=True, help="Database username")
    parser.add_argument("--db_password", required=True, help="Database password")
    parser.add_argument("--cob_dt",      required=True, help="Business date YYYY-MM-DD")
    parser.add_argument("--config_dir",  default="code_etl/bronze", help="Directory containing YAML configs")
    return parser.parse_args()


def run_initial_load(spark, cob_dt, jdbc_url, db_user, db_password, logger):
    """Load all Bronze tables from PostgreSQL."""
    import os

    results = {"success": [], "failed": []}

    for config_path in BRONZE_CONFIGS:
        table_name = Path(config_path).stem
        try:
            logger.info(f"{'='*60}")
            logger.info(f"Loading: {table_name} from {config_path}")

            config = load_config(config_path)

            # Render SQL
            sql = render_sql(config["sql"], {"cob_dt": cob_dt})

            # JDBC reader
            reader = (
                spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", f"({sql}) t")
                .option("user", db_user)
                .option("password", db_password)
                .option("fetchsize", config["source"].get("fetchsize", 10000))
                .option("driver", "org.postgresql.Driver")
            )

            # JDBC partitioning for large tables
            partition_cfg = config["source"].get("jdbc_partition")
            if partition_cfg:
                reader = (
                    reader
                    .option("partitionColumn", partition_cfg["partition_column"])
                    .option("lowerBound", str(partition_cfg["lower_bound"]))
                    .option("upperBound", str(partition_cfg["upper_bound"]))
                    .option("numPartitions", str(partition_cfg["num_partitions"]))
                )

            df = reader.load()

            # Add cob_dt column
            cob_dt_col = config["load"].get("cob_dt_from_column")
            if cob_dt_col:
                from pyspark.sql import functions as F
                df = df.withColumn("cob_dt", F.col(cob_dt_col).cast("date"))
            else:
                from pyspark.sql import functions as F
                df = df.withColumn("cob_dt", F.lit(cob_dt).cast("date"))

            # Write to Iceberg
            target = config["target"]
            iceberg_table = get_iceberg_table_name(
                catalog=target["catalog"],
                schema=target["schema"],
                table=target["table"]
            )

            logger.info(f"Writing to {iceberg_table}")
            write_to_iceberg(df, iceberg_table, logger)
            row_count = 0  # Skip count to avoid OOM on large tables

            results["success"].append((table_name, row_count))
            logger.info(f"✓ {table_name}: {row_count:,} rows loaded")

        except Exception as e:
            logger.error(f"✗ {table_name} FAILED: {str(e)}")
            results["failed"].append((table_name, str(e)))

    return results


def main():
    args = parse_arguments()
    logger = get_logger(__name__)

    spark = None
    try:
        spark = get_spark_session("bronze-bootstrap-initial-load")

        logger.info("=" * 60)
        logger.info("BRONZE BOOTSTRAP — INITIAL LOAD")
        logger.info("=" * 60)

        results = run_initial_load(
            spark, args.cob_dt, args.jdbc_url, args.db_user, args.db_password, logger
        )

        # Summary
        logger.info("=" * 60)
        logger.info("LOAD SUMMARY")
        logger.info("=" * 60)

        total_rows = 0
        for name, count in results["success"]:
            logger.info(f"  ✓ {name}: {count:,} rows")
            total_rows += count

        if results["failed"]:
            logger.info("")
            for name, error in results["failed"]:
                logger.info(f"  ✗ {name}: {error}")

        logger.info("")
        logger.info(f"Total: {len(results['success'])}/{len(BRONZE_CONFIGS)} tables loaded")
        logger.info(f"Total rows: {total_rows:,}")
        logger.info(f"Failed: {len(results['failed'])}")

    except Exception as e:
        logger.error(f"Bootstrap failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
