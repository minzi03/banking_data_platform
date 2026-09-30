"""
Migration một lần: partition các bảng Bronze có sẵn theo cob_dt.

Vì sao: DDL Bronze cũ chỉ partition 3/22 bảng. Trên bảng không partition,
overwritePartitions() thay TOÀN BỘ bảng, nên mỗi lần nạp xoá snapshot của các
ngày trước (lịch sử product/branch mất hẳn) và backfill một ngày cũ ghi đè luôn
ngày mới. DDL mới đã partition mọi bảng; stack tạo trước đó cần migrate bảng cũ.
write_to_iceberg() từ chối ghi vào bảng chưa migrate.

Cách làm (Iceberg partition evolution, không mất dữ liệu):
  1. ALTER TABLE … ADD PARTITION FIELD cob_dt
  2. rewrite_data_files(rewrite-all) — viết lại file cũ theo partition spec mới,
     để dynamic overwrite của lần chạy lại một ngày cũ thay đúng file của ngày đó.

Idempotent: bảng đã partition theo cob_dt hoặc chưa tồn tại thì bỏ qua.

Usage (trong spark-worker-1):
    spark-submit --master spark://spark-master:7077 \\
        code_etl/bronze/bootstrap/partition_bronze_by_cob_dt.py [--dry-run]
"""

import argparse
import sys
from pathlib import Path

import yaml

BRONZE_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BRONZE_ROOT.parent / "shared"))

from spark.iceberg_utils import partition_fields, table_exists  # noqa: E402
from spark.spark_session import get_spark_session  # noqa: E402
from utils.logger import get_logger  # noqa: E402

# Cùng tập workload mà DAG Bronze glob (code_etl/bronze/<schema>/*.yml).
WORKLOAD_DIRS = ("core_banking", "card_crm", "digital_banking")


def bronze_tables() -> list[str]:
    tables = []
    for directory in WORKLOAD_DIRS:
        for path in sorted((BRONZE_ROOT / directory).glob("*.yml")):
            target = yaml.safe_load(path.read_text(encoding="utf-8"))["target"]
            tables.append(f"{target['catalog']}.{target['schema']}.{target['table']}")
    return tables


def migrate(spark, table: str, dry_run: bool, logger) -> str:
    if not table_exists(spark, table):
        return "missing"
    fields = partition_fields(spark, table)
    if "cob_dt" in fields:
        return "ok"
    logger.warning(f"{table}: partition hiện tại {fields or 'không có'} → thêm cob_dt")
    if dry_run:
        return "would_migrate"
    catalog, name = table.split(".", 1)
    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD cob_dt")
    spark.sql(f"CALL {catalog}.system.rewrite_data_files(table => '{name}', options => map('rewrite-all', 'true'))")
    if "cob_dt" not in partition_fields(spark, table):
        raise RuntimeError(f"{table}: vẫn chưa partition theo cob_dt sau migration")
    return "migrated"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--dry-run", action="store_true", help="Chỉ liệt kê bảng cần migrate")
    args = parser.parse_args()

    logger = get_logger("partition_bronze_by_cob_dt")
    spark = get_spark_session(app_name="bronze-partition-migration")
    try:
        results = {table: migrate(spark, table, args.dry_run, logger) for table in bronze_tables()}
    finally:
        spark.stop()

    for table, status in results.items():
        logger.info(f"{status:14s} {table}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
