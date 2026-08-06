"""
Job SCD Type 1 dùng chung cho tầng Silver.

Được điều khiển bằng file YAML (metadata-driven).
Chiến lược: UPSERT — nếu record đã tồn tại thì CẬP NHẬT tại chỗ (ghi đè),
nếu chưa tồn tại thì CHÈN mới. Không lưu lịch sử thay đổi.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))
sys.path.insert(0, str(Path(__file__).parent))

from utils.yaml_loader import load_config
from utils.logger import get_logger
from spark.spark_session import get_spark_session
from common_utils import parse_arguments, get_target_table, load_source_df
from spark.iceberg_utils import table_exists


def validate_config(config: dict):
    """
    Kiểm tra file YAML có đủ các section bắt buộc không.
    Job SCD Type 1 cần: job, source, target, business_key, sql.
    """
    for field in ["job", "source", "target", "business_key", "sql"]:
        if field not in config:
            raise ValueError(f"Thiếu section bắt buộc trong config: {field}")
    if config["job"]["type"] != "scd_type1":
        raise ValueError(f"Sai loại job, mong đợi job.type=scd_type1, nhận được: {config['job']['type']}")


def run_scd_type1(spark, config: dict, cob_dt: str, logger):
    """
    Thực thi SCD Type 1: UPSERT dữ liệu nguồn vào bảng đích.

    Luồng xử lý:
    1. Đọc snapshot dữ liệu nguồn của ngày cob_dt.
    2. Đăng ký DataFrame thành temp view để dùng trong câu SQL MERGE.
    3. Tự động tạo câu MERGE:
       - MATCHED (record đã có trong target): cập nhật tất cả cột không phải business key.
       - NOT MATCHED (record mới): chèn toàn bộ cột.
    4. Chạy câu MERGE trên Spark (Iceberg hỗ trợ MERGE INTO).
    """
    target = get_target_table(config)
    business_keys = config["business_key"]

    source_df = load_source_df(spark, config, cob_dt)
    logger.info(f"Số dòng dữ liệu nguồn: {source_df.count()}")

    # Check table exists, create if not (first run scenario)
    if not table_exists(spark, target):
        logger.warning(f"Target table {target} does not exist. Creating from source DataFrame...")
        source_df.writeTo(target).overwritePartitions()
        logger.info(f"Created {target} with initial data load")
        return

    source_df.createOrReplaceTempView("source_view")

    src_cols = source_df.columns
    update_cols = [c for c in src_cols if c not in business_keys]

    join_cond = " AND ".join([f"t.{k} = s.{k}" for k in business_keys])
    update_set = ", ".join([f"t.{c} = s.{c}" for c in update_cols])

    insert_cols = ", ".join(src_cols)
    insert_vals = ", ".join([f"s.{c}" for c in src_cols])

    merge_sql = f"""
        MERGE INTO {target} t
        USING source_view s
        ON {join_cond}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    logger.info(f"Đang chạy MERGE INTO {target}")
    spark.sql(merge_sql)
    logger.info("SCD Type 1 hoàn tất")


def main():
    """Điểm vào của chương trình: parse args → validate config → chạy job → dọn dẹp."""
    args = parse_arguments("Silver SCD Type 1 Job")
    logger = get_logger(__name__)
    config = load_config(args.config)
    validate_config(config)
    spark = None
    try:
        spark = get_spark_session(app_name=f"silver-scd1-{config['target']['table']}")
        run_scd_type1(spark, config, args.cob_dt, logger)
    except Exception as e:
        logger.error(f"Job SCD Type 1 thất bại: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
