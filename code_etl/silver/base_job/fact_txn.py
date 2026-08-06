"""
Job Fact Table dùng chung cho tầng Silver.
Được điều khiển bằng file YAML (metadata-driven).

Chiến lược ghi: overwritePartitions — chỉ ghi đè đúng partition của ngày cob_dt,
các partition ngày khác không bị ảnh hưởng (partition-safe).
SQL trong YAML phải filter theo cob_dt để đảm bảo chỉ lấy data của ngày đó.

Đây là job chạy hàng ngày trên production — bảng đích phải đã tồn tại.
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
    Job Fact cần: job, source, target, business_key, sql.
    """
    for field in ["job", "source", "target", "business_key", "sql"]:
        if field not in config:
            raise ValueError(f"Thiếu section bắt buộc trong config: {field}")
    if config["job"]["type"] != "fact_txn":
        raise ValueError(f"Sai loại job, mong đợi job.type=fact_txn, nhận được: {config['job']['type']}")


def run_fact_txn(spark, config: dict, cob_dt: str, logger):
    """
    Thực thi job Fact Table: chạy SQL transform rồi ghi đè partition ngày cob_dt.

    Dùng overwritePartitions thay vì overwrite toàn bảng để:
    - Chỉ xóa và ghi lại partition của ngày cob_dt
    - Các ngày khác trong bảng không bị mất dữ liệu
    - An toàn khi chạy lại (idempotent)
    """
    target = get_target_table(config)

    fact_df = load_source_df(spark, config, cob_dt)

    # Check table exists — if not, create with initial load
    if not table_exists(spark, target):
        logger.warning(f"Target table {target} does not exist. Creating with initial load...")
        fact_df.writeTo(target).overwritePartitions()
        logger.info(f"Created {target} with initial data load ({fact_df.count()} rows)")
        return

    logger.info(f"Đang ghi vào {target} bằng overwritePartitions (an toàn theo partition)")
    fact_df.writeTo(target).overwritePartitions()
    logger.info("Ghi Fact table hoàn tất")


def main():
    """Điểm vào của chương trình: parse args → validate config → chạy job → dọn dẹp."""
    args = parse_arguments("Silver Fact Table Job")
    logger = get_logger(__name__)
    config = load_config(args.config)
    validate_config(config)
    spark = None
    try:
        spark = get_spark_session(app_name=f"silver-fact-{config['target']['table']}")
        run_fact_txn(spark, config, args.cob_dt, logger)
    except Exception as e:
        logger.error(f"Job Fact table thất bại: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
