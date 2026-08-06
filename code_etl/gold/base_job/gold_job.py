"""
Job dùng chung cho tầng Gold.
Được điều khiển bằng file YAML (metadata-driven).

Chạy SQL transform từ tầng Silver → ghi kết quả vào bảng Iceberg tầng Gold.
Chiến lược ghi: overwritePartitions — chỉ ghi đè partition của ngày cob_dt.

Hỗ trợ các loại job (job.type trong YAML):
  - mart360        : Bảng Customer 360 mart (tổng hợp thông tin khách hàng)
  - segment        : Bảng phân khúc khách hàng
  - time_analytics : Bảng phân tích theo chiều thời gian

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

# Danh sách loại job hợp lệ trong tầng Gold
VALID_JOB_TYPES = {"mart360", "segment", "time_analytics"}


def validate_config(config: dict):
    """
    Kiểm tra file YAML có đủ các section bắt buộc không.
    Gold job cần: job, source, target, sql.
    """
    for field in ["job", "source", "target", "sql"]:
        if field not in config:
            raise ValueError(f"Thiếu section bắt buộc trong config: {field}")
    job_type = config["job"].get("type")
    if job_type not in VALID_JOB_TYPES:
        raise ValueError(f"Loại job không hợp lệ '{job_type}'. Phải là một trong: {VALID_JOB_TYPES}")


def run_gold_job(spark, config: dict, cob_dt: str, logger):
    """
    Thực thi Gold job: chạy SQL transform rồi ghi đè partition ngày cob_dt.

    Dùng overwritePartitions để:
    - Chỉ xóa và ghi lại partition của ngày cob_dt
    - Không ảnh hưởng dữ liệu các ngày khác
    - Idempotent: chạy lại cùng ngày cho ra kết quả như nhau
    """
    target   = get_target_table(config)
    job_type = config["job"]["type"]

    result_df = load_source_df(spark, config, cob_dt)

    # Check table exists — if not, create with initial load
    if not table_exists(spark, target):
        logger.warning(f"[{job_type}] Target table {target} does not exist. Creating with initial load...")
        result_df.writeTo(target).overwritePartitions()
        logger.info(f"[{job_type}] Created {target} with initial data load")
        return

    logger.info(f"[{job_type}] Đang ghi vào {target} bằng overwritePartitions (an toàn theo partition)")
    result_df.writeTo(target).overwritePartitions()
    logger.info(f"[{job_type}] Ghi hoàn tất cho {target}")


def main():
    """Điểm vào của chương trình: parse args → validate config → chạy job → dọn dẹp."""
    args = parse_arguments("Gold Layer Job")
    logger = get_logger(__name__)
    spark = None
    try:
        config = load_config(args.config)
        validate_config(config)
        spark = get_spark_session(app_name=f"gold-{config['job']['type']}-{config['target']['table']}")
        run_gold_job(spark, config, args.cob_dt, logger)
    except Exception as e:
        logger.error(f"Gold job thất bại: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
