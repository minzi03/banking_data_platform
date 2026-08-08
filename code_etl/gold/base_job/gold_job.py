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

# Z-Ordering columns for frequently queried tables
# Key: table name, Value: list of columns to Z-Order by
ZORDER_COLUMNS = {
    "mart_customer_360": ["customer_id"],
    "rfm_segment": ["rfm_segment", "customer_id"],
    "churn_prediction": ["churn_risk", "customer_id"],
    "cross_sell_segment": ["cross_sell_score", "customer_id"],
    "campaign_target": ["campaign_type", "customer_id"],
    "customer_balance_summary": ["customer_id"],
    "customer_transaction_summary": ["customer_id"],
    "customer_product_summary": ["customer_id"],
    "customer_card_summary": ["customer_id"],
}


def _run_zorder_if_needed(spark, target: str, job_type: str, logger):
    """
    Run OPTIMIZE ZORDER for frequently queried tables.
    This co-locates data by the specified columns, improving read performance.
    Only runs for tables with < 1M rows to avoid long optimization times.
    """
    # Extract table name from full path (e.g., "lakehouse.gold.mart_customer_360" -> "mart_customer_360")
    table_name = target.split(".")[-1] if "." in target else target

    # Check if this table needs Z-Ordering
    if table_name not in ZORDER_COLUMNS:
        return

    try:
        # Check table row count - skip Z-Order for large tables (> 1M rows)
        row_count = spark.table(target).count()
        if row_count > 1_000_000:
            logger.info(f"[{job_type}] Skipping Z-Order for {target} ({row_count} rows > 1M)")
            return

        zorder_cols = ZORDER_COLUMNS[table_name]
        cols_str = ", ".join(zorder_cols)
        logger.info(f"[{job_type}] Running OPTIMIZE ZORDER BY ({cols_str}) on {target}")

        spark.sql(f"""
            OPTIMIZE {target}
            ZORDER BY ({cols_str})
        """)
        logger.info(f"[{job_type}] Z-Order completed for {target}")

    except Exception as e:
        # Z-Order failure is non-fatal, log warning and continue
        logger.warning(f"[{job_type}] Z-Order failed for {target}: {str(e)}")


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

    Sau khi ghi, thực hiện Z-Ordering cho các cột thường xuyên query
    để cải thiện performance khi đọc dữ liệu.
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

    # Run Z-Ordering for frequently queried columns (only for key tables)
    # This improves read performance by co-locating related data
    _run_zorder_if_needed(spark, target, job_type, logger)


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
