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

from common_utils import get_target_table, load_source_df, parse_arguments
from spark.iceberg_utils import table_exists
from spark.spark_session import get_spark_session
from utils.logger import get_logger
from utils.yaml_loader import load_config

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
        logger.warning(f"[{job_type}] Z-Order failed for {target}: {e}")


def _qualify(ref: str, catalog: str) -> str:
    """
    `silver.fact_txn_account` → `lakehouse.silver.fact_txn_account`.
    Tên đã đủ 3 phần thì giữ nguyên; tên 1 phần (temp view trong test) cũng giữ nguyên.
    """
    return f"{catalog}.{ref}" if ref.count(".") == 1 else ref


def assert_source_snapshots(spark, config: dict, cob_dt: str, logger) -> None:
    """
    Guard chính cho fail-loud: mọi snapshot-backed source khai báo trong
    validation.require_snapshots PHẢI có partition cob_dt đang xử lý.

    Vì sao guard này cần thiết, và vì sao require_non_empty KHÔNG thay thế được:
    các model grain customer neo vào dim_customer rồi LEFT JOIN fact. Nếu
    partition fact của cob_dt không tồn tại, query vẫn trả về đủ 1 dòng/khách
    với mọi metric = 0. Output KHÔNG rỗng, require_non_empty vẫn PASS, và Gold
    bị ghi đè bằng số 0 trông rất hợp lý. Đó là silent corruption, tệ hơn rỗng.
    """
    validation = config.get("validation") or {}
    required = validation.get("require_snapshots") or []
    if not required:
        return

    catalog = config["target"]["catalog"]
    missing = []
    for ref in required:
        table = _qualify(ref, catalog)
        found = spark.sql(
            f"SELECT 1 FROM {table} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1"
        ).take(1)
        if not found:
            missing.append(table)
        else:
            logger.info(f"Snapshot OK: {table} @ cob_dt={cob_dt}")

    if missing:
        raise RuntimeError(
            f"Thiếu snapshot nguồn cho cob_dt={cob_dt}: {', '.join(missing)}. "
            "Upstream chưa chạy hoặc partition đã bị xoá — dừng job thay vì "
            "ghi Gold bằng dữ liệu rỗng/toàn 0."
        )


def assert_non_empty(result_df, config: dict, cob_dt: str, logger) -> None:
    """
    Guard phụ: chặn ghi đè partition Gold bằng kết quả rỗng.
    Chỉ áp dụng khi validation.require_non_empty = true, vì có model
    hoàn toàn có thể rỗng một cách hợp lệ.
    """
    validation = config.get("validation") or {}
    if not validation.get("require_non_empty"):
        return

    if not result_df.take(1):
        target = config["target"]["table"]
        raise RuntimeError(
            f"Gold job '{target}' không sinh dòng nào cho cob_dt={cob_dt}. "
            "overwritePartitions() với DataFrame rỗng là no-op và sẽ để lại "
            "partition cũ mà không ai biết."
        )
    logger.info(f"Non-empty check OK cho cob_dt={cob_dt}")


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

    Trước khi transform: assert snapshot nguồn tồn tại (fail loud).
    Trước khi ghi: assert kết quả không rỗng (nếu config yêu cầu).
    """
    target   = get_target_table(config)
    job_type = config["job"]["type"]

    assert_source_snapshots(spark, config, cob_dt, logger)

    result_df = load_source_df(spark, config, cob_dt)

    assert_non_empty(result_df, config, cob_dt, logger)

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
    except Exception:
        logger.exception("Gold job thất bại")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
