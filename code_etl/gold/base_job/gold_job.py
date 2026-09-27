"""
Job dùng chung cho tầng Gold.
Được điều khiển bằng file YAML (metadata-driven).

Chạy SQL transform từ tầng Silver → ghi kết quả vào bảng Iceberg tầng Gold.
Chiến lược ghi: overwritePartitions — chỉ ghi đè partition của ngày cob_dt.

Hỗ trợ các loại job (job.type trong YAML):
  - mart360        : Bảng Customer 360 mart (tổng hợp thông tin khách hàng)
  - segment        : Bảng phân khúc khách hàng
  - time_analytics : Bảng phân tích theo chiều thời gian
  - risk           : Bảng rủi ro (loan portfolio, fraud, AML monitoring)

Đây là job chạy hàng ngày trên production — bảng đích phải đã tồn tại.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))
sys.path.insert(0, str(Path(__file__).parent))

from common_utils import get_target_table, load_source_df, parse_arguments
from spark.iceberg_utils import create_iceberg_table_if_not_exists, table_exists
from spark.spark_session import get_spark_session
from utils.logger import get_logger
from utils.yaml_loader import load_config

# Danh sách loại job hợp lệ trong tầng Gold
VALID_JOB_TYPES = {"mart360", "segment", "time_analytics", "risk"}

# Cột sắp xếp lại file sau khi ghi, cho các bảng query nhiều.
# Key: tên bảng, Value: danh sách cột (xem build_rewrite_sql)
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
    "customer_loan_summary": ["customer_id"],
    "loan_portfolio_risk": ["branch_code", "product_code"],
    "fraud_risk_txn": ["risk_level", "customer_id"],
    "aml_monitoring": ["alert_generated", "customer_id"],
}


# Marker cố định trong log khi bước tối ưu thất bại — grep / alert theo chuỗi này.
OPTIMIZE_FAILED_MARKER = "OPTIMIZE_FAILED"


def build_rewrite_sql(target: str, columns: list, cob_dt: str) -> str:
    """
    CALL rewrite_data_files của Iceberg, giới hạn trong partition cob_dt vừa ghi.

    Không dùng `OPTIMIZE … ZORDER BY`: đó là cú pháp Delta Lake, Spark + Iceberg
    trả PARSE_SYNTAX_ERROR (lỗi này từng bị nuốt thành WARNING ở mọi job Gold).

    ≥ 2 cột → zorder(...). 1 cột → sort tuyến tính: z-order một chiều chính là sort.
    """
    catalog, table = target.split(".", 1)
    if len(columns) > 1:
        sort_order = f"zorder({','.join(columns)})"
    else:
        sort_order = f"{columns[0]} ASC NULLS LAST"
    return (
        f"CALL {catalog}.system.rewrite_data_files("
        f"table => '{table}', "
        f"strategy => 'sort', "
        f"sort_order => '{sort_order}', "
        f"where => \"cob_dt = '{cob_dt}'\", "
        # rewrite-all: partition nhỏ (1–2 file) vẫn phải được sắp xếp lại —
        # mặc định Iceberg bỏ qua nhóm dưới 5 file.
        f"options => map('rewrite-all', 'true'))"
    )


def optimize_written_partition(spark, target: str, cob_dt: str, job_type: str, logger) -> bool:
    """
    Sắp xếp lại (z-order / sort) file của partition cob_dt vừa ghi.

    Chạy sau mỗi lần ghi nhưng chỉ trên partition vừa ghi, nên chi phí tỉ lệ với
    một ngày dữ liệu chứ không phải cả bảng.

    Chính sách lỗi: KHÔNG làm job fail — dữ liệu đã commit đúng, bảng chỉ đọc
    chậm hơn. Nhưng không im lặng: log WARNING mang marker OPTIMIZE_FAILED kèm
    bảng, cob_dt và loại lỗi; trả về False.
    Bảng không có trong ZORDER_COLUMNS → bỏ qua, trả về True.
    """
    columns = ZORDER_COLUMNS.get(target.split(".")[-1])
    if not columns:
        return True

    sql = build_rewrite_sql(target, columns, cob_dt)
    logger.info(f"[{job_type}] Tối ưu partition cob_dt={cob_dt} của {target}: {sql}")
    try:
        row = spark.sql(sql).first()
    except Exception as e:
        logger.warning(
            f"[{job_type}] {OPTIMIZE_FAILED_MARKER} table={target} cob_dt={cob_dt} "
            f"error={type(e).__name__}: {e} — dữ liệu đã ghi đúng, file chưa được sắp xếp lại"
        )
        return False

    logger.info(f"[{job_type}] Tối ưu xong {target} cob_dt={cob_dt}: {row.asDict() if row else None}")
    return True


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
        found = spark.sql(f"SELECT 1 FROM {table} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").take(1)
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

    Sau khi ghi, sắp xếp lại file của partition vừa ghi
    (optimize_written_partition) — lỗi ở bước này không làm job fail.

    Trước khi transform: assert snapshot nguồn tồn tại (fail loud).
    Trước khi ghi: assert kết quả không rỗng (nếu config yêu cầu).
    """
    target = get_target_table(config)
    job_type = config["job"]["type"]

    assert_source_snapshots(spark, config, cob_dt, logger)

    result_df = load_source_df(spark, config, cob_dt)

    assert_non_empty(result_df, config, cob_dt, logger)

    # Create the target explicitly before writing.  Iceberg's V2 writer cannot
    # create a missing table with overwritePartitions(); it resolves the target
    # relation before the write and fails with TABLE_OR_VIEW_NOT_FOUND.
    if not table_exists(spark, target):
        logger.warning(f"[{job_type}] Target table {target} does not exist; creating from result schema")
        create_iceberg_table_if_not_exists(result_df, target, logger)

    logger.info(f"[{job_type}] Đang ghi vào {target} bằng overwritePartitions (an toàn theo partition)")
    result_df.writeTo(target).overwritePartitions()
    logger.info(f"[{job_type}] Ghi hoàn tất cho {target}")

    optimize_written_partition(spark, target, cob_dt, job_type, logger)


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
