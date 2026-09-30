"""
Job SCD Type 2 dùng chung cho tầng Silver.

Được điều khiển bằng file YAML (metadata-driven).
Lưu toàn bộ lịch sử thay đổi của record trên bảng Iceberg.

Hai nhóm cột (hybrid Type 2 + Type 1):
  - tracked_columns (Type 2): thay đổi → đóng version cũ, mở version mới.
    Không khai báo → mọi cột trừ business key và cob_dt đều là tracked.
  - các cột còn lại (Type 1): thay đổi → ghi đè tại chỗ trên version hiện hành.
    Trước đây nhóm này không bao giờ được cập nhật: full_name, is_active… đứng yên
    ở giá trị của ngày nạp đầu tiên.

Luồng xử lý mỗi ngày (cob_dt = D):
  1. Đọc snapshot nguồn của D, deduplicate theo business key. Snapshot rỗng → FAIL
     (một full snapshot rỗng là upstream hỏng, không phải "mọi khách đã rời đi").
  2. Guard backfill: D < MAX(effective_from) → FAIL. Chạy lại một ngày cũ khi đã
     có version mới hơn sẽ đóng nhầm version tương lai và làm hỏng lịch sử.
  3. Idempotency cleanup cho D (xoá version mở bởi lần chạy trước, khôi phục
     version bị đóng bởi lần chạy trước).
  4. So với version hiện hành để tìm: key mới, key đổi tracked column, key chỉ
     đổi cột Type 1, và (nếu scd.close_missing_keys) key biến mất khỏi nguồn.
  5. Append version mới (INSERT TRƯỚC — nếu bước sau fail, rerun an toàn).
  6. MERGE đóng version cũ (đổi tracked hoặc biến mất): effective_to = D-1,
     is_current = 0. Guard effective_from < D để không đóng row vừa insert.
  7. MERGE ghi đè cột Type 1 trên version hiện hành của các key chỉ đổi Type 1.
"""

import sys
from datetime import datetime, timedelta
from functools import reduce
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))
sys.path.insert(0, str(Path(__file__).parent))

from common_utils import get_target_table, load_source_df, parse_arguments
from spark.iceberg_utils import create_iceberg_table_if_not_exists, table_exists
from spark.spark_session import get_spark_session
from utils.logger import get_logger
from utils.yaml_loader import load_config

OPEN_END_DATE = "9999-12-31"
# effective_from cho các row có sẵn khi một bảng SCD1 được nâng lên SCD2: không
# biết chúng có hiệu lực từ bao giờ, chỉ biết là trước lần chạy SCD2 đầu tiên.
UNKNOWN_START_DATE = "1900-01-01"


def validate_config(config: dict):
    """Kiểm tra file YAML có đủ các section bắt buộc không."""
    for field in ["job", "source", "target", "business_key", "scd", "sql"]:
        if field not in config:
            raise ValueError(f"Thiếu section bắt buộc trong config: {field}")
    if config["job"]["type"] != "scd_type2":
        raise ValueError(f"Sai loại job, mong đợi job.type=scd_type2, nhận được: {config['job']['type']}")
    for scd_field in ["effective_from_column", "effective_to_column", "current_flag_column"]:
        if scd_field not in config["scd"]:
            raise ValueError(f"Thiếu cấu hình: scd.{scd_field}")


def _build_sk_col_name(config: dict) -> str:
    """
    Lấy tên cột surrogate key: ưu tiên scd.sk_column trong config,
    fallback sang bỏ prefix 'dim_' nếu là bảng dim, hoặc dùng nguyên tên bảng.
    """
    explicit = config["scd"].get("sk_column")
    if explicit:
        return explicit
    table = config["target"]["table"]
    name = table.removeprefix("dim_")
    return name + "_sk"


def split_columns(src_cols: list, business_keys: list, tracked_columns: list | None) -> tuple[list, list]:
    """
    Chia cột nguồn thành (tracked Type 2, Type 1).

    business key và cob_dt không thuộc nhóm nào. Không khai báo tracked_columns
    → mọi cột còn lại là tracked (Type 2 thuần). Tên trong tracked_columns không
    có trong nguồn là lỗi cấu hình — báo ngay thay vì âm thầm không theo dõi gì.
    """
    candidates = [c for c in src_cols if c not in business_keys and c != "cob_dt"]
    if not tracked_columns:
        return candidates, []
    unknown = [c for c in tracked_columns if c not in candidates]
    if unknown:
        raise ValueError(f"tracked_columns không có trong SQL nguồn: {unknown}")
    tracked = [c for c in candidates if c in tracked_columns]
    type1 = [c for c in candidates if c not in tracked_columns]
    return tracked, type1


def _any_changed(columns: list, left: str = "s", right: str = "t"):
    """OR của các so sánh null-safe; không có cột nào → luôn False."""
    if not columns:
        return F.lit(False)
    return reduce(
        lambda a, b: a | b,
        [~F.col(f"{left}.{c}").eqNullSafe(F.col(f"{right}.{c}")) for c in columns],
    )


def _attach_scd2_columns(
    source_df, business_keys: list, cob_dt: str, sk_col: str, effective_col: str, expiry_col: str, current_flag: str
):
    """
    Gắn các cột metadata SCD2 vào DataFrame trước khi ghi vào bảng đích.
    SK = SHA-256(business_key_1 | ... | cob_dt) — duy nhất mỗi version.
    """
    return (
        source_df.withColumn(effective_col, F.to_date(F.lit(cob_dt)))
        .withColumn(expiry_col, F.to_date(F.lit(OPEN_END_DATE)))
        .withColumn(current_flag, F.lit(1))
        .withColumn(
            sk_col,
            F.sha2(
                F.concat_ws("|", *[F.col(k).cast("string") for k in business_keys], F.lit(cob_dt)),
                256,
            ),
        )
    )


def ensure_scd2_columns(
    spark, target: str, business_keys: list, sk_col: str, effective_col: str, expiry_col: str, current_flag: str, logger
) -> bool:
    """
    Nâng một bảng SCD1 có sẵn lên SCD2 (dim_product, dim_branch từng là SCD1).

    Thêm cột SK / effective_from / effective_to / is_current nếu thiếu và điền cho
    các row có sẵn: coi chúng là version hiện hành, hiệu lực từ UNKNOWN_START_DATE.
    Idempotent: bảng đã đủ cột thì không làm gì. Trả về True nếu đã nâng cấp.
    """
    existing = {row["col_name"] for row in spark.sql(f"DESCRIBE TABLE {target}").collect()}
    wanted = [
        (sk_col, "STRING"),
        (effective_col, "DATE"),
        (expiry_col, "DATE"),
        (current_flag, "INT"),
    ]
    missing = [(name, dtype) for name, dtype in wanted if name not in existing]
    if not missing:
        return False

    logger.warning(f"{target} thiếu cột SCD2 {[m[0] for m in missing]} — nâng cấp bảng SCD1 lên SCD2")
    cols_sql = ", ".join(f"{name} {dtype}" for name, dtype in missing)
    spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({cols_sql})")
    sk_expr = "sha2(concat_ws('|', " + ", ".join(f"CAST({k} AS STRING)" for k in business_keys)
    sk_expr += f", '{UNKNOWN_START_DATE}'), 256)"
    spark.sql(f"""
        UPDATE {target}
        SET {sk_col} = {sk_expr},
            {effective_col} = DATE '{UNKNOWN_START_DATE}',
            {expiry_col} = DATE '{OPEN_END_DATE}',
            {current_flag} = 1
        WHERE {current_flag} IS NULL
    """)
    return True


def assert_not_backfilling_past_history(spark, target: str, effective_col: str, cob_dt: str) -> None:
    """
    Chặn chạy SCD2 cho một ngày cũ hơn version mới nhất đã có.

    Cleanup chỉ hoàn tác được lần chạy của CHÍNH ngày đó. Chạy D khi đã có version
    mở ngày D' > D sẽ so nguồn cũ với version tương lai, đóng nhầm nó và mở một
    version "cũ" làm hiện hành — lịch sử bị đảo. Phải fail thay vì ghi.
    """
    latest = spark.sql(f"SELECT MAX({effective_col}) AS m FROM {target}").first()["m"]
    if latest is not None and str(latest) > cob_dt:
        raise RuntimeError(
            f"{target} đã có version hiệu lực từ {latest} > cob_dt={cob_dt}. "
            "SCD2 chỉ chạy tiến theo thời gian (hoặc chạy lại đúng ngày mới nhất); "
            "backfill ngày cũ sẽ làm hỏng lịch sử."
        )


def _idempotency_cleanup(
    spark, target: str, effective_col: str, expiry_col: str, current_flag: str, cob_dt: str, prev_dt: str, logger
):
    """
    Đảm bảo rerun an toàn cho cùng cob_dt:
      - Xóa các row được insert bởi lần chạy trước (effective_from = cob_dt, is_current = 1).
      - Khôi phục các row bị expire bởi lần chạy trước (effective_to = prev_dt, is_current = 0).
    Cột Type 1 không cần hoàn tác: lần chạy lại ghi đè bằng đúng giá trị nguồn của cob_dt.
    """
    spark.sql(f"""
        DELETE FROM {target}
        WHERE {effective_col} = DATE '{cob_dt}' AND {current_flag} = 1
    """)
    spark.sql(f"""
        UPDATE {target}
        SET {expiry_col} = DATE '{OPEN_END_DATE}', {current_flag} = 1
        WHERE {expiry_col} = DATE '{prev_dt}' AND {current_flag} = 0
    """)
    logger.info(f"Idempotency cleanup hoàn tất cho cob_dt={cob_dt}")


def classify_changes(source_df, target_current, business_keys: list, tracked: list, type1: list, close_missing: bool):
    """
    So snapshot nguồn với version hiện hành. Trả về 4 DataFrame (chỉ business key,
    trừ `new_rows` mang đủ cột nguồn):

      new_rows       key mới hoặc đổi tracked column → version mới cần append
      close_keys     key đổi tracked column, hoặc biến mất khỏi nguồn → đóng version cũ
      type1_rows     key chỉ đổi cột Type 1 → ghi đè tại chỗ (đủ cột nguồn)
    """
    src_cols = source_df.columns
    join_expr = [F.col(f"s.{k}") == F.col(f"t.{k}") for k in business_keys]
    joined = source_df.alias("s").join(target_current.alias("t"), join_expr, "left")

    exists = reduce(lambda a, b: a & b, [F.col(f"t.{k}").isNotNull() for k in business_keys])
    tracked_changed = _any_changed(tracked)
    type1_changed = _any_changed(type1)

    new_rows = joined.filter(~exists | tracked_changed).select([F.col(f"s.{c}").alias(c) for c in src_cols])
    changed_keys = joined.filter(exists & tracked_changed).select([F.col(f"s.{k}").alias(k) for k in business_keys])
    type1_rows = joined.filter(exists & ~tracked_changed & type1_changed).select(
        [F.col(f"s.{c}").alias(c) for c in src_cols]
    )

    close_keys = changed_keys
    if close_missing:
        missing_keys = target_current.select(*business_keys).join(
            source_df.select(*business_keys), on=business_keys, how="left_anti"
        )
        close_keys = changed_keys.unionByName(missing_keys)
    return new_rows, close_keys, type1_rows


def run_scd_type2(spark, config: dict, cob_dt: str, logger):
    """Thực thi đầy đủ quy trình SCD Type 2 cho ngày cob_dt."""
    target = get_target_table(config)
    business_keys = config["business_key"]
    scd = config["scd"]
    effective_col = scd["effective_from_column"]
    expiry_col = scd["effective_to_column"]
    current_flag = scd["current_flag_column"]
    close_missing = bool(scd.get("close_missing_keys", False))
    sk_col = _build_sk_col_name(config)
    prev_dt = (datetime.strptime(cob_dt, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

    # Bước 1: Đọc và deduplicate dữ liệu nguồn
    source_df = load_source_df(spark, config, cob_dt).dropDuplicates(business_keys)
    source_df.cache()
    source_count = source_df.count()
    logger.info(f"Số dòng dữ liệu nguồn: {source_count}")
    if source_count == 0:
        source_df.unpersist()
        raise RuntimeError(
            f"Snapshot nguồn của {target} cho cob_dt={cob_dt} rỗng. Bronze chưa nạp ngày này "
            "hoặc nạp hỏng — dừng thay vì coi mọi key là đã biến mất."
        )

    tracked, type1 = split_columns(source_df.columns, business_keys, config.get("tracked_columns"))

    # Bảng chưa tồn tại → tạo với lần nạp SCD2 đầu tiên
    if not table_exists(spark, target):
        logger.warning(f"Target table {target} does not exist. Creating with initial SCD2 load...")
        new_df = _attach_scd2_columns(source_df, business_keys, cob_dt, sk_col, effective_col, expiry_col, current_flag)
        create_iceberg_table_if_not_exists(new_df, target, logger)
        new_df.writeTo(target).append()
        logger.info(f"Created {target} with initial SCD2 data ({source_count} rows)")
        source_df.unpersist()
        return

    ensure_scd2_columns(spark, target, business_keys, sk_col, effective_col, expiry_col, current_flag, logger)

    # Bước 2: Guard backfill
    assert_not_backfilling_past_history(spark, target, effective_col, cob_dt)

    # Bước 3: Idempotency cleanup
    _idempotency_cleanup(spark, target, effective_col, expiry_col, current_flag, cob_dt, prev_dt, logger)

    # Bước 4: Phân loại thay đổi so với version hiện hành
    target_current = spark.table(target).filter(F.col(current_flag) == 1)
    target_count = target_current.count()
    if target_count < 10_000_000:
        logger.info(f"Using broadcast join for target ({target_count} rows)")
        target_current = broadcast(target_current)

    new_rows, close_keys, type1_rows = classify_changes(
        source_df, target_current, business_keys, tracked, type1, close_missing
    )
    close_keys.cache()
    type1_rows.cache()
    close_count = close_keys.count()
    type1_count = type1_rows.count()
    logger.info(f"Version cần đóng: {close_count} | cập nhật Type 1: {type1_count}")

    # Bước 5: Append version mới (INSERT TRƯỚC MERGE)
    new_df = _attach_scd2_columns(new_rows, business_keys, cob_dt, sk_col, effective_col, expiry_col, current_flag)
    new_df.writeTo(target).append()
    logger.info("Đã append version mới")

    suffix = f"{target.replace('.', '_')}_{cob_dt.replace('-', '')}"
    join_on = " AND ".join([f"t.{k} = c.{k}" for k in business_keys])

    # Bước 6: Đóng version cũ (đổi tracked column hoặc key biến mất)
    if close_count > 0:
        view_name = f"close_keys_{suffix}"
        close_keys.createOrReplaceTempView(view_name)
        spark.sql(f"""
            MERGE INTO {target} t
            USING {view_name} c
            ON {join_on} AND t.{current_flag} = 1 AND t.{effective_col} < DATE '{cob_dt}'
            WHEN MATCHED THEN UPDATE SET
                t.{expiry_col}   = DATE '{prev_dt}',
                t.{current_flag} = 0
        """)
        logger.info(f"Đã đóng {close_count} version cũ")

    # Bước 7: Ghi đè cột Type 1 trên version hiện hành
    if type1_count > 0:
        view_name = f"type1_rows_{suffix}"
        type1_rows.createOrReplaceTempView(view_name)
        set_clause = ", ".join(f"t.{col} = c.{col}" for col in type1)
        spark.sql(f"""
            MERGE INTO {target} t
            USING {view_name} c
            ON {join_on} AND t.{current_flag} = 1
            WHEN MATCHED THEN UPDATE SET {set_clause}
        """)
        logger.info(f"Đã cập nhật Type 1 cho {type1_count} version hiện hành")

    close_keys.unpersist()
    type1_rows.unpersist()
    source_df.unpersist()
    logger.info("SCD Type 2 hoàn tất")


def main():
    """Điểm vào: parse args → validate → chạy job → dọn dẹp."""
    args = parse_arguments("Silver SCD Type 2 Job")
    if args.cob_dt is None:
        raise ValueError("--cob_dt là bắt buộc cho SCD Type 2 job")
    logger = get_logger(__name__)
    config = load_config(args.config)
    validate_config(config)
    spark = None
    try:
        spark = get_spark_session(app_name=f"silver-scd2-{config['target']['table']}")
        run_scd_type2(spark, config, args.cob_dt, logger)
    except Exception:
        logger.exception("Job SCD Type 2 thất bại")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
