"""
Job SCD Type 2 dùng chung cho tầng Silver.

Được điều khiển bằng file YAML (metadata-driven).
Lưu toàn bộ lịch sử thay đổi của record trên bảng Iceberg.

Luồng xử lý mỗi ngày:
  1. Đọc snapshot dữ liệu nguồn của ngày cob_dt, deduplicate theo business key.
  2. Idempotency cleanup: xóa insert cũ + khôi phục expire cũ (nếu job đã chạy trước đó).
  3. Join với các record đang active (is_current=1) để phát hiện thay đổi.
  4. Append record mới (INSERT TRƯỚC) — nếu bước này fail, chưa expire gì → rerun an toàn.
  5. MERGE để "đóng" (expire) record cũ: gán effective_to = cob_dt-1, is_current = 0.
     Guard thêm effective_from < cob_dt để tránh vô tình expire row vừa insert ở bước 4.
"""

import sys
from datetime import datetime, timedelta
from functools import reduce
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))
sys.path.insert(0, str(Path(__file__).parent))

from utils.yaml_loader import load_config
from utils.logger import get_logger
from spark.spark_session import get_spark_session
from common_utils import parse_arguments, get_target_table, load_source_df
from spark.iceberg_utils import table_exists, create_iceberg_table_if_not_exists


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
    name = table[4:] if table.startswith("dim_") else table
    return name + "_sk"


def _attach_scd2_columns(source_df, business_keys: list, cob_dt: str, sk_col: str,
                          effective_col: str, expiry_col: str, current_flag: str):
    """
    Gắn các cột metadata SCD2 vào DataFrame trước khi ghi vào bảng đích.
    SK = SHA-256(business_key_1 | ... | cob_dt) — duy nhất mỗi version.
    """
    return (
        source_df
        .withColumn(effective_col, F.to_date(F.lit(cob_dt)))
        .withColumn(expiry_col,    F.to_date(F.lit("9999-12-31")))
        .withColumn(current_flag,  F.lit(1))
        .withColumn(
            sk_col,
            F.sha2(
                F.concat_ws("|", *[F.col(k).cast("string") for k in business_keys], F.lit(cob_dt)),
                256,
            ),
        )
    )


def _idempotency_cleanup(spark, target: str, effective_col: str, expiry_col: str,
                          current_flag: str, cob_dt: str, prev_dt: str, logger):
    """
    Đảm bảo rerun an toàn cho cùng cob_dt:
      - Xóa các row được insert bởi lần chạy trước (effective_from = cob_dt, is_current = 1).
      - Khôi phục các row bị expire bởi lần chạy trước (effective_to = prev_dt, is_current = 0).
    """
    spark.sql(f"""
        DELETE FROM {target}
        WHERE {effective_col} = DATE '{cob_dt}' AND {current_flag} = 1
    """)
    spark.sql(f"""
        UPDATE {target}
        SET {expiry_col} = DATE '9999-12-31', {current_flag} = 1
        WHERE {expiry_col} = DATE '{prev_dt}' AND {current_flag} = 0
    """)
    logger.info(f"Idempotency cleanup hoàn tất cho cob_dt={cob_dt}")


def run_scd_type2(spark, config: dict, cob_dt: str, logger):
    """Thực thi đầy đủ quy trình SCD Type 2 cho ngày cob_dt."""
    target        = get_target_table(config)
    business_keys = config["business_key"]
    scd           = config["scd"]
    effective_col = scd["effective_from_column"]
    expiry_col    = scd["effective_to_column"]
    current_flag  = scd["current_flag_column"]
    sk_col        = _build_sk_col_name(config)
    prev_dt       = (datetime.strptime(cob_dt, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

    # Bước 1: Đọc và deduplicate dữ liệu nguồn
    source_df = load_source_df(spark, config, cob_dt).dropDuplicates(business_keys)
    source_df.cache()
    logger.info(f"Số dòng dữ liệu nguồn: {source_df.count()}")

    # Check table exists — if not, create with initial SCD2 load
    if not table_exists(spark, target):
        logger.warning(f"Target table {target} does not exist. Creating with initial SCD2 load...")
        new_df = _attach_scd2_columns(
            source_df, business_keys, cob_dt, sk_col, effective_col, expiry_col, current_flag
        )
        create_iceberg_table_if_not_exists(new_df, target, logger)
        new_df.writeTo(target).append()
        logger.info(f"Created {target} with initial SCD2 data ({source_df.count()} rows)")
        source_df.unpersist()
        return

    src_cols = source_df.columns

    tracked_columns = config.get("tracked_columns")
    compare_cols = (
        [c for c in src_cols if c in tracked_columns]
        if tracked_columns
        else [c for c in src_cols if c not in business_keys + ["cob_dt"]]
    )

    # Bước 2: Idempotency cleanup
    _idempotency_cleanup(spark, target, effective_col, expiry_col, current_flag,
                         cob_dt, prev_dt, logger)

    # Bước 3: Phát hiện record thay đổi
    target_current = spark.table(target).filter(F.col(current_flag) == 1)

    # Use broadcast hint if target_current is small (< 10M rows)
    # This avoids expensive shuffle joins for dimension tables
    target_count = target_current.count()
    if target_count < 10_000_000:
        logger.info(f"Using broadcast join for target ({target_count} rows)")
        target_current = broadcast(target_current)

    join_expr = [source_df[k] == target_current[k] for k in business_keys]
    joined = source_df.alias("s").join(target_current.alias("t"), join_expr, "left")

    change_expr = reduce(
        lambda a, b: a | b,
        [
            F.coalesce(F.col(f"s.{c}").cast("string"), F.lit("__NULL__")) !=
            F.coalesce(F.col(f"t.{c}").cast("string"), F.lit("__NULL__"))
            for c in compare_cols
        ],
    )
    exists_in_target = reduce(
        lambda a, b: a & b,
        [F.col(f"t.{k}").isNotNull() for k in business_keys],
    )

    changed_keys = (
        joined
        .filter(exists_in_target & change_expr)
        .select([F.col(f"s.{k}").alias(k) for k in business_keys])
    )
    changed_keys.cache()
    changed_count = changed_keys.count()
    logger.info(f"Số record thay đổi: {changed_count}")

    # Bước 4: Append version mới (INSERT TRƯỚC MERGE)
    new_df = (
        joined
        .filter(~exists_in_target | change_expr)
        .select([F.col(f"s.{c}").alias(c) for c in src_cols])
    )
    new_df = _attach_scd2_columns(
        new_df, business_keys, cob_dt, sk_col, effective_col, expiry_col, current_flag
    )
    new_df.writeTo(target).append()
    logger.info("Đã append version mới")

    # Bước 5: Expire record cũ đã bị thay đổi
    if changed_count > 0:
        view_name = f"changed_keys_{target.replace('.', '_')}_{cob_dt.replace('-', '')}"
        changed_keys.createOrReplaceTempView(view_name)
        join_on = " AND ".join([f"t.{k} = c.{k}" for k in business_keys])

        spark.sql(f"""
            MERGE INTO {target} t
            USING {view_name} c
            ON {join_on} AND t.{current_flag} = 1 AND t.{effective_col} < DATE '{cob_dt}'
            WHEN MATCHED THEN UPDATE SET
                t.{expiry_col}   = DATE '{prev_dt}',
                t.{current_flag} = 0
        """)
        logger.info(f"Đã expire {changed_count} record cũ")

    changed_keys.unpersist()
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
    except Exception as e:
        logger.error(f"Job SCD Type 2 thất bại: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
