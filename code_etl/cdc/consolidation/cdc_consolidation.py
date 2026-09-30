"""
CDC Consolidation Engine — Silver Current-State Tables

Reads incremental events from Bronze CDC tables, deduplicates,
and MERGEs into Silver current-state tables (dim_customer_current, dim_account_current).

Usage:
    spark-submit --master spark://spark-master:7077 \
        cdc_consolidation.py \
        --config /opt/project/code_etl/cdc/consolidation/config/cdc_consolidation_customer.yml

Architecture:
    Bronze CDC (append-only) → incremental read → dedup → MERGE → Silver Current

Hai khái niệm tách bạch:

    TIẾN ĐỘ (đã đọc tới đâu)   = snapshot Iceberg của bảng Bronze CDC.
        Bronze CDC chỉ append, nên "các event chưa xử lý" chính là các append
        snapshot sau snapshot đã xử lý lần trước. Mỗi lượt chạy chốt MỘT snapshot
        cuối (end) và đọc đúng khoảng (watermark, end]. Mọi action trong lượt chạy
        (count, MERGE, cập nhật watermark) cùng nhìn một khoảng đó.

        Bản cũ dùng watermark (max __cdc_timestamp_ms, max __spark_batch_id) và
        đọc lại bảng mới nhất ở mỗi action. Hai lỗ hổng: event về Bronze muộn với
        ts nhỏ hơn watermark bị bỏ qua vĩnh viễn; event append giữa MERGE và lúc
        tính max bị đẩy qua watermark mà chưa hề được MERGE.

    THỨ TỰ (event nào mới hơn) = (__cdc_timestamp_ms, __spark_batch_id, __kafka_offset)
        — ADR-0010. Dedup trong một lượt dùng cả ba (offset phân định event cùng ms,
        cùng micro-batch, vd INSERT + UPDATE trong một transaction); guard MERGE giữa
        các lượt dùng ts (s.ts >= t.ts).

Limitations:
    - Dòng Bronze CDC ghi trước 2026-09-30 không có offset (NULL): dedup giữa hai dòng
      cũ cùng ms, cùng batch vẫn không tất định.
    - Key đã bị DELETE rồi nhận một event cũ hơn đến muộn sẽ được INSERT lại
      (bảng current không giữ tombstone).
"""

import argparse
import sys
from pathlib import Path

import pyspark.sql.functions as F
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))

WATERMARK_TABLE = "lakehouse.meta.cdc_watermark"

# =============================================================================
# Configuration
# =============================================================================


def load_config(config_path: str) -> dict:
    """Load YAML configuration for consolidation job."""
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return config


# =============================================================================
# Watermark Management
# =============================================================================


def ensure_watermark_table(spark: SparkSession):
    """Tạo bảng watermark nếu chưa có; thêm cột last_snapshot_id cho bảng cũ."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.meta")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
            table_name            STRING,
            last_snapshot_id      BIGINT,
            last_cdc_timestamp_ms BIGINT,
            last_spark_batch_id   BIGINT,
            last_processed_at     TIMESTAMP
        ) USING iceberg
    """)
    columns = {row["col_name"] for row in spark.sql(f"DESCRIBE TABLE {WATERMARK_TABLE}").collect()}
    if "last_snapshot_id" not in columns:
        spark.sql(f"ALTER TABLE {WATERMARK_TABLE} ADD COLUMNS (last_snapshot_id BIGINT)")


def read_watermark(spark: SparkSession, table_name: str) -> dict:
    """
    Đọc watermark của một bảng đích. Chưa có dòng → chưa từng chạy.

    Không nuốt exception: bản cũ bắt mọi lỗi rồi trả về 0, biến một lỗi catalog
    thành một lần full reprocess âm thầm.
    """
    rows = spark.sql(f"""
        SELECT last_snapshot_id, last_cdc_timestamp_ms, last_spark_batch_id
        FROM {WATERMARK_TABLE}
        WHERE table_name = '{table_name}'
    """).collect()
    if not rows:
        return {"snapshot_id": None, "timestamp_ms": None, "batch_id": None}
    row = rows[0]
    return {
        "snapshot_id": row["last_snapshot_id"],
        "timestamp_ms": row["last_cdc_timestamp_ms"],
        "batch_id": row["last_spark_batch_id"],
    }


def update_watermark(spark: SparkSession, table_name: str, snapshot_id: int, max_timestamp_ms, max_batch_id):
    """Ghi watermark SAU KHI MERGE thành công. ts/batch chỉ để quan sát, không điều khiển đọc."""

    def sql_value(v):
        return "CAST(NULL AS BIGINT)" if v is None else str(int(v))

    spark.sql(f"""
        MERGE INTO {WATERMARK_TABLE} t
        USING (
            SELECT
                '{table_name}' AS table_name,
                {sql_value(snapshot_id)} AS last_snapshot_id,
                {sql_value(max_timestamp_ms)} AS last_cdc_timestamp_ms,
                {sql_value(max_batch_id)} AS last_spark_batch_id,
                CURRENT_TIMESTAMP() AS last_processed_at
        ) s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
            last_snapshot_id = s.last_snapshot_id,
            last_cdc_timestamp_ms = s.last_cdc_timestamp_ms,
            last_spark_batch_id = s.last_spark_batch_id,
            last_processed_at = s.last_processed_at
        WHEN NOT MATCHED THEN INSERT
            (table_name, last_snapshot_id, last_cdc_timestamp_ms, last_spark_batch_id, last_processed_at)
            VALUES (s.table_name, s.last_snapshot_id, s.last_cdc_timestamp_ms, s.last_spark_batch_id,
                    s.last_processed_at)
    """)


# =============================================================================
# Incremental Read (theo snapshot Iceberg)
# =============================================================================


def current_snapshot_id(spark: SparkSession, source_table: str):
    """Snapshot hiện hành của bảng nguồn; None nếu bảng chưa có snapshot nào."""
    rows = spark.sql(f"""
        SELECT snapshot_id
        FROM {source_table}.history
        WHERE is_current_ancestor
        ORDER BY made_current_at DESC
        LIMIT 1
    """).collect()
    return rows[0]["snapshot_id"] if rows else None


def snapshot_exists(spark: SparkSession, source_table: str, snapshot_id: int) -> bool:
    """Snapshot còn trong lineage hiện hành (chưa bị expire / rollback)?"""
    return bool(
        spark.sql(f"""
            SELECT 1 FROM {source_table}.history
            WHERE snapshot_id = {int(snapshot_id)} AND is_current_ancestor
            LIMIT 1
        """).take(1)
    )


def plan_read(start_snapshot_id, end_snapshot_id, start_exists: bool) -> str:
    """
    Quyết định cách đọc một lượt:

        "skip"        không có snapshot mới (hoặc bảng rỗng)
        "full"        chưa có watermark, hoặc snapshot watermark đã bị expire —
                      đọc toàn bộ tại snapshot end. An toàn vì MERGE có guard thứ tự.
        "incremental" đọc các append trong khoảng (start, end]
    """
    if end_snapshot_id is None or start_snapshot_id == end_snapshot_id:
        return "skip"
    if start_snapshot_id is None or not start_exists:
        return "full"
    return "incremental"


def read_cdc_events(spark: SparkSession, source_table: str, mode: str, start_snapshot_id, end_snapshot_id):
    """Đọc event CDC đúng một khoảng snapshot cố định."""
    reader = spark.read.format("iceberg")
    if mode == "incremental":
        reader = reader.option("start-snapshot-id", str(start_snapshot_id)).option(
            "end-snapshot-id", str(end_snapshot_id)
        )
    else:
        reader = reader.option("snapshot-id", str(end_snapshot_id))
    return reader.load(source_table)


# =============================================================================
# Type Conversions
# =============================================================================


def _try_cast(col_name: str, sql_type: str):
    """try_cast: chuỗi rỗng / sai định dạng → NULL, kể cả khi bật ANSI."""
    return F.expr(f"try_cast(`{col_name}` AS {sql_type})")


def cast_columns(df: DataFrame, config: dict) -> DataFrame:
    """
    Apply type conversions defined in YAML config.

    Nguồn có thể là số (BIGINT/DECIMAL trong DDL Bronze CDC) hoặc chuỗi (payload
    JSON map<string,string>). Bản cũ so sánh `col != ""` trên cột số: Spark ép ""
    sang số thành NULL, cả điều kiện thành NULL, và MỌI giá trị rơi vào nhánh
    NULL — date_of_birth, open_date, balance… của Silver Current đều NULL.
    try_cast xử lý đồng nhất cả hai kiểu, không cần so sánh chuỗi.
    """
    conversions = config.get("conversions", {})

    for col_name, conv_type in conversions.items():
        if col_name not in df.columns:
            continue

        if conv_type == "epoch_days_to_date":
            # Debezium io.debezium.time.Date = số ngày kể từ 1970-01-01
            df = df.withColumn(col_name, F.date_add(F.lit("1970-01-01").cast("date"), _try_cast(col_name, "INT")))

        elif conv_type == "varchar_to_int":
            df = df.withColumn(col_name, _try_cast(col_name, "INT"))

        elif conv_type == "epoch_micros_to_timestamp":
            # Debezium MicroTimestamp; timestamp_micros giữ nguyên instant, không
            # đi qua chuỗi định dạng theo session timezone như from_unixtime.
            df = df.withColumn(col_name, F.expr(f"timestamp_micros(try_cast(`{col_name}` AS BIGINT))"))

        elif conv_type == "decimal_nullable":
            df = df.withColumn(col_name, _try_cast(col_name, "DECIMAL(18,2)"))

        else:
            raise ValueError(f"Loại conversion không hỗ trợ: {conv_type} (cột {col_name})")

    return df


# =============================================================================
# Deduplication
# =============================================================================


def deduplicate_latest(df: DataFrame, config: dict) -> DataFrame:
    """Keep only the latest event per business key."""
    business_key = config["business_key"]
    ts_col = config["metadata"]["event_timestamp_ms_column"]
    batch_col = config["metadata"]["batch_id_column"]

    # Window: timestamp DESC, batch_id DESC, rồi Kafka offset DESC (ADR-0010).
    # Hai event cùng key có thể trùng cả ms lẫn micro-batch — INSERT + UPDATE trong
    # một transaction (đo trên stack 2026-09-30). Cùng key → cùng partition Kafka, nên
    # offset phân định dứt khoát. Dòng Bronze trước khi có cột mang NULL, xếp sau.
    order = [F.col(ts_col).desc(), F.col(batch_col).desc()]
    if "__kafka_offset" in df.columns:
        order.append(F.col("__kafka_offset").desc_nulls_last())
    window = Window.partitionBy(business_key).orderBy(*order)

    # Keep first row (= latest event)
    df_deduped = df.withColumn("__rn", F.row_number().over(window)).filter(F.col("__rn") == 1).drop("__rn")

    return df_deduped


# =============================================================================
# MERGE into Silver Current
# =============================================================================


def build_merge_sql(
    target_table: str, business_key: str, op_col: str, ts_col: str, update_cols: list, insert_cols: list
):
    """
    MERGE trạng thái hiện hành, có guard thứ tự.

    Chỉ áp event vào dòng đích khi event đó KHÔNG cũ hơn trạng thái đang có
    (s.ts >= t.ts). Nhờ vậy event đến muộn hoặc replay không kéo trạng thái lùi,
    và chạy lại cùng khoảng event cho ra đúng trạng thái cũ (idempotent).
    """
    newer = f"(t.{ts_col} IS NULL OR s.{ts_col} >= t.{ts_col})"
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
    insert_cols_str = ", ".join(insert_cols)
    insert_vals_str = ", ".join([f"s.{c}" for c in insert_cols])
    return f"""
        MERGE INTO {target_table} t
        USING cdc_latest s
        ON t.{business_key} = s.{business_key}

        -- Matched + DELETE (không cũ hơn) → DELETE
        WHEN MATCHED AND s.{op_col} = 'DELETE' AND {newer} THEN DELETE

        -- Matched + non-DELETE (không cũ hơn) → UPDATE
        WHEN MATCHED AND s.{op_col} <> 'DELETE' AND {newer} THEN UPDATE SET
            {set_clause},
            t.__consolidated_at = CURRENT_TIMESTAMP()

        -- Not matched + non-DELETE → INSERT
        WHEN NOT MATCHED AND s.{op_col} <> 'DELETE' THEN INSERT ({insert_cols_str}, __consolidated_at)
            VALUES ({insert_vals_str}, CURRENT_TIMESTAMP())
    """


def merge_current_state(spark: SparkSession, df: DataFrame, config: dict):
    """MERGE deduplicated CDC events into Silver current-state table."""
    target_table = config["target_table"]
    business_key = config["business_key"]
    op_col = config["metadata"]["operation_column"]
    ts_col = config["metadata"]["event_timestamp_ms_column"]
    batch_col = config["metadata"]["batch_id_column"]

    target_columns = [row["col_name"] for row in spark.sql(f"DESCRIBE TABLE {target_table}").collect()]

    # DDL đặt tên cột nguồn là __source_spark_batch_id; Bronze gọi là __spark_batch_id.
    if "__source_spark_batch_id" in target_columns and batch_col in df.columns:
        df = df.withColumn("__source_spark_batch_id", F.col(batch_col))

    if "__consolidated_at" not in target_columns:
        spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS (__consolidated_at TIMESTAMP)")

    all_columns = df.columns
    insert_cols = [c for c in all_columns if c in target_columns and c != "__consolidated_at"]
    update_cols = [c for c in insert_cols if c not in [business_key, "__ingestion_time"]]

    df.createOrReplaceTempView("cdc_latest")
    spark.sql(build_merge_sql(target_table, business_key, op_col, ts_col, update_cols, insert_cols))


# =============================================================================
# Main Entry Point
# =============================================================================


def run(config_path: str, spark: SparkSession | None = None):
    """Main consolidation pipeline."""
    config = load_config(config_path)
    source_table = config["source_table"]
    table_name = config["target_table"].split(".")[-1]
    ts_col = config["metadata"]["event_timestamp_ms_column"]
    batch_col = config["metadata"]["batch_id_column"]

    print(f"\n{'=' * 60}")
    print(f"CDC Consolidation: {table_name}")
    print(f"{'=' * 60}")

    if spark is None:
        from spark.spark_session import get_spark_session

        spark = get_spark_session(app_name=f"cdc_consolidation_{table_name}")

    ensure_watermark_table(spark)
    watermark = read_watermark(spark, table_name)
    start = watermark["snapshot_id"]
    end = current_snapshot_id(spark, source_table)
    start_exists = start is not None and snapshot_exists(spark, source_table, start)
    mode = plan_read(start, end, start_exists)
    print(f"Watermark snapshot={start} → end snapshot={end} | mode={mode}")
    if start is not None and not start_exists:
        print(f"WARNING: snapshot watermark {start} không còn trong lineage (expire?) — đọc lại toàn bộ tại {end}")

    if mode == "skip":
        print("No new snapshots. Skipping.")
        return

    df_incremental = read_cdc_events(spark, source_table, mode, start, end)
    df_deduped = deduplicate_latest(cast_columns(df_incremental, config), config).cache()
    deduped_count = df_deduped.count()
    print(f"Unique keys in window: {deduped_count}")

    if deduped_count > 0:
        merge_current_state(spark, df_deduped, config)
        print("MERGE completed successfully")

    stats = df_deduped.agg(F.max(ts_col).alias("ts"), F.max(batch_col).alias("batch")).first()
    # Watermark chỉ tiến SAU KHI MERGE thành công; crash trước đó → lượt sau đọc lại
    # đúng khoảng này, MERGE idempotent nhờ guard thứ tự.
    update_watermark(
        spark,
        table_name,
        end,
        stats["ts"] if stats["ts"] is not None else watermark["timestamp_ms"],
        stats["batch"] if stats["batch"] is not None else watermark["batch_id"],
    )
    df_deduped.unpersist()
    print(f"Watermark updated: snapshot={end}")

    print(f"{'=' * 60}")
    print(f"Consolidation complete: {table_name}")
    print(f"{'=' * 60}\n")


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CDC Consolidation Engine")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()

    run(args.config)
