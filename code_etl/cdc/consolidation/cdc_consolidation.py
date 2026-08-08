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

Limitations:
    - Event ordering uses __cdc_timestamp_ms + __spark_batch_id (not Kafka offset)
    - Kafka metadata not persisted in Bronze CDC yet
    - Idempotent: re-run produces same result (MERGE is upsert/delete)
"""

import argparse
import yaml
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window


# =============================================================================
# Configuration
# =============================================================================

def load_config(config_path: str) -> dict:
    """Load YAML configuration for consolidation job."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


# =============================================================================
# Watermark Management
# =============================================================================

def ensure_watermark_table(spark: SparkSession):
    """Create watermark table if not exists."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.meta.cdc_watermark (
            table_name           VARCHAR(100),
            last_cdc_timestamp_ms BIGINT,
            last_spark_batch_id  BIGINT,
            last_processed_at    TIMESTAMP
        ) USING iceberg
    """)


def read_watermark(spark: SparkSession, table_name: str) -> dict:
    """Read current watermark for a table."""
    try:
        result = spark.sql(f"""
            SELECT last_cdc_timestamp_ms, last_spark_batch_id
            FROM lakehouse.meta.cdc_watermark
            WHERE table_name = '{table_name}'
        """).collect()

        if result:
            return {
                "timestamp_ms": result[0]["last_cdc_timestamp_ms"],
                "batch_id": result[0]["last_spark_batch_id"]
            }
    except Exception:
        pass

    # Default: start from beginning
    return {"timestamp_ms": 0, "batch_id": 0}


def update_watermark(spark: SparkSession, table_name: str,
                     max_timestamp_ms: int, max_batch_id: int):
    """Update watermark after successful MERGE."""
    spark.sql(f"""
        MERGE INTO lakehouse.meta.cdc_watermark t
        USING (
            SELECT
                '{table_name}' as table_name,
                {max_timestamp_ms} as last_cdc_timestamp_ms,
                {max_batch_id} as last_spark_batch_id,
                CURRENT_TIMESTAMP() as last_processed_at
        ) s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
            last_cdc_timestamp_ms = s.last_cdc_timestamp_ms,
            last_spark_batch_id = s.last_spark_batch_id,
            last_processed_at = s.last_processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)


# =============================================================================
# Incremental Read
# =============================================================================

def read_incremental_cdc(spark: SparkSession, config: dict, watermark: dict) -> DataFrame:
    """Read incremental CDC events from Bronze since last watermark."""
    source_table = config["source_table"]
    ts_col = config["metadata"]["event_timestamp_ms_column"]
    batch_col = config["metadata"]["batch_id_column"]

    # Read events after watermark
    df = spark.sql(f"""
        SELECT *
        FROM {source_table}
        WHERE ({ts_col} > {watermark['timestamp_ms']})
           OR ({ts_col} = {watermark['timestamp_ms']}
               AND {batch_col} > {watermark['batch_id']})
    """)

    return df


# =============================================================================
# Type Conversions
# =============================================================================

def cast_columns(df: DataFrame, config: dict) -> DataFrame:
    """Apply type conversions defined in YAML config."""
    conversions = config.get("conversions", {})

    for col_name, conv_type in conversions.items():
        if col_name not in df.columns:
            continue

        if conv_type == "epoch_days_to_date":
            # Convert epoch days to DATE: 1970-01-01 + interval
            # date_add requires INT, so cast BIGINT → INT
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNotNull() & (F.col(col_name) != ""),
                       F.date_add(F.lit("1970-01-01").cast("date"), F.col(col_name).cast("int")))
                .otherwise(F.lit(None).cast("date"))
            )

        elif conv_type == "varchar_to_int":
            # Convert VARCHAR "1"/"0" to INTEGER
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNotNull() & (F.col(col_name) != ""),
                       F.col(col_name).cast("int"))
                .otherwise(F.lit(None).cast("int"))
            )

        elif conv_type == "epoch_micros_to_timestamp":
            # Convert epoch microseconds to TIMESTAMP
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNotNull() & (F.col(col_name) != ""),
                       F.from_unixtime(F.col(col_name) / 1000000.0))
                .otherwise(F.lit(None).cast("timestamp"))
            )

        elif conv_type == "decimal_nullable":
            # Handle empty string → NULL for DECIMAL
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNotNull() & (F.col(col_name) != ""),
                       F.col(col_name))
                .otherwise(F.lit(None).cast("decimal(18,2)"))
            )

    return df


# =============================================================================
# Deduplication
# =============================================================================

def deduplicate_latest(df: DataFrame, config: dict) -> DataFrame:
    """Keep only the latest event per business key."""
    business_key = config["business_key"]
    ts_col = config["metadata"]["event_timestamp_ms_column"]
    batch_col = config["metadata"]["batch_id_column"]

    # Window: order by timestamp DESC, batch_id DESC (deterministic)
    window = Window.partitionBy(business_key).orderBy(
        F.col(ts_col).desc(),
        F.col(batch_col).desc()
    )

    # Keep first row (= latest event)
    df_deduped = (
        df.withColumn("__rn", F.row_number().over(window))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )

    return df_deduped


# =============================================================================
# MERGE into Silver Current
# =============================================================================

def merge_current_state(spark: SparkSession, df: DataFrame, config: dict):
    """MERGE deduplicated CDC events into Silver current-state table."""
    target_table = config["target_table"]
    business_key = config["business_key"]
    op_col = config["metadata"]["operation_column"]
    ts_col = config["metadata"]["event_timestamp_ms_column"]
    batch_col = config["metadata"]["batch_id_column"]

    # Get target table columns to ensure we only update existing columns
    target_columns = [row["col_name"] for row in spark.sql(f"DESCRIBE {target_table}").collect()]

    # Get source columns that exist in target (exclude business key and operation for UPDATE)
    # Include timestamp columns for traceability
    all_columns = df.columns
    update_cols = [c for c in all_columns if c in target_columns and c not in [
        business_key, op_col, "__ingestion_time"
    ]]

    # Create temporary view
    df.createOrReplaceTempView("cdc_latest")

    # Build SET clause for UPDATE
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])

    # Build INSERT columns and values (only columns that exist in target)
    insert_cols = [c for c in all_columns if c in target_columns]
    insert_cols_str = ", ".join([f"t.{c}" for c in insert_cols])
    insert_vals_str = ", ".join([f"s.{c}" for c in insert_cols])

    # Add __consolidated_at timestamp if not exists
    try:
        spark.sql(f"""
            ALTER TABLE {target_table} ADD COLUMNS (__consolidated_at TIMESTAMP)
        """)
    except Exception:
        pass  # Column already exists

    # MERGE logic
    merge_sql = f"""
        MERGE INTO {target_table} t
        USING cdc_latest s
        ON t.{business_key} = s.{business_key}

        -- Matched + DELETE → DELETE
        WHEN MATCHED AND s.{op_col} = 'DELETE' THEN DELETE

        -- Matched + non-DELETE → UPDATE
        WHEN MATCHED AND s.{op_col} <> 'DELETE' THEN UPDATE SET
            {set_clause},
            t.__consolidated_at = CURRENT_TIMESTAMP()

        -- Not matched + non-DELETE → INSERT
        WHEN NOT MATCHED AND s.{op_col} <> 'DELETE' THEN INSERT ({insert_cols_str}, __consolidated_at)
            VALUES ({insert_vals_str}, CURRENT_TIMESTAMP())
    """

    spark.sql(merge_sql)


# =============================================================================
# Main Entry Point
# =============================================================================

def run(config_path: str):
    """Main consolidation pipeline."""
    # Load config
    config = load_config(config_path)
    table_name = config["target_table"].split(".")[-1]

    print(f"\n{'='*60}")
    print(f"CDC Consolidation: {table_name}")
    print(f"{'='*60}")

    # Initialize Spark
    spark = (
        SparkSession.builder
        .appName(f"cdc_consolidation_{table_name}")
        .getOrCreate()
    )

    # Ensure watermark table exists
    ensure_watermark_table(spark)

    # Read watermark
    watermark = read_watermark(spark, table_name)
    print(f"Watermark: ts_ms={watermark['timestamp_ms']}, batch_id={watermark['batch_id']}")

    # Read incremental CDC events
    df_incremental = read_incremental_cdc(spark, config, watermark)
    event_count = df_incremental.count()
    print(f"Incremental events: {event_count}")

    if event_count == 0:
        print("No new events. Skipping.")
        return

    # Apply type conversions
    df_converted = cast_columns(df_incremental, config)

    # Deduplicate (keep latest per business key)
    df_deduped = deduplicate_latest(df_converted, config)
    deduped_count = df_deduped.count()
    print(f"After dedup: {deduped_count} unique records")

    # MERGE into Silver Current
    merge_current_state(spark, df_deduped, config)
    print(f"MERGE completed successfully")

    # Update watermark (only after successful MERGE)
    max_ts = df_deduped.agg(F.max(config["metadata"]["event_timestamp_ms_column"])).collect()[0][0]
    max_batch = df_deduped.agg(F.max(config["metadata"]["batch_id_column"])).collect()[0][0]

    update_watermark(spark, table_name, max_ts, max_batch)
    print(f"Watermark updated: ts_ms={max_ts}, batch_id={max_batch}")

    print(f"{'='*60}")
    print(f"Consolidation complete: {table_name}")
    print(f"{'='*60}\n")


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CDC Consolidation Engine")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()

    run(args.config)
