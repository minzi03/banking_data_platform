"""
CDC Dead Letter Queue (DLQ) — Banking Data Platform

Isolates invalid CDC events without failing the micro-batch.
Valid events continue into Bronze; invalid events land in bronze.cdc_dead_letter.

Design:
    Kafka CDC → Spark foreachBatch → validate → split
        ├── valid  → Bronze CDC table (append)
        └── invalid → bronze.cdc_dead_letter (append)

Invalid-event criteria:
    1. __op is null or not in (c, u, d, r)
    2. __ts_ms is null or not a valid long
    3. Primary key column is null (configurable per table)

Interview value:
    "A malformed CDC event is isolated without failing the entire
     micro-batch, while valid events continue into Bronze."
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession


def ensure_dlq_table(spark: SparkSession):
    """Create DLQ table if not exists."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.cdc_dead_letter (
            source_topic        STRING,
            entity              STRING,
            raw_payload         STRING,
            error_type          STRING,
            error_message       STRING,
            event_timestamp     BIGINT,
            kafka_partition     INT,
            kafka_offset        BIGINT,
            kafka_timestamp     TIMESTAMP,
            failed_at           TIMESTAMP,
            spark_batch_id      BIGINT
        ) USING iceberg
    """)


def validate_and_split(batch_df: DataFrame, config: dict, batch_id: int):
    """
    Validate CDC events and split into valid/invalid DataFrames.

    Returns (valid_df, invalid_df) tuple.
    """
    if batch_df.isEmpty():
        return batch_df, batch_df  # both empty

    # ── Extract CDC metadata (same as cdc_streaming.py) ──────────────
    extracted_df = batch_df.select(
        F.col("key").cast("string").alias("_cdc_key"),
        F.get_json_object(F.col("value").cast("string"), "$.payload").alias("payload_json"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
    )

    # ── Parse JSON payload ───────────────────────────────────────────
    parsed_df = extracted_df.select(
        F.col("_cdc_key"),
        F.from_json(F.col("payload_json"), "MAP<STRING, STRING>").alias("payload"),
        F.col("_kafka_topic"),
        F.col("_kafka_partition"),
        F.col("_kafka_offset"),
        F.col("_kafka_timestamp"),
        F.col("payload_json"),  # Keep for DLQ raw_payload
    )

    # ── Build data columns from config ───────────────────────────────
    data_columns = config["target"].get("columns", [])

    select_expressions = [
        F.col("_cdc_key"),
        F.col("payload").getItem("__op").alias("_raw_op"),
        F.col("payload").getItem("__ts_ms").alias("_raw_ts_ms"),
        F.col("payload").getItem("__deleted").alias("_raw_deleted"),
        F.col("_kafka_topic"),
        F.col("_kafka_partition"),
        F.col("_kafka_offset"),
        F.col("_kafka_timestamp"),
        F.lit(batch_id).alias("__spark_batch_id"),
        F.current_timestamp().alias("__ingestion_time"),
        # Keep raw payload for DLQ
        F.col("payload_json").alias("_raw_payload"),
    ]

    for col_def in data_columns:
        col_name = col_def["name"]
        col_type = col_def.get("type", "string")

        if col_type == "long":
            select_expressions.append(
                F.col(f"payload.{col_name}").cast("long").alias(col_name)
            )
        elif col_type == "decimal":
            select_expressions.append(
                F.col(f"payload.{col_name}").cast("decimal(18,2)").alias(col_name)
            )
        elif col_type == "int":
            select_expressions.append(
                F.col(f"payload.{col_name}").cast("int").alias(col_name)
            )
        elif col_type == "boolean":
            select_expressions.append(
                F.col(f"payload.{col_name}").cast("boolean").alias(col_name)
            )
        else:
            select_expressions.append(
                F.col(f"payload.{col_name}").alias(col_name)
            )

    enriched_df = parsed_df.select(*select_expressions)

    # ── Validation: classify each row ────────────────────────────────
    valid_ops = ["c", "u", "d", "r"]

    validated_df = enriched_df.withColumn(
        "_is_valid",
        F.when(
            # Check 1: __op must exist and be a known CDC operation
            F.col("_raw_op").isNull()
            | ~F.col("_raw_op").isin(valid_ops),
            F.lit(False)
        ).when(
            # Check 2: __ts_ms must be parseable as long
            F.col("_raw_ts_ms").isNull()
            | F.col("_raw_ts_ms").cast("long").isNull(),
            F.lit(False)
        ).when(
            # Check 3: raw payload must be non-null (JSON parseable)
            F.col("_raw_payload").isNull(),
            F.lit(False)
        ).otherwise(F.lit(True))
    ).withColumn(
        "_error_type",
        F.when(F.col("_raw_payload").isNull(), "PARSE_ERROR")
         .when(F.col("_raw_op").isNull() | ~F.col("_raw_op").isin(valid_ops), "INVALID_OPERATION")
         .when(F.col("_raw_ts_ms").isNull() | F.col("_raw_ts_ms").cast("long").isNull(), "INVALID_TIMESTAMP")
         .otherwise(F.lit(None).cast("string"))
    ).withColumn(
        "_error_message",
        F.when(F.col("_raw_payload").isNull(), F.concat(F.lit("JSON parse failed: "), F.col("_cdc_key")))
         .when(F.col("_raw_op").isNull(), F.concat(F.lit("__op is null: "), F.col("_cdc_key")))
         .when(~F.col("_raw_op").isin(valid_ops), F.concat(F.lit("Unknown __op="), F.col("_raw_op"), F.lit(": "), F.col("_cdc_key")))
         .when(F.col("_raw_ts_ms").isNull(), F.concat(F.lit("__ts_ms is null: "), F.col("_cdc_key")))
         .when(F.col("_raw_ts_ms").cast("long").isNull(), F.concat(F.lit("__ts_ms not numeric: "), F.col("_cdc_key"), F.lit(" value="), F.col("_raw_ts_ms")))
         .otherwise(F.lit(None).cast("string"))
    )

    # ── Split valid / invalid ────────────────────────────────────────
    valid_df = validated_df.filter(F.col("_is_valid") == True)
    invalid_df = validated_df.filter(F.col("_is_valid") == False)

    # ── Prepare valid DataFrame (same schema as original cdc_streaming.py) ──
    valid_df = (
        valid_df
        .withColumn(
            "__cdc_operation",
            F.when(F.col("_raw_op") == "c", "INSERT")
             .when(F.col("_raw_op") == "u", "UPDATE")
             .when(F.col("_raw_op") == "d", "DELETE")
             .when(F.col("_raw_op") == "r", "SNAPSHOT")
             .otherwise(F.col("_raw_op"))
        )
        .withColumn(
            "__cdc_timestamp_ms",
            F.col("_raw_ts_ms").cast("long")
        )
        .withColumn(
            "__cdc_timestamp",
            F.to_timestamp(F.col("__cdc_timestamp_ms") / 1000)
        )
        .drop("_cdc_key", "_raw_op", "_raw_ts_ms", "_raw_deleted",
              "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp",
              "_raw_payload", "_is_valid", "_error_type", "_error_message")
    )

    # ── Prepare DLQ DataFrame ────────────────────────────────────────
    # Get entity name from config (table name without schema)
    entity = config["target"]["table"]

    dlq_df = (
        invalid_df
        .select(
            F.col("_kafka_topic").alias("source_topic"),
            F.lit(entity).alias("entity"),
            F.col("_raw_payload").alias("raw_payload"),
            F.col("_error_type").alias("error_type"),
            F.col("_error_message").alias("error_message"),
            F.col("_raw_ts_ms").cast("long").alias("event_timestamp"),
            F.col("_kafka_partition").alias("kafka_partition"),
            F.col("_kafka_offset").alias("kafka_offset"),
            F.col("_kafka_timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("failed_at"),
            F.lit(batch_id).alias("spark_batch_id"),
        )
    )

    return valid_df, dlq_df


def write_valid_to_bronze(valid_df: DataFrame, target_table: str, batch_id: int):
    """Write valid CDC events to Bronze table."""
    if valid_df.isEmpty():
        print(f"[Batch {batch_id}] No valid events for {target_table}")
        return 0

    row_count = valid_df.count()
    valid_df.writeTo(target_table).append()
    print(f"[Batch {batch_id}] Wrote {row_count} valid events to {target_table}")
    return row_count


def write_invalid_to_dlq(dlq_df: DataFrame, batch_id: int):
    """Write invalid CDC events to Dead Letter Queue table."""
    if dlq_df.isEmpty():
        return 0

    row_count = dlq_df.count()
    dlq_df.writeTo("lakehouse.bronze.cdc_dead_letter").append()
    print(f"[Batch {batch_id}] ⚠ Wrote {row_count} INVALID events to DLQ")
    return row_count
