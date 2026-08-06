#!/usr/bin/env python3
"""
Spark Structured Streaming Job for CDC — Banking Data Platform

Reads CDC events from Kafka (produced by Debezium) and writes to Iceberg Bronze tables.

Usage:
    spark-submit --master spark://spark-master:7077 \
        cdc_streaming.py \
        --config /path/to/config.yml \
        --kafka_bootstrap kafka:9092
"""

import argparse
import os
import yaml
from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DecimalType, TimestampType, BooleanType
)


def create_spark_session() -> SparkSession:
    """Create SparkSession with Iceberg REST catalog configuration."""
    return (
        SparkSession.builder
        .appName("CDC_Streaming")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/lakehouse")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "Minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )


def get_cdc_schema(table_name: str) -> StructType:
    """
    Get the schema for CDC events based on table name.
    Debezium with ExtractNewRecordState produces flattened JSON with __op, __ts_ms fields.
    """
    # Use MapType for flexible JSON parsing - schema will be inferred
    return None  # Let Spark infer schema from JSON


def process_cdc_batch(batch_df, batch_id: int, target_table: str, config: Dict):
    """
    Process each micro-batch of CDC events and write to Iceberg.

    Args:
        batch_df: DataFrame containing CDC events from Kafka
        batch_id: Micro-batch ID
        target_table: Full Iceberg table name (e.g., bronze.core_account_cdc)
        config: YAML configuration
    """
    if batch_df.isEmpty():
        return

    # Debezium message structure: {"schema":{...},"payload":{...}}
    # Extract the payload JSON string first using get_json_object
    extracted_df = batch_df.select(
        F.col("key").cast("string").alias("_cdc_key"),
        F.get_json_object(F.col("value").cast("string"), "$.payload").alias("payload_json"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
    )

    # Now parse the payload JSON as MAP<STRING, STRING>
    parsed_df = extracted_df.select(
        F.col("_cdc_key"),
        F.from_json(F.col("payload_json"), "MAP<STRING, STRING>").alias("payload"),
        F.col("_kafka_topic"),
        F.col("_kafka_partition"),
        F.col("_kafka_offset"),
        F.col("_kafka_timestamp"),
    )

    # Get all data columns from config
    data_columns = config["target"].get("columns", [])

    # Build select list: extract data columns + CDC metadata
    # All values from MAP<STRING, STRING> are strings, so we need to cast them
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
    ]

    # Add data columns from JSON payload with proper type casting
    for col_def in data_columns:
        col_name = col_def["name"]
        col_type = col_def.get("type", "string")

        # Cast to appropriate type
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
            # Default: string
            select_expressions.append(
                F.col(f"payload.{col_name}").alias(col_name)
            )

    result = parsed_df.select(*select_expressions)

    # Transform Debezium operation codes
    # __op: c=create, u=update, d=delete, r=read/snapshot
    result = (
        result
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
    )

    # Drop internal fields
    result = result.drop("_cdc_key", "_raw_op", "_raw_ts_ms", "_raw_deleted",
                         "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp")

    # Write to Iceberg using append mode
    row_count = result.count()
    print(f"[Batch {batch_id}] Writing {row_count} rows to {target_table}")
    result.writeTo(target_table).append()

    print(f"[Batch {batch_id}] Completed writing to {target_table}")


def load_config(config_path: str) -> Dict:
    """Load YAML configuration file."""
    with open(config_path) as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="CDC Streaming Job")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--kafka_bootstrap", default="kafka:9092", help="Kafka bootstrap servers")
    parser.add_argument("--starting_offsets", default=None, help="Kafka starting offsets (e.g., earliest)")
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Create Spark session
    spark = create_spark_session()

    # Configuration
    kafka_topic = config["kafka"]["topic"]
    target_table = f"{config['target']['catalog']}.{config['target']['schema']}.{config['target']['table']}"
    checkpoint_location = config["kafka"]["checkpoint_location"]
    trigger_interval = config["kafka"].get("trigger_interval", "30 seconds")
    starting_offsets = args.starting_offsets or config["kafka"].get("starting_offsets", "latest")

    print(f"Starting CDC Streaming Job")
    print(f"  Kafka Topic: {kafka_topic}")
    print(f"  Target Table: {target_table}")
    print(f"  Checkpoint: {checkpoint_location}")
    print(f"  Trigger: {trigger_interval}")
    print(f"  Starting Offsets: {starting_offsets}")

    # Read from Kafka
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", config["kafka"].get("max_offsets_per_trigger", 100000))
        .load()
    )

    # Write stream with foreachBatch
    query = (
        stream_df.writeStream
        .foreachBatch(lambda df, id: process_cdc_batch(df, id, target_table, config))
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .queryName(f"cdc_{kafka_topic.replace('.', '_')}")
        .start()
    )

    print(f"Streaming query started: {query.id}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
