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


def process_cdc_batch(batch_df, batch_id: int, target_table: str, config: Dict, spark: SparkSession):
    """
    Process each micro-batch of CDC events and write to Iceberg.

    DLQ-enabled: valid events → Bronze, invalid events → cdc_dead_letter.
    The micro-batch never fails due to individual bad events.

    Args:
        batch_df: DataFrame containing CDC events from Kafka
        batch_id: Micro-batch ID
        target_table: Full Iceberg table name (e.g., bronze.core_account_cdc)
        config: YAML configuration
        spark: SparkSession for DDL operations
    """
    if batch_df.isEmpty():
        return

    # Import DLQ module
    from cdc_dlq import (
        ensure_dlq_table, validate_and_split,
        write_valid_to_bronze, write_invalid_to_dlq
    )

    # Ensure DLQ table exists
    ensure_dlq_table(spark)

    # Validate and split into valid / invalid
    valid_df, dlq_df = validate_and_split(batch_df, config, batch_id)

    # Write valid events to Bronze
    valid_count = write_valid_to_bronze(valid_df, target_table, batch_id)

    # Write invalid events to DLQ
    invalid_count = write_invalid_to_dlq(dlq_df, batch_id)

    if invalid_count > 0:
        print(f"[Batch {batch_id}] ⚠ DLQ: {invalid_count} invalid events routed to cdc_dead_letter")
        print(f"[Batch {batch_id}] Batch completed: {valid_count} valid → Bronze, {invalid_count} invalid → DLQ")
    else:
        print(f"[Batch {batch_id}] Batch completed: {valid_count} valid → Bronze, 0 invalid")


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
        .foreachBatch(lambda df, id: process_cdc_batch(df, id, target_table, config, spark))
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .queryName(f"cdc_{kafka_topic.replace('.', '_')}")
        .start()
    )

    print(f"Streaming query started: {query.id}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
