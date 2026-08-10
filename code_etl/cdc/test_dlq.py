#!/usr/bin/env python3
"""
P3.5 DLQ Runtime Test — Banking Data Platform

Tests that invalid CDC events are isolated into the DLQ table
while valid events continue into Bronze normally.

Test flow:
    1. Create DLQ table if not exists
    2. Inject 5 valid + 2 invalid CDC events into Kafka
    3. Run one micro-batch of the streaming job
    4. Verify:
       - 5 valid events in Bronze CDC table
       - 2 invalid events in cdc_dead_letter
       - Job did not crash

Usage:
    docker exec banking-spark-master bash -c \\
        "export PATH=/opt/spark/bin:\\$PATH && \\
         spark-submit --master spark://spark-master:7077 \\
             /opt/project/code_etl/cdc/test_dlq.py \\
             --config /opt/project/code_etl/cdc/config/cdc_core_customer.yml"
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

import yaml
from kafka import KafkaProducer
from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """Create SparkSession with Iceberg REST catalog configuration."""
    return (
        SparkSession.builder
        .appName("DLQ_Runtime_Test")
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
        .getOrCreate()
    )


def create_debezium_message(key: str, payload: dict, topic: str) -> dict:
    """Create a Debezium-style Kafka message."""
    return {
        "key": key.encode("utf-8") if key else None,
        "value": json.dumps({"schema": None, "payload": payload}).encode("utf-8"),
        "topic": topic,
        "timestamp": int(datetime.now().timestamp() * 1000),
    }


def get_current_offsets(topic: str) -> dict:
    """Get current end offsets for a Kafka topic."""
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=["kafka:9092"])
    partitions = c.partitions_for_topic(topic)
    if not partitions:
        c.close()
        return {}
    tps = [TopicPartition(topic, p) for p in partitions]
    c.assign(tps)
    end_offsets = c.end_offsets(tps)
    offsets = {tp.partition: end_offsets[tp] for tp in tps}
    c.close()
    return offsets


def inject_test_events(topic: str):
    """Inject valid and invalid CDC events into Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda v: v,
        key_serializer=lambda k: k,
    )

    print(f"\n{'='*60}")
    print(f"Injecting test events into {topic}")
    print(f"{'='*60}")

    # ── VALID events (5 total) ───────────────────────────────────────
    valid_events = [
        # Valid INSERT
        {
            "key": "test_dlq_1001",
            "payload": {
                "__op": "c", "__ts_ms": str(int(time.time() * 1000)),
                "customer_id": "997001", "full_name": "DLQ Test User 1",
                "cccd": "001001001001", "gender": "M", "phone": "0900000001",
                "email": "dlq1@test.com", "is_active": "1",
            }
        },
        # Valid UPDATE
        {
            "key": "test_dlq_1002",
            "payload": {
                "__op": "u", "__ts_ms": str(int(time.time() * 1000) + 1),
                "customer_id": "997002", "full_name": "DLQ Test User 2",
                "cccd": "001001001002", "gender": "F", "phone": "0900000002",
                "email": "dlq2@test.com", "is_active": "1",
            }
        },
        # Valid INSERT
        {
            "key": "test_dlq_1003",
            "payload": {
                "__op": "c", "__ts_ms": str(int(time.time() * 1000) + 2),
                "customer_id": "997003", "full_name": "DLQ Test User 3",
                "cccd": "001001001003", "gender": "M", "phone": "0900000003",
                "email": "dlq3@test.com", "is_active": "0",
            }
        },
        # Valid DELETE
        {
            "key": "test_dlq_1004",
            "payload": {
                "__op": "d", "__ts_ms": str(int(time.time() * 1000) + 3),
                "customer_id": "997004", "full_name": "DLQ Test User 4",
                "cccd": "001001001004", "gender": "F", "phone": "0900000004",
                "email": "dlq4@test.com", "is_active": "1",
            }
        },
        # Valid INSERT
        {
            "key": "test_dlq_1005",
            "payload": {
                "__op": "c", "__ts_ms": str(int(time.time() * 1000) + 4),
                "customer_id": "997005", "full_name": "DLQ Test User 5",
                "cccd": "001001001005", "gender": "M", "phone": "0900000005",
                "email": "dlq5@test.com", "is_active": "1",
            }
        },
    ]

    # ── INVALID events (2 total) ─────────────────────────────────────
    invalid_events = [
        # Invalid: missing __op
        {
            "key": "test_dlq_9001",
            "payload": {
                "__ts_ms": str(int(time.time() * 1000) + 5),
                "customer_id": "997099", "full_name": "Bad Event - No Op",
                "cccd": "000000000001", "gender": "M", "phone": "0999999901",
                "email": "bad1@test.com", "is_active": "1",
                # __op is MISSING
            }
        },
        # Invalid: __op is "x" (unknown)
        {
            "key": "test_dlq_9002",
            "payload": {
                "__op": "x", "__ts_ms": str(int(time.time() * 1000) + 6),
                "customer_id": "997098", "full_name": "Bad Event - Unknown Op",
                "cccd": "000000000002", "gender": "F", "phone": "0999999902",
                "email": "bad2@test.com", "is_active": "0",
            }
        },
    ]

    # Send all events
    for i, event in enumerate(valid_events):
        msg = create_debezium_message(event["key"], event["payload"], topic)
        producer.send(topic, key=msg["key"], value=msg["value"])
        print(f"  [VALID {i+1}/5] key={event['key']} customer_id={event['payload']['customer_id']}")

    for i, event in enumerate(invalid_events):
        msg = create_debezium_message(event["key"], event["payload"], topic)
        producer.send(topic, key=msg["key"], value=msg["value"])
        print(f"  [INVALID {i+1}/2] key={event['key']} → {event['payload'].get('__op', 'MISSING')}")

    producer.flush()
    producer.close()
    print("\nInjected 5 valid + 2 invalid events")


def run_streaming_batch(spark: SparkSession, config: dict, test_batch_id: int, pre_offsets: dict):
    """Run one micro-batch of the streaming job using offset-based filtering."""
    from cdc_dlq import (
        ensure_dlq_table,
        validate_and_split,
        write_invalid_to_dlq,
        write_valid_to_bronze,
    )

    target_table = f"{config['target']['catalog']}.{config['target']['schema']}.{config['target']['table']}"
    topic = config["kafka"]["topic"]

    print(f"\n{'='*60}")
    print(f"Running DLQ test micro-batch (batch_id={test_batch_id})")
    print(f"  Topic: {topic}")
    print(f"  Target: {target_table}")
    print(f"{'='*60}")

    # Ensure DLQ table exists
    ensure_dlq_table(spark)

    # Read only events injected AFTER pre_offsets
    # Build starting offsets JSON: {"topic": {"partition": offset}}
    starting_offsets = {topic: {str(p): o for p, o in pre_offsets.items()}}
    print(f"  Starting offsets: {starting_offsets}")

    test_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", topic)
        .option("startingOffsets", json.dumps(starting_offsets))
        .option("endingOffsets", "latest")
        .load()
    )

    if test_df.isEmpty():
        print("ERROR: No test events found after pre-injection offsets")
        return False

    total_count = test_df.count()
    print(f"Read {total_count} test events from Kafka")

    # Validate and split
    valid_df, dlq_df = validate_and_split(test_df, config, batch_id=test_batch_id)

    valid_count = valid_df.count()
    invalid_count = dlq_df.count()

    print("\nValidation results:")
    print(f"  Valid events:   {valid_count}")
    print(f"  Invalid events: {invalid_count}")
    print(f"  Total:          {valid_count + invalid_count}")

    # Write valid to Bronze
    write_valid_to_bronze(valid_df, target_table, batch_id=test_batch_id)

    # Write invalid to DLQ
    write_invalid_to_dlq(dlq_df, batch_id=test_batch_id)

    return True


def verify_results(spark: SparkSession, config: dict, test_batch_id: int):
    """Verify the DLQ test results."""
    target_table = f"{config['target']['catalog']}.{config['target']['schema']}.{config['target']['table']}"

    print(f"\n{'='*60}")
    print(f"Verifying DLQ test results (batch_id={test_batch_id})")
    print(f"{'='*60}")

    # Check Bronze: should have 5 test events in this batch
    bronze_count = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {target_table}
        WHERE customer_id BETWEEN 997001 AND 997005
        AND __spark_batch_id = {test_batch_id}
    """).collect()[0]["cnt"]

    print(f"\nBronze table ({target_table}):")
    print(f"  Test valid events in batch: {bronze_count} (expected: 5)")

    # Check DLQ: should have 2 invalid events from this batch
    dlq_count = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM lakehouse.bronze.cdc_dead_letter
        WHERE spark_batch_id = {test_batch_id}
    """).collect()[0]["cnt"]

    print("\nDLQ table (bronze.cdc_dead_letter):")
    print(f"  Invalid events from this batch: {dlq_count} (expected: 2)")

    if dlq_count > 0:
        print("\n  DLQ events detail:")
        dlq_rows = spark.sql(f"""
            SELECT source_topic, entity, error_type, error_message, kafka_offset
            FROM lakehouse.bronze.cdc_dead_letter
            WHERE spark_batch_id = {test_batch_id}
            ORDER BY failed_at DESC
        """).collect()
        for row in dlq_rows:
            print(f"    - {row['error_type']}: {row['error_message']}")
            print(f"      topic={row['source_topic']}, offset={row['kafka_offset']}")

    # Summary
    print(f"\n{'='*60}")
    passed = bronze_count == 5 and dlq_count == 2
    if passed:
        print("✅ DLQ TEST PASSED")
        print("   5 valid events → Bronze")
        print("   2 invalid events → DLQ")
        print("   Job completed without crash")
    else:
        print("❌ DLQ TEST FAILED")
        print("   Expected: 5 valid + 2 invalid")
        print(f"   Got: {bronze_count} valid + {dlq_count} invalid")
    print(f"{'='*60}\n")

    return passed


def main():
    parser = argparse.ArgumentParser(description="P3.5 DLQ Runtime Test")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    # Load config
    with open(args.config) as f:
        config = yaml.safe_load(f)

    # Create Spark session
    spark = create_spark_session()

    # Unique batch_id for this test run
    test_batch_id = int(time.time() * 1000)

    # Step 0: Get current offsets before injecting
    pre_offsets = get_current_offsets(config["kafka"]["topic"])
    print(f"Pre-injection offsets: {pre_offsets}")

    # Step 1: Inject test events
    inject_test_events(config["kafka"]["topic"])

    # Step 2: Run streaming batch (with offset filter)
    success = run_streaming_batch(spark, config, test_batch_id, pre_offsets)

    if not success:
        print("ERROR: Failed to run streaming batch")
        sys.exit(1)

    # Step 3: Verify results
    passed = verify_results(spark, config, test_batch_id)

    spark.stop()
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
