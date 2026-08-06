#!/usr/bin/env python3
"""Create CDC tables in Iceberg Bronze layer."""

import os
from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .appName("Create_CDC_Tables")
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
        .getOrCreate()
    )

    # Create core_account_cdc table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_account_cdc (
            account_id BIGINT,
            account_no VARCHAR(50),
            customer_id BIGINT,
            product_code VARCHAR(50),
            branch_code VARCHAR(50),
            account_type VARCHAR(50),
            currency VARCHAR(10),
            balance DECIMAL(18,2),
            open_date BIGINT,
            close_date BIGINT,
            status VARCHAR(20),
            last_updated BIGINT,
            __cdc_operation VARCHAR(10),
            __cdc_timestamp TIMESTAMP,
            __cdc_timestamp_ms BIGINT,
            __spark_batch_id BIGINT,
            __ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (DATE(__cdc_timestamp))
    """)
    print("Created table: core_account_cdc")

    # Create core_customer_cdc table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_customer_cdc (
            customer_id BIGINT,
            cccd VARCHAR(50),
            full_name VARCHAR(255),
            gender VARCHAR(10),
            date_of_birth BIGINT,
            phone VARCHAR(50),
            email VARCHAR(255),
            address VARCHAR(500),
            city VARCHAR(100),
            district VARCHAR(100),
            branch_code VARCHAR(50),
            customer_segment VARCHAR(50),
            kyc_status VARCHAR(50),
            register_date BIGINT,
            is_active VARCHAR(10),
            last_updated BIGINT,
            __cdc_operation VARCHAR(10),
            __cdc_timestamp TIMESTAMP,
            __cdc_timestamp_ms BIGINT,
            __spark_batch_id BIGINT,
            __ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (DATE(__cdc_timestamp))
    """)
    print("Created table: core_customer_cdc")

    # Create core_transaction_cdc table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_transaction_cdc (
            txn_id BIGINT,
            account_id BIGINT,
            customer_id BIGINT,
            txn_date BIGINT,
            txn_amount DECIMAL(18,2),
            txn_type VARCHAR(50),
            debit_credit VARCHAR(10),
            balance_after DECIMAL(18,2),
            channel VARCHAR(50),
            description VARCHAR(500),
            counter_account VARCHAR(50),
            created_ts BIGINT,
            last_updated BIGINT,
            __cdc_operation VARCHAR(10),
            __cdc_timestamp TIMESTAMP,
            __cdc_timestamp_ms BIGINT,
            __spark_batch_id BIGINT,
            __ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (DATE(__cdc_timestamp))
    """)
    print("Created table: core_transaction_cdc")

    # Create card_account_cdc table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.card_account_cdc (
            card_id BIGINT,
            card_no_masked VARCHAR(50),
            customer_id BIGINT,
            account_id BIGINT,
            product_code VARCHAR(50),
            card_type VARCHAR(50),
            card_brand VARCHAR(50),
            credit_limit DECIMAL(18,2),
            issue_date BIGINT,
            expiry_date BIGINT,
            status VARCHAR(20),
            last_updated BIGINT,
            __cdc_operation VARCHAR(10),
            __cdc_timestamp TIMESTAMP,
            __cdc_timestamp_ms BIGINT,
            __spark_batch_id BIGINT,
            __ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (DATE(__cdc_timestamp))
    """)
    print("Created table: card_account_cdc")

    # Create card_transaction_cdc table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.card_transaction_cdc (
            txn_id BIGINT,
            card_id BIGINT,
            customer_id BIGINT,
            txn_date BIGINT,
            txn_amount DECIMAL(18,2),
            txn_type VARCHAR(50),
            currency VARCHAR(10),
            merchant_name VARCHAR(255),
            merchant_category VARCHAR(100),
            channel VARCHAR(50),
            status VARCHAR(20),
            created_ts BIGINT,
            last_updated BIGINT,
            __cdc_operation VARCHAR(10),
            __cdc_timestamp TIMESTAMP,
            __cdc_timestamp_ms BIGINT,
            __spark_batch_id BIGINT,
            __ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (DATE(__cdc_timestamp))
    """)
    print("Created table: card_transaction_cdc")

    # Create online_transaction_cdc table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.online_transaction_cdc (
            transaction_id BIGINT,
            account_id BIGINT,
            device_id BIGINT,
            location_id BIGINT,
            customer_id BIGINT,
            transaction_type VARCHAR(50),
            channel VARCHAR(50),
            amount DECIMAL(18,2),
            currency VARCHAR(10),
            is_fraud VARCHAR(10),
            fraud_reason VARCHAR(500),
            status VARCHAR(20),
            transaction_date BIGINT,
            created_ts BIGINT,
            last_updated BIGINT,
            __cdc_operation VARCHAR(10),
            __cdc_timestamp TIMESTAMP,
            __cdc_timestamp_ms BIGINT,
            __spark_batch_id BIGINT,
            __ingestion_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (DATE(__cdc_timestamp))
    """)
    print("Created table: online_transaction_cdc")

    print("\nAll CDC tables created successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
