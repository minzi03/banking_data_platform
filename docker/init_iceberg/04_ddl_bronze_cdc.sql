-- =============================================================================
-- Bronze CDC Tables — Banking Data Platform
-- These tables store CDC events from Debezium streaming pipeline
-- =============================================================================

-- Core Account CDC
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_account_cdc (
    account_id BIGINT,
    account_name VARCHAR(255),
    account_type VARCHAR(50),
    balance DECIMAL(18,2),
    status VARCHAR(20),
    open_date VARCHAR(50),
    close_date VARCHAR(50),
    customer_id BIGINT,
    branch_id BIGINT,
    currency_code VARCHAR(10),
    created_at VARCHAR(50),
    updated_at VARCHAR(50),
    __cdc_operation VARCHAR(10),
    __cdc_timestamp TIMESTAMP,
    __cdc_timestamp_ms BIGINT,
    __spark_batch_id BIGINT,
    __ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (DATE(__cdc_timestamp));

-- Core Customer CDC
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_customer_cdc (
    customer_id BIGINT,
    customer_name VARCHAR(255),
    date_of_birth VARCHAR(50),
    gender VARCHAR(10),
    phone VARCHAR(50),
    email VARCHAR(255),
    address VARCHAR(500),
    city VARCHAR(100),
    country VARCHAR(100),
    customer_type VARCHAR(50),
    created_at VARCHAR(50),
    updated_at VARCHAR(50),
    __cdc_operation VARCHAR(10),
    __cdc_timestamp TIMESTAMP,
    __cdc_timestamp_ms BIGINT,
    __spark_batch_id BIGINT,
    __ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (DATE(__cdc_timestamp));

-- Core Transaction CDC
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_transaction_cdc (
    transaction_id BIGINT,
    account_id BIGINT,
    transaction_type VARCHAR(50),
    amount DECIMAL(18,2),
    currency_code VARCHAR(10),
    transaction_date VARCHAR(50),
    description VARCHAR(500),
    status VARCHAR(20),
    created_at VARCHAR(50),
    __cdc_operation VARCHAR(10),
    __cdc_timestamp TIMESTAMP,
    __cdc_timestamp_ms BIGINT,
    __spark_batch_id BIGINT,
    __ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (DATE(__cdc_timestamp));

-- Card Account CDC
CREATE TABLE IF NOT EXISTS lakehouse.bronze.card_account_cdc (
    card_account_id BIGINT,
    card_number VARCHAR(50),
    card_type VARCHAR(50),
    customer_id BIGINT,
    account_id BIGINT,
    credit_limit DECIMAL(18,2),
    current_balance DECIMAL(18,2),
    status VARCHAR(20),
    issue_date VARCHAR(50),
    expiry_date VARCHAR(50),
    created_at VARCHAR(50),
    updated_at VARCHAR(50),
    __cdc_operation VARCHAR(10),
    __cdc_timestamp TIMESTAMP,
    __cdc_timestamp_ms BIGINT,
    __spark_batch_id BIGINT,
    __ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (DATE(__cdc_timestamp));

-- Card Transaction CDC
CREATE TABLE IF NOT EXISTS lakehouse.bronze.card_transaction_cdc (
    card_txn_id BIGINT,
    card_account_id BIGINT,
    transaction_type VARCHAR(50),
    amount DECIMAL(18,2),
    merchant_name VARCHAR(255),
    transaction_date VARCHAR(50),
    status VARCHAR(20),
    created_at VARCHAR(50),
    __cdc_operation VARCHAR(10),
    __cdc_timestamp TIMESTAMP,
    __cdc_timestamp_ms BIGINT,
    __spark_batch_id BIGINT,
    __ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (DATE(__cdc_timestamp));

-- Online Transaction CDC
CREATE TABLE IF NOT EXISTS lakehouse.bronze.online_transaction_cdc (
    online_txn_id BIGINT,
    customer_id BIGINT,
    account_id BIGINT,
    transaction_type VARCHAR(50),
    amount DECIMAL(18,2),
    channel VARCHAR(50),
    status VARCHAR(20),
    transaction_date VARCHAR(50),
    created_at VARCHAR(50),
    __cdc_operation VARCHAR(10),
    __cdc_timestamp TIMESTAMP,
    __cdc_timestamp_ms BIGINT,
    __spark_batch_id BIGINT,
    __ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (DATE(__cdc_timestamp));

-- =============================================================================
-- Silver CDC Models (for dbt)
-- =============================================================================

-- Create Silver schema if not exists
CREATE SCHEMA IF NOT EXISTS lakehouse.silver_cdc;

-- =============================================================================
-- Meta Schema for CDC Watermarks
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS lakehouse.meta;

-- CDC Watermark Table
CREATE TABLE IF NOT EXISTS lakehouse.meta.cdc_watermark (
    table_name VARCHAR(100),
    last_cdc_timestamp TIMESTAMP,
    last_processed_at TIMESTAMP
) USING iceberg;
