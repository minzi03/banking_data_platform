-- =============================================================================
-- Bronze CDC Tables — Banking Data Platform (v3)
-- Schema matches YAML config columns exactly
-- =============================================================================

-- Core Customer CDC (matches cdc_core_customer.yml)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_customer_cdc (
    customer_id BIGINT,
    cccd VARCHAR(12),
    full_name VARCHAR(200),
    gender VARCHAR(10),
    date_of_birth BIGINT,
    phone VARCHAR(15),
    email VARCHAR(200),
    address VARCHAR(500),
    city VARCHAR(100),
    district VARCHAR(100),
    branch_code VARCHAR(10),
    customer_segment VARCHAR(20),
    kyc_status VARCHAR(20),
    register_date BIGINT,
    is_active VARCHAR(10),
    last_updated BIGINT,
    __cdc_operation VARCHAR(10),
    __cdc_timestamp TIMESTAMP,
    __cdc_timestamp_ms BIGINT,
    __spark_batch_id BIGINT,
    __ingestion_time TIMESTAMP
) USING iceberg
PARTITIONED BY (DATE(__cdc_timestamp));

-- Core Account CDC (matches cdc_core_account.yml)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_account_cdc (
    account_id BIGINT,
    account_no VARCHAR(50),
    customer_id BIGINT,
    product_code VARCHAR(20),
    branch_code VARCHAR(10),
    account_type VARCHAR(20),
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
PARTITIONED BY (DATE(__cdc_timestamp));

-- Core Transaction CDC (matches cdc_core_transaction.yml)
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
PARTITIONED BY (DATE(__cdc_timestamp));

-- Card Account CDC (matches cdc_card_account.yml)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.card_account_cdc (
    card_id BIGINT,
    card_no_masked VARCHAR(50),
    customer_id BIGINT,
    account_id BIGINT,
    product_code VARCHAR(20),
    card_type VARCHAR(50),
    card_brand VARCHAR(20),
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
PARTITIONED BY (DATE(__cdc_timestamp));

-- Card Transaction CDC (matches cdc_card_transaction.yml)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.card_transaction_cdc (
    txn_id BIGINT,
    card_id BIGINT,
    customer_id BIGINT,
    txn_date BIGINT,
    txn_amount DECIMAL(18,2),
    txn_type VARCHAR(50),
    currency VARCHAR(10),
    merchant_name VARCHAR(255),
    merchant_category VARCHAR(50),
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
PARTITIONED BY (DATE(__cdc_timestamp));

-- Online Transaction CDC (matches cdc_online_transaction.yml)
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
PARTITIONED BY (DATE(__cdc_timestamp));
