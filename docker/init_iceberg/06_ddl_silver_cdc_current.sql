-- =============================================================================
-- P1: CDC Consolidation — Silver Current-State Tables
-- Derived from Bronze CDC (append-only) → Silver Current (mutable latest state)
-- =============================================================================

-- DIM_CUSTOMER_CURRENT (latest state from CDC stream)
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_customer_current (
    customer_id              BIGINT,
    cccd                     VARCHAR(12),
    full_name                VARCHAR(200),
    gender                   VARCHAR(10),
    date_of_birth            DATE,
    phone                    VARCHAR(15),
    email                    VARCHAR(200),
    address                  VARCHAR(500),
    city                     VARCHAR(100),
    district                 VARCHAR(100),
    branch_code              VARCHAR(10),
    customer_segment         VARCHAR(20),
    kyc_status               VARCHAR(20),
    register_date            DATE,
    is_active                INTEGER,

    -- CDC metadata
    __cdc_operation          VARCHAR(10),
    __cdc_timestamp          TIMESTAMP,
    __cdc_timestamp_ms       BIGINT,

    -- Consolidation metadata
    __source_spark_batch_id  BIGINT,
    __consolidated_at        TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- DIM_ACCOUNT_CURRENT (latest state from CDC stream)
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_account_current (
    account_id               BIGINT,
    account_no               VARCHAR(50),
    customer_id              BIGINT,
    product_code             VARCHAR(20),
    branch_code              VARCHAR(10),
    account_type             VARCHAR(20),
    currency                 VARCHAR(10),
    balance                  DECIMAL(18,2),
    open_date                DATE,
    close_date               DATE,
    status                   VARCHAR(20),

    -- CDC metadata
    __cdc_operation          VARCHAR(10),
    __cdc_timestamp          TIMESTAMP,
    __cdc_timestamp_ms       BIGINT,

    -- Consolidation metadata
    __source_spark_batch_id  BIGINT,
    __consolidated_at        TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- ONLINE TRANSACTION CURRENT (latest state for Merchant QR/POS EOD reconciliation)
-- One row per transaction_id, consolidated from bronze.online_transaction_cdc.
CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_online_transaction_current (
    transaction_id      BIGINT,
    account_id          BIGINT,
    device_id           BIGINT,
    location_id         BIGINT,
    customer_id         BIGINT,
    transaction_type    VARCHAR(50),
    channel              VARCHAR(50),
    amount               DECIMAL(18,2),
    currency             VARCHAR(10),
    is_fraud             VARCHAR(10),
    fraud_reason         VARCHAR(500),
    status               VARCHAR(20),
    merchant_id          BIGINT,
    payment_channel      VARCHAR(10),
    refund_flag          INT,
    transaction_date     BIGINT,
    created_ts           BIGINT,
    last_updated         BIGINT,
    __cdc_operation      VARCHAR(10),
    __cdc_timestamp      TIMESTAMP,
    __cdc_timestamp_ms   BIGINT,
    __source_spark_batch_id BIGINT,
    __consolidated_at    TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- CDC Watermark (track consolidation progress)
-- last_snapshot_id = tiến độ (snapshot Bronze CDC đã MERGE); ts/batch chỉ để quan sát.
CREATE TABLE IF NOT EXISTS lakehouse.meta.cdc_watermark (
    table_name               VARCHAR(100),
    last_snapshot_id         BIGINT,
    last_cdc_timestamp_ms    BIGINT,
    last_spark_batch_id      BIGINT,
    last_processed_at        TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');
