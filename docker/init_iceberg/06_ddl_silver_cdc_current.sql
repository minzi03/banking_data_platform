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

-- CDC Watermark (track consolidation progress)
CREATE TABLE IF NOT EXISTS lakehouse.meta.cdc_watermark (
    table_name               VARCHAR,
    last_cdc_timestamp_ms    BIGINT,
    last_spark_batch_id      BIGINT,
    last_processed_at        TIMESTAMP,
    PRIMARY KEY (table_name)
)
USING iceberg;
