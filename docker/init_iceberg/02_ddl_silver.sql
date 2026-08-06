-- =============================================================================
-- DDL: Silver Layer — Iceberg Tables (8 dims + 5 facts = 13 tables)
-- Catalog: lakehouse  |  Schema: silver
-- Dims: SCD2 (customer, account) or SCD1 (others)
-- Facts: partitioned by cob_dt
-- =============================================================================

-- =============================================================================
-- DIMENSIONS — SCD Type 2 (full history)
-- =============================================================================

-- 1. DIM_CUSTOMER (SCD Type 2 — track phone, email, address, segment, kyc)
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_customer (
    customer_sk         STRING,
    customer_id         BIGINT,
    cccd                STRING,
    full_name           STRING,
    gender              STRING,
    date_of_birth       DATE,
    phone               STRING,
    email               STRING,
    address             STRING,
    city                STRING,
    district            STRING,
    branch_code         STRING,
    customer_segment    STRING,
    kyc_status          STRING,
    register_date       DATE,
    is_active           INT,
    effective_from      DATE,
    effective_to        DATE,
    is_current          INT,
    last_updated        TIMESTAMP
)
USING iceberg
PARTITIONED BY (is_current)
TBLPROPERTIES ('format-version' = '2');

-- 2. DIM_ACCOUNT (SCD Type 2 — track balance, status, close_date)
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_account (
    account_sk          STRING,
    account_id          BIGINT,
    account_no          STRING,
    customer_id         BIGINT,
    product_code        STRING,
    branch_code         STRING,
    account_type        STRING,
    currency            STRING,
    balance             DECIMAL(18,2),
    open_date           DATE,
    close_date          DATE,
    status              STRING,
    effective_from      DATE,
    effective_to        DATE,
    is_current          INT,
    last_updated        TIMESTAMP
)
USING iceberg
PARTITIONED BY (is_current)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- DIMENSIONS — SCD Type 1 (UPSERT, no history)
-- =============================================================================

-- 3. DIM_PRODUCT
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_product (
    product_code    STRING,
    product_name    STRING,
    product_group   STRING,
    product_type    STRING,
    currency        STRING,
    is_active       INT,
    launch_date     DATE,
    last_updated    TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 4. DIM_BRANCH
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_branch (
    branch_code     STRING,
    branch_name     STRING,
    region          STRING,
    city            STRING,
    district        STRING,
    address         STRING,
    manager_name    STRING,
    open_date       DATE,
    status          STRING,
    last_updated    TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 5. DIM_CARD
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_card (
    card_id         BIGINT,
    card_no_masked  STRING,
    customer_id     BIGINT,
    account_id      BIGINT,
    product_code    STRING,
    card_type       STRING,
    card_brand      STRING,
    credit_limit    DECIMAL(18,2),
    issue_date      DATE,
    expiry_date     DATE,
    status          STRING,
    last_updated    TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 6. DIM_EMPLOYEE
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_employee (
    employee_id     BIGINT,
    full_name       STRING,
    branch_code     STRING,
    role            STRING,
    hire_date       DATE,
    salary          DECIMAL(12,2),
    status          STRING,
    last_updated    TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 7. DIM_DEVICE
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_device (
    device_id           BIGINT,
    customer_id         BIGINT,
    device_type         STRING,
    device_fingerprint  STRING,
    operating_system    STRING,
    ip_address          STRING,
    is_trusted          INT,
    first_seen          TIMESTAMP,
    last_seen           TIMESTAMP,
    last_updated        TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 8. DIM_LOCATION
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_location (
    location_id         BIGINT,
    merchant_name       STRING,
    merchant_category   STRING,
    city                STRING,
    state               STRING,
    latitude            DECIMAL(10,7),
    longitude           DECIMAL(10,7),
    is_high_risk_area   INT,
    last_updated        TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- FACTS — partitioned by cob_dt
-- =============================================================================

-- 9. FACT_TXN_ACCOUNT (largest — ~1.2M rows/day)
CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_txn_account (
    txn_id              BIGINT,
    account_id          BIGINT,
    account_sk          STRING,
    customer_id         BIGINT,
    customer_sk         STRING,
    txn_date            TIMESTAMP,
    txn_amount          DECIMAL(18,2),
    txn_type            STRING,
    debit_credit        STRING,
    balance_after       DECIMAL(18,2),
    channel             STRING,
    description         STRING,
    counter_account     STRING,
    created_ts          TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- 10. FACT_CARD_txn (~600K rows/day)
CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_card_txn (
    txn_id              BIGINT,
    card_id             BIGINT,
    customer_id         BIGINT,
    customer_sk         STRING,
    txn_date            TIMESTAMP,
    txn_amount          DECIMAL(18,2),
    txn_type            STRING,
    currency            STRING,
    merchant_name       STRING,
    merchant_category   STRING,
    channel             STRING,
    status              STRING,
    created_ts          TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- 11. FACT_CRM_INTERACTION (~50K rows/day)
CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_crm_interaction (
    interaction_id      BIGINT,
    customer_id         BIGINT,
    customer_sk         STRING,
    interaction_date    TIMESTAMP,
    channel             STRING,
    direction           STRING,
    subject             STRING,
    category            STRING,
    status              STRING,
    assigned_to         STRING,
    satisfaction_score  INT,
    created_ts          TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- 12. FACT_ONLINE_TRANSACTION (~500K rows/day)
CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_online_transaction (
    transaction_id      BIGINT,
    account_id          BIGINT,
    device_id           BIGINT,
    location_id         BIGINT,
    customer_id         BIGINT,
    customer_sk         STRING,
    transaction_type    STRING,
    channel             STRING,
    amount              DECIMAL(18,2),
    currency            STRING,
    is_fraud            INT,
    fraud_reason        STRING,
    status              STRING,
    transaction_date    TIMESTAMP,
    created_ts          TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- 13. FACT_SUPPORT_TICKET (~25K rows/day)
CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_support_ticket (
    ticket_id           BIGINT,
    customer_id         BIGINT,
    customer_sk         STRING,
    issue_type          STRING,
    priority            STRING,
    status              STRING,
    date_opened         TIMESTAMP,
    date_resolved       TIMESTAMP,
    resolution_time_hrs DECIMAL(8,2),
    satisfaction_score  INT,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');
