-- =============================================================================
-- DDL: Bronze Layer — Iceberg Tables (21 bảng; core_loan_payment được tạo ở lần ghi đầu, cũng partition theo cob_dt)
-- Catalog: lakehouse  |  Schema: bronze
-- Strategy: full snapshot mỗi cob_dt, MỌI bảng partition theo cob_dt.
-- overwritePartitions() trên bảng KHÔNG partition ghi đè toàn bộ bảng, nên bảng
-- nào thiếu partition chỉ giữ được ngày mới nhất (mất lịch sử, backfill ghi đè).
-- =============================================================================

-- =============================================================================
-- 1. CORE_BRANCH (dimension — 100 rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_branch (
    branch_code     STRING,
    branch_name     STRING,
    region          STRING,
    city            STRING,
    district        STRING,
    address         STRING,
    manager_name    STRING,
    open_date       DATE,
    status          STRING,
    last_updated    TIMESTAMP,
    cob_dt          DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 2. CORE_PRODUCT (dimension — 13 rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_product (
    product_code    STRING,
    product_name    STRING,
    product_group   STRING,
    product_type    STRING,
    currency        STRING,
    is_active       INT,
    launch_date     DATE,
    last_updated    TIMESTAMP,
    cob_dt          DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 3. CORE_CUSTOMER (dimension — 10K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_customer (
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
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 4. CORE_ACCOUNT (dimension — 30K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_account (
    account_id      BIGINT,
    account_no      STRING,
    customer_id     BIGINT,
    product_code    STRING,
    branch_code     STRING,
    account_type    STRING,
    currency        STRING,
    balance         DECIMAL(18,2),
    open_date       DATE,
    close_date      DATE,
    status          STRING,
    last_updated    TIMESTAMP,
    cob_dt          DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 5. CORE_DEPOSIT (dimension — 15K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_deposit (
    deposit_id          BIGINT,
    account_id          BIGINT,
    customer_id         BIGINT,
    product_code        STRING,
    principal_amount    DECIMAL(18,2),
    interest_rate       DECIMAL(5,2),
    term_months         INT,
    open_date           DATE,
    maturity_date       DATE,
    status              STRING,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 6. CORE_LOAN (dimension — 5K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_loan (
    loan_id             BIGINT,
    customer_id         BIGINT,
    product_code        STRING,
    branch_code         STRING,
    loan_amount         DECIMAL(18,2),
    outstanding_balance DECIMAL(18,2),
    interest_rate       DECIMAL(5,2),
    term_months         INT,
    disbursement_date   DATE,
    maturity_date       DATE,
    loan_status         STRING,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 7. CORE_TXN_ACCOUNT (fact — 1.2M rows, partitioned by cob_dt)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_txn_account (
    txn_id          BIGINT,
    account_id      BIGINT,
    customer_id     BIGINT,
    txn_date        TIMESTAMP,
    txn_amount      DECIMAL(18,2),
    txn_type        STRING,
    debit_credit    STRING,
    balance_after   DECIMAL(18,2),
    channel         STRING,
    description     STRING,
    counter_account STRING,
    created_ts      TIMESTAMP,
    last_updated    TIMESTAMP,
    cob_dt          DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 8. CORE_EMPLOYEE (dimension — 1.8K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_employee (
    employee_id     BIGINT,
    full_name       STRING,
    branch_code     STRING,
    role            STRING,
    hire_date       DATE,
    salary          DECIMAL(12,2),
    status          STRING,
    last_updated    TIMESTAMP,
    cob_dt          DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 9. CORE_CARD (dimension — 6K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_card (
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
    last_updated    TIMESTAMP,
    cob_dt          DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 10. CORE_CARD_TXN (fact — 600K rows, partitioned by cob_dt)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_card_txn (
    txn_id              BIGINT,
    card_id             BIGINT,
    customer_id         BIGINT,
    txn_date            TIMESTAMP,
    txn_amount          DECIMAL(18,2),
    txn_type            STRING,
    currency            STRING,
    merchant_name       STRING,
    merchant_category   STRING,
    channel             STRING,
    status              STRING,
    entry_mode          STRING,
    decline_reason      STRING,
    created_ts          TIMESTAMP,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 11. CORE_CRM_INTERACTION (dimension — 50K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_crm_interaction (
    interaction_id      BIGINT,
    customer_id         BIGINT,
    interaction_date    TIMESTAMP,
    channel             STRING,
    direction           STRING,
    subject             STRING,
    category            STRING,
    status              STRING,
    assigned_to         STRING,
    satisfaction_score  INT,
    created_ts          TIMESTAMP,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 12. CORE_DEVICE (dimension — 50K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_device (
    device_id           BIGINT,
    customer_id         BIGINT,
    device_type         STRING,
    device_fingerprint  STRING,
    operating_system    STRING,
    ip_address          STRING,
    is_trusted          INT,
    first_seen          TIMESTAMP,
    last_seen           TIMESTAMP,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 13. CORE_LOCATION (dimension — 5K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_location (
    location_id         BIGINT,
    merchant_name       STRING,
    merchant_category   STRING,
    city                STRING,
    state               STRING,
    latitude            DECIMAL(10,7),
    longitude           DECIMAL(10,7),
    is_high_risk_area   INT,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 14. CORE_ONLINE_TRANSACTION (fact — 500K rows, partitioned by cob_dt)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_online_transaction (
    transaction_id      BIGINT,
    account_id          BIGINT,
    device_id           BIGINT,
    location_id         BIGINT,
    customer_id         BIGINT,
    transaction_type    STRING,
    channel             STRING,
    amount              DECIMAL(18,2),
    currency            STRING,
    is_fraud            INT,
    fraud_reason        STRING,
    status              STRING,
    transaction_date    TIMESTAMP,
    created_ts          TIMESTAMP,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 15. CORE_SUPPORT_TICKET (dimension — 25K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_support_ticket (
    ticket_id           BIGINT,
    customer_id         BIGINT,
    issue_type          STRING,
    priority            STRING,
    status              STRING,
    date_opened         TIMESTAMP,
    date_resolved       TIMESTAMP,
    resolution_time_hrs DECIMAL(8,2),
    satisfaction_score  INT,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 16. CORE_MCC_CODE (dimension — 84 rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_mcc_code (
    mcc_code            STRING,
    description         STRING,
    category_group      STRING,
    is_high_risk        INT,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 17. CORE_STANDING_ORDER (dimension — 15K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_standing_order (
    order_id            BIGINT,
    account_id          BIGINT,
    customer_id         BIGINT,
    order_type          STRING,
    beneficiary_name    STRING,
    beneficiary_account STRING,
    amount              DECIMAL(18,2),
    frequency           STRING,
    next_execute_date   DATE,
    status              STRING,
    created_date        DATE,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 18. CORE_MERCHANT (dimension — 2K rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_merchant (
    merchant_id         BIGINT,
    merchant_name       STRING,
    merchant_category   STRING,
    mcc_code            STRING,
    city                STRING,
    state               STRING,
    risk_category       STRING,
    is_active           INT,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 19. CORE_AML_RULE (dimension — cấu hình rule AML)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_aml_rule (
    rule_id             BIGINT,
    rule_name           STRING,
    rule_code           STRING,
    rule_type           STRING,
    description         STRING,
    threshold           DECIMAL(18,2),
    threshold_currency  STRING,
    window_hours        INT,
    severity            STRING,
    is_active           INT,
    regulatory_ref      STRING,
    created_by          STRING,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 20. CORE_AML_ALERT (dimension — case AML, trạng thái hiện tại)
-- Cột tuân thủ bị che với role observer: governance/rbac.py
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_aml_alert (
    alert_id            BIGINT,
    alert_number        STRING,
    rule_id             BIGINT,
    transaction_id      BIGINT,
    card_txn_id         BIGINT,
    customer_id         BIGINT,
    account_id          BIGINT,
    alert_type          STRING,
    risk_score          DECIMAL(5,2),
    risk_category       STRING,
    description         STRING,
    evidence_json       STRING,
    txn_amount          DECIMAL(18,2),
    txn_date            TIMESTAMP,
    channel             STRING,
    status              STRING,
    analyst_id          BIGINT,
    priority            STRING,
    due_date            DATE,
    notes               STRING,
    ctr_required        INT,
    sar_filed           INT,
    sar_reference       STRING,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    resolved_at         TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- =============================================================================
-- 21. CORE_AML_CUSTOMER_RISK (dimension — hồ sơ rủi ro AML theo khách)
-- Cột tuân thủ bị che với role observer: governance/rbac.py
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_aml_customer_risk (
    customer_id         BIGINT,
    risk_level          STRING,
    risk_score          DECIMAL(5,2),
    peps_flag           INT,
    sanctions_flag      INT,
    adverse_media_flag  INT,
    total_alerts        INT,
    open_alerts         INT,
    last_alert_date     DATE,
    last_review_date    DATE,
    next_review_date    DATE,
    edd_required        INT,
    edd_reason          STRING,
    source_of_wealth    STRING,
    expected_activity   STRING,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');
