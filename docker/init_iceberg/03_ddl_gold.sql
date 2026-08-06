-- =============================================================================
-- DDL: Gold Layer — Iceberg Tables (10 tables)
-- Catalog: lakehouse  |  Schema: gold
-- Types: mart360 (5), segment (4), time_analytics (1)
-- =============================================================================

-- =============================================================================
-- MART360 — Customer-level aggregated views
-- =============================================================================

-- 1. MART_CUSTOMER_360 (28+ KPIs, partitioned by cob_dt)
CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_customer_360 (
    customer_id                 BIGINT,
    customer_sk                 STRING,
    full_name_masked            STRING,
    age                         INT,
    gender                      STRING,
    primary_branch_code         STRING,
    customer_segment            STRING,
    kyc_status                  STRING,
    register_date               DATE,
    total_accounts              INT,
    total_cards                 INT,
    total_loans                 INT,
    has_credit_card             INT,
    has_savings                 INT,
    has_loan                    INT,
    total_deposit_balance       DECIMAL(18,2),
    total_loan_outstanding      DECIMAL(18,2),
    aum_total                   DECIMAL(18,2),
    aum_bucket                  STRING,
    txn_count_30d               INT,
    txn_amount_30d              DECIMAL(18,2),
    last_txn_date               TIMESTAMP,
    days_since_last_txn         INT,
    primary_channel             STRING,
    interaction_count_90d       INT,
    last_interaction_date       TIMESTAMP,
    rfm_recency_score           INT,
    rfm_frequency_score         INT,
    rfm_monetary_score          INT,
    rfm_segment                 STRING,
    churn_flag                  INT,
    cross_sell_credit_card_flag INT,
    cob_dt                      DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

-- Current serving view: exactly 1 row per customer (latest cob_dt)
CREATE OR REPLACE VIEW lakehouse.gold.mart_customer_360_current AS
SELECT
    customer_id,
    customer_sk,
    full_name_masked,
    age,
    gender,
    primary_branch_code,
    customer_segment,
    kyc_status,
    register_date,
    total_accounts,
    total_cards,
    total_loans,
    has_credit_card,
    has_savings,
    has_loan,
    total_deposit_balance,
    total_loan_outstanding,
    aum_total,
    aum_bucket,
    txn_count_30d,
    txn_amount_30d,
    last_txn_date,
    days_since_last_txn,
    primary_channel,
    interaction_count_90d,
    last_interaction_date,
    rfm_recency_score,
    rfm_frequency_score,
    rfm_monetary_score,
    rfm_segment,
    churn_flag,
    cross_sell_credit_card_flag,
    cob_dt
FROM (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY cob_dt DESC
        ) AS rn
    FROM lakehouse.gold.mart_customer_360 t
) x
WHERE rn = 1;

-- 2. CUSTOMER_BALANCE_SUMMARY
CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_balance_summary (
    customer_id             BIGINT,
    customer_sk             STRING,
    total_account_balance   DECIMAL(18,2),
    avg_account_balance     DECIMAL(18,2),
    aum_total               DECIMAL(18,2),
    aum_bucket              STRING,
    cob_dt                  DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_balance_summary_current AS
SELECT customer_id, customer_sk, total_account_balance, avg_account_balance, aum_total, aum_bucket, cob_dt
FROM (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cob_dt DESC) AS rn
    FROM lakehouse.gold.customer_balance_summary t
) x
WHERE rn = 1;

-- 3. CUSTOMER_TRANSACTION_SUMMARY
CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_transaction_summary (
    customer_id             BIGINT,
    customer_sk             STRING,
    acct_txn_count_30d      INT,
    acct_txn_amount_30d     DECIMAL(18,2),
    acct_credit_count_30d   INT,
    acct_debit_count_30d    INT,
    card_txn_count_30d      INT,
    card_txn_amount_30d     DECIMAL(18,2),
    total_txn_count_30d     INT,
    total_txn_amount_30d    DECIMAL(18,2),
    last_txn_date           TIMESTAMP,
    cob_dt                  DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_transaction_summary_current AS
SELECT customer_id, customer_sk, acct_txn_count_30d, acct_txn_amount_30d, acct_credit_count_30d, acct_debit_count_30d, card_txn_count_30d, card_txn_amount_30d, total_txn_count_30d, total_txn_amount_30d, last_txn_date, cob_dt
FROM (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cob_dt DESC) AS rn
    FROM lakehouse.gold.customer_transaction_summary t
) x
WHERE rn = 1;

-- 4. CUSTOMER_PRODUCT_SUMMARY
CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_product_summary (
    customer_id         BIGINT,
    customer_sk         STRING,
    total_accounts      INT,
    cnt_casa_active     INT,
    cnt_td_active       INT,
    total_cards         INT,
    cnt_credit_cards    INT,
    cnt_debit_cards     INT,
    has_credit_card     INT,
    has_savings         INT,
    has_loan            INT,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_product_summary_current AS
SELECT customer_id, customer_sk, total_accounts, cnt_casa_active, cnt_td_active, total_cards, cnt_credit_cards, cnt_debit_cards, has_credit_card, has_savings, has_loan, cob_dt
FROM (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cob_dt DESC) AS rn
    FROM lakehouse.gold.customer_product_summary t
) x
WHERE rn = 1;

-- 5. CUSTOMER_CARD_SUMMARY
CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_card_summary (
    customer_id                     BIGINT,
    customer_sk                     STRING,
    total_cards                     INT,
    cnt_credit_active               INT,
    cnt_debit_active                INT,
    max_credit_limit                DECIMAL(18,2),
    total_card_txn_count_30d        INT,
    total_card_txn_amount_30d       DECIMAL(18,2),
    avg_card_txn_amount_30d         DECIMAL(18,2),
    distinct_merchant_categories    INT,
    last_card_txn_date              TIMESTAMP,
    cob_dt                          DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_card_summary_current AS
SELECT customer_id, customer_sk, total_cards, cnt_credit_active, cnt_debit_active, max_credit_limit, total_card_txn_count_30d, total_card_txn_amount_30d, avg_card_txn_amount_30d, distinct_merchant_categories, last_card_txn_date, cob_dt
FROM (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cob_dt DESC) AS rn
    FROM lakehouse.gold.customer_card_summary t
) x
WHERE rn = 1;

-- =============================================================================
-- SEGMENTATION — Customer segmentation tables
-- =============================================================================

-- 6. RFM_SEGMENT
CREATE TABLE IF NOT EXISTS lakehouse.gold.rfm_segment (
    customer_id     BIGINT,
    customer_sk     STRING,
    recency_days    INT,
    frequency       BIGINT,
    monetary        DECIMAL(18,2),
    r_score         INT,
    f_score         INT,
    m_score         INT,
    rfm_score       INT,
    rfm_segment     STRING,
    cob_dt          DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.gold.rfm_segment_current AS
SELECT customer_id, customer_sk, recency_days, frequency, monetary, r_score, f_score, m_score, rfm_score, rfm_segment, cob_dt
FROM (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cob_dt DESC) AS rn
    FROM lakehouse.gold.rfm_segment t
) x
WHERE rn = 1;

-- 7. CHURN_PREDICTION
CREATE TABLE IF NOT EXISTS lakehouse.gold.churn_prediction (
    customer_id         BIGINT,
    customer_sk         STRING,
    txn_cnt_30d         BIGINT,
    txn_cnt_90d         BIGINT,
    txn_amt_30d         DECIMAL(18,2),
    txn_amt_90d         DECIMAL(18,2),
    days_since_last_txn INT,
    churn_risk          STRING,
    is_churn_candidate  INT,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.gold.churn_prediction_current AS
SELECT customer_id, customer_sk, txn_cnt_30d, txn_cnt_90d, txn_amt_30d, txn_amt_90d, days_since_last_txn, churn_risk, is_churn_candidate, cob_dt
FROM (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cob_dt DESC) AS rn
    FROM lakehouse.gold.churn_prediction t
) x
WHERE rn = 1;

-- 8. CROSS_SELL_SEGMENT
CREATE TABLE IF NOT EXISTS lakehouse.gold.cross_sell_segment (
    customer_id         BIGINT,
    customer_sk         STRING,
    customer_segment    STRING,
    no_credit_card      INT,
    no_debit_card       INT,
    primary_opportunity STRING,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.gold.cross_sell_segment_current AS
SELECT customer_id, customer_sk, customer_segment, no_credit_card, no_debit_card, primary_opportunity, cob_dt
FROM (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cob_dt DESC) AS rn
    FROM lakehouse.gold.cross_sell_segment t
) x
WHERE rn = 1;

-- 9. CAMPAIGN_TARGET
CREATE TABLE IF NOT EXISTS lakehouse.gold.campaign_target (
    customer_id             BIGINT,
    customer_sk             STRING,
    rfm_segment             STRING,
    rfm_score               INT,
    recency_days            INT,
    frequency               BIGINT,
    monetary                DECIMAL(18,2),
    churn_risk              STRING,
    is_churn_candidate      INT,
    days_since_last_txn     INT,
    customer_segment        STRING,
    aum_total               DECIMAL(18,2),
    aum_bucket              STRING,
    primary_branch_code     STRING,
    primary_opportunity     STRING,
    no_credit_card          INT,
    campaign_type           STRING,
    cob_dt                  DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.gold.campaign_target_current AS
SELECT customer_id, customer_sk, rfm_segment, rfm_score, recency_days, frequency, monetary, churn_risk, is_churn_candidate, days_since_last_txn, customer_segment, aum_total, aum_bucket, primary_branch_code, primary_opportunity, no_credit_card, campaign_type, cob_dt
FROM (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cob_dt DESC) AS rn
    FROM lakehouse.gold.campaign_target t
) x
WHERE rn = 1;

-- =============================================================================
-- TIME ANALYTICS — Branch-level aggregated views
-- =============================================================================

-- 10. MART_BRANCH_MONTHLY_SUMMARY
CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_branch_monthly_summary (
    branch_code         STRING,
    branch_name         STRING,
    region              STRING,
    city                STRING,
    txn_year            INT,
    txn_month           INT,
    txn_quarter         INT,
    active_customers    BIGINT,
    txn_count           BIGINT,
    total_txn_amount    DECIMAL(18,2),
    avg_txn_amount      DECIMAL(18,2),
    total_credit_amount DECIMAL(18,2),
    total_debit_amount  DECIMAL(18,2),
    top_channel         STRING,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (cob_dt)
TBLPROPERTIES ('format-version' = '2');
