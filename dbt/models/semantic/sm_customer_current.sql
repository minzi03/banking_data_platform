-- Semantic Model: sm_customer_current
-- Business-purpose: current-serving customer 360, 1 row/customer

{{
    config(
        materialized='ephemeral',
        tags=['semantic', 'current_serving']
    )
}}

select
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
from {{ source('gold', 'mart_customer_360') }}
where cob_dt = (select max(cob_dt) from {{ source('gold', 'mart_customer_360') }})
