-- Semantic Model: sm_campaign_current
-- Business-purpose: current-serving campaign target list, 1 row/customer

{{
    config(
        materialized='ephemeral',
        tags=['semantic', 'current_serving']
    )
}}

select
    customer_id,
    customer_sk,
    rfm_segment,
    rfm_score,
    recency_days,
    frequency,
    monetary,
    churn_risk,
    is_churn_candidate,
    days_since_last_txn,
    customer_segment,
    aum_total,
    aum_bucket,
    primary_branch_code,
    primary_opportunity,
    no_credit_card,
    campaign_type,
    cob_dt
from {{ source('gold', 'campaign_target_current') }}
