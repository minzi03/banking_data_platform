-- Semantic Model: sm_churn_current
-- Business-purpose: current-serving churn prediction, 1 row/customer

{{
    config(
        materialized='ephemeral',
        tags=['semantic', 'current_serving']
    )
}}

select
    customer_id,
    customer_sk,
    txn_cnt_30d,
    txn_cnt_90d,
    txn_amt_30d,
    txn_amt_90d,
    days_since_last_txn,
    churn_risk,
    is_churn_candidate,
    cob_dt
from {{ source('gold', 'churn_prediction_current') }}
