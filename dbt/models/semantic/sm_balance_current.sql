-- Semantic Model: sm_balance_current
-- Business-purpose: current-serving customer balance summary, 1 row/customer

{{
    config(
        materialized='ephemeral',
        tags=['semantic', 'current_serving']
    )
}}

select
    customer_id,
    customer_sk,
    total_account_balance,
    avg_account_balance,
    aum_total,
    aum_bucket,
    cob_dt
from {{ source('gold', 'customer_balance_summary_current') }}
