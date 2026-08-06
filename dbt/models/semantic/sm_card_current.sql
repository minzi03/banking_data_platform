-- Semantic Model: sm_card_current
-- Business-purpose: current-serving customer card portfolio summary, 1 row/customer

{{
    config(
        materialized='ephemeral',
        tags=['semantic', 'current_serving']
    )
}}

select
    customer_id,
    customer_sk,
    total_cards,
    cnt_credit_active,
    cnt_debit_active,
    max_credit_limit,
    total_card_txn_count_30d,
    total_card_txn_amount_30d,
    avg_card_txn_amount_30d,
    distinct_merchant_categories,
    last_card_txn_date,
    cob_dt
from {{ source('gold', 'customer_card_summary_current') }}
