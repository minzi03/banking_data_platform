-- Semantic Model: sm_transaction_current
-- Business-purpose: current-serving customer transaction summary, 1 row/customer

{{
    config(
        materialized='ephemeral',
        tags=['semantic', 'current_serving']
    )
}}

select
    customer_id,
    customer_sk,
    acct_txn_count_30d,
    acct_txn_amount_30d,
    acct_credit_count_30d,
    acct_debit_count_30d,
    card_txn_count_30d,
    card_txn_amount_30d,
    total_txn_count_30d,
    total_txn_amount_30d,
    last_txn_date,
    cob_dt
from {{ source('gold', 'customer_transaction_summary_current') }}
