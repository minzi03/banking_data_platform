-- Semantic Model: sm_product_current
-- Business-purpose: current-serving customer product portfolio summary, 1 row/customer

{{
    config(
        materialized='ephemeral',
        tags=['semantic', 'current_serving']
    )
}}

select
    customer_id,
    customer_sk,
    total_accounts,
    cnt_casa_active,
    cnt_td_active,
    total_cards,
    cnt_credit_cards,
    cnt_debit_cards,
    has_credit_card,
    has_savings,
    has_loan,
    cob_dt
from {{ source('gold', 'customer_product_summary_current') }}
