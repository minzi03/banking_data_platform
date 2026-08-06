-- Semantic Model: sm_cross_sell_current
-- Business-purpose: current-serving cross-sell segment, 1 row/customer

{{
    config(
        materialized='ephemeral',
        tags=['semantic', 'current_serving']
    )
}}

select
    customer_id,
    customer_sk,
    customer_segment,
    no_credit_card,
    no_debit_card,
    primary_opportunity,
    cob_dt
from {{ source('gold', 'cross_sell_segment_current') }}
