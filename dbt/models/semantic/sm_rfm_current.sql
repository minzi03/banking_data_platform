-- Semantic Model: sm_rfm_current
-- Business-purpose: current-serving RFM segment, 1 row/customer

{{
    config(
        materialized='ephemeral',
        tags=['semantic', 'current_serving']
    )
}}

select
    customer_id,
    customer_sk,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    rfm_score,
    rfm_segment,
    cob_dt
from {{ source('gold', 'rfm_segment_current') }}
