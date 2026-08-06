-- =============================================================================
-- Semantic Model: Account
-- Business Logic: Account dimension for metric definitions
-- Source: Gold layer (Spark-created Iceberg tables)
-- =============================================================================

{{
    config(
        materialized='ephemeral',
        tags=['semantic']
    )
}}

select
    b.customer_id,
    b.customer_sk,
    b.total_account_balance,
    b.avg_account_balance,
    b.aum_total,
    b.aum_bucket,
    b.cob_dt
from {{ source('gold', 'customer_balance_summary_current') }} b
