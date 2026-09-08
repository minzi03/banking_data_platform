-- =============================================================================
-- Singular Test: No Duplicate Customers per Serving Model
-- =============================================================================
-- Validates that each serving model has exactly one row per customer_id
-- for the given cob_dt. This is the fundamental grain contract.
-- =============================================================================

{% set cob_dt = var('cob_dt', '1900-01-01') %}

WITH duplicate_check AS (
    SELECT
        customer_id,
        COUNT(*) AS row_count
    FROM {{ ref('mart_customer_360_current') }}
    WHERE cob_dt = date '{{ cob_dt }}'
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)

SELECT
    customer_id,
    row_count,
    'Duplicate customer_id in mart_customer_360_current' AS failure_reason
FROM duplicate_check
