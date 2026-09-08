-- =============================================================================
-- Singular Test: Serving Snapshot Alignment
-- =============================================================================
-- Validates that:
--   1. Serving tables have rows for the requested cob_dt
--   2. All serving tables have the same cob_dt (consistency check)
--   3. No serving table is empty
-- =============================================================================
-- Usage:
--   dbt test --select assert_serving_snapshot_alignment --vars '{"cob_dt": "2025-06-30"}'
-- =============================================================================

{% set cob_dt = var('cob_dt', '1900-01-01') %}

WITH serving_counts AS (
    SELECT
        'rfm_segment_current' AS table_name,
        COUNT(*) AS row_count,
        MAX(cob_dt) AS served_cob_dt
    FROM {{ ref('rfm_segment_current') }}
    WHERE cob_dt = date '{{ cob_dt }}'

    UNION ALL

    SELECT
        'mart_customer_360_current',
        COUNT(*),
        MAX(cob_dt)
    FROM {{ ref('mart_customer_360_current') }}
    WHERE cob_dt = date '{{ cob_dt }}'

    UNION ALL

    SELECT
        'churn_prediction_current',
        COUNT(*),
        MAX(cob_dt)
    FROM {{ ref('churn_prediction_current') }}
    WHERE cob_dt = date '{{ cob_dt }}'

    UNION ALL

    SELECT
        'cross_sell_segment_current',
        COUNT(*),
        MAX(cob_dt)
    FROM {{ ref('cross_sell_segment_current') }}
    WHERE cob_dt = date '{{ cob_dt }}'
)

SELECT
    table_name,
    row_count,
    served_cob_dt,
    'Expected cob_dt=' || '{{ cob_dt }}' || ' but got ' || served_cob_dt AS failure_reason
FROM serving_counts
WHERE row_count = 0
   OR served_cob_dt IS NULL
   OR served_cob_dt != date '{{ cob_dt }}'
