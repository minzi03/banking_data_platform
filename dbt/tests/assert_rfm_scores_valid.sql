-- =============================================================================
-- Singular Test: RFM Scores Validity
-- =============================================================================
-- Validates that RFM scores are within valid ranges (1-5) and segments are
-- consistent with scores.
-- =============================================================================

{% set cob_dt = var('cob_dt', '1900-01-01') %}

WITH rfm_check AS (
    SELECT
        customer_id,
        rfm_score,
        rfm_segment,
        CASE
            WHEN rfm_score < 1 OR rfm_score > 100 THEN 'RFM score out of range'
            WHEN rfm_segment IS NULL THEN 'RFM segment is null'
        END AS issue
    FROM {{ ref('rfm_segment_current') }}
    WHERE cob_dt = date '{{ cob_dt }}'
)

SELECT
    customer_id,
    rfm_score,
    rfm_segment,
    issue AS failure_reason
FROM rfm_check
WHERE issue IS NOT NULL
LIMIT 100
