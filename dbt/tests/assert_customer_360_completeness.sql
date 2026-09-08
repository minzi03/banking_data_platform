-- =============================================================================
-- Singular Test: Customer 360 Completeness
-- =============================================================================
-- Validates that the Customer 360 serving table has:
--   1. At least 1000 customers (basic sanity check)
--   2. All three segments present (RETAIL, PRIORITY, VIP)
--   3. All KYC statuses present (VERIFIED, PENDING, REJECTED)
-- =============================================================================

{% set cob_dt = var('cob_dt', '1900-01-01') %}

WITH customer_counts AS (
    SELECT
        COUNT(DISTINCT customer_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN customer_segment = 'RETAIL' THEN customer_id END) AS retail_count,
        COUNT(DISTINCT CASE WHEN customer_segment = 'PRIORITY' THEN customer_id END) AS priority_count,
        COUNT(DISTINCT CASE WHEN customer_segment = 'VIP' THEN customer_id END) AS vip_count,
        COUNT(DISTINCT CASE WHEN kyc_status = 'VERIFIED' THEN customer_id END) AS verified_count,
        COUNT(DISTINCT CASE WHEN kyc_status = 'PENDING' THEN customer_id END) AS pending_count,
        COUNT(DISTINCT CASE WHEN kyc_status = 'REJECTED' THEN customer_id END) AS rejected_count
    FROM {{ ref('mart_customer_360_current') }}
    WHERE cob_dt = date '{{ cob_dt }}'
)

SELECT
    CASE
        WHEN total_customers < 1000 THEN 'FAIL: Less than 1000 customers (' || total_customers || ')'
        WHEN retail_count = 0 THEN 'FAIL: No RETAIL customers found'
        WHEN priority_count = 0 THEN 'FAIL: No PRIORITY customers found'
        WHEN vip_count = 0 THEN 'FAIL: No VIP customers found'
        WHEN verified_count = 0 THEN 'FAIL: No VERIFIED KYC customers found'
    END AS failure_reason,
    total_customers,
    retail_count,
    priority_count,
    vip_count
FROM customer_counts
WHERE total_customers < 1000
   OR retail_count = 0
   OR priority_count = 0
   OR vip_count = 0
   OR verified_count = 0
