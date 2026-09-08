-- =============================================================================
-- Singular Test: Balances Non-Negative
-- =============================================================================
-- Validates that financial amounts in the Customer 360 are non-negative
-- where applicable (balance, AUM, transaction amounts).
-- =============================================================================

{% set cob_dt = var('cob_dt', '1900-01-01') %}

SELECT
    customer_id,
    total_deposit_balance,
    total_loan_outstanding,
    aum_total,
    'Negative balance detected' AS failure_reason
FROM {{ ref('mart_customer_360_current') }}
WHERE cob_dt = date '{{ cob_dt }}'
  AND (
      total_deposit_balance < 0
      OR aum_total < 0
      OR total_loan_outstanding < 0
  )
