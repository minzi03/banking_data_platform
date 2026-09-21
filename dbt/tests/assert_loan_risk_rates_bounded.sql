-- Singular Test: Loan Risk Rates Bounded
-- Validates overdue_rate and late_payment_rate are between 0 and 1
{% set cob_dt = var("cob_dt", "1900-01-01") %}
SELECT branch_code, product_code, overdue_rate, late_payment_rate,
  'overdue_rate=' || CAST(overdue_rate AS varchar)
    || ', late_payment_rate=' || CAST(late_payment_rate AS varchar) AS failure_reason
FROM {{ ref("loan_portfolio_risk_current") }}
WHERE cob_dt = date '{{ cob_dt }}'
  AND (overdue_rate < 0 OR overdue_rate > 1
    OR late_payment_rate < 0 OR late_payment_rate > 1)
