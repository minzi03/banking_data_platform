-- =============================================================================
-- Serving: merchant_ranking_current   (merchant QR/POS, khoá new2)
-- =============================================================================
-- Xếp hạng merchant theo MCC + tăng trưởng so kỳ trước. Grain merchant_id ×
-- txn_year × txn_month. Snapshot theo cob_dt.
--
-- Xem merchant_daily_summary_current.sql để biết lý do materialized=table và
-- cách cob_dt đến từ var.
-- =============================================================================

{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'merchant']) }}

{% set cob_dt = var('cob_dt', '1900-01-01') %}

select *
from {{ source('gold', 'mart_merchant_ranking') }}
where cob_dt = date '{{ cob_dt }}'
