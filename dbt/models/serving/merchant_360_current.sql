-- =============================================================================
-- Serving: merchant_360_current   (merchant QR/POS, khoá new2)
-- =============================================================================
-- Merchant 360° current snapshot — grain 1 dòng/merchant (cob_dt partition của
-- mart lịch sử). Phục vụ app tư vấn sản phẩm (advisory_product_hint).
--
-- Xem merchant_daily_summary_current.sql để biết lý do materialized=table và
-- cách cob_dt đến từ var.
-- =============================================================================

{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'merchant']) }}

{% set cob_dt = var('cob_dt', '1900-01-01') %}

select *
from {{ source('gold', 'mart_merchant_360') }}
where cob_dt = date '{{ cob_dt }}'
