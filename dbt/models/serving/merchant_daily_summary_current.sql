-- =============================================================================
-- Serving: merchant_daily_summary_current   (merchant QR/POS, khoá new2)
-- =============================================================================
-- Ownership:  Spark sở hữu historical Gold; dbt + Trino sở hữu serving current.
--
-- LƯU Ý GRAIN: khác các serving model customer (1 dòng/customer). Bảng này là
-- grain merchant_id × txn_date (doanh thu theo ngày) — KHÔNG có khoá unique
-- đơn, nên không khai data_tests [unique] trên merchant_id ở _serving_models.yml.
-- Snapshot theo cob_dt (partition của mart lịch sử).
--
-- materialized=table, KHÔNG phải view: Trino + Iceberg REST không hỗ trợ
-- createView (NOT_SUPPORTED). cob_dt đến từ var, cố ý không dùng MAX(cob_dt).
--
--     dbt build --select serving --vars '{"cob_dt": "YYYY-MM-DD"}'
-- =============================================================================

{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'merchant']) }}

{% set cob_dt = var('cob_dt', '1900-01-01') %}

select *
from {{ source('gold', 'mart_merchant_daily_summary') }}
where cob_dt = date '{{ cob_dt }}'
