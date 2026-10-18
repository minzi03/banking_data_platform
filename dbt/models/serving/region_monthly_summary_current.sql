-- =============================================================================
-- Serving: region_monthly_summary_current   (khoá new2 §2.2)
-- =============================================================================
-- Phân tích khu vực cho BI/app: doanh thu theo tỉnh/thành, tăng trưởng MoM,
-- xếp hạng khu vực trong tháng.
--
-- Grain: 1 dòng / city / txn_month — KHÔNG unique theo city, nên không khai
-- data_tests [unique] trên city ở _serving_models.yml.
--
-- Snapshot theo cob_dt (partition của mart lịch sử), đồng bộ với các serving
-- model khác. materialized=table: Trino + Iceberg REST không hỗ trợ createView.
-- =============================================================================

{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'region']) }}

{% set cob_dt = var('cob_dt', '1900-01-01') %}

select *
from {{ source('gold', 'mart_region_monthly_summary') }}
where cob_dt = date '{{ cob_dt }}'
