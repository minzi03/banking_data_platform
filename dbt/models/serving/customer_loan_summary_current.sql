-- =============================================================================
-- Serving: customer_loan_summary_current (wave 2)
-- =============================================================================
-- Gold lịch sử do Spark sở hữu; current-serving do dbt + Trino sở hữu.
-- Grain: 1 dòng / customer_id cho cob_dt được yêu cầu.
-- Không dùng MAX(cob_dt): tránh phục vụ snapshot cũ khi pipeline mới lỗi.
-- =============================================================================

{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'wave2']) }}

{% set cob_dt = var('cob_dt', '1900-01-01') %}

select *
from {{ source('gold', 'customer_loan_summary') }}
where cob_dt = date '{{ cob_dt }}'
