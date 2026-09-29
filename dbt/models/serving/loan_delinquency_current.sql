{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'risk']) }}
{% set cob_dt = var('cob_dt', '1900-01-01') %}
-- Nợ quá hạn theo khoản vay tại cob_dt: DPD, nhóm nợ (TT 11/2021, theo số ngày
-- quá hạn), roll pair, vintage. Nguồn: gold.loan_delinquency (ROADMAP 3.7).
select * from {{ source('gold', 'loan_delinquency') }}
where cob_dt = date '{{ cob_dt }}'
