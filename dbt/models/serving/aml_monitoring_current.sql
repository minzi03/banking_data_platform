{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'risk']) }}
{% set cob_dt = var('cob_dt', '1900-01-01') %}
select * from {{ source('gold', 'aml_monitoring') }}
where cob_dt = date '{{ cob_dt }}'
