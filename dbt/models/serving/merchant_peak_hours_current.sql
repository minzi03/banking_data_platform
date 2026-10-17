-- =============================================================================
-- Serving: merchant_peak_hours_current   (khoá new2 §3.2 — use case 3)
-- =============================================================================
-- "Giờ cao điểm: số giao dịch và doanh thu theo khung giờ trong ngày, cập nhật
--  liên tục."
--
-- Grain: 1 dòng / merchant × giờ (0–23, giờ nghiệp vụ ICT).
--
-- VÌ SAO PHẢI QUY VỀ ICT Ở ĐÂY (khác bảng theo phút):
--   Cửa sổ 1 PHÚT không phụ thuộc múi giờ — gom theo UTC hay ICT cho cùng kết
--   quả phân hoạch. Cửa sổ 1 GIỜ thì khác hẳn: 17:00 ICT = 10:00 UTC. Gom theo
--   UTC sẽ khiến "giờ cao điểm" của merchant Việt Nam (tan tầm ~17–19h) hiện ra
--   là 10–12h. Đây đúng là lớp bug timezone đã gặp ở Gold (lệch 29,2% dòng
--   giữa Spark ICT và Trino UTC), nên quy đổi phải TƯỜNG MINH, không dựa vào
--   session timezone của engine.
--
-- materialized=table (Trino + Iceberg REST không hỗ trợ createView).
-- =============================================================================

{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'merchant', 'realtime']) }}

{% set cob_dt = var('cob_dt', '1900-01-01') %}

select
    merchant_id,
    -- Giờ nghiệp vụ ICT, tường minh theo từng plugin engine. dbt-trino không có
    -- from_utc_timestamp của Spark, nên dùng at_timezone + hour.
    hour(at_timezone(cast(transaction_date as timestamp), 'Asia/Ho_Chi_Minh')) as txn_hour,
    count(case when status = 'SUCCESS' then transaction_id end) as success_txn_count,
    sum(case when status = 'SUCCESS' then amount else 0 end) as revenue,
    avg(case when status = 'SUCCESS' then amount end) as avg_txn_amount,
    date '{{ cob_dt }}' as cob_dt
from {{ source('silver', 'fact_online_transaction') }}
where cob_dt = date '{{ cob_dt }}'
  and merchant_id is not null
group by merchant_id,
         hour(at_timezone(cast(transaction_date as timestamp), 'Asia/Ho_Chi_Minh'))
