-- =============================================================================
-- Serving: merchant_revenue_per_minute_current   (khoá new2 §3.2 — use case 2)
-- =============================================================================
-- "Doanh thu theo cửa hàng theo phút: phục vụ dashboard real-time."
--
-- Grain: 1 dòng / merchant × phút (cửa sổ 1 phút, giờ nghiệp vụ ICT).
--
-- Trước đây bảng trống thì không ai phát hiện: 3 use case Phase 2 được liệt kê
-- trong sản phẩm bàn giao nhưng platform chưa có bảng nào.
--
-- Lưu ý grain thời gian:
--   `date_trunc('minute', transaction_date)` gom theo phút của giờ LƯU TRỮ (UTC
--   theo ADR-0004). Cửa sổ 1 phút không phụ thuộc múi giờ nên gom UTC là đúng
--   và ổn định — khác với ngày nghiệp vụ, nơi UTC vs ICT lệch hẳn một ngày.
--
-- Chỉ giao dịch SUCCESS mới tính doanh thu; FAILED/PENDING vẫn đếm để theo dõi
-- tỷ lệ lỗi theo phút (vận hành cần biết khi cổng thanh toán có sự cố).
--
-- materialized=table (Trino + Iceberg REST không hỗ trợ createView).
-- =============================================================================

{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'merchant', 'realtime']) }}

{% set cob_dt = var('cob_dt', '1900-01-01') %}

select
    merchant_id,
    date_trunc('minute', transaction_date) as txn_minute,
    count(case when status = 'SUCCESS' then transaction_id end) as success_txn_count,
    count(case when status = 'FAILED' then transaction_id end) as failed_txn_count,
    sum(case when status = 'SUCCESS' then amount else 0 end) as revenue,
    avg(case when status = 'SUCCESS' then amount end) as avg_txn_amount,
    count(case when payment_channel = 'QR' then transaction_id end) as qr_count,
    count(case when payment_channel = 'POS' then transaction_id end) as pos_count,
    date '{{ cob_dt }}' as cob_dt
from {{ source('silver', 'fact_online_transaction') }}
where cob_dt = date '{{ cob_dt }}'
  and merchant_id is not null
group by merchant_id, date_trunc('minute', transaction_date)
