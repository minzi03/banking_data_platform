-- =============================================================================
-- Serving: merchant_payment_notification_current   (khoá new2 §3.2 — use case 1)
-- =============================================================================
-- "Thông báo tiền về: mỗi giao dịch QR thành công được ghi vào bảng thông báo
--  theo merchant trong vài giây."
--
-- Grain: 1 dòng / merchant — giao dịch THÀNH CÔNG gần nhất của merchant đó.
-- Đây là bảng phục vụ badge "vừa nhận được tiền" trên app merchant: mỗi
-- merchant chỉ cần thấy thông báo mới nhất, không cần toàn bộ lịch sử.
--
-- Vì sao không phải "1 dòng / giao dịch": bảng log mọi giao dịch sẽ phình vô
-- hạn trên Postgres serving. Ứng dụng poll theo merchant_id; khoá upsert
-- merchant_id giữ bảng luôn ~= số merchant.
--
-- PII (§4.3): merchant_name KHÔNG xuất ra đây — chỉ merchant_id. Ứng dụng đã
-- biết merchant của mình.
--
-- materialized=table (Trino + Iceberg REST không hỗ trợ createView).
-- =============================================================================

{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'merchant', 'realtime']) }}

{% set cob_dt = var('cob_dt', '1900-01-01') %}

with successful_txn as (
    select
        merchant_id,
        transaction_id,
        amount,
        currency,
        payment_channel,
        transaction_date,
        -- Giờ nghiệp vụ (ICT) để app hiển thị, KHÔNG dùng giờ UTC của storage.
        cast(transaction_date as timestamp) as notified_at
    from {{ source('silver', 'fact_online_transaction') }}
    where cob_dt = date '{{ cob_dt }}'
      and merchant_id is not null
      and status = 'SUCCESS'
),

latest_per_merchant as (
    select
        merchant_id,
        transaction_id,
        amount,
        currency,
        payment_channel,
        transaction_date,
        notified_at,
        row_number() over (
            partition by merchant_id
            order by transaction_date desc, transaction_id desc
        ) as rn
    from successful_txn
)

select
    merchant_id,
    transaction_id as latest_transaction_id,
    amount as latest_amount,
    currency,
    payment_channel,
    transaction_date as latest_transaction_date,
    notified_at as latest_notified_at,
    date '{{ cob_dt }}' as cob_dt
from latest_per_merchant
where rn = 1
