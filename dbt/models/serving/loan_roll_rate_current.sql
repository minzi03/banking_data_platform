{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'risk']) }}
-- Ma trận roll rate một kỳ tại cob_dt: mỗi dòng là một cặp (bucket kỳ trước →
-- bucket kỳ này). roll_rate = tỷ trọng trong các khoản cùng bucket kỳ trước,
-- nên mỗi from_bucket cộng lại bằng 1.
--
-- Chỉ tính khoản đã có ít nhất hai kỳ tới hạn (prev_dpd_bucket không NULL) và
-- còn trong bảng cân đối (WRITTEN_OFF không có kỳ trả nên tự bị loại).
with pairs as (
    select
        prev_dpd_bucket as from_bucket,
        dpd_bucket as to_bucket,
        count(*) as loans,
        sum(outstanding_balance) as outstanding_balance,
        cob_dt
    from {{ ref('loan_delinquency_current') }}
    where prev_dpd_bucket is not null
    group by prev_dpd_bucket, dpd_bucket, cob_dt
)
select
    from_bucket,
    to_bucket,
    loans,
    cast(outstanding_balance as decimal(18, 2)) as outstanding_balance,
    cast(loans as double) / sum(loans) over (partition by from_bucket) as roll_rate,
    cob_dt
from pairs
