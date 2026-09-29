{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'risk']) }}
-- Vintage tại cob_dt: theo tháng giải ngân, tỷ lệ khoản TỪNG quá hạn 30+ / 90+
-- ngày trên toàn lịch sử trả nợ. Một cob_dt cho một điểm của mỗi đường cong
-- vintage (tại months_on_book hiện tại của cohort); ghép nhiều cob_dt để có
-- cả đường cong.
--
-- Loại WRITTEN_OFF: generator không sinh lịch trả cho chúng, nên ever_* = 0 là
-- thiếu dữ liệu, không phải "chưa từng quá hạn".
select
    vintage_month,
    count(*) as loans,
    cast(sum(loan_amount) as decimal(18, 2)) as disbursed_amount,
    min(months_on_book) as min_months_on_book,
    max(months_on_book) as max_months_on_book,
    avg(cast(ever_30_plus as double)) as ever_30_plus_rate,
    avg(cast(ever_90_plus as double)) as ever_90_plus_rate,
    avg(cast(is_npl as double)) as npl_loan_rate,
    cob_dt
from {{ ref('loan_delinquency_current') }}
where loan_status <> 'WRITTEN_OFF'
group by vintage_month, cob_dt
