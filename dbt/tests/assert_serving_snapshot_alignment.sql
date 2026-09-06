-- =============================================================================
-- Fail-loud cho toàn bộ serving layer
-- =============================================================================
-- Model lọc theo var cob_dt (xem model SQL) nên không bao giờ phục
-- vụ nhầm ngày cũ. Nhưng khi Gold thiếu partition đó, nó tạo bảng RỖNG và
-- `dbt run` vẫn báo thành công — silent corruption ở dạng khác: consumer thấy
-- 0 khách hàng thay vì thấy lỗi.
--
-- Test trả về dòng (→ FAIL) khi bất kỳ serving object nào:
--   - rỗng, hoặc
--   - chứa nhiều hơn một cob_dt, hoặc
--   - phục vụ cob_dt khác var đã yêu cầu
--
-- Cùng triết lý với require_snapshots / require_non_empty ở gold_job.py.
-- =============================================================================

-- var có default sentinel giống các model: không truyền --vars thì project vẫn
-- PARSE được (`dbt docs generate`, `dbt ls`, CI...), còn khi build thật mà
-- thiếu var thì served_cob_dt sẽ khác requested → test FAIL.
{% set requested_cob_dt = var('cob_dt', '1900-01-01') %}

with serving as (
    select 'rfm_segment_current' as model, count(*) as row_count,
           count(distinct cob_dt) as distinct_cob_dt,
           max(cast(cob_dt as varchar)) as served_cob_dt
    from {{ ref('rfm_segment_current') }}
    union all
    select 'churn_prediction_current' as model, count(*) as row_count,
           count(distinct cob_dt) as distinct_cob_dt,
           max(cast(cob_dt as varchar)) as served_cob_dt
    from {{ ref('churn_prediction_current') }}
    union all
    select 'customer_transaction_summary_current' as model, count(*) as row_count,
           count(distinct cob_dt) as distinct_cob_dt,
           max(cast(cob_dt as varchar)) as served_cob_dt
    from {{ ref('customer_transaction_summary_current') }}
    union all
    select 'customer_balance_summary_current' as model, count(*) as row_count,
           count(distinct cob_dt) as distinct_cob_dt,
           max(cast(cob_dt as varchar)) as served_cob_dt
    from {{ ref('customer_balance_summary_current') }}
    union all
    select 'customer_card_summary_current' as model, count(*) as row_count,
           count(distinct cob_dt) as distinct_cob_dt,
           max(cast(cob_dt as varchar)) as served_cob_dt
    from {{ ref('customer_card_summary_current') }}
    union all
    select 'customer_product_summary_current' as model, count(*) as row_count,
           count(distinct cob_dt) as distinct_cob_dt,
           max(cast(cob_dt as varchar)) as served_cob_dt
    from {{ ref('customer_product_summary_current') }}
    union all
    select 'cross_sell_segment_current' as model, count(*) as row_count,
           count(distinct cob_dt) as distinct_cob_dt,
           max(cast(cob_dt as varchar)) as served_cob_dt
    from {{ ref('cross_sell_segment_current') }}
    union all
    select 'campaign_target_current' as model, count(*) as row_count,
           count(distinct cob_dt) as distinct_cob_dt,
           max(cast(cob_dt as varchar)) as served_cob_dt
    from {{ ref('campaign_target_current') }}
    union all
    select 'mart_customer_360_current' as model, count(*) as row_count,
           count(distinct cob_dt) as distinct_cob_dt,
           max(cast(cob_dt as varchar)) as served_cob_dt
    from {{ ref('mart_customer_360_current') }}
)
select
    model,
    row_count,
    distinct_cob_dt,
    served_cob_dt,
    '{{ requested_cob_dt }}' as requested_cob_dt
from serving
where row_count = 0
   or distinct_cob_dt <> 1
   or served_cob_dt <> '{{ requested_cob_dt }}'
