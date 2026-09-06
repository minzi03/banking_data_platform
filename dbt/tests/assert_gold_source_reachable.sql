-- =============================================================================
-- Smoke test connectivity — KHÔNG dựa vào `dbt debug`
-- =============================================================================
-- `dbt debug` chỉ mở kết nối tới Trino rồi báo "All checks passed!", nó KHÔNG
-- validate catalog. Khi profile dùng sai tên catalog (`lakehouse` thay vì
-- `iceberg`), debug vẫn xanh còn mọi model chết CATALOG_NOT_FOUND.
--
-- Test này chạm vào một bảng Gold có thật, nên:
--   - sai catalog / thiếu bảng → query lỗi → dbt build FAIL
--   - bảng rỗng               → trả về dòng   → dbt build FAIL
-- =============================================================================

select
    'gold.rfm_segment không đọc được hoặc rỗng' as failure_reason,
    count(*) as row_count
from {{ source('gold', 'rfm_segment') }}
having count(*) = 0
