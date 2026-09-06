-- =============================================================================
-- Serving: campaign_target_current   (wave 2)
-- =============================================================================
-- Ownership:  Spark sở hữu Bronze/Silver/historical Gold.
--             dbt + Trino sở hữu tầng current-serving.
-- Thay cho:   CTAS table Spark tạo lúc init rồi không refresh (issue ⑤)
--
-- materialized=table, KHÔNG phải view: Trino + Iceberg REST catalog không hỗ
-- trợ createView (NOT_SUPPORTED, xác minh khi chạy pilot). Hệ quả: freshness
-- không tự nhiên — dbt DAG phải chạy mỗi cob_dt, cùng snapshot với Gold DAG.
--
-- cob_dt đến từ var, CỐ Ý không dùng MAX(cob_dt): MAX sẽ âm thầm phục vụ D1 khi
-- pipeline D2 fail, tức serving tự rơi về "latest available" trong khi semantics
-- mong muốn là "current verified processing date". Nhất quán với
-- require_snapshots trong gold_job.py.
--
--     dbt build --select serving --vars '{"cob_dt": "YYYY-MM-DD"}'
--
-- Grain: 1 dòng / customer_id.
-- =============================================================================

{{ config(materialized='table', schema='serving', tags=['serving', 'current_serving', 'wave2']) }}

-- ─── Vì sao dùng sentinel thay vì raise_compiler_error ──────────────────────
-- Bản đầu tiên dùng exceptions.raise_compiler_error khi thiếu var. Nhưng dbt
-- PARSE toàn bộ project trước mọi lệnh, nên nó làm chết cả những lệnh không
-- đụng tới serving (`dbt test --select assert_gold_source_reachable`,
-- `dbt docs generate`, `dbt ls` trong CI...). Quá nghiêm, và làm project không
-- inspect được nếu chưa chọn ngày.
--
-- Sentinel 1900-01-01 giữ nguyên tính fail-loud mà không phá parseability:
-- không truyền var → serving ra 0 dòng → assert_serving_snapshot_alignment
-- FAIL (rỗng, và served_cob_dt khác requested). Vẫn không có đường nào
-- publish lặng lẽ dữ liệu sai.
{% set cob_dt = var('cob_dt', '1900-01-01') %}

-- select * là CHỦ Ý: serving = lát cắt current của chính bảng historical, nên
-- schema phải bám theo nó. Liệt kê cột bằng tay ở 9 model sẽ drift lặng lẽ khi
-- Gold đổi schema. Ràng buộc cột quan trọng nằm ở _serving_models.yml.
select *
from {{ source('gold', 'campaign_target') }}
where cob_dt = date '{{ cob_dt }}'
