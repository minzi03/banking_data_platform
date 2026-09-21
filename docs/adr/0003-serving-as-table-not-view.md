# ADR-0003 — Tầng serving là `table`, không phải `view`

**Status**: Accepted
**Ngày**: 2026-09 (chuyển thể thành ADR 2026-09-22)
**Liên quan**: [`0002`](0002-cross-engine-catalog-naming.md) · [`../DBT_DEPLOYMENT.md`](../DBT_DEPLOYMENT.md)

---

## Context

Tầng Gold lưu dữ liệu **lịch sử**: một dòng cho mỗi khách hàng, cho mỗi `cob_dt`. Consumer (Superset, API, SQL trong README) thì hầu như luôn muốn **lát cắt hiện tại**: một dòng cho mỗi khách hàng, của `cob_dt` mới nhất.

Cách tự nhiên nhất để biểu diễn "lát cắt hiện tại của một bảng lịch sử" là một **view**. View không nhân bản dữ liệu, luôn phản ánh trạng thái mới nhất, và không cần lịch chạy.

Nhưng object serving do **Trino** sở hữu, không phải Spark — consumer thật đều đi qua Trino. Và **Iceberg REST catalog của Trino không hỗ trợ `createView`**: thử tạo view trả về `NOT_SUPPORTED`. Điều này được xác minh lúc chạy pilot, không phải suy đoán từ tài liệu.

## Decision

Materialize tầng serving thành **table**, dbt sở hữu, publish theo từng `cob_dt`:

```yaml
# dbt/dbt_project.yml
serving:
  # view không khả dụng: Iceberg REST catalog của Trino không hỗ trợ createView
  +materialized: table
  +schema: serving
```

Kèm hai quyết định phụ, cả hai đều là hệ quả trực tiếp:

**`select *` trong model serving là chủ ý.** Serving là lát cắt current của chính bảng historical, nên schema phải bám theo nó. Liệt kê cột bằng tay ở 13 model sẽ drift lặng lẽ khi Gold đổi schema — và drift kiểu đó không làm gì đỏ cả, nó chỉ làm mất cột ở tầng BI. Ràng buộc cột quan trọng nằm ở `_serving_models.yml`, không ở danh sách `SELECT`.

**Sentinel `1900-01-01` thay vì `raise_compiler_error`.** Bản đầu tiên raise lỗi khi thiếu var `cob_dt`. Nhưng dbt **parse toàn bộ project trước mọi lệnh**, nên nó làm chết cả những lệnh không đụng tới serving: `dbt docs generate`, `dbt ls`, `dbt test --select assert_gold_source_reachable` trong CI. Quá nghiêm, và làm project không inspect được nếu chưa chọn ngày.

Sentinel giữ được tính fail-loud mà không phá parseability: không truyền var → serving ra 0 dòng → `assert_serving_snapshot_alignment` FAIL. Vẫn không có đường nào publish lặng lẽ dữ liệu sai.

## Consequences

**Được**

- Tầng serving hoạt động được trên Trino, là nơi consumer thật đứng.
- Query serving nhanh hơn view: không phải quét toàn bộ lịch sử rồi lọc `max(cob_dt)` mỗi lần đọc.
- `select *` làm schema serving tự bám Gold, loại bỏ một nguồn drift thủ công.

**Mất**

- **Dữ liệu bị nhân bản.** Mỗi `cob_dt` publish là một lần ghi lại toàn bộ lát cắt current. Đây là cái giá trực tiếp của việc không dùng được view.
- **Serving có thể cũ.** Table chỉ mới bằng lần `dbt build` gần nhất. View thì luôn mới. Phải có `assert_serving_snapshot_alignment` để phát hiện lệch — tức là quyết định này **đẻ thêm** một invariant phải bảo trì.
- Phụ thuộc vào một giới hạn của engine. Nếu Trino hỗ trợ `createView` trên Iceberg REST trong tương lai, quyết định này nên được xem lại — và khi đó viết ADR mới, không sửa ADR này.
- `select *` đánh đổi: chống được drift thủ công, nhưng một cột thêm vào Gold sẽ lặng lẽ xuất hiện ở serving mà không ai review.

## Evidence

```text
dbt/dbt_project.yml:32                          comment ghi lý do NOT_SUPPORTED
dbt/models/serving/*.sql                        13 model, mỗi file có header giải thích
dbt/models/serving/_serving_models.yml          ràng buộc cột
docs/evidence/metrics-manifest.yaml             serving.objects_present, current_snapshot_alignment
```

Lưu ý: `NOT_SUPPORTED` được xác minh khi chạy pilot thật, không phải đọc từ tài liệu Trino. Đó là khác biệt giữa một giới hạn đã đo và một giả định.
