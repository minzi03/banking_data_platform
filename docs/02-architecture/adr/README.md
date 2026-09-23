# Architecture Decision Records

Mỗi ADR ghi lại **một quyết định kiến trúc**: bối cảnh lúc đó, cái đã chọn, và hệ quả phải sống chung.

## Vì sao có thư mục này

33 chỗ giải thích *"Vì sao / Lý do"* đang nằm rải trong comment của 20 file `.py` / `.yml` / `.sql`. Đó là phần tài sản trí tuệ lớn nhất của dự án — và không ai đọc được nếu không mở đúng file.

ADR kéo chúng ra chỗ đọc được, **không thay thế** comment trong code. Comment giải thích cho người đang sửa dòng đó; ADR giải thích cho người đang đánh giá hệ thống.

## Quy tắc

**ADR là bất biến.** Không sửa nội dung một ADR đã `Accepted`. Khi quyết định thay đổi, viết ADR mới và đổi `Status` của cái cũ thành `Superseded by ADR-XXXX`. Lịch sử quyết định sai cũng có giá trị — nó cho thấy vì sao phương án hiện tại được chọn.

**Trạng thái**: `Proposed` · `Accepted` · `Superseded by ADR-XXXX` · `Deprecated`

**Khuôn**: Context → Decision → Consequences → Evidence. Phần *Consequences* phải nêu cả cái mất, không chỉ cái được — một ADR chỉ liệt kê ưu điểm là quảng cáo, không phải tài liệu kỹ thuật.

## Danh sách

| # | Quyết định | Trạng thái |
|---|---|---|
| [0002](0002-cross-engine-catalog-naming.md) | Spark catalog `lakehouse` ≠ Trino catalog `iceberg` | Accepted |
| [0003](0003-serving-as-table-not-view.md) | Tầng serving là `table`, không phải `view` | Accepted |
| [0004](0004-business-date-under-utc-session.md) | Business date suy ra tường minh, session bắt buộc UTC | Accepted |
| [0005](0005-fail-loud-before-overwrite.md) | Fail loud trước khi ghi đè partition | Accepted |

| [0006](0006-metadata-driven-jobs.md) | Job metadata-driven bằng YAML cho cả ba tầng | Accepted |
| [0007](0007-overwrite-partitions-by-cob-dt.md) | `overwritePartitions` theo `cob_dt`, không dùng MERGE | Accepted |
| [0008](0008-evidence-manifest-as-verifier.md) | Evidence manifest là verifier, không phải nơi dump số | Accepted |
| [0010](0010-cdc-event-ordering.md) | Thứ tự sự kiện CDC dùng `(timestamp_ms, batch_id)` | Accepted |
| [0012](0012-parameterised-seed-not-second-seeder.md) | Tham số hoá generator bằng `--scale`, không viết seeder thứ hai | Accepted |
| [0013](0013-declared-sources-match-sql.md) | Khai báo nguồn phải khớp với SQL | Accepted |
| [0014](0014-kimball-over-data-vault.md) | Kimball star schema, Data Vault chỉ ở mức tài liệu ánh xạ | Accepted |
| [0015](0015-trino-access-control-generated-from-rbac.md) | Access control của Trino sinh từ `rbac.py`, danh tính do client tự khai | Accepted |

ADR 0002–0014 được chuyển thể từ lý do **đã có sẵn bằng văn bản** trong code hoặc trong `technical-debt.md`. Không có phần nào được suy diễn thêm. ADR 0015 viết cùng lúc với thay đổi nó ghi lại.

### Hai số không được dùng

| # | Lý do |
|---|---|
| **0009** | Sentinel `1900-01-01` thay `raise_compiler_error` — đã nằm trong [`0003`](0003-serving-as-table-not-view.md) như quyết định hệ quả. Tách ra thành ADR riêng sẽ trùng lặp, mà ADR là bất biến nên không sửa được 0003 để gỡ phần đó ra |
| **0011** | DLQ giữ toạ độ Kafka còn Bronze CDC thì không — đã nằm trong [`0010`](0010-cdc-event-ordering.md). Docstring của `cdc_consolidation.py` gọi đây là *limitation* kèm chữ "yet", tức là một khoảng trống đã chấp nhận chứ không phải hai quyết định độc lập |

Số thứ tự **không được tái sử dụng**. Bỏ trống rõ ràng tốt hơn là gán lại cho quyết định khác — người đọc sau sẽ không phải tự hỏi ADR-0011 nói gì.

### Chưa viết — cần tác giả xác nhận lý do

| # dự kiến | Quyết định | Vì sao chưa viết được |
|---|---|---|
| **0001** | **Iceberg thay vì Delta Lake** | `ARCHITECTURE.md` mô tả Iceberg nhưng **không ghi lý do chọn nó thay vì Delta**. Khoá học nền (`thamkhao/Buoi 4`) dạy Iceberg, nên nhiều khả năng quyết định được kế thừa chứ không được suy ra. Viết ADR này bằng cách bịa ra bảng so sánh Iceberg-vs-Delta sẽ là đúng loại tuyên bố không kiểm chứng mà dự án này tồn tại để chống. **Cần tác giả nêu lý do thật** — kể cả nếu lý do là "khoá học dạy vậy", đó vẫn là một context hợp lệ và trung thực |

### Ứng viên cho ADR tiếp theo

Chưa có lý do bằng văn bản ở đâu, nên chưa chuyển thể được. Ghi ra để không bị quên:

| Quyết định | Cần gì để viết |
|---|---|
| Vì sao Superset chứ không phải Power BI/Metabase | Lý do chọn — hiện chỉ có kết quả, không có lập luận |
| Vì sao MinIO chứ không phải object storage khác | Như trên |
| Vì sao Airflow chứ không phải Dagster/Prefect | Như trên |
| PII masking ở Gold/serving, không ở Bronze | Nêu trong `SECURITY.md` như hiện trạng, chưa có ADR ghi lý do |
