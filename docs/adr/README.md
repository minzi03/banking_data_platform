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

Bốn ADR trên được chuyển thể từ lý do **đã có sẵn bằng văn bản** trong code hoặc trong `technical-debt.md`. Không có phần nào được suy diễn thêm.

### Chưa viết — cần tác giả xác nhận lý do

| # dự kiến | Quyết định | Vì sao chưa viết được |
|---|---|---|
| **0001** | **Iceberg thay vì Delta Lake** | `ARCHITECTURE.md` mô tả Iceberg nhưng **không ghi lý do chọn nó thay vì Delta**. Khoá học nền (`thamkhao/Buoi 4`) dạy Iceberg, nên nhiều khả năng quyết định được kế thừa chứ không được suy ra. Viết ADR này bằng cách bịa ra bảng so sánh Iceberg-vs-Delta sẽ là đúng loại tuyên bố không kiểm chứng mà dự án này tồn tại để chống. **Cần tác giả nêu lý do thật** — kể cả nếu lý do là "khoá học dạy vậy", đó vẫn là một context hợp lệ và trung thực |

### Chưa viết — lý do đã có trong code, chỉ cần chuyển thể

| # dự kiến | Quyết định | Nguồn hiện tại |
|---|---|---|
| 0006 | Metadata-driven YAML thay vì code cho cả 3 tầng | kế thừa template khoá học, `*/base_job/` |
| 0007 | `overwritePartitions` theo `cob_dt` thay vì MERGE | docstring `gold_job.py` |
| 0008 | Evidence manifest là nguồn sự thật duy nhất cho số liệu | `scripts/generate_metrics_manifest.py` |
| 0009 | Sentinel `1900-01-01` thay vì `raise_compiler_error` | comment `mart_customer_360_current.sql` |
| 0010 | CDC watermark `(timestamp, batch_id)` theo từng bảng | `code_etl/cdc/` |
| 0011 | DLQ giữ Kafka partition/offset, Bronze CDC thì không | `code_etl/cdc/` |
| 0012 | Seed generator tham số hoá `--scale`, không tách mini-seeder | `technical-debt.md` TD-6 |
| 0013 | Khai báo nguồn phải khớp SQL | `test_declared_sources_match_sql.py` |
| 0014 | Kimball star schema thay vì Data Vault 2.0 | `docs/DATA_VAULT_MAPPING.md` |
