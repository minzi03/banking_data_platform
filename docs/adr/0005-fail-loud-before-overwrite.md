# ADR-0005 — Fail loud trước khi ghi đè partition

**Status**: Accepted
**Ngày**: 2026-09 (chuyển thể thành ADR 2026-09-22)
**Liên quan**: [`0004`](0004-business-date-under-utc-session.md) · [`../COURSE_BASELINE_DIFF.md`](../COURSE_BASELINE_DIFF.md) §3

---

## Context

Engine Gold ban đầu — kế thừa từ template khoá học — ghi thẳng, không kiểm tra gì:

```python
result_df = load_source_df(spark, config, cob_dt)
result_df.writeTo(target).overwritePartitions()
```

Hai kịch bản hỏng mà cách viết này không phát hiện được:

**Kịch bản 1 — silent corruption bằng số 0.** Các model grain khách hàng neo vào `dim_customer` rồi LEFT JOIN các bảng fact. Nếu partition fact của `cob_dt` không tồn tại, query **vẫn trả về đủ một dòng cho mỗi khách hàng**, với mọi metric bằng 0. Output không rỗng. Số dòng đúng như mong đợi. Mọi kiểm tra "có dữ liệu không" đều pass. Và bảng Gold bị ghi đè bằng một tập số 0 trông hoàn toàn hợp lý.

**Kịch bản 2 — ghi đè rỗng là no-op.** `overwritePartitions()` với DataFrame rỗng **không xoá gì cả**. Nó để nguyên partition cũ. Job báo thành công, Airflow xanh, và bảng vẫn mang dữ liệu của ngày hôm trước mà không ai biết.

Cả hai đều tệ hơn một exception. Một job chết thì có người sửa; một bảng sai mà mọi chỉ báo đều xanh thì có người ra quyết định dựa trên nó.

## Decision

Chặn **trước khi ghi**, không báo sau khi ghi. Ba lớp trong `code_etl/gold/base_job/gold_job.py`:

```python
assert_source_snapshots(spark, config, cob_dt, logger)   # ① partition nguồn có tồn tại?
result_df = load_source_df(spark, config, cob_dt)
assert_non_empty(result_df, config, cob_dt, logger)      # ② kết quả có rỗng không?
if not table_exists(spark, target):                      # ③ bảng đích có tồn tại?
    create_iceberg_table_if_not_exists(result_df, target, logger)
result_df.writeTo(target).overwritePartitions()
```

**Guard ① là guard chính, và `require_non_empty` không thay thế được nó.** Đây là điểm dễ hiểu nhầm nhất: kịch bản 1 tạo ra output *không rỗng*, nên một check "không rỗng" sẽ pass và để lỗi đi tiếp. Chỉ có kiểm tra trực tiếp sự tồn tại của partition nguồn mới bắt được.

Guard ② khai báo qua `validation.require_non_empty`, mặc định tắt — vì có model hoàn toàn có thể rỗng một cách hợp lệ (ví dụ một ngày không có cảnh báo AML nào).

## Consequences

**Được**

- Hai chế độ hỏng âm thầm chuyển thành fail rõ ràng, có thông điệp nêu đúng bảng và `cob_dt` nào thiếu.
- `require_snapshots` trở thành khai báo phụ thuộc đọc được, dùng được cho cả lineage.

**Mất**

- **Khai báo thừa biến guard thành nguồn lỗi giả.** Nếu `require_snapshots` liệt kê một bảng mà SQL không đọc, job chết vì thiếu partition của thứ nó không cần. Chuyện này đã xảy ra với `customer_360.yml` và `silver.fact_online_transaction`. Phải thêm `tests/governance/test_declared_sources_match_sql.py` để giữ khai báo khớp với SQL — tức là guard này **đẻ thêm** một invariant phải bảo trì.
- Mỗi guard là một lần quét bảng thêm trước khi transform chạy.
- Guard chỉ kiểm tra partition **tồn tại**, không kiểm tra nó **đúng**. Một partition có dữ liệu sai vẫn qua được.
- `create_iceberg_table_if_not_exists` làm mờ ranh giới giữa bootstrap và daily run: một lỗi đánh máy trong tên bảng đích sẽ tạo bảng mới thay vì báo lỗi.

## Evidence

```text
code_etl/gold/base_job/gold_job.py:94-145        assert_source_snapshots, assert_non_empty
tests/governance/test_declared_sources_match_sql.py   giữ khai báo khớp SQL (49 test)
docs/COURSE_BASELINE_DIFF.md §3                  template gốc 85 dòng, không guard nào
```

Docstring trong `gold_job.py` giữ nguyên lập luận đầy đủ cho người đang sửa code; ADR này dành cho người đang đánh giá hệ thống.
