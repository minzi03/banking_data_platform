# ADR-0007 — `overwritePartitions` theo `cob_dt`, không dùng MERGE

**Status**: Accepted
**Ngày**: kế thừa từ template khoá học · chuyển thể thành ADR 2026-09-22
**Liên quan**: [`0005`](0005-fail-loud-before-overwrite.md)

---

## Context

Bảng Gold là snapshot lịch sử: một dòng cho mỗi khách hàng, cho mỗi `cob_dt`. Chạy lại một ngày phải cho ra kết quả giống hệt lần đầu — nếu không, backfill và retry đều không an toàn.

Ba cách ghi khả dĩ:

| Cách | Vấn đề |
|---|---|
| `append` | Chạy lại nhân đôi dữ liệu của ngày đó |
| `overwrite` toàn bảng | Xoá sạch lịch sử các ngày khác |
| `MERGE` theo khoá | Cần định nghĩa khoá và điều kiện match cho từng model; đắt hơn vì phải đọc bảng đích |

## Decision

Dùng `writeTo(target).overwritePartitions()` với bảng partition theo `cob_dt`.

```python
result_df.writeTo(target).overwritePartitions()
```

Iceberg chỉ thay thế các partition **xuất hiện trong DataFrame ghi vào**. Model chỉ sinh dữ liệu cho một `cob_dt`, nên chỉ partition đó bị thay.

Tính chất thu được: **idempotent theo partition**. Chạy lại cùng `cob_dt` cho kết quả giống hệt; các ngày khác không bị đụng tới.

MERGE vẫn được dùng ở chỗ nó thực sự cần: SCD2 ở Silver, và CDC consolidation ([`0010`](0010-cdc-event-ordering.md)) — những nơi phải cập nhật bản ghi hiện hữu thay vì thay cả partition.

## Consequences

**Được**

- Chạy lại an toàn, không cần dọn dẹp thủ công trước.
- Backfill một ngày cụ thể không ảnh hưởng ngày khác.
- Không phải định nghĩa khoá merge cho từng model.
- Nhanh hơn MERGE: không phải đọc bảng đích để so khớp.

**Mất**

- **Ghi rỗng là no-op im lặng.** `overwritePartitions()` với DataFrame rỗng **không xoá gì** — nó để nguyên partition cũ và báo thành công. Đây chính là lý do `assert_non_empty` phải tồn tại ([`0005`](0005-fail-loud-before-overwrite.md)). Cái giá của quyết định này là một guard bắt buộc phải đi kèm.
- Không cập nhật được từng dòng. Sửa một khách hàng nghĩa là tính lại và ghi lại toàn bộ partition.
- Ràng buộc mọi Gold model phải partition theo `cob_dt`. Một model có grain khác — ví dụ theo tháng — vẫn phải mang cột `cob_dt`.
- Partition nhiều ngày trong một lần ghi sẽ thay **tất cả** các ngày đó. Không có cảnh báo nào nếu DataFrame vô tình chứa nhiều `cob_dt`.

## Evidence

```text
code_etl/gold/base_job/gold_job.py:161-198    run_gold_job, docstring giải thích idempotency
docker/init_iceberg/03_ddl_gold.sql           PARTITIONED BY (cob_dt)
code_etl/cdc/consolidation/cdc_consolidation.py   nơi MERGE vẫn được dùng
```
