# ADR-0002 — Spark catalog `lakehouse` ≠ Trino catalog `iceberg`

**Status**: Accepted
**Ngày**: 2026-09-14 (chuyển thể thành ADR 2026-09-22)
**Liên quan**: [`../technical-debt.md`](../../05-quality/technical-debt.md) TD-7

---

## Context

Cùng một bảng Iceberg được hai engine đọc, và mỗi engine gọi catalog bằng một tên khác nhau:

```text
Spark  →  lakehouse.silver.dim_customer     (spark-defaults.conf)
Trino  →  iceberg.silver.dim_customer       (catalog properties)
```

Hai tên này đến từ hai file cấu hình độc lập, không có ràng buộc nào ép chúng khớp nhau — và cũng không có lý do kỹ thuật nào bắt buộc chúng phải khác nhau. Chúng khác nhau vì được cấu hình ở hai thời điểm, bởi hai tầng khác nhau của stack.

Hệ quả: code kết nối Trino mà dùng `lakehouse` sẽ fail lúc chạy với `Catalog 'lakehouse' not found`.

**Chuyện này đã xảy ra bốn lần.** Lần gần nhất là Streamlit dashboard (TD-7) dùng `catalog="lakehouse"`.

Điều khiến lỗi này nguy hiểm hơn một lỗi chính tả bình thường: **nó không crash lúc import.** Module load bình thường, test unit pass, CI xanh. Nó chỉ nổ khi có người thật mở dashboard và chạy query thật.

## Decision

Giữ nguyên hai tên khác nhau, **và** thêm một static check chặn tên Spark lọt vào code Trino-facing.

Không đổi tên cho khớp nhau, vì:
- Đổi tên Spark catalog phá mọi `spark.sql()` đang dùng `lakehouse.*`
- Đổi tên Trino catalog phá mọi query trong dbt, Superset, README, và tài liệu
- Chi phí đổi tên cao hơn chi phí một test tĩnh

`tests/governance/test_trino_catalog_contract.py` quét các file `.py`/`.sql` có dấu hiệu kết nối Trino (`trino.dbapi.connect`, `run_trino_query`, `--catalog`) và fail nếu thấy `lakehouse` được dùng làm tham số catalog.

## Consequences

**Được**

- Lỗi chuyển từ runtime (người dùng phát hiện) sang CI (máy phát hiện), vì đây là lỗi tĩnh nên phải bắt tĩnh.
- Tên catalog trở thành **contract có tên**, không còn là quy ước ngầm.

**Mất**

- Mỗi khi thêm một consumer Trino mới, phải nhớ nó nằm trong phạm vi quét — hoặc mở rộng `search_dirs` trong test. Test dựa trên danh sách thư mục, nên một consumer đặt ngoài danh sách sẽ không được bảo vệ.
- Sự bất đối xứng vẫn còn: người mới vào vẫn sẽ hỏi "vì sao hai tên khác nhau?". ADR này là câu trả lời, nhưng nó không làm hệ thống bớt lạ.
- Test bắt được `catalog="lakehouse"` dạng literal. Nếu tên catalog được dựng động (nối chuỗi, đọc từ config), test sẽ không thấy.

## Evidence

```text
tests/governance/test_trino_catalog_contract.py     static check, chạy trong job Lint & Format
docs/technical-debt.md TD-7                         4 lần tái phát, acceptance criteria
docs/evidence/metrics-manifest.yaml                 environment.spark_catalog: lakehouse
                                                    environment.trino_catalog: iceberg
```

Ghi chú cho người đọc sau: bốn lần tái phát là con số đáng chú ý hơn bản thân quyết định. Khi một lỗi lặp lại đến lần thứ tư, vấn đề không nằm ở người viết code mà ở chỗ hệ thống cho phép nó xảy ra mà không báo.
