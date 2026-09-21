# ADR-0013 — Khai báo nguồn phải khớp với SQL

**Status**: Accepted
**Ngày**: 2026-09-21
**Liên quan**: [`0005`](0005-fail-loud-before-overwrite.md) · [`0006`](0006-metadata-driven-jobs.md)

---

## Context

Job metadata-driven ([`0006`](0006-metadata-driven-jobs.md)) khai báo bảng nguồn ở **ba chỗ**, mỗi chỗ phục vụ một mục đích khác nhau:

```yaml
source.tables:                 # lineage và tài liệu
upstream_flags:                # SqlSensor chờ upstream trong Airflow
validation.require_snapshots:  # assert_source_snapshots() — RÀNG BUỘC CHẶN
```

Ba danh sách này do người viết duy trì bằng tay, và không có gì ép chúng khớp với `sql` bên dưới.

Đây là **mặt trái của guard ở [`0005`](0005-fail-loud-before-overwrite.md)**. Guard đó đúng và cần thiết. Nhưng nó chỉ đúng khi khai báo phản ánh thực tế: nếu `require_snapshots` liệt kê một bảng mà SQL không hề đọc, job sẽ **chết vì thiếu partition của thứ nó không cần** — một lỗi hoàn toàn giả.

Hai ca đã xảy ra thật:

**`gold/mart360/customer_360.yml`** khai báo `silver.fact_online_transaction` ở cả ba chỗ, trong khi SQL **không chứa chuỗi "online"**. DDL Gold thì đã có sẵn 6 cột `digital_*` chưa bao giờ được điền — một feature dừng giữa chừng: DDL và khai báo đã vào, SQL thì chưa.

**`silver/facts/fact_online_transaction.yml`** khai báo `silver.dim_device` và `silver.dim_location` ở `source.tables`, kèm comment header ghi *"Joins: dim_customer + dim_device + dim_location"*, nhưng SQL chỉ JOIN `dim_customer`.

Hai ca ngược chiều nhau: một cái khai báo thừa gây chặn giả, một cái khai báo sai làm lineage và comment nói dối.

## Decision

Cưỡng chế bằng test tĩnh — `tests/governance/test_declared_sources_match_sql.py`:

1. **Mọi bảng** trong `source.tables`, `upstream_flags`, `validation.require_snapshots` **phải xuất hiện trong `sql`** của job đó.
2. `require_snapshots` phải là **tập con** của `source.tables` — chặn chiều ngược lại, nơi một bảng bị guard chặn nhưng không có trong lineage.
3. Có guard chống pass rỗng: nếu glob tìm thấy dưới 40 config, test fail thay vì xanh một cách vô nghĩa.

So khớp theo **tên bảng** (phần cuối của `schema.table`), không theo chuỗi đầy đủ — vì SQL viết fully-qualified theo catalog (`lakehouse.silver.fact_txn_account`) còn khai báo thì không có catalog.

Danh sách miễn trừ `ALLOWED_UNUSED` để **rỗng có chủ ý**. Mỗi mục thêm vào phải kèm lý do; một miễn trừ không lý do sẽ tái tạo đúng lỗi mà test này tồn tại để chặn.

## Consequences

**Được**

- Guard `assert_source_snapshots` chỉ chặn vì lý do thật.
- `source.tables` trở thành lineage đáng tin, không phải danh sách được cập nhật tuỳ hứng.
- Quét toàn bộ 47 config ở cả ba tầng, nên lỗi tương tự ở Bronze hay Silver cũng bị bắt.
- Chạy trong job `Lint & Format` — phản hồi trong vài giây, không cần Docker.

**Mất**

- **So khớp bằng chuỗi con có thể cho dương tính giả.** Một bảng tên `customer` sẽ "xuất hiện" trong bất kỳ SQL nào chứa chữ `customer` — kể cả trong tên cột hay comment. Test này chặn được khai báo thừa rõ ràng, không chặn được mọi trường hợp tinh vi.
- Bảng chỉ xuất hiện trong **comment** của SQL vẫn tính là "có dùng".
- Thêm một invariant phải bảo trì. Nói thẳng: quyết định [`0005`](0005-fail-loud-before-overwrite.md) **đẻ ra** nhu cầu cho quyết định này. Guard tốt không miễn phí — nó tạo ra bề mặt lỗi mới phải canh.
- Không bắt được chiều thứ ba: bảng **được SQL đọc mà không khai báo**. Trường hợp đó vẫn lọt.

## Evidence

```text
tests/governance/test_declared_sources_match_sql.py    49 test, quét 47 config
code_etl/gold/mart360/customer_360.yml                 ca 1, sửa bằng cách hoàn tất SQL
code_etl/silver/facts/fact_online_transaction.yml      ca 2, sửa bằng cách gỡ khai báo
docs/REFERENCE_DATASET_ANALYSIS.md §4.4                audit phát hiện ban đầu
```

Hai ca được sửa theo **hai hướng ngược nhau** — một cái viết thêm SQL, một cái gỡ khai báo — vì ý định đằng sau chúng khác nhau. Test chỉ ép hai thứ khớp nhau; nó không quyết định hộ bên nào đúng.
