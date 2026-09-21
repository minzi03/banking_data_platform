# ADR-0006 — Job metadata-driven bằng YAML cho cả ba tầng

**Status**: Accepted
**Ngày**: kế thừa từ template khoá học · chuyển thể thành ADR 2026-09-22
**Liên quan**: [`0013`](0013-declared-sources-match-sql.md) · [`../COURSE_BASELINE_DIFF.md`](../../09-analysis/COURSE_BASELINE_DIFF.md)

---

## Context

Bronze có 18 nguồn, Silver có 16 model, Gold có 14. Nếu mỗi cái là một script Python riêng, sẽ có 48 file lặp lại cùng một khung: parse args, tạo SparkSession, đọc nguồn, transform, ghi, đóng session.

Lặp lại kiểu đó không chỉ dài dòng. Nó làm mọi sửa đổi ngang tầng trở thành 48 lần sửa — và lần thứ 47 sẽ bị bỏ sót.

## Decision

Tách **logic nghiệp vụ** (YAML) khỏi **engine thực thi** (Python):

```text
code_etl/bronze/base_job/ingestion_jdbc.py   ← engine chung
code_etl/bronze/core_banking/customer.yml    ← khai báo

code_etl/silver/base_job/scd_type1.py
code_etl/silver/base_job/scd_type2.py
code_etl/silver/base_job/fact_txn.py
code_etl/silver/dims/dim_customer.yml

code_etl/gold/base_job/gold_job.py
code_etl/gold/mart360/customer_360.yml
```

YAML khai báo nguồn, đích, SQL, và ràng buộc validation. Engine lo SparkSession, guard, ghi partition, Z-Order.

Đây là quyết định **kế thừa** từ template khoá học, không phải do dự án này nghĩ ra. Nó được giữ lại vì đã chứng minh giá trị khi mở rộng từ 10 lên 14 Gold model và từ 13 lên 18 nguồn Bronze mà không phải sửa engine.

## Consequences

**Được**

- Thêm một model là thêm một file YAML, không phải một file Python.
- Sửa hành vi ngang tầng chỉ sửa một chỗ. `assert_source_snapshots` ([`0005`](0005-fail-loud-before-overwrite.md)) thêm vào một lần và có hiệu lực cho cả 14 Gold model.
- YAML đọc được bởi công cụ, nên viết được test tĩnh quét toàn bộ 47 config — chính là cách [`0013`](0013-declared-sources-match-sql.md) hoạt động.

**Mất**

- **SQL trong YAML không được kiểm tra cú pháp** cho tới lúc chạy. Không có linter, không có autocomplete, không có type check. Một dấu phẩy thiếu chỉ lộ ra khi spark-submit chạy.
- Jinja trong SQL (`{{ cob_dt }}`) là một tầng template nữa phải hiểu, và lỗi template cũng chỉ hiện lúc runtime.
- Logic phức tạp không biểu diễn được bằng YAML sẽ phải nhét vào SQL, làm query phình to. `customer_360.yml` hiện có hơn 100 dòng SQL trong một trường YAML.
- Debug khó hơn: stack trace trỏ vào engine chung, không trỏ vào model gây lỗi. Phải đọc log để biết config nào đang chạy.

## Evidence

```text
code_etl/{bronze,silver,gold}/base_job/     engine chung
47 file YAML có khối `sql`                  đếm bởi test_declared_sources_match_sql.py
docs/COURSE_BASELINE_DIFF.md §2             template gốc đã có pattern này
```
