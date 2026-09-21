# ADR-0004 — Business date suy ra tường minh, Spark session bắt buộc UTC

**Status**: Accepted
**Ngày**: 2026-09 (chuyển thể thành ADR 2026-09-22)
**Liên quan**: `docs/evidence/metrics-manifest.yaml` → `manifest.time_semantics`

---

## Context

Dữ liệu lưu ở **UTC**. Nghiệp vụ tính theo **Asia/Ho_Chi_Minh (UTC+7)**. Một giao dịch lúc `2026-09-17 23:30 ICT` là `2026-09-17 16:30 UTC` — cùng một khoảnh khắc, nhưng nếu lấy nhầm ngày thì nó rơi sang báo cáo hôm khác.

Vấn đề sâu hơn: **Spark `TIMESTAMP` là LTZ** (local-time-zone). Nó không lưu timezone; nó render giá trị theo `spark.sql.session.timeZone` lúc đọc. Nghĩa là cùng một biểu thức SQL cho ra **kết quả khác nhau** tuỳ session timezone của người chạy.

Đã đo được:

```text
session = UTC                  → ra đúng ngày ICT
session = Asia/Ho_Chi_Minh     → dịch múi giờ HAI LẦN, sai 1 ngày ở vùng biên
session = America/New_York     → sai hẳn
```

Đây là loại lỗi tệ nhất: không có exception, không có dòng log nào đỏ. Job chạy xong, bảng có đủ dòng, chỉ có các giao dịch gần nửa đêm rơi sai ngày. Sai lệch nhỏ, âm thầm, và chỉ lộ ra khi ai đó đối soát tay.

## Decision

Ba phần, phải đi cùng nhau:

**1. Business date luôn suy ra tường minh từ timestamp UTC**, không bao giờ dựa vào `CAST(ts AS DATE)` trần:

```sql
-- Spark
CAST(from_utc_timestamp(<ts>, 'Asia/Ho_Chi_Minh') AS DATE)

-- Trino
CAST(<ts> AT TIME ZONE 'Asia/Ho_Chi_Minh' AS DATE)
```

**2. `spark.sql.session.timeZone = UTC` là điều kiện tiên quyết được CƯỠNG CHẾ**, không phải giả định. `assert_utc_session()` trong `code_etl/shared/spark/spark_session.py` kiểm tra khi khởi tạo session và fail nếu khác.

**3. `cob_dt` là ngày orchestration**, độc lập hoàn toàn với session timezone. Nó do Airflow truyền vào, không suy ra từ dữ liệu.

Điểm mấu chốt của quyết định này nằm ở phần 2. Biểu thức ở phần 1 **chỉ đúng dưới session UTC**. Viết biểu thức đúng mà không cưỡng chế session là để lại một quả mìn: nó chạy đúng trên máy người viết và sai trên máy người khác.

## Consequences

**Được**

- Business date là hàm thuần của dữ liệu, không phụ thuộc môi trường chạy.
- Điều kiện tiên quyết trở thành assert, nên vi phạm sẽ fail ngay lúc tạo session thay vì sai lặng lẽ ở tầng Gold.
- Có biểu thức chuẩn cho cả hai engine, nên Spark và Trino cho cùng kết quả.

**Mất**

- Biểu thức dài và khó đọc. Mọi model có chiều thời gian đều phải lặp lại nó — `aml_monitoring.yml` dùng nó bốn lần trong một query.
- `assert_utc_session()` là điểm phụ thuộc chung: nếu ai đó khởi tạo SparkSession không qua `get_spark_session()`, assert không chạy và bảo vệ biến mất.
- Không có kiểm tra tĩnh nào chặn việc viết `CAST(ts AS DATE)` trần trong một model mới. Bảo vệ hiện nằm ở review và ở assert lúc runtime, không ở CI.
- Nếu sau này có thị trường thứ hai với múi giờ khác, hằng số `'Asia/Ho_Chi_Minh'` nằm rải trong các YAML sẽ phải sửa từng chỗ.

## Evidence

```text
code_etl/shared/spark/spark_session.py          assert_utc_session()
docs/evidence/metrics-manifest.yaml             manifest.time_semantics (canonical expressions + note)
code_etl/gold/risk/aml_monitoring.yml           ví dụ áp dụng thực tế
```

`time_semantics.session_timezone_dependency_allowed: False` trong manifest ghi lại rằng đây là ràng buộc, không phải sở thích.
