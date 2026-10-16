# ADR-0019 — Serving real-time bằng PostgreSQL, không phải Iceberg/Trino

**Status**: Accepted
**Ngày**: 2026-10-15 (implemented 2026-10-16)
**Liên quan**: [`0003`](0003-serving-as-table-not-view.md) · [`0018`](0018-incremental-transaction-facts.md) · [`code_etl/cdc/reconcile_cdc.py`](../../../code_etl/cdc/reconcile_cdc.py) · [`code_etl/serving/sync_postgres.py`](../../../code_etl/serving/sync_postgres.py)

> **Cập nhật 2026-10-16**: phần quyết định đã được hiện thực — `code_etl/serving/sync_postgres.py` + `airflow/dags/ops/ops_serving_sync_dag.py` (Gold/serving Iceberg → PostgreSQL `serving.*`, upsert `ON CONFLICT`, bảng tạo tự động từ schema Iceberg theo ADR-0006). Trạng thái chuyển `Proposed` → `Accepted`. Phần "thêm backend PostgreSQL" giờ là hiện thực, không còn là đề xuất.

---

## Context

Tầng serving hiện tại ghi vào **Iceberg** (`lakehouse.serving.*`), sinh từ Gold bằng `code_etl/serving/bootstrap/run_serving.py`, và được đọc qua **Trino** (`streamlit/app.py` mở kết nối Trino, catalog `iceberg`). Đây là lựa chọn đúng cho **analytics lịch sử**: dữ liệu lớn, truy vấn phân tích, độ trễ chấp nhận được theo lô ngày.

Nhưng nghiệp vụ **Merchant Acquiring (thanh toán QR/POS)** — nguồn `thamkhao/bootcamp_class/new2` — đặt ra một loại consumer khác:

- **Thông báo tiền về**: mỗi giao dịch QR thành công hiển thị cho merchant trong **vài giây**.
- **Doanh thu theo cửa hàng theo phút**: dashboard real-time, cập nhật liên tục.
- **Giờ cao điểm**: cập nhật liên tục trong ngày.

Ba use case này là **point lookup + upsert tần suất cao theo khóa** (`transaction_id`, `merchant_id`), không phải scan phân tích. Iceberg/Trino tối ưu cho scan lớn, không tối ưu cho: (a) ghi upsert tần suất giây, (b) point lookup độ trễ thấp, (c) latency ổn định khi các micro-batch liên tục commit snapshot mới.

Vấn đề bị làm rõ bởi câu hỏi trong new2: *"vì sao app/dashboard real-time đọc từ Postgres thay vì Iceberg/Trino?"* — và bởi một quan sát đã kiểm chứng: **repo hiện chưa có bất kỳ writer Postgres nào** (`grep` cho `ON CONFLICT` / JDBC write trong `code_etl` + `airflow/dags` trả về rỗng). Serving chỉ có một backend.

## Decision

Giữ **Iceberg serving cho analytics** (không đổi — xem ADR-0003), và **thêm một serving backend PostgreSQL chỉ cho bảng real-time**, khi Phase 2 streaming được triển khai:

| Tầng | Backend | Consumer | Đặc tính |
|---|---|---|---|
| Analytics lịch sử | Iceberg (`lakehouse.serving.*`) → Trino | Superset, Streamlit, API đọc lịch sử | scan lớn, theo lô ngày |
| Real-time | PostgreSQL serving | Merchant App, Grafana real-time | point lookup + upsert giây |

Quy tắc bất biến:

- **Ghi Postgres bằng upsert idempotent**: staging → `INSERT ... ON CONFLICT (pk) DO UPDATE`. Không `TRUNCATE`+insert.
- **Bảng real-time partition theo ngày**, index theo truy vấn của ứng dụng (không index tràn).
- **Đồng bộ Gold→Postgres bằng Airflow DAG** (không phải job streaming), để số liệu lịch sử cho app đọc cùng backend.
- **Đối soát cuối ngày**: tổng doanh thu/số GD từ đường streaming phải **khớp** báo cáo batch cùng `cob_dt`, nếu không thì fail — mở rộng `reconcile_cdc.py` (thêm mode `eod`) hoặc `code_etl/shared/ops/eod_reconciliation.py`.
- Iceberg **không** bị thay thế. Hai backend phục vụ hai loại truy vấn khác nhau, không phải một backend nhanh hơn backend kia.

## Consequences

**Được**

- Đáp ứng được SLA "thông báo tiền về trong vài giây" mà Iceberg/Trino không đảm bảo ổn định khi có commit snapshot liên tục.
- Upsert theo khóa là primitive gốc của Postgres; biểu diễn use case "1 dòng mới nhất mỗi merchant/giao dịch" tự nhiên hơn.
- Vẫn giữ Iceberg làm tầng analytics — không mất time-travel/schema-evolution cho phân tích.

**Mất**

- **Thêm một backend phải đồng bộ.** Giờ có hai nơi chứa "sự thật serving"; phải có DAG sync + đối soát, nếu không chúng lệch nhau lặng lẽ.
- **Thêm một invariant phải bảo trì**: "streaming = batch cuối ngày". Đây chính là lý do reconciliation phải đi kèm, không phải tùy chọn.
- **Phụ thuộc thêm một hạ tầng**: Postgres serving cần schema/DDL/index riêng, nằm ngoài Iceberg catalog.
- Nếu độ trễ thật chỉ cần vài phút (không phải vài giây), **quyết định này là thừa** — Iceberg serving đã đủ, và thêm Postgres chỉ tăng chi phí vận hành. Quyết định này chỉ đúng khi SLA thật sự dưới vài giây.

## Evidence

```text
code_etl/serving/bootstrap/run_serving.py     serving hiện ghi Iceberg, không Postgres
streamlit/app.py:96                            consumer mở kết nối Trino, catalog "iceberg"
code_etl/cdc/reconcile_cdc.py                  reconcile nguồn↔topic↔Bronze, KHÔNG có eod
code_etl/shared/ops/eod_reconciliation.py      eod reconcile: count + control-total (balance tie-out)
airflow/dags/ops/ops_eod_reconciliation_dag.py DAG gate chạy 22:00 hằng ngày
code_etl/serving/sync_postgres.py              Iceberg serving → PostgreSQL upsert ON CONFLICT
code_etl/serving/config/serving_sync.yml       khai báo bảng + khoá upsert (metadata-driven)
airflow/dags/ops/ops_serving_sync_dag.py       DAG sync chạy 23:00, sau dbt_serving_publish
grep ON CONFLICT code_etl airflow/dags         → rỗng: chưa có writer Postgres nào
docker/docker-compose.yml:111                  service `postgres` đã có (core DB), dùng chung được
thamkhao/bootcamp_class/new2/architecture.png  sơ đồ: Spark SS → PostgreSQL Serving → Grafana
thamkhao/dataset_thamkhao/new_data1/data2      gl_control_total pattern (SUM balance by date/ccy, tol 1.0)
```

Lưu ý: khoảng trống "chưa có writer Postgres" được xác minh bằng `grep` trên repo, không suy đoán. Đã hiện thực 2026-10-16 (xem cập nhật đầu file). Nếu sau này độ trễ thật chỉ cần vài phút (không phải vài giây), PostgreSQL serving là thừa — khi đó viết ADR mới đánh dấu cái này `Superseded`, không sửa nội dung đã Accepted.
