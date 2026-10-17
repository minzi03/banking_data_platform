# Oracle như nguồn thứ hai — driver map & mock reference

**Khoá new2 (C2).** Đóng gap: "Core Banking chạy Oracle, Digital Banking chạy
PostgreSQL — platform phải nạp được cả hai".

> **Trạng thái**: driver map + cấu hình mẫu đã sẵn sàng và có test. Chưa nối
> một Oracle thật (không có instance trong stack). Đây là lựa chọn có chủ ý —
> xem [Phạm vi](#phạm-vi) bên dưới.

---

## 1. Vì sao cần

JD Market Analysis ([`docs/09-analysis/JD_MARKET_ANALYSIS.md`](../09-analysis/JD_MARKET_ANALYSIS.md))
ghi Oracle xuất hiện trong phần lớn JD Data Engineer ngân hàng (ABBANK, VietinBank…).
Trong đề bài new2, kiến trúc đích tách hai nguồn:

| Nguồn | Nội dung | Lý do |
|---|---|---|
| **PostgreSQL** | merchant, store, account, giao dịch QR/POS | Digital Banking, OLTP hiện đại |
| **Oracle** | khu vực, chi nhánh, MCC, sản phẩm | Core Banking legacy, đã chạy nhiều năm |
| **SFTP** | file POS + settlement | Đối soát cuối ngày |

Platform đã có PostgreSQL và SFTP. Thiếu Oracle là thiếu một chân của kiến trúc.

## 2. Cái gì đã có sẵn

Ba mảnh đã tồn tại và **giờ được test chặt**:

### 2.1 Driver map — `code_etl/bronze/base_job/ingestion_jdbc.py`

```python
_DRIVER_MAP = {
    "jdbc:postgresql:": "org.postgresql.Driver",
    "jdbc:oracle:":     "oracle.jdbc.OracleDriver",   # ← đã có
    "jdbc:mysql:":      "com.mysql.cj.jdbc.Driver",
    "jdbc:sqlserver:":  "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}
```

Spark chọn driver theo **prefix của `jdbc_url`**. Vì vậy URL phải mang đúng
scheme `jdbc:oracle:` — đây chính là điểm từng hỏng (xem §3).

### 2.2 Connection → URL — `airflow/plugins/jdbc_conn_utils.py`

`conn_type` của Airflow Connection quyết định scheme:

| `conn_type` | URL sinh ra | Default port |
|---|---|---|
| `postgres` | `jdbc:postgresql://host:port/schema` | 5432 |
| `oracle` | `jdbc:oracle:thin:@//host:port/SERVICE_NAME` | 1521 |
| `jdbc` | `host` (URL đầy đủ do người dùng nhập) | — |

> **Oracle dùng SERVICE_NAME, không phải SID.** Đây là khác biệt hay gặp nhất khi
> chuyển từ tài liệu cũ sang Oracle 12c+ multitenant. URL thin có dạng
> `@//host:port/service`, SID có dạng `@host:port:sid` — platform dùng dạng đầu.

### 2.3 Config mẫu — `code_etl/bronze/templates/oracle_source_reference.yml`

File tham chiếu, **không chạy** (nằm trong `templates/`, bị loại khỏi mọi DAG glob).
Nó là khuôn mẫu hợp lệ với `validate_config()` và ghi rõ khác biệt cú pháp SQL.

## 3. Lỗi từng tồn tại (và đã sửa)

`jdbc_jinja_args()` trước đây trả template **cứng `jdbc:postgresql://`**:

```python
# TRƯỚC
"jdbc_url": f"jdbc:postgresql://{{{{ {c}.host }}}}..."
```

Hệ quả: một Airflow Connection `conn_type=oracle` vẫn sinh URL Postgres →
Spark nạp driver Postgres → kết nối vào Oracle thất bại **lúc task chạy**, không
phải lúc parse DAG. Đây là loại hỏng im lặng: DAG xanh trên UI, chỉ đỏ khi thực thi.

`_build_jdbc_url()` thậm chí từ chối hẳn Oracle (`conn_type` không có trong
`builders`) — nhưng hàm đó chỉ dùng ở runtime, còn DAG dùng template.

**Sau khi sửa**, scheme sinh tại thời điểm render theo `conn.conn_type`, nên cùng
một DAG template phục vụ được cả hai. Test `TestJdbcJinjaRendering` render thật
qua Jinja2 và chốt: Oracle **không bao giờ** ra prefix `postgresql`.

## 4. Bật Oracle thật — 4 bước

```bash
# 1. Driver JAR (cạnh postgresql jar trong docker/Dockerfile.spark)
https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/23.5.0.24.07/ojdbc11-23.5.0.24.07.jar

# 2. Airflow Connection: conn_type = oracle
#    Host=ora-core  Port=1521  Schema=ORCLPDB1  Login=etl  Password=…
#    (DAG dùng jdbc_jinja_args → URL thin đúng schame)

# 3. Copy config mẫu sang thư mục DAG thật
cp code_etl/bronze/templates/oracle_source_reference.yml \
   code_etl/bronze/core_banking/region_oracle.yml

# 4. Sửa schema/table/SQL theo bảng Oracle thật, rồi chạy
make bronze-ingest CONFIG=code_etl/bronze/core_banking/region_oracle.yml COB_DT=2026-10-16
```

## 5. Khác biệt cú pháp SQL cần nhớ

| Việc | PostgreSQL | Oracle |
|---|---|---|
| Chuyển múi giờ | `timezone('Asia/Ho_Chi_Minh', ts)` | `ts AT TIME ZONE 'Asia/Ho_Chi_Minh'` |
| Cắt về ngày | `::date` / `CAST(... AS DATE)` | `TRUNC(ts)` |
| Giới hạn dòng | `LIMIT n` | `ROWNUM <= n` / `FETCH FIRST n ROWS ONLY` |
| Chuỗi nối | `\|\|` | `\|\|` (giống) |
| Boolean | `true/false` | `1/0` (NUMBER) |

Toàn bộ `cob_dt` của platform hiện tính trong SQL Postgres
(`timezone('Asia/Ho_Chi_Minh', timezone('UTC', transaction_date))::date`).
Một workload Oracle phải tự làm phép tương đương bằng `AT TIME ZONE` — **không**
dựa vào `now()` của server.

## 6. Phạm vi

**Đã làm**: driver map, connection builder, Jinja template (render từ `conn_type`),
config tham chiếu hợp lệ, tài liệu, 9 test mới.

**Chưa làm**: một instance Oracle trong `docker-compose`. Lý do:

- Oracle image không có bản chính thức miễn phí phân phối qua Docker Hub cho CI.
- Mục tiêu B10 là *"driver map & mock reference"* — chứng minh platform hiểu và
  cấu hình đúng nguồn Oracle, không phải dựng một Oracle thật.
- Thêm service Oracle (~4GB RAM) sẽ phá ngân sách 16GB vừa thiết lập ở
  [docker-compose profiles](docker-compose-profiles).

Khi có Oracle thật (hoặc Testcontainers/Oracle XE), chỉ cần thêm service + đổi
`source.type` trong config — không phải sửa code.

## 7. Liên quan

- [`tests/plugins/test_jdbc_conn_utils.py`](../../tests/plugins/test_jdbc_conn_utils.py) — 18 test, gồm render Oracle
- [`code_etl/bronze/templates/oracle_source_reference.yml`](../../code_etl/bronze/templates/oracle_source_reference.yml)
- [`docs/09-analysis/JD_MARKET_ANALYSIS.md`](../09-analysis/JD_MARKET_ANALYSIS.md) — vì sao Oracle quan trọng ở thị trường VN
