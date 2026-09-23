<!-- FILE SINH RA — đừng sửa tay. Nguồn: governance/rbac.py.
     Sinh lại: py -3 scripts/generate_rbac_matrix.py -->

# RBAC Matrix — ai đọc, ghi được gì qua Trino

> **Nguồn sự thật**: [`governance/rbac.py`](../../governance/rbac.py). Tài liệu này và
> [`docker/init_trino/rules.json`](../../docker/init_trino/rules.json) đều **sinh ra** từ đó
> ([ADR-0015](../02-architecture/adr/0015-trino-access-control-generated-from-rbac.md)).
> CI đỏ nếu một trong hai lệch khỏi `rbac.py`.
>
> **Phạm vi**: chỉ truy vấn **đi qua Trino**. Spark, MinIO và Postgres không bị ma trận này
> ràng buộc — xem §5 trước khi dựa vào nó.

8 role · 14 user · 8 khối luật trong `rules.json`.

## 1. User → role

Trino khớp luật theo **tên user**, không theo group. Mỗi user thuộc đúng một tổ hợp role,
nên thuộc đúng một khối luật.

| Role | Mô tả | User |
|---|---|---|
| `admin` | Full access to all resources | `admin`, `trino`, `trino_admin` |
| `etl_user` | Read/Write access for ETL pipelines | `airflow_etl` |
| `analytics` | Read-only access to gold/silver/serving, PII in Silver masked | `analytics_report` |
| `readonly` | Read-only access to gold and serving | `readonly_viewer` |
| `data_steward` | Manage data governance, contracts, and quality — kế thừa `analytics` | `data_steward_user` |
| `serving_builder` | dbt: đọc Gold, tạo/thay bảng trong serving | `dbt` |
| `serving_consumer` | Superset, API, Streamlit, ML: chỉ đọc serving | `customer_api`, `ml`, `streamlit`, `superset` |
| `observer` | Freshness exporter, metrics manifest: đọc mọi tầng, PII bị che | `freshness_exporter`, `manifest_collector` |

## 2. Role × schema

Quyền trên **bảng** trong từng schema của catalog `iceberg`, sau khi gộp role cha.

| Role | Catalog `iceberg` | `bronze` | `silver` | `silver_cdc` | `gold` | `sandbox` | `serving` | schema khác | Sở hữu schema |
|---|---|---|---|---|---|---|---|---|---|
| `admin` | all (mọi catalog) | A | A | A | A | A | A | A | mọi schema |
| `etl_user` | all | W | W | W | W | W | W | W | `bronze`, `gold`, `sandbox`, `silver`, `silver_cdc` |
| `analytics` | read-only | — | R ⁽ᵐ⁾ | — | R | R | R | — | — |
| `readonly` | read-only | — | — | — | R | — | R | — | — |
| `data_steward` | all | — | R ⁽ᵐ⁾ | — | R | W | R | — | `sandbox` |
| `serving_builder` | all | — | — | — | R | — | W | — | `serving` |
| `serving_consumer` | read-only | — | — | — | — | — | R | — | — |
| `observer` | read-only | R ⁽ᵐ⁾ | R ⁽ᵐ⁾ | R | R | R | R | — | — |

| Ký hiệu | Privilege Trino |
|---|---|
| `R` | SELECT |
| `W` | SELECT, INSERT, DELETE, UPDATE, OWNERSHIP |
| `A` | SELECT, INSERT, DELETE, UPDATE, OWNERSHIP, GRANT_SELECT |
| `—` | không có luật nào khớp — Trino từ chối |
| `⁽ᵐ⁾` | một số bảng trong schema bị che cột — §3 |

`OWNERSHIP` trên bảng là thứ cho phép `CREATE`/`DROP TABLE`. *Sở hữu schema* chỉ quyết
định `CREATE`/`DROP SCHEMA`. Quyền được khai **theo cả schema**: mô hình hiện tại không có
grant theo từng bảng, và generator từ chối nếu ai đó khai.

Mọi role không phải admin còn đọc được `system.metadata` và `system.jdbc` — client (dbt,
Superset, JDBC) cần chúng để liệt kê bảng, và Trino tự lọc chúng theo quyền của user hỏi.

## 3. Cột bị che

Che **lúc đọc**: Trino thay giá trị cột bằng biểu thức dưới đây. Dữ liệu lưu trên MinIO
vẫn là bản gốc.

### 3.1 Ai thấy PII gốc

| Role | `bronze.core_customer` | `bronze.core_customer_cdc` | `silver.dim_customer` | `silver.dim_customer_current` |
|---|---|---|---|---|
| `admin` | **bản gốc** | **bản gốc** | **bản gốc** | **bản gốc** |
| `etl_user` | **bản gốc** | **bản gốc** | **bản gốc** | **bản gốc** |
| `analytics` | không đọc được | không đọc được | che 6 cột | che 6 cột |
| `readonly` | không đọc được | không đọc được | không đọc được | không đọc được |
| `data_steward` | không đọc được | không đọc được | che 6 cột | che 6 cột |
| `serving_builder` | không đọc được | không đọc được | không đọc được | không đọc được |
| `serving_consumer` | không đọc được | không đọc được | không đọc được | không đọc được |
| `observer` | che 6 cột | che 6 cột | che 6 cột | che 6 cột |

### 3.2 Biểu thức che

| Bảng | Cột | Biểu thức Trino | Áp cho role |
|---|---|---|---|
| `bronze.core_customer` | `address` | `CAST('[REDACTED]' AS VARCHAR)` | `observer` |
| `bronze.core_customer` | `cccd` | `concat('***********', substr(cccd, -4))` | `observer` |
| `bronze.core_customer` | `date_of_birth` | `date_trunc('year', date_of_birth)` | `observer` |
| `bronze.core_customer` | `email` | `concat(substr(email, 1, 1), '*****', '@', split_part(email, '@', 2))` | `observer` |
| `bronze.core_customer` | `full_name` | `concat(substr(full_name, 1, 1), '**')` | `observer` |
| `bronze.core_customer` | `phone` | `concat(substr(phone, 1, 3), '****', substr(phone, 8))` | `observer` |
| `bronze.core_customer_cdc` | `address` | `CAST('[REDACTED]' AS VARCHAR)` | `observer` |
| `bronze.core_customer_cdc` | `cccd` | `concat('***********', substr(cccd, -4))` | `observer` |
| `bronze.core_customer_cdc` | `date_of_birth` | `CAST(NULL AS BIGINT)` | `observer` |
| `bronze.core_customer_cdc` | `email` | `concat(substr(email, 1, 1), '*****', '@', split_part(email, '@', 2))` | `observer` |
| `bronze.core_customer_cdc` | `full_name` | `concat(substr(full_name, 1, 1), '**')` | `observer` |
| `bronze.core_customer_cdc` | `phone` | `concat(substr(phone, 1, 3), '****', substr(phone, 8))` | `observer` |
| `silver.dim_customer` | `address` | `CAST('[REDACTED]' AS VARCHAR)` | `analytics`, `data_steward`, `observer` |
| `silver.dim_customer` | `cccd` | `concat('***********', substr(cccd, -4))` | `analytics`, `data_steward`, `observer` |
| `silver.dim_customer` | `date_of_birth` | `date_trunc('year', date_of_birth)` | `analytics`, `data_steward`, `observer` |
| `silver.dim_customer` | `email` | `concat(substr(email, 1, 1), '*****', '@', split_part(email, '@', 2))` | `analytics`, `data_steward`, `observer` |
| `silver.dim_customer` | `full_name` | `concat(substr(full_name, 1, 1), '**')` | `analytics`, `data_steward`, `observer` |
| `silver.dim_customer` | `phone` | `concat(substr(phone, 1, 3), '****', substr(phone, 8))` | `analytics`, `data_steward`, `observer` |
| `silver.dim_customer_current` | `address` | `CAST('[REDACTED]' AS VARCHAR)` | `analytics`, `data_steward`, `observer` |
| `silver.dim_customer_current` | `cccd` | `concat('***********', substr(cccd, -4))` | `analytics`, `data_steward`, `observer` |
| `silver.dim_customer_current` | `date_of_birth` | `date_trunc('year', date_of_birth)` | `analytics`, `data_steward`, `observer` |
| `silver.dim_customer_current` | `email` | `concat(substr(email, 1, 1), '*****', '@', split_part(email, '@', 2))` | `analytics`, `data_steward`, `observer` |
| `silver.dim_customer_current` | `full_name` | `concat(substr(full_name, 1, 1), '**')` | `analytics`, `data_steward`, `observer` |
| `silver.dim_customer_current` | `phone` | `concat(substr(phone, 1, 3), '****', substr(phone, 8))` | `analytics`, `data_steward`, `observer` |

Gold và `serving` không cần mask đọc: chúng chỉ mang PII đã che **lúc ghi** (`full_name_masked`).
Phạm vi che là nhóm *Cao nhất* và *Cao* của PII_INVENTORY §3 trên bảng khách hàng; nhóm
*Trung bình* (account_no, device_id, lat/long) và *Thấp* chưa che.
Danh mục đầy đủ bảng/cột chứa PII: [`PII_INVENTORY.md`](PII_INVENTORY.md).

## 4. Truy vấn và thông tin hệ thống

| Hành động | Ai |
|---|---|
| Chạy truy vấn | mọi user |
| Xem / kill truy vấn của **user khác** | `admin`, `trino`, `trino_admin` |
| Đọc / ghi `system_information` | `admin`, `trino`, `trino_admin` |

## 5. Ma trận này KHÔNG bảo đảm gì

1. **Chưa có xác thực.** Trino tin tên user mà client tự khai. Ai kết nối được tới cổng
   Trino và khai `admin` thì nhận quyền admin. Ma trận phân quyền đúng cho client **trung
   thực** — nó chặn nhầm lẫn và lộ PII vô ý, không chặn người cố ý.
2. **Spark không đi qua Trino.** Job Spark đọc/ghi Iceberg trực tiếp qua REST catalog và
   MinIO — không luật nào ở đây áp cho nó.
3. **MinIO và Postgres ngoài phạm vi.** Ai có credential MinIO đọc được file Parquet gốc,
   gồm PII ở Bronze/Silver. `opslakehouse` trên Postgres có quyền riêng của Postgres.
4. **Kiểm chứng.** `tests/governance/test_trino_access_control.py` mô phỏng ngữ nghĩa
   file-based access control của Trino 443 (luật khớp đầu tiên) trên `rules.json` sinh ra;
   theo docstring của test, evaluator đó đã được đối chiếu với chính Trino 443. Test chạy trong
   unit CI, không cần stack.

Chi tiết và lộ trình: [ADR-0015](../02-architecture/adr/0015-trino-access-control-generated-from-rbac.md),
[`PII_INVENTORY.md`](PII_INVENTORY.md) §7, [`RUNBOOK.md`](../../RUNBOOK.md) §9.

## 6. Đổi quyền

```bash
# 1. sửa ROLES / USERS trong governance/rbac.py
py -3 scripts/generate_trino_access_control.py   # sinh docker/init_trino/rules.json
py -3 scripts/generate_rbac_matrix.py            # sinh tài liệu này
py -3 -m pytest tests/governance/test_trino_access_control.py tests/governance/test_rbac_matrix_current.py
# 2. restart Trino để nạp rules.json mới
```
