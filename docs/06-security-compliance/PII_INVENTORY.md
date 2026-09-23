# Kiểm Kê Dữ Liệu Cá Nhân

> Cập nhật: 2026-09-23 · Bản đồ tài liệu: [`../INDEX.md`](../INDEX.md)
> Liên quan: [`../../SECURITY.md`](../../SECURITY.md) · [`../03-data/DATA_DICTIONARY.md`](../03-data/DATA_DICTIONARY.md) · [`../05-quality/DATA_QUALITY.md`](../05-quality/DATA_QUALITY.md)

Đây là bản kiểm kê mà `DATA_DICTIONARY.md` trỏ tới. Trình sinh từ điển tự khai:

> *"Đây là chỉ báo, không phải bản kiểm kê đầy đủ — bản kiểm kê thật là
> `PII_INVENTORY.md`."*

Kiểm kê được dựng bằng cách parse toàn bộ DDL dưới `docker/init_iceberg/` và
`docker/init_postgres/`, loại 5 file theo cùng danh sách `SKIP_DDL` mà trình
sinh từ điển dùng. Tài liệu chỉ ghi **tên bảng và tên cột** — không có giá trị dữ
liệu nào.

**Dữ liệu trong dự án này là dữ liệu sinh tổng hợp**, không phải dữ liệu khách
hàng thật. Kiểm kê vẫn cần thiết vì nó mô tả *cấu trúc* sẽ chứa dữ liệu thật nếu
ai đó mang mẫu này sang hệ thống có dữ liệu thật — đúng cảnh báo mà
[`SECURITY.md`](../../SECURITY.md) đã nêu.

---

## 1. Tóm tắt điều hành

| Câu hỏi | Trả lời |
|---|---|
| Bao nhiêu bảng chứa dữ liệu cá nhân? | **31** (23 trong lakehouse, 8 trong PostgreSQL nguồn) |
| Bronze có được che? | **Lưu gốc, che lúc đọc qua Trino** — với user không phải admin/ETL (§7) |
| Silver có được che? | **Lưu gốc, che lúc đọc qua Trino** — `silver.dim_customer` vẫn *lưu* `cccd`, `full_name`, `phone`, `email`, `address` nguyên bản |
| Gold có được che? | **Có** — chỉ còn `full_name_masked`, không còn `cccd`/`phone`/`email` |
| Tầng serving có được che? | **Có**, bằng cách thừa hưởng từ Gold (`select *`) |
| Lakehouse có kiểm soát truy cập? | **Có ở Trino, chưa có xác thực** — Spark và MinIO không đi qua lớp này (§7) |
| Có phân loại dữ liệu ở dạng máy đọc được? | **Không** (§9) |

Câu quan trọng nhất: **bản gốc vẫn được lưu nguyên; lớp bảo vệ là Trino che lúc
đọc — và Trino tin tên user mà client tự khai.** Client làm đúng không còn thấy
`cccd` gốc; người cố ý khai tên `admin`, hoặc đọc thẳng MinIO bằng Spark, thì vẫn
thấy.

---

## 2. PII nằm ở đâu

### Lakehouse — Bronze (giá trị gốc)

| Bảng | Cột mang dữ liệu cá nhân |
|---|---|
| `bronze.core_customer` | `cccd` · `full_name` · `date_of_birth` · `phone` · `email` · `address` |
| `bronze.core_customer_cdc` | `cccd` · `full_name` · `date_of_birth` · `phone` · `email` · `address` |
| `bronze.core_employee` | `full_name` |
| `bronze.core_branch` | `address` · `manager_name` |
| `bronze.core_account` | `account_no` |
| `bronze.core_account_cdc` | `account_no` |
| `bronze.core_card` | `card_no_masked` |
| `bronze.card_account_cdc` | `card_no_masked` |
| `bronze.core_device` | `device_id` · `ip_address` |
| `bronze.online_transaction_cdc` | `device_id` |
| `bronze.core_online_transaction` | `device_id` |
| `bronze.core_location` | `latitude` · `longitude` |

### Lakehouse — Silver (giá trị gốc)

| Bảng | Cột mang dữ liệu cá nhân |
|---|---|
| `silver.dim_customer` | `cccd` · `full_name` · `date_of_birth` · `phone` · `email` · `address` |
| `silver.dim_customer_current` | `cccd` · `full_name` · `date_of_birth` · `phone` · `email` · `address` |
| `silver.dim_employee` | `full_name` |
| `silver.dim_branch` | `address` · `manager_name` |
| `silver.dim_account` | `account_no` |
| `silver.dim_account_current` | `account_no` |
| `silver.dim_card` | `card_no_masked` |
| `silver.dim_device` | `device_id` · `ip_address` |
| `silver.fact_online_transaction` | `device_id` |
| `silver.dim_location` | `latitude` · `longitude` |

### Lakehouse — Gold, serving, sandbox (đã che)

| Bảng | Cột | Trạng thái |
|---|---|---|
| `gold.mart_customer_360` | `full_name_masked` | đã che tại nguồn ghi |
| `serving.mart_customer_360_current` | `full_name_masked` | thừa hưởng từ Gold qua `select *` |
| `sandbox.dim_customer_masked` | `full_name_masked` · `phone_masked` · `email_masked` · `cccd_hash` · `age_group_decade` | bản phái sinh |
| `sandbox.mart_customer_360_masked` | `full_name_masked` · `age` | bản phái sinh |

**Không** bảng Gold hay serving nào mang `cccd`, `phone`, `email`, `address`.

Trong bảng trên chỉ `gold.mart_customer_360` được tính vào con số 31 ở §1: ba
bảng còn lại không có DDL trong repo — dbt và `pii_masking.py` tạo chúng lúc
chạy. Nghĩa là chúng cũng **không** xuất hiện trong `DATA_DICTIONARY.md`.

### PostgreSQL nguồn

`core_banking.customer` · `core_banking.employee` · `core_banking.branch` ·
`core_banking.account` · `card_crm.card` · `digital_banking.device` ·
`digital_banking.location` · `digital_banking.online_transaction` — cùng tập cột
như Bronze tương ứng, vì Bronze là bản sao trung thực của nguồn.

---

## 3. Phân loại theo mức nhạy cảm

Đây là phân loại **thủ công**, không có trong repo dưới dạng máy đọc được (§9).

| Mức | Cột | Vì sao |
|---|---|---|
| **Cao nhất** | `cccd` | Số định danh cá nhân do nhà nước cấp. Không thể đổi, dùng làm khoá định danh trên mọi hệ thống |
| **Cao** | `full_name` + `date_of_birth` + `address` | Bộ ba này định danh trực tiếp một người |
| **Cao** | `phone` · `email` | Kênh liên lạc trực tiếp, dùng cho chiếm quyền tài khoản |
| **Trung bình** | `account_no` · `card_no_masked` | Định danh tài chính. `card_no_masked` đã ở dạng che ngay từ nguồn |
| **Trung bình** | `device_id` · `ip_address` | Dấu vết thiết bị — định danh gián tiếp, dùng để lần theo hành vi |
| **Trung bình** | `latitude` · `longitude` | Ở `dim_location` là địa lý **chi nhánh/địa điểm**, không phải vị trí khách hàng. Nhưng khi join với `fact_online_transaction` thì thành dấu vết di chuyển của người — và `aml_monitoring` làm đúng việc đó để tính `geo_velocity_flag` |
| **Thấp** | `manager_name` · `full_name` của nhân viên | Dữ liệu nhân sự, không phải dữ liệu khách hàng |
| **Giả danh hoá** | `cccd_hash` | SHA-256 có salt. Mức bảo vệ thật phụ thuộc salt được giữ bí mật — xem §6 |

`latitude`/`longitude` là ví dụ vì sao kiểm kê theo cột là chưa đủ: một cột vô
hại ở bảng này thành dữ liệu nhạy cảm sau một phép join. Bản kiểm kê ghi cột;
đánh giá rủi ro phải theo **đường dùng**.

---

## 4. Ba cơ chế che, ba quy tắc khác nhau

Cùng một trường `full_name` được che theo **ba** cách khác nhau ở ba chỗ:

| Nơi | Biểu thức | Kết quả (minh hoạ trên tên ví dụ) |
|---|---|---|
| Gold, `customer_360.yml:225` | `CONCAT(SPLIT(c.full_name,' ')[0], ' **')` | giữ họ, bỏ hết phần còn lại |
| `pii_masking.py`, `_mask_name_udf()` | tách theo khoảng trắng, giữ họ + chữ cái đầu của tên đệm và tên | giữ họ và hai chữ cái đầu |
| Trino, lúc đọc (`governance/rbac.py`, §7) | `concat(substr(full_name, 1, 1), '**')` | chỉ giữ chữ cái đầu |

Không quy tắc nào sai. Nhưng chúng **khác nhau**, và không test nào so các bên.
Bản Trino che mạnh nhất, bản "masking" nhẹ nhất.

Trino cũng che các trường khác theo quy tắc riêng, không trùng `pii_masking.py`:
`cccd` giữ 4 số cuối (thay vì hash), `date_of_birth` làm tròn về đầu năm (thay vì
nhóm chục năm), `address` thay bằng `[REDACTED]` (thay vì giữ city + district).

Các quy tắc khác trong `pii_masking.py`:

```text
phone   giữ 3 số đầu + 3 số cuối
email   giữ chữ đầu + chữ cuối của local part, giữ nguyên domain
cccd    sha2(cccd || salt, 256) — tất định, để vẫn join được
address chỉ giữ city + district
dob     chuyển thành age_group_decade (làm tròn xuống chục năm)
```

`cccd_hash` tất định là lựa chọn có chủ ý: nó cho phép join giữa các bảng đã che
mà không cần khoá gốc. Đánh đổi: hash tất định giữ nguyên tính duy nhất, nên nó
vẫn là một **định danh giả** — hai bản ghi cùng người vẫn ghép được.

### Bất đối xứng chưa có lý do ghi lại

`sandbox.dim_customer_masked` chuyển ngày sinh thành `age_group_decade` (nhóm 10
năm). `sandbox.mart_customer_360_masked` giữ `age` **chính xác**. Cùng một job,
cùng một mục đích sử dụng, hai mức chi tiết khác nhau về tuổi. Không có ghi chú
nào giải thích.

---

## 5. Che không bảo vệ bản gốc

```text
silver.dim_customer  (cccd, phone, email nguyên bản)
        │
        ├──► gold.mart_customer_360          che khi GHI  →  serving
        │
        └──► sandbox.dim_customer_masked     bản sao đã che, tạo lại mỗi ngày
```

`pii_masking.py` chạy `CREATE OR REPLACE TABLE lakehouse.sandbox.*_masked`. Nó
**không** sửa `silver.dim_customer`. Nghĩa là:

- bản gốc vẫn còn nguyên và vẫn đọc được
- bảng `sandbox` là tiện ích cho Marketing/CRM, **không** phải một tầng kiểm soát
- không có gì buộc ai phải dùng bảng `sandbox` thay vì bảng gốc

Đây là lựa chọn hợp lý **khi** có kiểm soát truy cập theo schema. Từ PR #39,
Trino có lớp đó (§7): tầng tiêu thụ chỉ đọc được `serving`, và role phân tích đọc
Silver thì thấy PII đã che. Nhưng lớp này chưa có xác thực, và không phủ Spark —
nên bản gốc được bảo vệ trước truy cập *nhầm*, chưa được bảo vệ trước truy cập
*cố ý*.

`SECURITY.md` nói cùng điều này: che **lúc ghi** chỉ áp ở Gold/serving; Bronze và
Silver vẫn *lưu* giá trị gốc, và chỉ được che **lúc đọc** qua Trino.

DAG: `ops_pii_masking_daily_dag`, 08:00 hằng ngày, chờ `gold_all_dag` thành công
trước khi chạy.

---

## 6. Salt của `cccd_hash`: không còn giá trị dự phòng

> **Đã khắc phục** 2026-09-22 ở PR #33. Bản đầu của mục này mô tả một lỗ hổng
> đang sống; phần "Trước đây" dưới đây giữ lại để người đọc hiểu vì sao cơ chế
> hiện tại có hình dạng như vậy.

### Trước đây

`code_etl/shared/ops/pii_masking.py` fail-loud khi thiếu salt, nhưng DAG gọi nó
lại **luôn** cung cấp một giá trị: `Variable.get("pii_hash_salt", default_var=...)`
với một chuỗi cố định nằm trong file đã commit của repo công khai. Guard kiểm
**sự hiện diện** của salt, không kiểm **chất lượng**, nên `default_var` lấp đúng
cái lỗ mà guard định chặn: Variable chưa đặt thì job vẫn chạy, với salt ai đọc
repo cũng biết.

Vì sao điều đó nghiêm trọng: `cccd_hash = sha2(cccd || salt)` là hash tất định.
CCCD Việt Nam là 12 chữ số, và không gian thực tế nhỏ hơn 10¹² nhiều vì ba số đầu
là mã tỉnh và có thành phần ngày sinh. Với salt đã biết, đây là phép liệt kê,
không phải phá mã — giả danh hoá trên danh nghĩa.

### Hiện tại

`airflow/dags/ops/ops_pii_masking_daily_dag.py` không còn `default_var`. Salt
được đọc từ Airflow Variable `pii_hash_salt` **lúc render task**, qua hàm
`resolve_pii_hash_salt` đăng ký làm Jinja macro (`user_defined_macros`):

| Tình huống | Hành vi |
|---|---|
| Variable đã đặt | task chạy với salt đó |
| Variable chưa đặt, hoặc rỗng | **task** fail, thông báo nêu tên Variable và lệnh đặt nó |
| Parse DAG | không đọc Variable — DAG vẫn import được, không truy vấn metadata DB mỗi lượt parse |

Đọc lúc render chứ không lúc parse là có chủ ý: đọc Variable không default ở
module level sẽ làm DAG **lỗi import** và biến mất khỏi UI thay vì hiện đỏ.
`tests/dags/test_ops_pii_masking_daily_dag.py` khoá các hành vi trên, gồm cả việc
không có `default_var=` nào trong file DAG.

Cách đặt salt, và hệ quả khi xoay vòng nó: [`RUNBOOK.md`](../../RUNBOOK.md) §8.

### Còn lại

- **Xoay vòng salt làm vô hiệu mọi `cccd_hash` đã publish.** Hash tất định nên
  salt mới cho hash khác trên cùng một người: phải dựng lại cả hai bảng
  `sandbox.*_masked`, và mọi join downstream trên `cccd_hash` sẽ trả 0 dòng qua
  mốc xoay vòng thay vì báo lỗi. Không có đường re-hash cho bên đã giữ hash cũ.
- **Airflow không che giá trị Variable này trên UI** — `pii_hash_salt` không khớp
  mẫu tên nhạy cảm nào của Airflow, nên giá trị đọc được ở *Admin → Variables* và
  ở tab *Rendered Template*. Muốn che thì thêm `salt` vào
  `[core] sensitive_var_conn_names`.
- Guard trong `pii_masking.py` vẫn chỉ kiểm sự hiện diện: một salt yếu hay ngắn
  vẫn được chấp nhận. Chặn nay nằm ở đầu DAG, không nằm ở job.
- `docker/.env` cũng đặt `PII_HASH_SALT`, nhưng file đó được gitignore và không
  được track (`.gitignore:6`); `.env.example` chỉ có `CHANGE_ME`. Chạy
  `pii_masking.py` trực tiếp ngoài DAG thì salt đến từ môi trường, không từ
  Variable.
- Salt cũ vẫn còn trong **lịch sử git** của repo công khai. Nó không còn được
  dùng, nhưng mọi `cccd_hash` từng sinh bằng salt đó vẫn liệt kê được — cần dựng
  lại bảng `sandbox` bằng salt mới nếu môi trường nào từng chạy với nó.

---

## 7. Kiểm soát truy cập: có ở Trino, chưa có xác thực

> **Cập nhật 2026-09-23 (PR #39).** Hai bản trước của mục này ghi "lakehouse không
> có kiểm soát truy cập", rồi đính chính rằng file luật Trino có tồn tại nhưng không
> có tác dụng. Cả hai đúng tại thời điểm viết. Từ PR #39, Trino thực thi luật thật.
> Phần "Trước đây" bên dưới giữ lại phát hiện đó ở dạng rút gọn.

### Hiện tại

`governance/rbac.py` là nguồn sự thật duy nhất: 8 role, 14 user.
`scripts/generate_trino_access_control.py` sinh `docker/init_trino/rules.json` từ
đó; test chặn drift giữa hai file. `access-control.properties` bật plugin `file`
và được mount vào đúng `/etc/trino/` ở compose, CI compose và Terraform. Quyết
định: [ADR-0015](../02-architecture/adr/0015-trino-access-control-generated-from-rbac.md).

| Client | User Trino | Đọc được PII nguyên bản? |
|---|---|---|
| dbt | `dbt` | không — đọc `gold`, ghi `serving`; cả hai chỉ mang `full_name_masked` |
| Superset, API, Streamlit, ML | `superset` · `customer_api` · `streamlit` · `ml` | không — chỉ đọc `serving` |
| Freshness exporter, metrics manifest | `freshness_exporter` · `manifest_collector` | không — đọc mọi tầng, PII bị che |
| Phân tích, data steward | `analytics_report` · `data_steward_user` | không — đọc Silver, PII bị che; không đọc Bronze |
| ETL | `airflow_etl` | **có** — pipeline ghi các tầng thì phải thấy giá trị thật |
| Vận hành, CI, `docker exec … trino` | `admin` · `trino` · `trino_admin` | **có** |
| Bất kỳ tên nào khác | — | không đọc được bảng dữ liệu nào |

Luật che nằm trên 4 bảng mang PII khách hàng nguyên bản: Silver `dim_customer` và
`dim_customer_current` (cho role phân tích), cùng Bronze `core_customer` và
`core_customer_cdc` (thêm cho role quan sát). Mỗi bảng che 6 cột: `cccd`,
`full_name`, `phone`, `email`, `address`, `date_of_birth`. Quy tắc che: §4.

Bốn lý do khiến file luật cũ không có tác dụng (xem "Trước đây") đều có test
chặn tái diễn trong `tests/governance/test_trino_access_control.py`.

**Đã kiểm trên engine thật**: bước CI `Trino access control enforced` chạy
`scripts/verify_trino_access_control.py` trên Trino 443 với dữ liệu thật: 20/20
kiểm tra đạt ở PR #39. **Chưa kiểm ở runtime**: mask trên `bronze.core_customer_cdc`
và `silver.dim_customer_current` — CI không tạo hai bảng này. Kiểm trên stack chính
bằng lệnh ở §11.

### Chưa phủ

| Khoảng trống | Hệ quả |
|---|---|
| **Chưa có xác thực** | Trino tin tên user mà client tự khai (`X-Trino-User`). Ai kết nối được cổng 8080/8085 đều khai được `admin` và đọc `cccd` gốc. Luật chặn truy cập *nhầm*, không chặn truy cập *cố ý* |
| **Spark và MinIO không đi qua Trino** | Job Spark đọc ghi thẳng Iceberg REST + MinIO. Ai có credential MinIO đọc được file Parquet gốc |
| **Nhóm nhạy cảm "Trung bình" chưa che** | `account_no`, `device_id`, `ip_address`, `latitude`/`longitude` (§3) đọc được nguyên bản bởi mọi role đọc được bảng chứa chúng |
| PostgreSQL nguồn | Có kiểm soát riêng — `docker/init_postgres/05_security.sql` tạo role và `GRANT`. Không liên quan tới luật Trino |

Đây vẫn là lý do §5 quan trọng: mô hình "giữ bản gốc, che lúc đọc" giờ đã có lớp
thực thi, nhưng lớp đó mới vững bằng mức xác thực của nó.

### Trước đây: file luật có trong repo nhưng không có tác dụng

Trước PR #39, `docker/init_trino/access-control.properties` khai 195 dòng luật
theo 4 group, nhưng Trino không nạp nó. Mỗi lý do 1–3 tự nó đã đủ:

1. **Không được mount.** Compose chỉ mount `./init_trino/catalog`; file nằm ngoài
   thư mục đó.
2. **Sai định dạng.** Không có `access-control.name=file`, và luật viết thành các
   cặp `key=value` lặp lại thay vì một file luật JSON.
3. **Không có group.** Không có group provider nào, nên không user nào thuộc group.
4. **Che cột không tồn tại.** 5/9 luật che nhắm vào cột Gold mà DDL không có.

Không có file cấu hình thì Trino dùng access control `default`: cho phép tất cả.
`rbac.py` khi đó cũng không được thực thi: không file nào ngoài test `import` nó,
và nó mắc cùng lỗi che cột Gold không tồn tại.

Ma trận role × dataset × quyền in được bằng lệnh ở §11. Tài liệu `RBAC_MATRIX.md`
riêng vẫn chưa có.

---

## 8. Heuristic của từ điển bỏ sót `cccd`

`DATA_DICTIONARY.md` đánh dấu **31 cột** nghi chứa PII bằng regex tên cột:

```python
# scripts/generate_data_dictionary.py:69
PII_HINTS = re.compile(
    r"full_name|first_name|last_name|email|phone|address|"
    r"id_number|dob|date_of_birth|national_id",
    re.IGNORECASE,
)
```

Mẫu này có `national_id` và `id_number` nhưng **không có `cccd`** — tên cột thật
được dùng trong toàn bộ dự án. Nên trường nhạy cảm nhất của nền tảng **không
được đánh dấu** trong từ điển, ở cả 5 bảng chứa nó.

Các cột khác cũng bị bỏ sót vì cùng lý do:

| Cột | Số bảng | Loại |
|---|---:|---|
| `cccd` | 5 | số định danh cá nhân |
| `device_id` | 7 | dấu vết thiết bị |
| `account_no` | 6 | định danh tài chính |
| `card_no_masked` | 4 | định danh thẻ (đã che ở nguồn) |
| `latitude` / `longitude` | 3 mỗi cột | địa lý |
| `manager_name` | 4 | tên người |
| `ip_address` | 2 | dấu vết mạng |

Trình sinh đã tự khai giới hạn này (*"sẽ bỏ sót cột nhạy cảm đặt tên không theo
mẫu thông dụng"*), nên đây không phải tuyên bố sai — nhưng con số 31 **không**
nên đọc là "31 cột PII". Bản kiểm kê ở §2 là con số dùng được.

Ghi chú: heuristic **cố ý** đánh dấu cả cột đã che như `full_name_masked`, vì cột
đã che vẫn nằm trong lineage PII. Đó là lựa chọn đúng.

---

## 9. Data contract không có chỗ khai phân loại

34 contract trong `governance/datasets/` có đúng 11 khoá cấp cao, giống nhau ở
cả 34 file:

```text
dataset_id · owner · business_purpose · refresh_sla · quality_class
layer · physical_location · dag_id · upstream_dataset_ids
quality_rules · ai_governance
```

**Không có** `pii_columns`, `classification`, `sensitivity` hay tương đương. Đã
kiểm từng file.

`ai_governance` có `risk_tier` (`limited_risk`, …) nhưng đó là phân loại rủi ro
theo **mục đích dùng AI**, không phải theo độ nhạy cảm của dữ liệu.

Hệ quả: **không có phân loại dữ liệu ở dạng máy đọc được trong repo.** Ba nguồn
tín hiệu hiện có đều gián tiếp:

1. heuristic tên cột của từ điển — bỏ sót `cccd` (§8)
2. danh sách cột hardcode trong `pii_masking.py`
3. tài liệu này — thủ công, cần cập nhật tay

Không cái nào ràng buộc được cái nào. Thêm cột PII mới vào Silver sẽ **không**
làm gì đỏ.

Rủi ro liên quan: `serving/mart_customer_360_current.sql` dùng `select *` (có
ghi chú giải thích là chủ ý). Nên mọi cột PII thêm vào Gold sẽ tự động xuất hiện
ở tầng serving, không qua bước xem xét nào.

---

## 10. Xoá không phải là xoá, trong ít nhất 7 ngày

`silver.dim_customer` và `bronze.core_customer` đều nằm trong `DIM_TABLES` của
`code_etl/shared/ops/iceberg_maintenance.py`, và DAG bảo trì tuần chạy chúng ở
chế độ `expire_only`:

```python
expire_snapshots(spark, table, retain_days=7, min_snapshots=3)
```

Với Iceberg, xoá hay sửa một hàng chỉ tạo snapshot mới; giá trị cũ còn đọc được
bằng time travel cho tới khi snapshot hết hạn. Nên một yêu cầu xoá dữ liệu cá
nhân thực thi bằng `DELETE`/`UPDATE` sẽ:

- vẫn đọc được trong **7 ngày**, và
- vẫn đọc được cho tới khi có thêm **3 snapshot** mới, vì `min_snapshots=3` giữ
  lại 3 snapshot gần nhất bất kể tuổi

Hai điều kiện là **và**, không phải hoặc. Với bảng ghi mỗi ngày, mức trên là
khoảng 7 ngày; với bảng ghi thưa hơn thì lâu hơn.

Đây là cấu hình bảo trì chung, **không** phải chính sách lưu trữ dữ liệu cá nhân.
Không có chính sách nào như thế trong repo.

---

## 11. Cách kiểm lại

Dựng lại bản kiểm kê ở §2 từ DDL (không cần stack):

```bash
py -3 -c "import re,glob,os;SKIP={'04_ddl_bronze_cdc_old.sql','08_ddl_data_vault_example.sql','05_security.sql','06_ddl_superset.sql','07_ddl_mlflow.sql'};C=re.compile(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.\"]+)\s*\((.*?)\n\s*\)',re.I|re.S);P=re.compile(r'^(full_name|full_name_masked|customer_name|manager_name|email|phone|address|date_of_birth|cccd|card_no_masked|device_id|latitude|longitude|account_no|ip_address)$',re.I);[print(f'{fqn.strip(chr(34)):45s} {h}') for f in sorted(glob.glob('docker/init_*/*.sql')) if os.path.basename(f) not in SKIP for fqn,b in C.findall(open(f,encoding='utf-8').read()) if (h:=[m.group(1) for l in b.split(chr(10)) if (m:=re.match(r'\s*([a-z_][a-z0-9_]*)\s+([A-Z]+)',l.strip(),re.I)) and P.match(m.group(1))])]"
```

Kiểm xem contract có khai phân loại chưa:

```bash
py -3 -c "import yaml,glob,collections;k=collections.Counter();[k.update(yaml.safe_load(open(p,encoding='utf-8')).keys()) for p in glob.glob('governance/datasets/*.yaml')];print(dict(k))"
```

Kiểm luật Trino (§7). Tĩnh, không cần stack:

```bash
py -3 scripts/generate_trino_access_control.py --check     # rules.json khớp rbac.py
py -3 -m pytest -q tests/governance/test_trino_access_control.py
py -3 governance/rbac.py                                   # in role, user và ma trận quyền
```

Khi stack đang chạy — Trino có nạp luật không, mask có chạy đúng kiểu không:

```bash
py -3 scripts/verify_trino_access_control.py --container banking-trino
```

Nếu mọi kiểm tra "phải bị chặn" lại *thành công*, Trino không nạp luật — thường vì
`access-control.properties` không được mount vào `/etc/trino/`.

**Windows**: đặt `PYTHONIOENCODING=utf-8` trước lệnh `py -3`.

---

## 12. Chưa có

| Thiếu | Ảnh hưởng |
|---|---|
| Xác thực người dùng Trino | §7 — tên user do client tự khai; ai khai `admin` cũng đọc được `cccd` gốc |
| Kiểm soát truy cập ở Spark / MinIO | §7 — đường đọc ghi trực tiếp, không qua luật Trino |
| Che nhóm nhạy cảm "Trung bình" | §7 — `account_no`, `device_id`, `ip_address`, lat/long chưa che |
| Trường `classification` trong data contract | §9 — thêm cột PII mới không làm gì đỏ |
| `cccd` trong `PII_HINTS` | §8 — trường nhạy cảm nhất không được từ điển đánh dấu |
| Test so ba quy tắc che `full_name` | §4 — ba biểu thức khác nhau, không gì so |
| Lý do cho bất đối xứng `age` vs `age_group_decade` | §4 |
| Chính sách lưu trữ / xoá dữ liệu cá nhân | §10 — chỉ có cấu hình bảo trì chung |
| Quy trình xử lý yêu cầu xoá (right to erasure) | không có runbook |
| `RBAC_MATRIX.md` · `AUDIT_TRAIL.md` · `REGULATORY_MAPPING.md` | ba tài liệu còn lại của nhóm này |

---

## 13. Trạng thái hiện tại

```text
bảng có PII      31  (lakehouse 23 · PostgreSQL nguồn 8)
che ở Bronze     lúc đọc qua Trino, trừ admin/ETL · lưu gốc
che ở Silver     lúc đọc qua Trino, trừ admin/ETL · lưu gốc
che ở Gold       có — chỉ full_name_masked, không có cccd/phone/email
che ở serving    có, thừa hưởng từ Gold qua select *
bản sao đã che   2 bảng trong lakehouse.sandbox, tạo lại 08:00 hằng ngày
salt cccd_hash   Airflow Variable, đọc lúc render task, không có giá trị dự phòng
quy tắc che      3 quy tắc khác nhau cho cùng trường full_name
kiểm soát        Trino: có, chưa xác thực · Spark/MinIO: không · PostgreSQL nguồn: có
phân loại máy đọc  không
time travel      PII đã xoá còn đọc được ≥ 7 ngày và ≥ 3 snapshot
```
