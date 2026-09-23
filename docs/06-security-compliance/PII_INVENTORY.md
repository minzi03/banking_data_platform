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
| Bronze có được che? | **Không.** Bronze giữ giá trị gốc |
| Silver có được che? | **Không.** `silver.dim_customer` giữ `cccd`, `full_name`, `phone`, `email`, `address` nguyên bản |
| Gold có được che? | **Có** — chỉ còn `full_name_masked`, không còn `cccd`/`phone`/`email` |
| Tầng serving có được che? | **Có**, bằng cách thừa hưởng từ Gold (`select *`) |
| Lakehouse có kiểm soát truy cập? | **Không** (§7) |
| Có phân loại dữ liệu ở dạng máy đọc được? | **Không** (§9) |

Câu quan trọng nhất: **che PII ở dự án này là tạo bản sao đã che, không phải bảo
vệ bản gốc.** Ai đọc được Trino hay Spark thì đọc được `cccd` nguyên bản.

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

Cùng một trường `full_name` được che theo **hai** cách khác nhau ở hai chỗ:

| Nơi | Biểu thức | Kết quả (minh hoạ trên tên ví dụ) |
|---|---|---|
| Gold, `customer_360.yml:225` | `CONCAT(SPLIT(c.full_name,' ')[0], ' **')` | giữ họ, bỏ hết phần còn lại |
| `pii_masking.py`, `_mask_name_udf()` | tách theo khoảng trắng, giữ họ + chữ cái đầu của tên đệm và tên | giữ họ và hai chữ cái đầu |

Không quy tắc nào sai. Nhưng chúng **khác nhau**, không tài liệu nào trước đây
nói là khác, và không test nào so hai bên. Bản Gold che mạnh hơn bản "masking".

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

Đây là lựa chọn hợp lý **khi** có kiểm soát truy cập theo schema. Dự án này chưa
có (§7), nên hiện tại bảng `sandbox` là tự nguyện.

`SECURITY.md` đã nói đúng điều này: *"masking hiện áp ở tầng Gold/serving,
**không** ở Bronze."* Tài liệu này chỉ bổ sung rằng Silver cũng không.

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

## 7. Lakehouse không có kiểm soát truy cập

`governance/rbac.py` định nghĩa 5 role, 24 permission, trong đó 4 permission của
role `analytics` có biểu thức `column_mask`:

```text
admin         3 permission
etl_user      6
analytics    10   ← 4 permission có column_mask
readonly      3
data_steward  2   (kế thừa từ analytics)
```

**Không gì gọi module này.** Đã kiểm: ngoài chính `governance/rbac.py` và các
test của nó, không file `.py` nào `import` nó. Nó là một **mô hình quyền**, không
phải một cơ chế thực thi.

Và không có lớp thực thi nào khác ở tầng lakehouse:

| Lớp | Có kiểm soát? |
|---|---|
| Trino | **Không** — không có file cấu hình access-control nào trong repo |
| Spark / Iceberg REST | **Không** |
| PostgreSQL nguồn | **Có** — `docker/init_postgres/05_security.sql` tạo role và `GRANT`. Nhưng nó chỉ phủ DB **nguồn**, không phủ lakehouse |

Nên: bất kỳ ai kết nối được tới Trino đều `SELECT cccd FROM silver.dim_customer`
được. `column_mask` trong `rbac.py` không chạy ở đâu cả.

Đây là lý do §5 quan trọng: mô hình "giữ bản gốc, tạo bản đã che" phụ thuộc hoàn
toàn vào một lớp kiểm soát truy cập chưa tồn tại.

Ma trận role × dataset × quyền: `RBAC_MATRIX.md` (chưa có).

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

Kiểm xem `rbac.py` đã được gọi chưa:

```bash
git grep -l "from governance.rbac\|governance import rbac" -- "*.py"
```

Xem role và permission hiện khai:

```bash
py -3 -c "import sys;sys.path.insert(0,'.');from governance.rbac import ROLES;[print(n,len(r.permissions),r.parent_roles) for n,r in ROLES.items()]"
```

**Windows**: đặt `PYTHONIOENCODING=utf-8` trước lệnh `py -3`.

---

## 12. Chưa có

| Thiếu | Ảnh hưởng |
|---|---|
| Kiểm soát truy cập ở lakehouse | §7 — `cccd` đọc được bởi mọi client Trino |
| `rbac.py` được thực thi | 24 permission và 4 `column_mask` không chạy ở đâu |
| Trường `classification` trong data contract | §9 — thêm cột PII mới không làm gì đỏ |
| `cccd` trong `PII_HINTS` | §8 — trường nhạy cảm nhất không được từ điển đánh dấu |
| Test so hai quy tắc che `full_name` | §4 — hai biểu thức khác nhau, không gì so |
| Lý do cho bất đối xứng `age` vs `age_group_decade` | §4 |
| Chính sách lưu trữ / xoá dữ liệu cá nhân | §10 — chỉ có cấu hình bảo trì chung |
| Quy trình xử lý yêu cầu xoá (right to erasure) | không có runbook |
| `RBAC_MATRIX.md` · `AUDIT_TRAIL.md` · `REGULATORY_MAPPING.md` | ba tài liệu còn lại của nhóm này |

---

## 13. Trạng thái hiện tại

```text
bảng có PII      31  (lakehouse 23 · PostgreSQL nguồn 8)
che ở Bronze     không
che ở Silver     không
che ở Gold       có — chỉ full_name_masked, không có cccd/phone/email
che ở serving    có, thừa hưởng từ Gold qua select *
bản sao đã che   2 bảng trong lakehouse.sandbox, tạo lại 08:00 hằng ngày
salt cccd_hash   Airflow Variable, đọc lúc render task, không có giá trị dự phòng
quy tắc che      2 quy tắc khác nhau cho cùng trường full_name
kiểm soát        lakehouse: không · PostgreSQL nguồn: có
phân loại máy đọc  không
time travel      PII đã xoá còn đọc được ≥ 7 ngày và ≥ 3 snapshot
```
