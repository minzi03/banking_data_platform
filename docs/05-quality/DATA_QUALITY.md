# Chất Lượng Dữ Liệu

> Cập nhật: 2026-09-23 · Bản đồ tài liệu: [`../INDEX.md`](../INDEX.md)
> Liên quan: [`TESTING_STRATEGY.md`](TESTING_STRATEGY.md) · [`EVIDENCE_MANIFEST.md`](EVIDENCE_MANIFEST.md) · [`../04-operations/INCIDENT_RUNBOOK.md`](../04-operations/INCIDENT_RUNBOOK.md)

Mọi con số đo trực tiếp trên repo tại `2026-09-22` bằng cách parse
`dq_rules.yml`, `quarantine_rules.yml`, các file DAG và `DATA_DICTIONARY.md`.
Kết quả chạy thật trên stack (§6a) đo ngày `2026-09-23`. Chỗ nào chưa đo được
thì nói là chưa đo.

> **Đọc §6a trước nếu bạn vận hành.** Tới `2026-09-23`, job DQ và quarantine
> **chưa từng chạy được** trên `spark-worker-1`: worker chạy Python 3.8, còn code
> dùng cú pháp 3.9+ nên chết ngay lúc import. Mọi mô tả hành vi runtime trong
> tài liệu này trước ngày đó là suy từ code, không phải quan sát. Đã sửa (TD-10);
> sau đó Silver DQ đỏ vì check đếm mọi snapshot — cũng đã sửa (TD-11): giờ
> Silver và Gold `exit 0` trên dữ liệu hiện tại.

---

## 1. Bốn cơ chế, hai tư thế

| Cơ chế | Tư thế | Chạy ở đâu | Số lượng |
|---|---|---|---:|
| Guard fail-loud trong Gold job | **Ngăn chặn** | trong job, trước khi ghi | 2 guard |
| DQ check | Phát hiện | job Spark riêng, sau ETL | 88 check / 29 bảng |
| Quarantine | Phát hiện | job Spark riêng, sau DQ | 18 rule / 4 bảng |
| dbt test | Phát hiện | sau `dbt build` | 117 |

Chỉ **một** cơ chế là ngăn chặn, và nó chỉ có ở tầng Gold.

Điều này quan trọng hơn mọi con số khác trong tài liệu: **DQ check ở dự án này
là phát hiện, không phải ngăn chặn.** Dữ liệu xấu đã nằm trong Silver, Gold và
cả tầng serving nhiều giờ trước khi có gì kiểm nó. DQ không chặn được việc ghi;
nó chỉ nói cho bạn biết chuyện đã xảy ra.

---

## 2. Dòng thời gian, và khoảng hở trong đó

Lấy từ `schedule_interval` của từng DAG (không phải từ
`production_schedule.yml` — xem §10):

```text
02:00  Bronze  ×3 DAG   (core_banking · card_crm · digital_banking)
04:00  Silver
06:00  Gold            ← guard fail-loud chạy Ở ĐÂY, trong job
07:00  dbt run         → publish tầng serving
08:00  DQ check        ← lần đầu có ai soi dữ liệu (chạy được từ 2026-09-23, §6a)
08:00  dbt test        (117 test trên serving)
09:00  quarantine
09:00  contract validation
```

Một hàng dữ liệu sai trong Silver lúc 04:00 sẽ:

1. được Gold đọc lúc 06:00
2. được publish ra serving lúc 07:00
3. **bị phát hiện lúc 08:00** — bốn tiếng sau, và đã đi qua hai tầng

Không có rollback. Không có cách nào "giữ lại" partition cho tới khi DQ xanh.
Kiến trúc này chấp nhận đánh đổi đó; tài liệu chỉ cần nói rõ để không ai tưởng
DQ là một cái cổng.

---

## 3. DQ check: 88 check trên 29 bảng

`code_etl/shared/ops/data_quality.py` đọc `dq_rules.yml`, chạy check qua Spark,
ghi kết quả vào `opslakehouse.data_quality_log` (PostgreSQL, qua JDBC).

```text
29 mục bảng   ·   88 check   ·   81 FAIL + 7 WARN
```

Từ `2026-09-22` cả 29 mục đều trỏ vào bảng có thật, nên 29/88 vừa là số khai vừa
là số chạy được. Trước đó một mục trỏ vào bảng không tồn tại và con số thật là
28/86 — xem §6. `tests/governance/test_dq_rules_resolve.py` nạp file thật và
chặn khoảng lệch đó tái xuất hiện.

Phân bố loại check:

| Loại | Số lần dùng |
|---|---:|
| `row_count` | 29 |
| `null_check` | 24 |
| `referential_integrity` | 16 |
| `unique_check` | 13 |
| `range_check` | 4 |
| `reconciliation` | 2 |

### 6 loại được dùng, 9 loại được cài

`CHECK_DISPATCH` có **9** khoá. Ba khoá **chưa bao giờ được cấu hình** trong
`dq_rules.yml`:

| Loại | Trạng thái thật |
|---|---|
| `schema_drift` | Có cài, không dùng qua DQ — nhưng có DAG riêng (`ops_schema_drift_dag.py`) gọi thẳng `governance/schema_drift.py`, nên **năng lực vẫn tồn tại**, chỉ không đi qua đường này |
| `freshness_check` | Có cài, **không đường nào gọi tới**. Chỉ tới được qua `dq_rules.yml`, và ở đó không có cấu hình nào |
| `anomaly_detection` | Như trên — `governance/anomaly_detection.py` chỉ tới được qua DQ dispatch, và DQ chưa bao giờ gọi |

Nên `dq_check_types = 9` trong evidence manifest là con số **đúng** (nó đếm khoá
trong `CHECK_DISPATCH`, đúng như `definition` khai) nhưng dễ đọc thành "9 loại
check đang chạy". Đang chạy là **6**.

### Ngữ nghĩa severity

```python
severity = check.get("severity", "FAIL")     # mặc định FAIL
...
if severity == "WARN" and status == "FAIL":
    status = "WARN"                          # hạ cấp, không bỏ qua
```

Mặc định là `FAIL` — nghĩa là quên khai severity thì check sẽ chặn, không phải
âm thầm bỏ qua. Đó là hướng mặc định đúng.

`WARN` vẫn được **ghi vào log** với status `WARN`; nó chỉ không tính vào
`fail_count`. Cuối job:

```python
if fail_count > 0:
    sys.exit(1)
```

Kết quả được ghi vào PostgreSQL **trước** khi `sys.exit`, nên một lượt DQ đỏ vẫn
để lại bằng chứng đầy đủ, không mất trắng. Cùng tinh thần với run artifact của
evidence manifest.

### Exception là FAIL — đúng

Cả 9 hàm check đều bọc `try/except` và trả `("FAIL", "N/A", f"Error: {e}")`. Một
check nổ vì bảng không tồn tại, cột sai tên, hay Spark lỗi **không** được tính
là pass. Đây là quyết định fail-loud đúng — nó *sẽ* làm lỗi ở §6 nổi lên thay vì
im lặng, nếu job chạy được tới đó. Nó chưa bao giờ chạy tới (§6a).

### Nhưng tên check sai thì fail OPEN

```python
executor = CHECK_DISPATCH.get(check_name)
if not executor:
    log.warning(f"Unknown check type: {check_name}, skipping")
    continue
```

Gõ sai `null_check` thành `nul_check` → check **không chạy**, **không** có dòng
nào trong `data_quality_log`, và job vẫn `exit 0`. Bảng đó trông như đã được
kiểm.

Đây là lỗ fail-open duy nhất trong đường DQ, và nó nằm ở đúng chỗ khó thấy
nhất: một cảnh báo trong log của một job mà không ai đọc log (§9).

Hành vi runtime **chưa đổi** — vẫn `warning` rồi `continue`. Cái đã đổi là tên
check sai không vào được `main` nữa: `test_dq_rules_resolve.py` đối chiếu mọi
`name:` với khoá của `CHECK_DISPATCH`, nên lỗi bị bắt lúc commit thay vì lúc
chạy. Đó là dịch chuyển thời điểm phát hiện, không phải sửa fail-open.

---

## 4. Phủ sóng: đo được

| Tầng | Có rule | Tổng bảng | Tỷ lệ |
|---|---:|---:|---:|
| silver | 13 | 17 | 76% |
| gold | 10 | 14 | 71% |
| bronze | 6 | 22 | 27% |
| **lakehouse** | **29** | **53** | **55%** |

(Gold là 10 từ `2026-09-22`. Trước đó đếm 9: mục thứ 10 trỏ vào bảng không tồn
tại, §6.)

Bảng **không** có DQ rule:

```text
silver   dim_customer_current · dim_account_current · dim_deposit · dim_loan
gold     aml_monitoring · fraud_risk_txn · loan_portfolio_risk
         customer_loan_summary
bronze   16 bảng core_* (toàn bộ đường batch)
```

Hai chỗ đáng chú ý:

**Bronze chỉ phủ đúng đường CDC.** 6 bảng có rule là
`card_account_cdc`, `card_transaction_cdc`, `core_account_cdc`,
`core_customer_cdc`, `core_transaction_cdc`, `online_transaction_cdc`. Toàn bộ
16 bảng `core_*` nạp theo batch **không có rule nào**. Đường batch là đường nạp
chính; nó đang được phủ ít hơn đường CDC.

**`aml_monitoring` và `fraud_risk_txn` không có DQ rule.** Đây là hai bảng
rủi ro, và là hai bảng mà ngưỡng vừa được hiệu chỉnh lại. Chúng được phủ bởi
guard fail-loud (§7) và bởi invariant manifest, nhưng không có DQ check nào.

---

## 5. Quarantine: 18 rule trên 4 bảng Silver

`quarantine_rules.yml` khai 5 nhóm rule trên 4 bảng nguồn
(`dim_account` xuất hiện hai lần: `invalid_account` và `overdue_account`):

```text
lakehouse.silver.dim_customer      → quarantine.invalid_customer
lakehouse.silver.dim_account       → quarantine.invalid_account
lakehouse.silver.dim_account       → quarantine.overdue_loan
lakehouse.silver.dim_branch        → quarantine.invalid_branch
lakehouse.silver.fact_txn_account  → quarantine.suspicious_transaction
```

18 violation rule, **ba** mức severity: 6 `FAIL`, 10 `WARN`, 2 `INFO`.

```python
"status": "QUARANTINED" if severity == "FAIL" else "WARNED",
```

Chỉ `FAIL` mới thực sự đưa hàng vào bảng quarantine; `WARN` và `INFO` chỉ được
ghi nhận. Lưu ý: DQ dùng **hai** mức (`FAIL`/`WARN`), quarantine dùng **ba**
(`FAIL`/`WARN`/`INFO`). Hai từ vựng khác nhau cho cùng một khái niệm, không có
gì đồng bộ chúng.

Rule mang ngữ nghĩa nghiệp vụ thật, không chỉ kiểm kỹ thuật — ví dụ
`negative_balance` chỉ là vi phạm **khi** `account_type = 'CASA'`, và
`large_transaction` đặt ở `> 1.000.000.000 VND` với severity `WARN` (giao dịch
tỷ đồng là bất thường, không phải bất hợp pháp).

Xử lý sau đó: `scripts/resolve_quarantine.py`, có `--dry_run` và `--execute` —
mặc định là `--dry_run`, nên một lần chạy nhầm sẽ không tự sửa dữ liệu.

### 5 bảng đích không có DDL

Bốn bảng `source_table` đều tồn tại — đã đối chiếu DDL bằng
`test_dq_rules_resolve.py` cùng lượt với `dq_rules.yml`. Nhưng **không DDL nào
tạo `lakehouse.quarantine.*`**, kể cả schema (`create_schemas.sql` tạo
bronze/silver/gold/sandbox/staging). Nên `write_to_quarantine` luôn ném ngay ở
`spark.table(target_table)`, log ERROR, và trả `0`:

```python
except Exception as e:
    log.error(f"Error writing to quarantine table {target_table}: {e}")
    return 0
```

Hàng vi phạm vẫn được **đếm** và vẫn làm job `exit 1` khi có `FAIL`, nhưng không
được ghi đi đâu. Đã xác nhận khi chạy thật ngày `2026-09-23`: mọi lần ghi đều
`TABLE_OR_VIEW_NOT_FOUND`, trong khi log vẫn in *"8970 records quarantined"*.
Khoảng hở này được ghi lại bằng một assertion ngược trong
`test_quarantine_target_tables_have_no_ddl` — test đó đỏ khi ai đó thêm DDL, tức
đúng lúc cần đảo nó thành "mọi `target_table` phải tồn tại".

---

## 6. Rule trỏ vào bảng không tồn tại — đã sửa 2026-09-22

Đến `2026-09-22`, `dq_rules.yml` khai `lakehouse.gold.branch_monthly_summary`.
Bảng đó **không tồn tại**: tên thật là `mart_branch_monthly_summary`, thiếu tiền
tố `mart_`. Bốn nguồn độc lập đều viết đủ tiền tố, chỉ file rule viết thiếu:

```text
docker/init_iceberg/03_ddl_gold.sql:212   CREATE TABLE ... mart_branch_monthly_summary
code_etl/gold/time_analytics/branch_monthly_summary.yml:28   table: mart_branch_monthly_summary
dbt/models/gold/_gold_sources.yml:220     - name: mart_branch_monthly_summary
docs/03-data/DATA_DICTIONARY.md           mart_branch_monthly_summary
```

**Hệ quả dự đoán**: theo §3, exception được tính là FAIL, nên bảng không tồn tại
*sẽ* sinh 2 FAIL và `exit 1`.

**Hệ quả thật** (đo ngày `2026-09-23`): không có gì cả, vì job chết từ trước khi
đọc `dq_rules.yml` — xem §6a. Bản trước của mục này viết task `dq_gold_checks`
"fail mỗi ngày"; đó là suy luận từ code, và **sai cơ chế**. Cũng không có log
task nào của DAG DQ trong volume log Airflow, nên chưa ai thấy nó chạy.

### Cái đáng ghi lại không phải lỗi gõ

Sửa tên là một dòng. Chuyện đáng ghi là **vì sao nó sống được**:
`tests/ops/test_data_quality.py` — file mang đúng tên module — kiểm loader bằng
fixture `sample_dq_rules`, một YAML tổng hợp dựng trong `tmp_path`. Nó chứng minh
loader chạy đúng. **Không test nào nạp `dq_rules.yml` thật.** Nên 822 unit test
xanh trong khi file rule production hỏng.

Cùng lớp lỗ với "binding chỉ neo README" ở
[`EVIDENCE_MANIFEST.md` §6.4](EVIDENCE_MANIFEST.md): cơ chế kiểm tồn tại và
chạy tốt, chỉ là không trỏ vào tạo tác thật.

### Vì sao tên này dễ gõ sai

Tên **file config** là `branch_monthly_summary.yml` trong khi target là
`mart_branch_monthly_summary` — sao chép tên file ra chính là chuỗi sai.
`customer_360.yml` → `mart_customer_360` cũng vậy, nên 2 trong 14 config Gold có
tên file khác tên bảng và cái bẫy vẫn còn. Đã cân nhắc đổi tên file và **không
làm**: đường dẫn đó được `gold_mart360_dag.py:61`, `initial_load.py:70`,
`tests/gold/test_gold_sql_invariants.py:203` (khớp theo tên file) và hai tài liệu
tham chiếu — sửa 5 file để lấp 1 trong 2 chỗ lệch, mà không thêm được lớp bảo vệ
nào so với test dưới đây.

### Cái đang chặn nó quay lại

`tests/governance/test_dq_rules_resolve.py` nạp **file thật** (`dq_rules.yml` và
`quarantine_rules.yml`) và đối chiếu:

```text
mọi khoá bảng      phải có CREATE TABLE trong docker/init_iceberg/*.sql
mọi ref_table      như trên   (referential_integrity)
mọi source_table   như trên   (reconciliation, nhóm quarantine)
mọi tên check      phải là khoá của CHECK_DISPATCH
```

Danh sách bảng **parse từ DDL**, dùng lại `parse_ddl` và `SKIP_DDL` của
`scripts/generate_data_dictionary.py` — viết cứng danh sách trong test chỉ dời
chỗ của drift sang một file mới hơn. Test có guard chống pass rỗng (ngưỡng tối
thiểu cho số bảng DDL, số mục rule, số check) nên một lần parse hỏng không thể
xanh bằng cách không kiểm gì.

Cả hai kiểu hỏng đã được kiểm chứng ngược trước khi viết mục này: tiêm lại tên
bảng cũ và một `nul_check` gõ sai, bộ test đỏ ở cả hai, thông điệp lỗi nêu đúng
tên sai và gợi ý ứng viên trong DDL.

**Đã xác minh trên stack ngày `2026-09-23`** (sau khi sửa §6a): Gold DQ chạy
với `dq_rules.yml` thật, `mart_branch_monthly_summary` PASS với 12.800 dòng,
20/20 check PASS, `exit 0`. Đây là `spark-submit` trong đúng container mà DAG
dùng — chưa phải một lượt Airflow, vì Airflow không được bật.

Ghi lại đầy đủ ở [`technical-debt.md` TD-9](technical-debt.md).

---

## 6a. Job DQ chưa từng chạy được — đã sửa 2026-09-23

Kiểm §6 trên stack thì lộ ra lỗi lớn hơn: `spark-worker-1` chạy **Python
3.8.10**, còn code ops/governance dùng cú pháp 3.9+ trong annotation. Module chết
ngay lúc import:

```text
File "/opt/project/code_etl/shared/ops/data_quality.py", line 60, in <module>
    def load_rules(path: str) -> dict[str, Any]:
TypeError: 'type' object is not subscriptable
```

Không riêng Gold — **cả ba tầng**, và cả quarantine, schema drift, contract
validation, iceberg maintenance: 11/14 entry point chạy trên worker chết như vậy,
từ commit đầu tiên. CI chạy Python 3.11 nên không bao giờ thấy; ruff đặt
`target-version = py310` và rule `UP` còn chủ động đẩy cú pháp mới vào.

Sửa: `from __future__ import annotations` ở 12 module, và thay `zoneinfo` (chỉ có
từ 3.9) bằng offset UTC+7 cố định trong `iceberg_maintenance.py`.
Một test quét tĩnh để lỗi không quay lại. Chi tiết và bảng trước/sau ở
[`technical-debt.md` TD-10](technical-debt.md).

Sau đó image Spark được nâng lên Python 3.10 (Ubuntu 22.04, Java 17), đúng mức
sàn repo đã khai, và CI cũng chuyển sang 3.10. Test đổi tên thành
`test_worker_python_compat.py` và canh năm nơi khai phiên bản — image,
`requires-python`, ruff `target-version`, `WORKER_PYTHON`, `python-version` của
mọi workflow CI — cùng các API 3.11+ mà worker không có.

### Chạy thật, lần đầu

`spark-submit` trên `spark-worker-1`, `--cob_dt 2026-09-22`:

| Job | Kết quả | `exit` |
|---|---|:-:|
| DQ `--layer gold` | 20 check · **20 PASS** | 0 |
| DQ `--layer silver` | 62 check · 53 PASS · 1 WARN · **8 FAIL** | 1 |
| DQ `--layer bronze` | 6 check · **6 FAIL** — cả 6 bảng `*_cdc` có 0 dòng | 1 |
| quarantine `--layer all` | 18 rule · 14 PASS · 3 WARN · 1 FAIL | 1 |

Job giờ **chạy hết và báo cáo** — `exit 1` là kết quả của check, không phải crash.

**8 FAIL của Silver không phải lỗi dữ liệu.** Đã đối chiếu từng cái:

- 5 bảng fact: `unique_check` đếm trên **cả 8 snapshot `cob_dt`**; trong từng
  snapshot có **0** trùng.
- `dim_customer`, `dim_account` (SCD2): mỗi khoá có 2 phiên bản; các dòng
  `is_current = 1` là duy nhất.
- `dim_card` "1 orphan": orphan đó là `NULL` — 2.672 thẻ không có `account_id`, và
  left-anti join đếm `NULL` như một giá trị.

Nguyên nhân chung: check đọc **cả bảng**, nhận `--cob_dt` nhưng không lọc theo
nó, và không biết SCD2. Một check luôn đỏ trên dữ liệu lành sẽ dạy người ta bỏ
qua nó.

Bronze đỏ vì Kafka/Debezium không chạy trong môi trường này — không nói gì về CDC.

### Phạm vi check: một snapshot, phiên bản hiện hành — đã sửa 2026-09-23

Quyết định: một lượt DQ hằng ngày hỏi về **snapshot của ngày đang kiểm**, không
phải cả lịch sử. `_scoped_table` trong `data_quality.py` lọc theo cột có thật lúc
chạy:

```text
có cột cob_dt       → cob_dt = DATE '<cob_dt của lượt DQ>'
có cột is_current   → CAST(is_current AS INT) = 1
không có cột nào    → cả bảng (dim SCD1, bảng CDC)
```

Kèm theo: `referential_integrity` bỏ qua FK `NULL` (cột bắt buộc thì khai
`null_check`), nhãn cận của `range_check` in đúng `>=0` thay vì `<=None`, và mọi
kết quả ghi rõ đã đọc phạm vi nào (`[cob_dt=2026-09-22]`, `[is_current]`).

**Hệ quả có chủ đích:** `row_count` trên bảng có `cob_dt` giờ FAIL khi thiếu
snapshot của ngày đang kiểm. Trước đây nó PASS miễn còn snapshot cũ nào.

**Một ngoại lệ, khai tường minh.** `bronze.core_customer` **không** partition theo
`cob_dt` (khác `core_txn_account`): mỗi lần nạp ghi đè cả bảng, nên bảng chỉ giữ
lần nạp mới nhất, và lọc theo ngày của lượt DQ ra 0 dòng. Rule reconciliation của
`dim_customer` khai `source_scope: whole_table`. Test
`test_reconciliation_source_scope_matches_partitioning` buộc lựa chọn này khớp DDL
theo cả hai chiều.

Chạy lại trên stack, `--cob_dt 2026-09-22`:

| Job | Trước | Sau |
|---|---|---|
| DQ silver | 8 FAIL · `exit 1` | 61 PASS · 1 WARN · **0 FAIL** · `exit 0` |
| DQ gold | 20 PASS | 20 PASS |
| DQ bronze | 6 FAIL (CDC rỗng) | không đổi |

WARN còn lại là thật: `89994 values out of range >=0 [cob_dt=2026-09-22]` — số
tiền âm trong `fact_txn_account` của một snapshot (trước đây 720.270 vì cộng cả
8 snapshot).

Lỗi cấy vào (trên temp view của Spark, không đụng lakehouse) vẫn đỏ: trùng
**trong** một snapshot, hai dòng `is_current` cho một khoá, và FK chỉ khớp với phiên
bản dim đã hết hiệu lực — cả ba FAIL; còn trùng **giữa** các snapshot, phiên bản cũ
của SCD2 và FK `NULL` đều PASS. Chi tiết ở
[`technical-debt.md` TD-11](technical-debt.md).

---

## 7. Tầng ngăn chặn: hai guard của Gold

`code_etl/gold/base_job/gold_job.py` — đây là chỗ duy nhất dữ liệu bị **chặn
trước khi ghi**.

### `assert_source_snapshots()` — chống silent corruption

Mọi bảng khai trong `validation.require_snapshots` phải có partition `cob_dt`
đang xử lý, nếu không job `raise RuntimeError`.

Lý do nó **không** thay được bằng `require_non_empty`, ghi nguyên trong
docstring: model grain customer neo vào `dim_customer` rồi `LEFT JOIN` fact. Nếu
partition fact thiếu, query vẫn trả đủ một dòng mỗi khách với mọi metric = 0.
Output **không rỗng** → `require_non_empty` PASS → Gold bị ghi đè bằng số 0
trông rất hợp lý. Xem [ADR-0005](../02-architecture/adr/0005-fail-loud-before-overwrite.md).

### `assert_non_empty()` — chống no-op im lặng

`overwritePartitions()` với DataFrame rỗng là **no-op**: partition cũ ở lại, và
không có lỗi nào. Guard này chặn đúng chỗ đó.

### Mức áp dụng, đo được

```text
14/14 Gold config có require_non_empty
11/14 Gold config có require_snapshots
```

Ba config không có `require_snapshots` là
`customer_balance_summary`, `customer_product_summary`, `cross_sell_segment` —
và cả ba **chỉ đọc bảng dimension** (`dim_customer`, `dim_account`, `dim_card`,
`dim_loan`), không join fact nào. Hazard mà guard này chống chỉ phát sinh khi có
`LEFT JOIN` fact, nên việc bỏ guard ở đây là **nhất quán với lý do tồn tại của
nó**, không phải sót. Đã kiểm từng file.

### Bronze và Silver không có guard tương đương

`code_etl/bronze/base_job/ingestion_jdbc.py` và
`code_etl/silver/base_job/{scd_type1,scd_type2,fact_txn}.py` **không** có
`assert_*` nào. Tầng ngăn chặn chỉ tồn tại ở Gold.

---

## 8. dbt: 117 test ở tầng serving

Tầng serving do dbt sở hữu và có bộ test riêng, ngoài `dq_rules.yml`:

```text
_serving_models.yml   98 generic  (81 not_null · 12 unique · 17 accepted_values)
_gold_sources.yml     12 generic
dbt/tests/             7 singular
                     ───
                     117
```

7 singular test mang ngữ nghĩa nghiệp vụ, không chỉ kiểm cột:
`assert_serving_snapshot_alignment`, `assert_customer_360_completeness`,
`assert_no_duplicate_customers`, `assert_rfm_scores_valid`,
`assert_balances_non_negative`, `assert_loan_risk_rates_bounded`,
`assert_gold_source_reachable`.

`dbt_data_quality_dag.py` chạy generic rồi singular, 08:00 hằng ngày.

### Task `log_dq_results` không log gì

```python
log_dq_results = BashOperator(
    task_id="log_dq_results",
    bash_command=f"{DBT_EXEC} \"cd {DBT_DIR} && echo 'DQ tests passed for cob_dt={DATA_COB_DT}'\"",
)
```

Nó `echo` một chuỗi cố định. Không ghi gì vào `data_quality_log`, không đọc kết
quả dbt. Và vì nó nằm sau `dbt_test_singular` trong chuỗi phụ thuộc, nó không
bao giờ chạy khi test đỏ — nên câu "DQ tests passed" không bao giờ **sai**, chỉ
là vô nghĩa. Tên task hứa một việc mà nó không làm.

Hệ quả thật: **117 dbt test không để lại dấu vết nào** trong
`data_quality_log`. Muốn biết chúng ra sao phải đọc log Airflow.

---

## 9. Không ai được báo

```python
# ops_data_quality_dag.py:30   và   ops_quarantine_dag.py:26
"email_on_failure": False,
```

Cả DQ và quarantine đều tắt thông báo. Khi DQ đỏ:

- task Airflow fail → DAG fail
- kết quả nằm trong `opslakehouse.data_quality_log`
- **không** email, không Slack, không callback

Và `data_quality_log` / `quarantine_log` là **write-only**: ngoài chính hai job
ghi vào chúng, không script nào, không dbt model nào, không dashboard nào đọc
chúng. `streamlit/app.py` chỉ nhắc chuỗi *"5 quarantine tables"* trong phần chữ,
không query bảng nào.

Nên đường duy nhất để biết dữ liệu có vấn đề là **có người tự mở Airflow UI**.
§6 và §6a tồn tại được lâu chính vì lý do này — §6a là cả một job không import
nổi, và không gì báo.

Cách xử lý khi phát hiện: [`../04-operations/INCIDENT_RUNBOOK.md`](../04-operations/INCIDENT_RUNBOOK.md).

---

## 10. `production_schedule.yml` là dự định, không phải cấu hình

File này tự khai đúng vai của nó:

> *"DAGs will be updated to use these schedules when deployed to production."*

Không file `.py` nào đọc nó. Lịch thật nằm trong `schedule_interval` của từng
DAG. Tôi đã đối chiếu từng dòng: với những DAG nó có liệt, hai bên **khớp**
(Bronze 02:00, Silver 04:00, Gold 06:00, dbt 07:00, DQ 08:00, quarantine 09:00).

Nhưng nó thiếu **4** trong 20 DAG file:

```text
data_quality/dbt_data_quality_dag.py     (08:00)
compliance/regulatory_reporting_dag.py
ops/ops_schema_drift_dag.py              (09:00)
ops/ops_ml_churn_dag.py
```

Không test nào so hai bên, nên khoảng lệch này sẽ tiếp tục rộng ra.

---

## 11. Cách chạy

DQ cho một tầng (cần stack đang chạy):

```bash
docker exec banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/project/code_etl/shared/ops/data_quality.py --cob_dt 2026-09-22 --layer silver
```

Quarantine:

```bash
docker exec banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/project/code_etl/shared/ops/quarantine.py --cob_dt 2026-09-22 --layer all
```

Xử lý quarantine, chạy thử trước:

```bash
docker exec banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/project/scripts/resolve_quarantine.py --cob_dt 2026-09-22 --dry_run
```

Đọc kết quả DQ gần nhất:

```bash
docker exec -i banking-postgres psql -U banking_admin -d banking_db -c "SELECT cob_dt, table_name, check_name, check_status, details FROM opslakehouse.data_quality_log WHERE check_status <> 'PASS' ORDER BY checked_at DESC LIMIT 50;"
```

dbt test trên serving:

```bash
docker exec banking-dbt bash -lc "cd /opt/dbt && dbt test --target docker --select serving"
```

Kiểm rule file mà không cần stack — mọi tên bảng và tên check phải phân giải
được (chạy cái này sau khi thêm rule mới, trước khi commit):

```bash
py -3 -m pytest tests/governance/test_dq_rules_resolve.py -q
```

Kiểm code chạy trên worker có dùng thứ mới hơn Python của worker không (§6a) —
chạy sau khi sửa bất cứ gì trong `code_etl/`, `governance/`, `docker/Dockerfile.spark`
hay `pyproject.toml`:

```bash
py -3 -m pytest tests/governance/test_worker_python_compat.py -q
```

Đếm nhanh số mục:

```bash
py -3 -c "import yaml;d=yaml.safe_load(open('code_etl/shared/ops/dq_rules.yml',encoding='utf-8'));print(len(d['tables']),'bảng')"
```

**Windows**: đặt `PYTHONIOENCODING=utf-8` trước lệnh `py -3`, nếu không cp1258
sẽ giết tiến trình ở thông báo tiếng Việt.

---

## 12. Chưa có

| Thiếu | Ảnh hưởng |
|---|---|
| `ops_contract_validation_dag` chạy được | pydantic đã có trên worker, nhưng `enforcement.py` chạy như script không thấy package `governance`, và không có CLI để nhận `--layer`/`--validate` (TD-10) |
| Thông báo khi DQ đỏ | §9 — chỉ biết nếu tự mở Airflow UI |
| DDL cho `lakehouse.quarantine.*` | §5 — hàng vi phạm được đếm nhưng không ghi được đi đâu |
| Ai đọc `data_quality_log` | write-only; không dashboard, không dbt model |
| DQ rule cho 16 bảng Bronze batch | đường nạp chính đang phủ ít hơn đường CDC |
| DQ rule cho `aml_monitoring`, `fraud_risk_txn` | hai bảng rủi ro không có DQ check |
| Guard fail-loud ở Bronze/Silver | tầng ngăn chặn chỉ có ở Gold |
| `freshness_check`, `anomaly_detection` được cấu hình | có cài, không đường nào gọi tới |
| Đồng bộ từ vựng severity | DQ 2 mức, quarantine 3 mức, không gì so chúng |
| dbt test ghi vào `data_quality_log` | 117 test không để lại dấu vết (§8) |
| Ngưỡng SLA độ tươi | `SLA_AND_FRESHNESS.md` chưa có |
| Kiểm `production_schedule.yml` ↔ DAG | thiếu 4 DAG, không gì bắt (§10) |

---

## 13. Trạng thái hiện tại

```text
DQ check        88 check · 29 bảng, tất cả phân giải được · 6/9 loại đang dùng
phủ sóng        silver 13/17 · gold 10/14 · bronze 6/22 (chỉ CDC)
quarantine      18 rule · 4 bảng Silver · 6 FAIL + 10 WARN + 2 INFO
dbt             117 test (110 generic + 7 singular) trên serving
guard           2 guard, chỉ ở Gold · 14/14 non_empty · 11/14 snapshots
phạm vi check   snapshot cob_dt của lượt DQ · is_current cho SCD2 (TD-11)
chạy thật       2026-09-23 · gold 20/20 PASS · silver 61 PASS + 1 WARN, 0 FAIL
                · bronze 6 FAIL (CDC không chạy) · quarantine 1 FAIL, không ghi được
lỗi đang sống   0 trong đường DQ — còn mở: quarantine không có bảng đích (§5)
hợp đồng tĩnh   test_dq_rules_resolve.py — 150 test trên rule file thật
                test_worker_python_compat.py — code worker khớp Python 3.10 của image
thông báo       không có
```
