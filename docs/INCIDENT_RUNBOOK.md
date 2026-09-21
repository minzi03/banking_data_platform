# Incident Runbook — Sự Cố Dữ Liệu

> **Đây không phải** [`RUNBOOK.md`](../RUNBOOK.md). Tài liệu đó trả lời *"chạy cái này thế nào?"*.
> Tài liệu này trả lời *"nó hỏng rồi, làm gì?"*.
>
> Cập nhật: 2026-09-22 · Bản đồ tài liệu: [`INDEX.md`](INDEX.md)

---

## Cách dùng

Mỗi kịch bản theo bốn bước cố định:

```text
TRIỆU CHỨNG  →  CHẨN ĐOÁN  →  XỬ LÝ  →  XÁC MINH ĐÃ KHỎI
```

**Bước 4 là bước hay bị bỏ nhất và là bước quan trọng nhất.** Pipeline dữ liệu hỏng theo kiểu không báo lỗi: job xanh, bảng có dòng, số thì sai. "Chạy lại thấy không đỏ nữa" **không phải** bằng chứng đã khỏi.

## Ba bảng tra cứu đầu tiên

Gần như mọi chẩn đoán bắt đầu từ một trong ba bảng này trong PostgreSQL (`opslakehouse`):

```sql
-- 1. Job nào đã chạy, cho ngày nào, xong chưa
--    status: R = Running, S = Success  (KHÔNG có mã cho Failed — xem ghi chú dưới)
SELECT job_name, schema_name, status, start_time, end_time, cob_dt
FROM opslakehouse.flag_job_etl
WHERE cob_dt = DATE '<cob_dt>'
ORDER BY start_time DESC;

-- 2. Kiểm tra chất lượng nào fail
SELECT check_name, table_name, check_status, expected_value, actual_value, details
FROM opslakehouse.data_quality_log
WHERE cob_dt = DATE '<cob_dt>' AND check_status IN ('FAIL', 'WARN')
ORDER BY checked_at DESC;

-- 3. Bản ghi nào bị cách ly
SELECT * FROM opslakehouse.quarantine_log
WHERE cob_dt = DATE '<cob_dt>';
```

> ⚠️ **Giới hạn đã biết**: `flag_job_etl.status` chỉ có `R` và `S` (ràng buộc `chk_flag_status`). Một job chết giữa chừng sẽ **nằm mãi ở `R`**, không có trạng thái `F`. Nên `status = 'R'` ở một ngày đã qua nghĩa là *"chạy dở rồi chết"*, không phải *"đang chạy"*. Đây là điểm cần cải thiện, không phải hành vi đúng.

---

## S1 — Thiếu snapshot nguồn (guard chặn Gold)

### Triệu chứng

Gold job chết với:

```text
RuntimeError: Thiếu snapshot nguồn cho cob_dt=2026-09-17: lakehouse.silver.fact_txn_account.
Upstream chưa chạy hoặc partition đã bị xoá — dừng job thay vì ghi Gold bằng dữ liệu rỗng/toàn 0.
```

**Đây là guard hoạt động đúng, không phải lỗi của guard.** Xem [`adr/0005`](adr/0005-fail-loud-before-overwrite.md) để hiểu nó ngăn chuyện gì.

### Chẩn đoán

Phân biệt ba nguyên nhân khác nhau — cách xử lý hoàn toàn khác nhau:

```sql
-- (a) Upstream có chạy không?
SELECT job_name, status, start_time, end_time
FROM opslakehouse.flag_job_etl
WHERE cob_dt = DATE '<cob_dt>' AND schema_name = 'silver';
```

```sql
-- (b) Partition có tồn tại thật không? (chạy qua Trino)
SELECT cob_dt, count(*) FROM iceberg.silver.fact_txn_account
WHERE cob_dt = DATE '<cob_dt>' GROUP BY cob_dt;
```

| Kết quả | Nguyên nhân | Sang bước |
|---|---|---|
| Không có dòng nào trong `flag_job_etl` | Upstream **chưa chạy** | Xử lý (a) |
| `status = 'R'`, ngày đã qua | Upstream **chết giữa chừng** | Xử lý (b) |
| `status = 'S'` nhưng partition rỗng | Upstream **báo xong mà không ghi gì** — nghiêm trọng nhất | Xử lý (c) |
| Partition có dữ liệu, guard vẫn chặn | **Khai báo thừa** | Xử lý (d) |

### Xử lý

**(a) Upstream chưa chạy** — chạy DAG Silver cho đúng `cob_dt`, rồi chạy lại Gold. Không cần can thiệp gì thêm.

**(b) Upstream chết giữa chừng** — tìm nguyên nhân gốc trong log Airflow trước. Chạy lại Silver: `overwritePartitions` theo `cob_dt` nên idempotent, chạy lại an toàn. Xoá dòng `R` cũ trong `flag_job_etl` nếu nó chặn sensor.

**(c) `status = 'S'` nhưng partition rỗng** — **dừng lại, đừng chạy lại ngay.** Đây là dấu hiệu job báo thành công mà không ghi gì, tức là có một đường thoát lỗi âm thầm ở đâu đó. Chạy lại có thể che mất bằng chứng. Ghi lại log của lần chạy đó trước, rồi mới xử lý. Xem TD-5 trong [`technical-debt.md`](technical-debt.md) — mẫu "báo thành công mà không làm gì" đã xuất hiện 7 lần.

**(d) Khai báo thừa** — bảng bị liệt kê trong `validation.require_snapshots` mà SQL không hề đọc. Đã xảy ra với `customer_360.yml`. Kiểm tra bằng:

```bash
py -3 -m pytest tests/governance/test_declared_sources_match_sql.py -q
```

Nếu test này đỏ thì đây chính là nguyên nhân — sửa khai báo, đừng nới guard.

### Xác minh đã khỏi

```sql
-- Partition Gold tồn tại VÀ không phải toàn số 0
SELECT cob_dt, count(*) AS rows,
       count_if(total_accounts = 0 AND total_cards = 0 AND aum_total = 0) AS all_zero_rows
FROM iceberg.gold.mart_customer_360
WHERE cob_dt = DATE '<cob_dt>' GROUP BY cob_dt;
```

`all_zero_rows` xấp xỉ bằng `rows` nghĩa là bạn vừa tái tạo đúng kịch bản mà guard tồn tại để chặn. Số dòng đúng **không đủ** để kết luận đã khỏi.

---

## S2 — Gold không sinh dòng nào

### Triệu chứng

```text
RuntimeError: Gold job 'mart_customer_360' không sinh dòng nào cho cob_dt=2026-09-17.
overwritePartitions() với DataFrame rỗng là no-op và sẽ để lại partition cũ mà không ai biết.
```

### Chẩn đoán

Câu hỏi duy nhất: **rỗng có hợp lệ không?**

Một số model rỗng là bình thường — ví dụ `aml_monitoring` vào ngày không có cảnh báo nào. Model khác thì rỗng luôn là bất thường: `mart_customer_360` rỗng nghĩa là không có khách hàng nào, điều không thể xảy ra nếu `dim_customer` có dữ liệu.

```sql
-- Bảng neo có dữ liệu không?
SELECT count(*) FROM iceberg.silver.dim_customer WHERE is_current = 1;
```

`dim_customer` có dữ liệu mà Gold ra rỗng → lỗi nằm trong mệnh đề `WHERE` hoặc điều kiện JOIN của SQL, không phải ở dữ liệu.

### Xử lý

Nếu rỗng **hợp lệ** cho model đó: gỡ `validation.require_non_empty` khỏi YAML và **ghi lý do vào commit message**. Đừng gỡ chỉ để job xanh.

Nếu rỗng **bất thường**: sửa SQL. Nghi ngờ trước tiên ở biểu thức business date — xem [`adr/0004`](adr/0004-business-date-under-utc-session.md). Một `CAST(ts AS DATE)` trần dưới session sai múi giờ sẽ lọc rỗng ở vùng biên ngày.

### Xác minh đã khỏi

Chạy lại và so số dòng với ngày liền trước. Chênh lệch đột ngột (ví dụ 90% ít hơn) nghĩa là chưa khỏi hẳn.

---

## S3 — Data Quality fail → quarantine

### Triệu chứng

`ops_data_quality_dag` đỏ, hoặc có dòng trong `quarantine_log`.

### Chẩn đoán

```sql
SELECT check_name, table_name, expected_value, actual_value, details
FROM opslakehouse.data_quality_log
WHERE cob_dt = DATE '<cob_dt>' AND check_status = 'FAIL';
```

Phân biệt hai loại:

| Loại | Ví dụ | Hướng xử lý |
|---|---|---|
| **Dữ liệu nguồn sai** | balance âm ở tài khoản tiết kiệm | Cách ly bản ghi, để pipeline chạy tiếp, báo lại nguồn |
| **Logic pipeline sai** | FK integrity fail hàng loạt | **Dừng**, sửa transform, chạy lại |

Dấu hiệu phân biệt: sai nguồn thường ảnh hưởng **một tỷ lệ nhỏ và ổn định** qua các ngày. Sai logic thường **đột ngột và diện rộng**.

### Xử lý

Bản ghi vi phạm đã được `code_etl/shared/ops/quarantine.py` ghi vào `opslakehouse.quarantine_log` theo `quarantine_rules.yml`. Pipeline chạy tiếp với phần còn lại.

**Không** xoá dòng khỏi `quarantine_log` để làm sạch dashboard. Đó là audit trail.

### Xác minh đã khỏi

Số bản ghi cách ly ngày hôm sau phải quay về mức nền. Nếu vẫn cao, nguyên nhân gốc chưa được xử lý — mới chỉ cách ly triệu chứng.

---

## S4 — Schema drift

### Triệu chứng

`ops_schema_drift_dag` báo cột thêm / mất / đổi kiểu.

### Chẩn đoán

Đây là kịch bản nguy hiểm nhất trong tài liệu này, vì **nó thường không làm gì đỏ cả**. Job chạy xong, dashboard vẫn lên, chỉ có ý nghĩa của số liệu bắt đầu sai.

Phân loại thay đổi — `governance/schema_drift.py` trả về ba nhóm:

| Nhóm | Mức độ | Vì sao |
|---|---|---|
| `added_columns` | thường an toàn | Model downstream dùng `SELECT` tường minh sẽ bỏ qua. **Trừ khi** có model dùng `SELECT *` |
| `removed_columns` | **breaking** | Model downstream tham chiếu cột đó sẽ fail, hoặc tệ hơn là join lệch |
| `type_changes` | **breaking** | `int → string` hoặc `timestamp → text` gây cast sai âm thầm |

Câu hỏi kiểm tra thêm — lấy từ checklist trong [`BOOTCAMP_CURRICULUM_ANALYSIS.md`](BOOTCAMP_CURRICULUM_ANALYSIS.md) §4.2:

```text
□ count(*) vẫn ổn nhưng null_rate của field chính có tăng bất thường không?
□ Model downstream nào đang dùng SELECT * ?
```

Model serving **cố ý** dùng `SELECT *` — xem [`adr/0003`](adr/0003-serving-as-table-not-view.md). Nghĩa là cột thêm vào Gold sẽ tự xuất hiện ở serving mà không ai review.

### Xử lý

`added_columns` → cập nhật contract trong `governance/datasets/`, cho đi tiếp.

`removed_columns` hoặc `type_changes` → **dừng publish Gold**, xử lý với nguồn trước.

> ⚠️ **Giới hạn hiện tại**: `ops_schema_drift_dag` chạy 09:00 **sau** `ops_data_quality_dag`, như một safety check hậu kiểm. Nó **phát hiện nhưng không chặn**. Việc dừng publish hiện là thao tác **thủ công**. Và nó chỉ phủ 3 bảng: `dim_customer`, `dim_account`, `dim_loan`.
>
> Thêm severity và quyền chặn là mục 2.2 trong [`ROADMAP.md`](ROADMAP.md).

### Xác minh đã khỏi

Không đủ nếu chỉ thấy DAG xanh. Phải so sánh một metric nghiệp vụ trước và sau — ví dụ tổng `aum_total` — và giải thích được mọi chênh lệch.

---

## S5 — CDC lag hoặc bất nhất

### Triệu chứng

Silver Current cũ hơn nguồn; `reconcile_cdc.py` exit non-zero.

### Chẩn đoán

```bash
docker compose exec -w /opt/project spark-worker-1 \
  spark-submit code_etl/cdc/reconcile_cdc.py --mode spark
```

Script này **read-only và cố ý exit non-zero khi invariant bị vi phạm**, nên dùng được làm gate trong Airflow. Nó đối chiếu ba tầng: bảng nguồn Postgres → topic Kafka → bảng `lakehouse.bronze.core_*_cdc`.

Tham số duy nhất là `--mode` (`spark` | `local`) — script **không** nhận `--cob_dt`, vì CDC là luồng liên tục chứ không theo partition ngày như batch.

Kiểm tra watermark:

```sql
SELECT * FROM lakehouse.meta.cdc_watermark;
```

Thứ tự sự kiện dùng `__cdc_timestamp_ms` + `__spark_batch_id`, **không dùng Kafka offset**. Watermark đứng yên nghĩa là consolidation không tiến, dù streaming job vẫn có thể đang chạy.

### Xử lý

| Điểm tắc | Cách xác định |
|---|---|
| Connector chết | Debezium connector status |
| Streaming job chết | Bronze CDC không có dòng mới |
| Consolidation tắc | Bronze CDC tăng nhưng watermark đứng |

Bronze CDC là **append-only**, nên chạy lại consolidation an toàn.

### Xác minh đã khỏi

Chạy lại `reconcile_cdc.py` và yêu cầu **exit code 0**. Đọc đúng mã thoát:

```bash
spark-submit code_etl/cdc/reconcile_cdc.py --mode spark > .recon.log 2>&1
rc=$?
echo "rc=$rc"
```

**Không** nối `| tail` rồi đọc `$?` — bạn sẽ nhận mã của `tail`. Đây chính là mẫu lỗi mà TD-5 ghi nhận 7 lần.

---

## S6 — Serving lệch snapshot

### Triệu chứng

`assert_serving_snapshot_alignment` fail, hoặc dashboard hiện số của ngày cũ.

### Chẩn đoán

Tầng serving là **table**, không phải view — xem [`adr/0003`](adr/0003-serving-as-table-not-view.md). Nó chỉ mới bằng lần `dbt build` gần nhất.

```sql
SELECT max(cob_dt) AS gold_max FROM iceberg.gold.mart_customer_360;
SELECT max(cob_dt) AS serving_max FROM iceberg.serving.mart_customer_360_current;
```

Hai số khác nhau → dbt chưa chạy cho ngày mới.

Serving **rỗng** → nhiều khả năng thiếu var `cob_dt`: model dùng sentinel `1900-01-01`, cho ra 0 dòng thay vì lỗi parse.

### Xử lý

```bash
dbt build --select serving --vars '{"cob_dt": "<cob_dt>"}'
```

### Xác minh đã khỏi

`serving_max` khớp `gold_max`, **và** `assert_serving_snapshot_alignment` pass.

---

## S7 — Backfill sai ngày

### Triệu chứng

Chạy lại với `cob_dt` sai, ghi đè dữ liệu tốt.

### Chẩn đoán

`overwritePartitions()` chỉ đụng partition của `cob_dt` được truyền. Thiệt hại giới hạn trong đúng ngày đó — nhưng dữ liệu cũ của ngày đó **đã bị thay thế**.

### Xử lý

Iceberg có time travel. Xem lịch sử snapshot:

```sql
SELECT * FROM iceberg.gold."mart_customer_360$snapshots" ORDER BY committed_at DESC;
```

Đọc lại trạng thái trước khi ghi đè bằng `VERSION AS OF <snapshot_id>`, rồi ghi lại partition từ nguồn.

> ⚠️ **Cửa sổ khôi phục là 7 ngày.** `expire_snapshots` trong `code_etl/shared/ops/iceberg_maintenance.py` chạy với `retain_days=7, min_snapshots=3`, được `ops_maintenance_weekly_dag` gọi lúc **03:00 Chủ nhật** (`0 3 * * 0`).
>
> Nghĩa là: snapshot cũ hơn 7 ngày sẽ bị xoá ở lần dọn kế tiếp, nhưng `min_snapshots=3` bảo đảm luôn còn ít nhất 3 snapshot gần nhất dù chúng bao lâu. Sự cố phát hiện trong vòng 7 ngày thì chắc chắn khôi phục được; muộn hơn thì phải kiểm tra `$snapshots` xem còn không.

### Xác minh đã khỏi

So số dòng và một metric tổng (ví dụ `sum(aum_total)`) với ngày liền kề. Giá trị phải liên tục, không có bậc nhảy.

---

## S8 — `Catalog 'lakehouse' not found`

### Triệu chứng

Code kết nối Trino fail với `Catalog 'lakehouse' not found`. Không crash lúc import — chỉ nổ khi query chạy thật.

### Chẩn đoán

Spark dùng `lakehouse`, Trino dùng `iceberg`. Xem [`adr/0002`](adr/0002-cross-engine-catalog-naming.md). Đã xảy ra **4 lần**.

```bash
py -3 -m pytest tests/governance/test_trino_catalog_contract.py -q
```

### Xử lý

Đổi `lakehouse` → `iceberg` trong code Trino-facing. Nếu test pass mà lỗi vẫn xảy ra, file đó nằm ngoài `search_dirs` của test — **mở rộng danh sách**, đừng chỉ sửa file.

### Xác minh đã khỏi

Chạy query thật qua Trino, không chỉ import module.

---

## Sau sự cố: RCA

Mỗi sự cố cấp S1–S8 nên để lại một ghi chép ngắn trả lời bốn câu:

```text
1. Cái gì hỏng, và AI phát hiện ra — máy hay người?
   Nếu là người, đó là khoảng trống quan trọng hơn bản thân sự cố.

2. Vì sao nó lọt qua được các gate đang có?

3. Invariant nào lẽ ra phải bắt được? Nó thiếu, hay có mà quá lỏng?

4. Sửa gì để lần sau máy bắt được thay vì người?
```

Câu 3 là câu quan trọng nhất, và câu trả lời **không bao giờ** là nới một gate hiện có. Nếu một gate đỏ vì lý do chính đáng, sửa nguyên nhân. Nếu nó đỏ vì lý do sai, sửa gate cho đúng — đừng tắt nó.

Sự cố lặp lại từ 3 lần trở lên thì ghi thành mục có tên trong [`technical-debt.md`](technical-debt.md), như TD-5 (7 lần) và TD-7 (4 lần) đang làm.

---

## Khoảng trống đã biết của chính tài liệu này

Ghi ra để không ai tưởng là đã phủ hết:

- **Chưa có ngưỡng cảnh báo**: không có định nghĩa "CDC lag bao nhiêu thì báo động". Cần `SLA_AND_FRESHNESS.md` — [`DOCUMENTATION_PLAN.md`](DOCUMENTATION_PLAN.md) §3 nhóm C.
- **Chưa có đường leo thang**: dự án một người nên chưa cần, nhưng đây là thứ đầu tiên phải thêm khi có người thứ hai.
- **Chưa diễn tập**: các quy trình trên viết từ đọc code và thông điệp lỗi thật, **chưa được diễn tập trên sự cố thật**. Một runbook chưa chạy thử là một giả thuyết.
- **`flag_job_etl` không có trạng thái Failed** — xem ghi chú ở đầu tài liệu.
