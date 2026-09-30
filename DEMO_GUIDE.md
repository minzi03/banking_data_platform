# Demo Guide — Banking Data Platform (tài liệu demo duy nhất)

Tài liệu này là **nguồn demo duy nhất** của repo (các bản demo cũ đã xoá 2026-09-30, TD-20).

> **Trạng thái kiểm chứng (2026-09-30).** Đã chạy trên stack Docker local theo
> **đường nâng cấp** (stack có sẵn dữ liệu, `cob_dt` 2026-09-22 và 2026-09-23):
> image Airflow mới, `bronze-partition-migrate`, chuỗi Bronze → Silver → Gold → dbt
> qua Airflow, SCD2 ngày thứ hai, CDC (insert / update / delete / cùng transaction /
> DLQ / replay / restart), API trên Trino, contract / DQ / drift / PII / quarantine /
> maintenance, `dbt build` PASS=154, `mf`. Kết quả đo được ghi ngay ở từng bước.
> Không cần stack: unit test, Spark regression trong image worker (PySpark 3.5.3).
> **Dựng từ đầu (clean start)** do job CI *Trino Integration* kiểm trên stack mới;
> `down -v` trên máy local không chạy vì xoá dữ liệu. Chỗ còn ⚠️ là chưa chạy.

Mỗi bước gồm: **What** (thành phần / hành vi được cho xem) · **Why** (ý nghĩa
kiến trúc) · **Command** (lệnh cần chạy) · **Expected** (kết quả mong đợi) ·
**Verify** (cách xác nhận) · **Talking points** (ý cần nói khi demo / phỏng vấn).

---

## 0. Chuẩn bị shell

Mọi lệnh chạy bằng **bash** (Linux/macOS, hoặc Git Bash / WSL trên Windows),
đứng ở thư mục gốc repo. Khai báo các helper sau một lần cho cả buổi demo:

```bash
export COB=2026-09-29        # cob_dt của lần demo (ngày làm việc đã kết thúc)
export COB2=2026-09-30       # ngày kế tiếp, dùng cho bước SCD2

# Trino chỉ nhận HTTPS + mật khẩu (ADR-0016). User `trino` lấy mật khẩu từ env
# TRINO_PASSWORD có sẵn trong container (secrets/trino/env/trino-server.env).
TQ() { docker exec -i banking-trino trino --server https://localhost:8443 \
        --truststore-path /etc/trino/secrets/trino.pem --user trino --password \
        --catalog iceberg --output-format ALIGNED --execute "$1"; }

# psql trong container nguồn, dùng credential của chính container
PQ() { docker exec -i banking-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' <<< "$1"; }

# Airflow CLI
AF() { docker exec banking-airflow-scheduler airflow "$@"; }

# Trigger một DAG cho đúng cob_dt (mọi DAG đọc dag_run.conf["cob_dt"], xem airflow/plugins/cob_dt.py)
TRIG() { AF dags unpause "$1" >/dev/null; AF dags trigger "$1" --conf "{\"cob_dt\": \"$2\"}"; }
# Lần nạp ĐẦU trên stack mới: ba bảng giao dịch nạp theo ngày nghiệp vụ (ADR-0018), nên
# phải nạp cả lịch sử một lần — thêm backfill_from (job full_snapshot bỏ qua khoá này).
TRIG_FIRST() { AF dags unpause "$1" >/dev/null; AF dags trigger "$1" --conf "{\"cob_dt\": \"$2\", \"backfill_from\": \"1900-01-01\"}"; }

# Trạng thái các lần chạy gần nhất của một DAG
RUNS() { AF dags list-runs -d "$1" -o plain | head -5; }
```

---

## 1. Architecture

**What.** Năm mặt phẳng: nguồn PostgreSQL → di chuyển dữ liệu (Spark JDBC; Debezium → Kafka → Spark Streaming)
→ lakehouse Bronze/Silver/Gold trên Iceberg + MinIO → serving (dbt qua Trino) → điều khiển
(Airflow, contract, DQ, lineage, CI, evidence manifest). Xem hình `docs/images/banking_data_platform_architecture_4.png`.

**Why.** Tách "ai sở hữu lịch sử" (Spark, Gold phân vùng theo `cob_dt`) khỏi "ai phục vụ bản hiện hành" (dbt/Trino, `iceberg.serving.*`).

**Command.** Mở `README.md` §Architecture và `ARCHITECTURE.md`.

**Expected / Verify.** Sơ đồ khớp với thư mục: `code_etl/{bronze,silver,gold,cdc}`, `dbt/models/serving`, `airflow/dags`.

**Talking points.**
- Đề bài: Customer 360 + cross-sell, `mart_customer_360` 25+ KPI, 1 dòng/khách, truy vấn qua Trino, lưu lịch sử customer/account/product/branch, Medallion trên Iceberg + MinIO, Spark batch, Trino, Airflow.
- CDC, governance, observability là phần mở rộng ngoài đề — nói rõ như vậy.
- Spark catalog tên `lakehouse`, Trino catalog tên `iceberg`: cùng dữ liệu, hai tên theo engine.

---

## 2. Environment

**What.** Dựng 29 service (25 chạy lâu + 4 job khởi tạo một lần).

**Why.** Tái lập được từ một bản clone sạch; mọi bước chuẩn bị ẩn trước đây giờ nằm trong `make up`.

**Command.**
```bash
cp docker/.env.example docker/.env
# Sửa mọi giá trị CHANGE_ME trong docker/.env. Linux: đặt DOCKER_GID = $(stat -c %g /var/run/docker.sock)
make up                      # = kiểm tra docker/.env → sinh secret Trino (scripts/bootstrap_trino_auth.py) → compose up
# Windows không có python3: make PYTHON="py -3" up
```

**Expected.** `docker compose ps` hiện 25 service `running`/`healthy`; `mc`, `iceberg-init`, `airflow-init`, `om-migrate` `exited (0)`.

**Verify.**
```bash
docker compose -f docker/docker-compose.yml ps
docker logs banking-iceberg-init | tail -20      # "All DDL scripts executed successfully!"
TQ "SHOW SCHEMAS"                                  # bronze, silver, gold, serving, sandbox, quarantine, meta …
TQ "SHOW TABLES FROM silver"                       # có dim_customer_current, dim_account_current
```

**Talking points.**
- `iceberg-init` chạy `init_all.sh` với `set -euo pipefail`: DDL hỏng thì container hỏng (bản cũ nuốt exit code qua `| tail`).
- `06_ddl_silver_cdc_current.sql` giờ được chạy tự động; trước đây stack mới thiếu bảng Silver Current.
- Airflow gửi mọi job Spark/dbt bằng `docker exec` vào container có Iceberg jar; image Airflow có docker CLI, không có Spark.

---

## 3. Source

**What.** Sinh dữ liệu ngân hàng giả lập vào PostgreSQL (core_banking, card_crm, digital_banking).

**Why.** `--as-of` neo giao dịch mới nhất vào đúng `cob_dt`; lệch ngày thì KPI 30 ngày về 0 (TD-16).

**Command.**
```bash
make seed AS_OF=$COB
```

**Expected.** Log kết thúc "Seed data generated successfully". Quy mô mặc định: 10.000 khách, 30.000 tài khoản, 1,2M giao dịch tài khoản, 600K giao dịch thẻ, 500K giao dịch online.

**Verify.**
```bash
PQ "SELECT (SELECT COUNT(*) FROM core_banking.customer) customers,
           (SELECT COUNT(*) FROM core_banking.account)  accounts,
           (SELECT COUNT(*) FROM core_banking.txn_account) txns,
           (SELECT MAX(txn_date)::date FROM core_banking.txn_account) newest_txn;"
```
`newest_txn` = `$COB`.

**Talking points.** PostgreSQL là source of truth; không layer nào ghi ngược vào nguồn.

---

## 4. Batch (Airflow)

**What.** Chuỗi DAG hằng ngày: 3 Bronze (02:00) → `silver_all_dag` (04:00) → `gold_all_dag` (06:00) → `dbt_serving_publish` (07:00) → ops (08:00–09:00).

**Why.** DAG nối nhau bằng **cờ dữ liệu** trong `opslakehouse.flag_job_etl` cho **cùng một cob_dt**, không bằng tên DAG.

**Command.**
```bash
AF dags list-import-errors                 # mong đợi: No data found
AF variables set pii_hash_salt "$(openssl rand -hex 32)"   # một lần; PII masking cần salt
# Lần đầu trên stack mới (Bronze/Silver nạp lịch sử giao dịch vào partition từng ngày):
for d in bronze_core_banking_dag bronze_card_crm_dag bronze_digital_banking_dag \
         silver_all_dag gold_all_dag dbt_serving_publish; do TRIG_FIRST $d $COB; done
# Các ngày sau: TRIG $d <ngày> — chỉ nạp giao dịch của đúng ngày đó.
```
Silver chờ đủ 3 cờ Bronze, Gold chờ Silver, dbt chờ `GOLD_COMPLETE` — lần đầu chạy một `cob_dt` thì trigger cùng lúc là đúng. Sensor coi upstream xong khi **dòng cờ mới nhất** của (job, `cob_dt`) là `S` (`etl_flag.upstream_success_sql`); Gold/dbt ghi `R` cho `GOLD_COMPLETE`/`SERVING_COMPLETE` ngay khi bắt đầu. **Chạy lại một `cob_dt` đã có cờ `S`:** trigger upstream trước (vài giây) rồi mới tới downstream, để `R` của lượt mới kịp che `S` cũ.

> **Lưu ý khi unpause.** Unpause một DAG có lịch tạo **ngay** một lượt chạy theo lịch cho khoảng gần nhất, tức `cob_dt` = hôm qua. Nếu `$COB` cũng là hôm qua, DAG có hai lượt cho cùng ngày; `max_active_runs=1` cho Bronze/Silver/Gold nên hai lượt chạy nối tiếp, kết quả như nhau (idempotent) nhưng tốn gấp đôi thời gian. Với các DAG ops (DQ, contract, PII, drift), lượt theo lịch của một ngày chưa có dữ liệu sẽ chờ cờ tới hết timeout. Muốn tránh: `AF dags list-runs -d <dag>` sau khi unpause và đánh dấu lượt `scheduled__…` là failed trên UI.

**Expected.** Cả 6 DAG `success`. Ước lượng trên máy 16 GB: Bronze ~10 phút, Silver ~5 phút, Gold ~6 phút, dbt ~2 phút.

**Verify.**
```bash
for d in bronze_core_banking_dag silver_all_dag gold_all_dag dbt_serving_publish; do RUNS $d; done
PQ "SELECT job_name, status, cob_dt FROM opslakehouse.flag_job_etl
    WHERE cob_dt = DATE '$COB' AND status = 'S' ORDER BY end_time;"
```
Có dòng `S` cho 3 Bronze, `silver_all_dag`, `gold_all_dag`, `GOLD_COMPLETE`, `SERVING_COMPLETE`.

**Talking points.**
- `cob_dt` = ngày ICT của `data_interval_start`, hoặc `dag_run.conf["cob_dt"]` (đường `conf` đã chạy trên stack; lượt theo lịch mới quan sát khi unpause, xem lưu ý trên). Bản cũ dùng `{{ ds }}` (ngày UTC): DAG chạy trước 07:00 nhận D-2, DAG chạy sau nhận D-1, nên dbt chờ cờ Gold của một ngày chưa chạy. Đã render bằng Airflow 2.10.0 thật: 13/13 DAG có lịch hằng ngày cùng ra một ngày.
- Chạy không cần Airflow (một cob_dt): `make bronze-bootstrap COB_DT=$COB`, rồi hai lệnh `--in-process` trong RUNBOOK §3.

---

## 5. Bronze

**What.** 22 bảng full snapshot mỗi `cob_dt`, **mọi bảng partition theo `cob_dt`**.

**Why.** `overwritePartitions()` chỉ an toàn trên bảng có partition: ghi đè đúng ngày của nó, chạy lại cùng ngày cho cùng kết quả. Trên bảng không partition, nó thay **toàn bộ** bảng.

**Command.**
```bash
TQ "SELECT cob_dt, COUNT(*) FROM bronze.core_customer GROUP BY cob_dt ORDER BY cob_dt"
TQ "SELECT partition, record_count FROM bronze.\"core_branch\$partitions\""
TQ "SELECT COUNT(*) rows_, COUNT(DISTINCT customer_id) keys_ FROM bronze.core_customer WHERE cob_dt = DATE '$COB'"
```

**Expected.** Mỗi `cob_dt` đã nạp là một partition. `rows_ = keys_` = 10.000 (không trùng key trong một snapshot).

**Verify.** `core_branch$partitions` có cột `partition` chứa `cob_dt`.

**Talking points.**
- 18/22 bảng Bronze từng không partition: mỗi lần nạp xoá snapshot cũ, nên lịch sử product/branch mất hẳn. DDL đã sửa; stack cũ chạy `make bronze-partition-migrate` một lần (partition evolution + rewrite). Đo 2026-09-30: 13 bảng được migrate, số dòng trước/sau giống hệt trên 23 bảng, chạy lại báo `ok` cả 22.
- `write_to_iceberg()` từ chối ghi snapshot vào bảng chưa partition theo `cob_dt` (fail-loud, không âm thầm xoá lịch sử).

---

## 6. Silver

**What.** 10 dim (SCD2: customer, account, product, branch; SCD1: card, employee, device, location, deposit, loan) và 6 fact.

**Command.**
```bash
TQ "SELECT COUNT(*) total, SUM(is_current) current_rows, COUNT(DISTINCT customer_id) keys_
    FROM silver.dim_customer"
TQ "SELECT customer_id, COUNT(*) FROM silver.dim_customer WHERE is_current = 1
    GROUP BY 1 HAVING COUNT(*) > 1"                                    -- mong đợi 0 dòng
TQ "SELECT cob_dt, COUNT(*) rows_, COUNT(DISTINCT txn_id) txns FROM silver.fact_txn_account
    WHERE cob_dt = DATE '$COB' GROUP BY 1"
TQ "SELECT COUNT(*) FROM silver.fact_txn_account f
    WHERE cob_dt = DATE '$COB' AND f.customer_sk IS NULL"               -- FK tới dim_customer
```

**Expected.** `current_rows = keys_`; không có key nào có hai version hiện hành; fact `rows_ = txns` = 1.200.000; `customer_sk` NULL = 0.

**Talking points.** Fact là full snapshot mỗi `cob_dt` (nên đếm giao dịch phải giới hạn trong một snapshot). Fact gắn SK của version dim **hiệu lực tại cob_dt**, không phải `is_current`.

---

## 7. Data Modeling — SCD1 / SCD2 thật

**What.** Tạo một thay đổi thật ở nguồn, nạp ngày kế tiếp, xem lịch sử.

**Why.** Đề bài: lưu lịch sử customer/account/**product/branch**.

**Command.**
```bash
PQ "UPDATE core_banking.customer SET customer_segment = 'VIP', last_updated = NOW() WHERE customer_id = 1;
    UPDATE core_banking.branch SET manager_name = 'Tran Demo', last_updated = NOW()
      WHERE branch_code = (SELECT MIN(branch_code) FROM core_banking.branch);
    UPDATE core_banking.customer SET full_name = full_name || ' Demo', last_updated = NOW() WHERE customer_id = 2;"
for d in bronze_core_banking_dag bronze_card_crm_dag bronze_digital_banking_dag silver_all_dag; do TRIG $d $COB2; done
```

**Expected.** (đo 2026-09-30, `$COB`=2026-09-22, `$COB2`=2026-09-23)
- customer 1 (đổi `customer_segment` — tracked): thêm 1 version. Version cũ `effective_to = $COB`, `is_current = 0`; version mới `effective_from = $COB2`.
- branch nhỏ nhất (đổi `manager_name`): 2 version, như trên.
- customer 2 (đổi `full_name` — cột Type 1): không thêm version; `full_name` cập nhật tại chỗ trên version **hiện hành** (version lịch sử giữ giá trị cũ).
- Không key nào có hai dòng `is_current = 1`. Gold chạy `$COB` thấy segment cũ, chạy `$COB2` thấy segment mới (join dim **as-of** `cob_dt`).

**Verify.**
```bash
TQ "SELECT customer_id, customer_sk, customer_segment, effective_from, effective_to, is_current
    FROM silver.dim_customer WHERE customer_id IN (1, 2) ORDER BY customer_id, effective_from"
TQ "SELECT branch_code, branch_sk, manager_name, effective_from, effective_to, is_current
    FROM silver.dim_branch WHERE branch_code = (SELECT MIN(branch_code) FROM silver.dim_branch)
    ORDER BY effective_from"
```

**Talking points.**
- SK = sha256(business key | cob_dt), duy nhất theo version.
- Cleanup idempotent (chạy lại cùng ngày cho cùng kết quả).
- Chặn backfill ngày cũ hơn version mới nhất.
- Key biến mất khỏi full snapshot được đóng version (`close_missing_keys`).
- Gold chạy lại `$COB` vẫn thấy segment cũ vì chọn dim **as-of cob_dt**.

---

## 8. Gold

**What.** 15 bảng Gold phân vùng theo `cob_dt`: 6 mart360, 4 segmentation, 1 time analytics, 4 risk.

**Command.**
```bash
TQ "SELECT COUNT(*) rows_, COUNT(DISTINCT customer_id) customers FROM gold.mart_customer_360 WHERE cob_dt = DATE '$COB'"
TQ "SELECT rfm_segment, COUNT(*) n, ROUND(AVG(recency_days),1) avg_recency, ROUND(AVG(frequency),1) avg_freq,
           ROUND(AVG(monetary),0) avg_monetary
    FROM gold.rfm_segment WHERE cob_dt = DATE '$COB' GROUP BY 1 ORDER BY avg_monetary DESC"
TQ "SELECT campaign_type, COUNT(*) FROM gold.campaign_target WHERE cob_dt = DATE '$COB' GROUP BY 1"
```

**Expected.** `rows_ = customers` = 10.000. Champions có recency **thấp nhất**, frequency và monetary **cao nhất**; Hibernating ngược lại. Đo 2026-09-22: Champions 1.703 khách, recency 0,4 ngày, 67,9 giao dịch, 683 triệu; Hibernating 831 khách, 5,6 giao dịch.

**Talking points.**
- Chiều điểm RFM từng bị đảo (kế thừa từ template khoá học): `NTILE` gán 1 cho khách tốt nhất, nên "Champions" là khách tệ nhất và campaign Upsell nhắm sai người. Test `tests/gold/test_rfm_scoring_direction.py` fail trên SQL cũ và pass trên SQL mới.
- Guard fail-loud trước khi ghi: thiếu snapshot nguồn thì dừng thay vì ghi số 0.

---

## 9. CDC end-to-end (đã chạy trên stack 2026-09-30)

**What.** PostgreSQL WAL → Debezium → Kafka → Spark Structured Streaming → Bronze CDC → consolidation → Silver Current.

**Why.** Gần real-time mà không quét lại toàn bảng nguồn; Bronze CDC là lịch sử append-only, Silver Current là trạng thái mới nhất.

**Command.**
```bash
TRIG cdc_register_connectors $COB            # 3 connector Debezium
TRIG cdc_streaming_pipeline $COB              # 6 streaming query (chạy nền trong spark-worker-1)
TRIG cdc_consolidation_pipeline $COB          # lượt đầu: đọc toàn bộ Bronze CDC

# 9a — trước khi đổi
PQ "SELECT customer_id, email FROM core_banking.customer WHERE customer_id = 3"
TQ "SELECT customer_id, email, date_of_birth, __cdc_operation FROM silver.dim_customer_current WHERE customer_id = 3"

# 9b — đổi nguồn (UPDATE), rồi đợi ~60 s cho micro-batch 30 s
PQ "UPDATE core_banking.customer SET email = 'cdc.demo@example.com' WHERE customer_id = 3"
docker exec banking-kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic postgresql.banking.core_banking.customer --from-beginning --timeout-ms 15000 \
  | grep -m1 'cdc.demo@example.com'
TQ "SELECT customer_id, email, __cdc_operation, __cdc_timestamp, __spark_batch_id
    FROM bronze.core_customer_cdc WHERE customer_id = 3 ORDER BY __cdc_timestamp DESC LIMIT 3"

# 9c — gộp vào Silver Current (hoặc đợi lịch */10 phút)
TRIG cdc_consolidation_pipeline $COB
TQ "SELECT customer_id, email, date_of_birth, __cdc_operation, __consolidated_at
    FROM silver.dim_customer_current WHERE customer_id = 3"
TQ "SELECT * FROM meta.cdc_watermark"
```

**Expected.**
- 9b: Kafka có event chứa email mới. Bronze CDC có dòng `UPDATE` mới nhất.
- 9c: Silver Current có email mới **và `date_of_birth` khác NULL**. `cdc_watermark.last_snapshot_id` = snapshot mới nhất của `bronze.core_customer_cdc`.

**Các kịch bản khác.**

| Kịch bản | Lệnh | Mong đợi |
|---|---|---|
| INSERT | `PQ "INSERT INTO core_banking.customer (customer_id, full_name, gender, date_of_birth, email, customer_segment, kyc_status, register_date, is_active) VALUES (990001, 'CDC Demo', 'M', DATE '1990-01-01', 'insert@example.com', 'RETAIL', 'VERIFIED', CURRENT_DATE, 1)"` | Sau consolidation: `TQ "SELECT * FROM silver.dim_customer_current WHERE customer_id = 990001"` có 1 dòng, `__cdc_operation = INSERT` |
| DELETE | `PQ "DELETE FROM core_banking.customer WHERE customer_id = 990001"` (khách vừa tạo, không có bảng con tham chiếu) | Sau consolidation: key 990001 biến mất khỏi Silver Current. **DLQ không tăng** (tombstone bị bỏ qua) |
| Idempotent / replay | Chạy lại `TRIG cdc_consolidation_pipeline $COB` khi không có event mới | Log `No new snapshots. Skipping.`; Silver Current không đổi |
| DLQ | `echo '{"payload":{"customer_id":"999999","__op":"x","__ts_ms":"1"}}' \| docker exec -i banking-kafka kafka-console-producer --bootstrap-server kafka:9092 --topic postgresql.banking.core_banking.customer` | `TQ "SELECT error_type, error_message FROM bronze.cdc_dead_letter ORDER BY failed_at DESC LIMIT 3"` → `INVALID_OPERATION`. Streaming không dừng |
| Restart / checkpoint | `TRIG cdc_streaming_stop_all $COB`, rồi `TRIG cdc_streaming_pipeline $COB` | Query tiếp tục từ checkpoint `s3a://lakehouse/checkpoints/cdc/*`, không nhân đôi Bronze |
| Thứ tự trong một transaction | `PQ "BEGIN; INSERT … (990004, … 'v1@example.com' …); UPDATE core_banking.customer SET email = 'v2@example.com' WHERE customer_id = 990004; COMMIT;"` | Bronze CDC: hai dòng **cùng** `__cdc_timestamp_ms` và `__spark_batch_id`, khác `__kafka_offset`. Silver Current: `v2` |
| Cột tiền | `PQ "UPDATE core_banking.account SET balance = balance + 1000.55 WHERE account_id = 1"` | `silver.dim_account_current.balance` = giá trị nguồn (không NULL) |

**Talking points.**
- Tiến độ consolidation = **snapshot Iceberg** của Bronze CDC. Mỗi lượt đọc đúng khoảng (watermark, end], nên event về muộn không bị bỏ sót. Thứ tự event = (`__cdc_timestamp_ms`, `__spark_batch_id`, `__kafka_offset`): INSERT + UPDATE trong một transaction trùng cả ms lẫn micro-batch, chỉ offset phân định (ADR-0010). MERGE không ghi đè trạng thái mới hơn bằng event cũ hơn.
- Bản cũ biến `date_of_birth`, `register_date`, `open_date`, `close_date` thành NULL (so sánh cột số với `""`), còn `balance`/`txn_amount`/`amount` NULL vì Debezium gửi NUMERIC dạng bytes base64 (`decimal.handling.mode=precise`); connector giờ dùng `string`.
- Mỗi query streaming giới hạn `spark.cores.max=1`: không có giới hạn, query đầu chiếm 6/8 core và 4 query còn lại + consolidation `WAITING` mãi trong khi Airflow báo success.
- Không claim exactly-once: replay-safe + MERGE idempotent. Kafka offset chỉ lưu ở DLQ (ADR-0010).

---

## 10. Airflow

**What.** 19 DAG (18 file), 0 lỗi import.

**Command.**
```bash
AF dags list -o plain
AF dags list-import-errors
AF tasks list gold_all_dag --tree | head -30
```

**Expected.** Không có lỗi import. `gold_all_dag`: `dag_start → check_silver_all_dag → phase1 (14 job) → phase2 (campaign_target) → dag_end → gold_complete`.

**Talking points.**
- Spark chạy trong `banking-spark-worker-1` qua `docker exec`; không DAG nào dùng `SparkSubmitOperator` (có test chặn).
- Password JDBC đi qua env (`docker exec -e DB_PASSWORD`), không nằm trong argv.
- `regulatory_reporting` và `dbt_seed` (không hoạt động) đã xoá 2026-09-30: còn 18 file DAG, 19 DAG.
- `ops_ml_churn_dag` chạy tay, cần `ml/requirements.txt` trên worker.

---

## 11. dbt / Trino

**What.** dbt (qua Trino) publish 16 bảng `iceberg.serving.*_current` cho **đúng một cob_dt** + 1 time spine MetricFlow.

**Command.**
```bash
docker exec banking-dbt sh -lc "cd /usr/src/dbt && dbt build --target docker --select serving --vars '{\"cob_dt\": \"$COB\"}'"
TQ "SELECT COUNT(*) rows_, COUNT(DISTINCT customer_id) customers, MIN(cob_dt), MAX(cob_dt)
    FROM serving.mart_customer_360_current"
TQ "SELECT customer_id, full_name_masked, aum_total, rfm_segment, has_credit_card, txn_count_30d
    FROM serving.mart_customer_360_current
    WHERE cross_sell_credit_card_flag = 1 AND rfm_segment IN ('Champions','Loyal Customers')
      AND days_since_last_txn <= 30
    ORDER BY aum_total DESC LIMIT 10"
# Semantic layer: 13 metric định nghĩa một lần (dbt/models/semantic/_semantic_models.yml)
docker exec banking-dbt sh -lc "cd /usr/src/dbt && mf query   --metrics npl_ratio,npl_loan_count,late_payment_rate --group-by metric_time__day"
```

**Expected.** `dbt build` PASS=154 (17 model + 137 test; chạy **không** kèm `--vars` thì 2 test singular FAIL có chủ đích vì `cob_dt` rơi về sentinel 1900-01-01). `mf query` ra một dòng cho `$COB` (đo 2026-09-30: NPL 7,24%, 252 khoản, late payment 2,10%; thiếu `--group-by` thì bảng rỗng). Serving có `rows_ = customers`, `MIN(cob_dt) = MAX(cob_dt) = $COB`. Query cross-sell (use case chính của đề) trả về danh sách khách.

**Talking points.** Serving là `table`, không phải view (Iceberg REST của Trino không hỗ trợ `createView`). `cob_dt` truyền tường minh, không dùng `MAX(cob_dt)`: thiếu snapshot thì build fail thay vì âm thầm phục vụ dữ liệu cũ.

---

## 12. Analytics / API

**What.** FastAPI đọc `iceberg.serving` bằng user Trino `customer_api`.

**Command.**
```bash
curl -s localhost:8000/health
curl -s "localhost:8000/customer/1/overview?cob_dt=$COB" | python -m json.tool
curl -s "localhost:8000/customer/1/risk-score?cob_dt=$COB" | python -m json.tool
curl -s -o /dev/null -w "%{http_code}\n" "localhost:8000/customer/1/overview?cob_dt=2026-09-29'%20OR%20'1'='1"
```

**Expected.** Overview/risk/recommendations trả JSON; request injection và ngày sai trả **422**; khách không tồn tại trả **404** (đã chạy trên Trino 2026-09-30). `recommended_products` không lặp và không có "None".

**Talking points.**
- `cob_dt` là tham số truy vấn có kiểu `date`, không nối vào SQL; hai endpoint join có cột `cob_dt` được qualify.
- Streamlit/Superset: có code và dashboard JSON, nhưng chưa được xác nhận chạy (TD-7) — không demo như tính năng đã kiểm chứng.

---

## 13. Governance

**What.** Data contract (34), DQ (9 loại check), quarantine, PII masking, RBAC/masking của Trino, lineage.

**Command.**
```bash
for d in ops_data_quality_dag ops_contract_validation_dag ops_pii_masking_daily_dag ops_quarantine_dag; do TRIG $d $COB; done
PQ "SELECT table_name, check_name, check_status, COUNT(*) FROM opslakehouse.data_quality_log
    WHERE cob_dt = DATE '$COB' GROUP BY 1,2,3 ORDER BY 3,1 LIMIT 30"
PQ "SELECT dataset_id, check_status, COUNT(*) FROM opslakehouse.contract_validation_log
    WHERE cob_dt = DATE '$COB' GROUP BY 1,2 ORDER BY 2,1 LIMIT 40"
TQ "SELECT customer_id, full_name_masked, phone_masked FROM sandbox.dim_customer_masked LIMIT 5"
```

**Expected.** DQ/contract có log cho `$COB`. Bảng sandbox có tên/điện thoại đã che.

**Talking points.**
- DQ và contract là **phát hiện** (chạy sau khi publish), không chặn. Chặn trước khi ghi chỉ có guard trong Gold job và dbt test.
- DQ, contract và quarantine đọc cùng một phạm vi: một snapshot `cob_dt`, version SCD2 hiện hành.
- OpenMetadata chạy, nhưng catalog chưa được nạp lại (`table_entity = 0`, TD). Lineage ghi vào `opslakehouse.lineage_log` khi chạy `ops_lineage_dag`.

---

## 14. Testing / CI

**Command.**
```bash
pip install pyyaml pydantic pytest pytest-cov jinja2 fastapi==0.115.0 trino==0.327.0 httpx==0.27.2
python -m pytest tests/ -m "not integration" -q                          # unit
pip install 'pyspark==3.5.3'   # cần Java 17
python -m pytest -m integration -q tests/gold/test_gold_fanout_regression.py tests/gold/test_business_date_semantics.py \
  tests/gold/test_risk_mart_semantics.py tests/bronze/test_cdc_bronze_schema.py tests/gold/test_loan_delinquency_mart.py \
  tests/gold/test_schema_guard.py tests/gold/test_rfm_scoring_direction.py tests/silver/test_scd_type2.py \
  tests/cdc/test_cdc_consolidation.py
python scripts/verify_readme_metrics.py
```

**Expected (đo 2026-09-30, ngoài stack).**
- Unit: 1.876 passed, 1 skipped (có pyspark) / 1.876 passed, 6 skipped (môi trường CI không có pyspark).
- Spark regression: 133 passed (9 file trong job `gold-spark-regression`).
- README khớp manifest: 22/22.

**Talking points.** CI job `trino-integration` dựng Trino/Iceberg thật, chạy 34 test integration và một SCD2 transition thật. Nó chạy trên GitHub Actions, không chạy trong đợt sửa này.

---

## 15. Final result

| Yêu cầu đề bài | Cho xem ở bước |
|---|---|
| `mart_customer_360` 25+ KPI, 1 dòng/khách, qua Trino | 8, 11 |
| Tự động phân khúc cho marketing campaign | 8 (RFM, campaign_target), 11 (cross-sell) |
| Lịch sử customer/account/product/branch | 5, 6, 7 |
| Medallion trên Iceberg + MinIO, Spark batch, Trino, Airflow | 2, 4–8, 10, 11 |

Sau buổi demo đầu tiên trên stack thật, chạy lại evidence manifest (RUNBOOK §Evidence) để các số runtime trong README được đo lại.
