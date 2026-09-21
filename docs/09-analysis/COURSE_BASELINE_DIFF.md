# Đối Chiếu Dự Án Với Template Gốc Của Khoá Học

> **Ngày đo**: 2026-09-21
> **Nguồn**: `thamkhao/Buoi 1` … `Buoi 11` + `thamkhao/code_project_cuoi/` — 422 file, 102.339.998 bytes
> **Mục đích**: Xác định chính xác **dự án này đã xây thêm gì** so với template mà khoá học phát cho học viên
> **Tài liệu liên quan**: [`BOOTCAMP_CURRICULUM_ANALYSIS.md`](BOOTCAMP_CURRICULUM_ANALYSIS.md) · [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md)

---

## 0. Phát hiện quan trọng nhất

`thamkhao/code_project_cuoi/final_project (1)/lakehouse_etl/` **chính là template mà dự án này được fork từ đó**. Cấu trúc thư mục, tên file, tên DAG trùng khớp:

```text
airflow/dags/gold/gold_mart360_dag.py          ← trùng tên trong repo
airflow/dags/gold/gold_segmentation_dag.py     ← trùng
airflow/dags/gold/gold_time_analytics_dag.py   ← trùng
airflow/dags/ops/ops_maintenance_weekly_dag.py ← trùng
airflow/dags/ops/ops_pii_masking_daily_dag.py  ← trùng
code_etl/gold/base_job/gold_job.py             ← trùng
code_etl/gold/mart360/customer_360.yml         ← trùng
```

Điều này cho phép đo **chính xác phần giá trị gia tăng**, thay vì so sánh định tính với "các dự án khác".

---

## 1. Kho tài liệu

| Thư mục | Files | Bytes | Nội dung |
|---|---:|---:|---|
| `Buoi 1` | 17 | 13.054.539 | Lakehouse intro · docker-compose stack (MinIO + Spark) |
| `Buoi 2` | 7 | 11.445.025 | Spark core · 3 notebook (DataFrame, SQL, file formats) |
| `Buoi 3` | 13 | 24.694.299 | spark-submit production · 7 job ingest Postgres → fact |
| `Buoi 4` | 5 | 124.279 | Iceberg: getting started · schema evolution · time travel/ACID |
| `Buoi 5` | 21 | 7.345.998 | Trino cluster · catalog config · 3 view SQL |
| `Buoi 6` | 20 | 7.468.666 | Airflow: 6 lab DAG (dependency, scheduling, retry, **idempotent backfill**) |
| `Buoi 7` | 35 | 6.711.253 | Bronze: snapshot · incremental · rolling · backfill idempotency · `flag_etl` |
| `Buoi 8-9` | 11 | 12.433.667 | Silver: SCD1 · SCD2 · DDL setup |
| `Buoi 10` | 153 | 8.193.580 | Streaming: 3 case Kafka/CDC + **final_project template** |
| `Buoi 11` | 6 | 10.159.622 | Governance & performance tuning (slide) |
| `code_project_cuoi` | 134 | 709.070 | **Template final project hoàn chỉnh** |

Ghi chú: `video*.txt` chỉ chứa URL YouTube (48 bytes mỗi file), không phải transcript — không có nội dung để phân tích.

---

## 2. Diff cấp thành phần: template → repo hiện tại

| Hạng mục | Template gốc | Repo hiện tại | Chênh |
|---|---:|---:|---|
| File tracked | 134 | **427** | +293 |
| File `.py` | 42 | **170** | +128 |
| **File test** | **0** | **51** | **+51** |
| Bronze YAML | 13 | 18 | +5 |
| Silver YAML | 10 | 16 | +6 |
| Gold YAML | 10 | 14 | +4 |
| DAG file | 11 | 27 | +16 |
| File `.sql` | 20 | 46 | +26 |
| Docker service | 12 | **29** | +17 |
| **dbt project** | **0** | **1** (13 model) | **+1** |
| **CI workflow** | **0** | **3** (10 job) | **+3** |
| Tài liệu `.md` | 8 | 30 | +22 |
| Thư mục top-level | 7 | 17 | +10 |

**Thư mục hoàn toàn mới** (template không có):

```text
tests/        governance/   dbt/       api/       ml/
streamlit/    terraform/    scripts/   demo/      openmetadata/
```

---

## 3. Diff cấp code: `gold_job.py`

Đây là file cho thấy rõ nhất khác biệt về tư duy kỹ thuật.

```text
Template gốc :  85 dòng
Repo hiện tại: 221 dòng   (+160%)
```

### 3.1 Template gốc — ghi Gold không có guard nào

```python
def run_gold_job(spark, config, cob_dt, logger):
    target   = get_target_table(config)
    job_type = config["job"]["type"]

    result_df = load_source_df(spark, config, cob_dt)     # ← chạy SQL
    result_df.writeTo(target).overwritePartitions()       # ← ghi thẳng
```

Không kiểm tra snapshot nguồn. Không kiểm tra kết quả rỗng. Không kiểm tra bảng đích tồn tại.

### 3.2 Repo hiện tại — ba lớp bảo vệ trước khi ghi

```python
assert_source_snapshots(spark, config, cob_dt, logger)   # ① partition nguồn có tồn tại?
result_df = load_source_df(spark, config, cob_dt)
assert_non_empty(result_df, config, cob_dt, logger)      # ② kết quả có rỗng không?
if not table_exists(spark, target):                      # ③ bảng đích có tồn tại?
    create_iceberg_table_if_not_exists(result_df, target, logger)
result_df.writeTo(target).overwritePartitions()
_run_zorder_if_needed(spark, target, job_type, logger)   # ④ tối ưu đọc
```

### 3.3 Vì sao guard ① là bắt buộc — và vì sao template gốc nguy hiểm

Docstring trong [`code_etl/gold/base_job/gold_job.py`](../../code_etl/gold/base_job/gold_job.py) giải thích chính xác lỗ hổng của template:

> "các model grain customer neo vào `dim_customer` rồi LEFT JOIN fact. Nếu partition fact của `cob_dt` không tồn tại, query vẫn trả về đủ 1 dòng/khách với mọi metric = 0. **Output KHÔNG rỗng, `require_non_empty` vẫn PASS, và Gold bị ghi đè bằng số 0 trông rất hợp lý. Đó là silent corruption, tệ hơn rỗng.**"

Với template gốc, kịch bản này xảy ra **âm thầm**: DAG xanh, bảng Gold có đủ số dòng, mọi KPI bằng 0. Đây đúng là trường hợp mà bài viết schema drift của khoá học mô tả — *"job vẫn xanh, nhưng bảng downstream bắt đầu sai nghĩa"* — nhưng bản thân template của khoá lại mắc chính lỗi đó.

### 3.4 Các khác biệt khác

| | Template | Repo |
|---|---|---|
| `VALID_JOB_TYPES` | `mart360, segment, time_analytics` | `+ risk` |
| Z-Ordering | không có | 13 bảng, skip khi >1M dòng, lỗi non-fatal |
| Tạo bảng nếu thiếu | không (yêu cầu bảng tồn tại sẵn) | có, kèm giải thích giới hạn Iceberg V2 writer |
| Log lỗi | `logger.error(..., exc_info=True)` | `logger.exception(...)` |

---

## 4. Diff cấp kiến trúc: CDC

**Template final project hoàn toàn không có CDC.**

```text
Tìm trong template: *cdc* · *debezium* · *kafka*  →  0 kết quả
Docker services template: 12 (oracle, postgres, minio, mc, iceberg-rest,
    spark-master, spark-worker-1, jupyter, trino, airflow-init,
    airflow-webserver, airflow-scheduler)   ← không có Kafka
```

CDC chỉ xuất hiện ở **lab Buổi 10** như bài tập rời (`lab_spark_streaming_b10`), không được tích hợp vào final project.

### 4.1 So sánh CDC: lab Buổi 10 vs repo

Lab Buổi 10 `case2_core_account_cdc/spark_job.py` (209 dòng) — Debezium → Kafka → Spark Streaming → MERGE INTO Iceberg.

| Khía cạnh | Lab Buổi 10 | Repo hiện tại |
|---|---|---|
| Đích ghi | 1 bảng phẳng `lakehouse.streaming.core_account` | Bronze CDC (append-only) → consolidation → Silver Current |
| **Dedup** | `Window.partitionBy(account_id).orderBy(desc(ts_ms))` — **chỉ trong 1 batch** | watermark `(timestamp, batch_id)` per table — **xuyên batch** |
| **DLQ** | không có | có, giữ Kafka partition/offset/timestamp |
| Số connector | 1 | 3 · 12 topic |
| Bảng Bronze CDC | — | 6 |

**Điểm yếu đã xác định của lab**: dedup chỉ trong phạm vi micro-batch. Nếu cùng `account_id` đến ở batch N rồi batch N+1 với `ts_ms` **nhỏ hơn** (out-of-order), MERGE ở batch N+1 vẫn ghi đè — mất bản ghi mới hơn. Watermark xuyên batch của repo xử lý được trường hợp này.

### 4.2 Timezone — hai cách giải khác nhau

Lab xử lý ở **tầng ingest**, với comment thừa nhận đây là workaround:

```python
# Debezium coi TIMESTAMP(noTZ) của Postgres là UTC, nhưng thực tế data là giờ VN (UTC+7).
to_utc_timestamp((col("rec.updated_at")/lit(1_000_000)).cast("timestamp"),
                 "Asia/Ho_Chi_Minh").alias("source_updated_at")
```

Repo xử lý ở **tầng business date**, có invariant cưỡng chế:

```sql
CAST(from_utc_timestamp(txn_date, 'Asia/Ho_Chi_Minh') AS DATE)
```

kèm `assert_utc_session` bảo đảm Spark session luôn ở UTC — nếu không, biểu thức trên sai âm thầm. Lab không có cơ chế tương đương.

---

## 5. Các pattern của khoá học đã áp dụng đúng

Template dạy nhiều thứ tốt, và repo đã giữ lại:

| Pattern | Nguồn | Repo |
|---|---|---|
| Metadata-driven (base_job + YAML) cho cả 3 tầng | template | ✅ giữ và mở rộng |
| `overwritePartitions` theo `cob_dt` | template | ✅ |
| SCD1 / SCD2 với `effective_from`/`effective_to`/`is_current` | `code_etl/silver/base_job/scd_type2.py` | ✅ |
| Surrogate key = `sha2(business_key|cob_dt, 256)` | Buổi 8-9 | ✅ |
| Audit flag table (`flag_job_etl`) | Buổi 7 `flag_etl.py` | ✅ dùng trong `SqlSensor` |
| Iceberg maintenance (compaction, expire snapshot) | `shared/ops/iceberg_maintenance.py` | ✅ `ops_maintenance_weekly_dag` |
| PII masking DAG | `shared/ops/pii_masking.py` | ✅ `ops_pii_masking_daily_dag` |
| Idempotent backfill | Buổi 6 `lab6_idempotent_backfill.py` | ✅ |
| Bronze ingestion patterns: snapshot · incremental · rolling | Buổi 7 | ✅ |

**Đây là nền tảng tốt và nên được ghi nhận** — dự án không phải xây từ số 0, và các quyết định kiến trúc gốc (metadata-driven, partition-safe write, SCD2 chuẩn) là đúng đắn.

---

## 6. Phần giá trị gia tăng — tóm tắt

Sắp theo mức độ khác biệt:

### 6.1 Không tồn tại trong template

| Hạng mục | Quy mô |
|---|---|
| **Bộ test** | 51 file · 647 test collected (`-m "not integration"`), 646 passed / 1 skipped |
| **Evidence manifest** | 40 metric node · 22 invariant · 18 README binding |
| **CI/CD** | 3 workflow · 10 job · có PR-blocking gate với negative test |
| **Governance** | 10 module · 33 data contract · 9 DQ check type |
| **dbt serving** | 13 model qua Trino |
| **CDC end-to-end** | 3 connector · 12 topic · 6 bảng Bronze CDC · DLQ |
| **Risk domain** | 3 Gold model: AML typology · fraud · loan portfolio risk |
| **Downstream** | FastAPI (6 endpoint) · MLflow · Streamlit · Superset · OpenMetadata · Terraform · Prometheus/Grafana |
| **Compliance** | BCBS 239 + SBV reporting DAG |

### 6.2 Sửa lỗi thiết kế của template

| Lỗ hổng template | Cách repo xử lý |
|---|---|
| Ghi Gold không kiểm tra snapshot nguồn → silent corruption toàn số 0 | `assert_source_snapshots()` |
| `overwritePartitions()` với DataFrame rỗng là no-op, giữ partition cũ | `assert_non_empty()` |
| Dedup CDC chỉ trong batch → mất bản ghi khi out-of-order | watermark `(timestamp, batch_id)` xuyên batch |
| Timezone xử lý ad-hoc ở tầng ingest | biểu thức business-date + `assert_utc_session` cưỡng chế |
| Không có test → không biết khi nào hỏng | 51 file test, gate chặn PR |

---

## 7. Ý nghĩa cho thị trường

Đối chiếu với [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md):

Trong 272 repo đối thủ trên GitHub (`repo.md`), 136 repo là "lakehouse", 24 repo là banking. Phần lớn trong số đó **dừng ở mức template này** — Medallion + Iceberg + Trino + Airflow, chạy được, không có test, không có contract, không có gate.

Phần giá trị gia tăng ở Mục 6 tương ứng trực tiếp với những từ khoá thị trường đang hỏi nhiều nhất:

| Giá trị gia tăng | Từ khoá thị trường | Mật độ |
|---|---|---|
| 33 data contract | `data contract` | 2,3/100k (↑5,8×) |
| `assert_source_snapshots` + schema drift | `schema drift` / `data quality` | 1,3 / 23,4 |
| CDC + watermark + DLQ | `CDC` / `failure recovery` | 4,4 |
| 51 file test + CI gate | `CI/CD` | 16,6 |
| Evidence manifest | `traceability` / `reconciliation` | 2,8 |
| Risk domain (AML/fraud) | JD Fraud Detection chuyên biệt | — |

**Cách kể chuyện hiệu quả nhất** không phải "tôi làm final project của khoá học", mà là:

> "Tôi bắt đầu từ template của khoá, phát hiện nó ghi Gold mà không kiểm tra partition nguồn — tạo silent corruption toàn số 0 mà mọi check đều xanh — rồi xây lớp invariant, test và CI gate để không lặp lại."

Đó là câu chuyện mà JD Binance mô tả: *"able to design reproducible reconciliation, anomaly detection, backfill, and degradation strategies — not just completing data development tasks."*

---

## 8. Hạng mục còn thiếu so với template và lab

| Thiếu | Nguồn | Đánh giá |
|---|---|---|
| Oracle như nguồn thứ hai | template có service `oracle` | Repo chỉ dùng PostgreSQL. ABBANK JD yêu cầu Oracle — cân nhắc **P3**, chi phí cao |
| `vacuum` Iceberg | Buổi 11 | Repo có `expire_snapshots` + `rewrite_data_files`, không có `vacuum`. Kiểm tra xem có cần không |
| 3 Trino view mẫu | Buổi 5 `views/*.sql` | Repo dùng dbt `materialized: table` thay view (Iceberg REST không hỗ trợ `createView`) — **quyết định đúng, đã ghi nhận** |

---

## 9. Ghi chú bảo trì

- Corpus nằm ngoài repo (`thamkhao/`), không version control. Checksum thư mục tại thời điểm đo (MD5 của chuỗi MD5 từng file, bỏ qua `__pycache__`):

  | Thư mục | Files | dir-md5 (16 ký tự đầu) |
  |---|---:|---|
  | `Buoi 1` | 17 | `c6d8aca17493639f` |
  | `Buoi 2` | 7 | `ec594d3f4f940f9c` |
  | `Buoi 3` | 13 | `90119bd4b6c8e846` |
  | `Buoi 4` | 5 | `d42f8f85bcb463b3` |
  | `Buoi 5` | 21 | `ba365612316a320c` |
  | `Buoi 6` | 20 | `2fa74d50ae62df7e` |
  | `Buoi 7` | 35 | `50bb5f21e12d0195` |
  | `Buoi 8-9` | 11 | `5fe197e0c41d6627` |
  | `Buoi 10` | 153 | `0dc786b3051525b5` |
  | `Buoi 11` | 6 | `0414969e1e3212a3` |
  | `code_project_cuoi` | 134 | `3625563bec4d903d` |

- Các con số phía repo (427 file, 51 test file, 29 service, 10 CI job, 33 contract…) đo bằng `git ls-files` và parse YAML tại 2026-09-21, `main` @ `b787616`.
- Phân loại **`metric_type: manual`** theo chuẩn `docs/evidence/metrics-manifest.yaml` — **không** đưa vào `verify_readme_metrics.py`.
- Không trích con số nào từ tài liệu này vào README mà không kèm ngày đo.
