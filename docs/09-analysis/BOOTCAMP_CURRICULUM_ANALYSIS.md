# Phân Tích Giáo Trình Bootcamp — Đối Chiếu Với Thị Trường & Dự Án

> **Ngày đo**: 2026-09-21
> **Nguồn**: 15 file trong `thamkhao/bootcamp_class/` — 5 giáo trình + 1 community feed
> **Mục đích**: Xác định phần nào của giáo trình đáng áp dụng vào Banking Data Platform, dựa trên **đối chiếu với corpus JD đã đo**, không dựa trên uy tín của khoá học
> **Tài liệu liên quan**: [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md) — nguồn của mọi con số thị trường trích dẫn ở đây

---

## 0. Phương pháp

Tài liệu này **không** đếm tần suất trên giáo trình — 5 giáo trình không phải mẫu thống kê. Thay vào đó:

1. Đọc toàn bộ 15 file, lập **ma trận phủ sóng** (khái niệm × khoá học).
2. Với mỗi khái niệm, tra **mật độ đo được trong corpus JD** (`JD_MARKET_ANALYSIS.md` Mục 0 — 2.770.320 ký tự, ~490 posting, pin bằng MD5).
3. Đối chiếu với **hiện trạng repo** đã kiểm chứng bằng lệnh.

Kết quả là phán quyết 3 chiều: *giáo trình dạy* × *thị trường hỏi* × *dự án có*.

**Corpus pin bằng checksum:**

| File | Bytes | MD5 |
|---|---:|---|
| `MasterClass_...Modern Banking From Batch to Real-Time.txt` | 15.898 | `c3ac0a4bf7689f67429406af4d3b2412` |
| `MasterClass_...Modern Banking_ From Batch to Real-Time.xlsx` | 15.190.999 | `4bf2318b4132a54af111c0762c8975a0` |
| `Master Class DataOps...From Pipeline to Production.txt` | 14.463 | `980ee7409feb58c6df9ae726ac3c7c5f` |
| `new/Master Class DataOps for Data Platforms.txt` | 7.178 | `93a63caa97f306de66168ff06428261b` |
| `new/Master Class Data Engineer Financial Data Platform.xlsx` | 16.941.598 | `9d3648ac4322c5a1d53aea1ef18dfca2` |
| `new/Master Class Data Engineer Financial Data Platform.pdf` | 15.825.508 | `d9ea2b5a3315516e827b735952ae4a21` |
| `new/MasterClass Data Engineer Financial Data Platform.txt` | 6.219 | `8bf208453c0f73badcfcdf13b8572251` |
| `SERIES BOOTCAMP BIGDATA PLATFORM MASTER .txt` | 13.988 | `e0103fad4737dd0f90f7da1f4a056dc5` |
| `Bootcamp Analytics Engineer.txt` | 3.203 | `3d7e9c5c13de892d93b7a6598330935e` |
| `Bootcamp Analytics Engineer.xlsx` | 17.203 | `61ce1f55535c37a163c24c08f3217d31` |
| `new/DATA ENGINEER FULL-STACK K22.md` | 54.840 | `9d3b1f2f8df1687336c6c4aac349ed12` |
| `new/Bootcamp AWS 2026.txt` | 6.702 | `35f281cf3c61cc652572d07f45f0ec72` |
| `new/KHOÁ HỌC...AWS...Nội dung chương trình.pdf` | 104.078 | `162193d14296753fe4ef61600c6b8e31` |
| `new/KHOÁ HỌC...AWS....xlsx` | 51.007 | `118eb2bb0fd320505a387cf73d711176` |
| `new/CHUẨN HÓA NGHIỆP VỤ – DỮ LIỆU.txt` | 3.318 | `7b9e458917e4cf45aaefed85c3b63f98` |

Tổng: 48.256.202 bytes. Bản `.xlsx` và `.pdf` là phiên bản có cấu trúc của cùng nội dung `.txt` — đã đối chiếu, không mâu thuẫn.

---

## 1. Kho tài liệu

| # | Giáo trình | Thời lượng | Mentor | Nền tảng cược vào |
|---|---|---|---|---|
| 1 | **Data Engineering for Modern Banking: From Batch to Real-Time** | 12 buổi + capstone | **Hồ Đức Huy** — Sr Big Data Engineer, VietinBank (+ LPBank, MB Bank, FPT Software) | On-prem OSS: Spark · Iceberg · MinIO · Trino · Airflow · Kafka |
| 2 | **DataOps for Data Platforms: From Pipeline to Production** | 11 buổi + capstone | **Nguyễn Hoàng Quốc Anh** — Sr Data/DataOps Engineer, Techcombank | Stack-agnostic (DWH *hoặc* Lakehouse) |
| 3 | **Data Engineer Financial Data Platform: Ingestion → Lakehouse → Production** | 13 buổi / 5 phase | **Trần Sỹ Hùng** — DE, Grasshopper Asia (Singapore) | Snowflake + dbt |
| 4 | **SERIES BOOTCAMP BIGDATA PLATFORM** (3 khoá) | 5–6 buổi/khoá | Hùng (Snowflake) · **Nguyễn Đình Tương** — Rightship, ex-BIDV/MB Bank (Databricks) · **Lê Phong Vũ** — Lead DE MoMo (MS Fabric) | Vendor platform |
| 5 | **Bootcamp Analytics Engineer** | 5 buổi | — | dbt |
| 6 | **Bootcamp AWS 2026** | 8 buổi, 500k VNĐ | Trần Thái Gia Bảo | AWS (20 dịch vụ) |
| — | `DATA ENGINEER FULL-STACK K22.md` | 563 dòng | 4 giảng viên (IX · BRG · MoMo · Rightship) | **Community feed — file giàu tín hiệu nhất** |

### 1.1 Mạng lưới mentor = tín hiệu domain

```text
VietinBank · Techcombank · ABBANK · MB Bank · BIDV · LPBank
MoMo · Tiki · VNG · CIC Data · FPT Software · Viettel · Grasshopper Asia (SG)
```

**Toàn bộ mentor đều từ ngân hàng/fintech Việt Nam.** Đây không phải trùng hợp — nó xác nhận banking/fintech là nơi tập trung năng lực DE cao cấp ở VN, và củng cố lựa chọn domain của dự án này.

---

## 2. Ma trận phủ sóng — phần hội tụ

Sáu giáo trình khác nhau về nền tảng nhưng dạy chung một xương sống:

| Khái niệm | Banking | DataOps | Financial | Series | AE | AWS |
|---|:-:|:-:|:-:|:-:|:-:|:-:|
| Python · SQL | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Spark / PySpark | ✅ | ✅ | ✅ | ✅ | — | ✅ |
| Medallion Bronze/Silver/Gold | ✅ | ✅ | ✅ | ✅ | — | ✅ |
| Airflow orchestration | ✅ | ✅ | ✅ | ○ | — | ✅ |
| Kafka / CDC | ✅ | — | ✅ | ○ | — | ✅ |
| Table format (Iceberg/Delta) | ✅ | ✅ | ○ | ✅ | — | — |
| SCD Type 1/2 | ✅ | — | ✅ | ✅ | ✅ | — |
| Star Schema (fact/dim) | ✅ | — | ✅ | ✅ | ✅ | ✅ |
| Data Quality | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| PII masking / governance | ✅ | ✅ | ✅ | ✅ | — | ✅ |
| CI/CD | ✅ | ✅ | ✅ | ✅ | — | — |
| Docker Compose | ✅ | ✅ | ✅ | — | — | — |
| Portfolio project bắt buộc | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

`○` = có nhắc nhưng không phải trọng tâm.

**Kết luận**: xương sống này ≈ 80% nội dung mọi khoá. Dự án hiện đã phủ **toàn bộ** cột này.

---

## 3. Đối chiếu giáo trình ↔ thị trường ↔ dự án

Đây là phần có giá trị quyết định. Cột "JD" là mật độ đo được (lần/100k ký tự) và số tuyệt đối trên toàn corpus 2,77M ký tự.

### 3.1 Giáo trình dạy — thị trường xác nhận — dự án có ✅

| Khái niệm | Dạy ở | JD | Dự án |
|---|---|---|---|
| Iceberg (ACID, time travel, schema evolution, MoR/CoW) | Banking, Series | **7,5** | ✅ REST catalog |
| dbt (model, test, docs, macro, lineage) | AE, Financial | **11,1** | ✅ 13 serving model |
| Trino | Banking | **5,1** | ✅ + contract test chống lẫn catalog |
| Airflow (DAG, retry, SLA, backfill, sensor) | 4/6 khoá | **22,0** | ✅ 20 DAG file |
| Data Quality | 5/6 khoá | **23,4** | ✅ 9 DQ check type |
| Kafka + Debezium CDC | Banking, Financial | **15,1** / 4,4 | ✅ 3 connector · 12 topic · DLQ |
| Reconciliation | DataOps, Financial | **2,8** | ✅ |
| Idempotency | DataOps | 0,9 (28) | ✅ `overwritePartitions` theo `cob_dt` |
| Backfill | DataOps, Banking | **3,2** | ✅ |
| Lineage | DataOps, AE, Financial | **8,1** | ✅ OpenMetadata + module `lineage` |
| CI/CD | 4/6 khoá | **16,6** | ✅ 10 CI job |
| PII masking | 4/6 khoá | 1,3 | ✅ |
| Schema evolution | Banking, Series | **1,3** | ⚠️ `ops_schema_drift_dag` phát hiện nhưng chưa chặn — xem Mục 7 P1-1 |

### 3.2 Giáo trình dạy — thị trường KHÔNG nhắc ⛔

Đo trên **toàn bộ** 2.770.320 ký tự:

| Khái niệm | Dạy ở | JD | Dự án | Phán quyết |
|---|---|---:|---|---|
| **OBT / One Big Table** | Financial (topic chính, Buổi 4) | **0** | ❌ | **Không làm.** Dạy như trade-off cốt lõi, thị trường không nhắc một lần |
| **Quarantine pattern** | Financial (Buổi 8) | **0** | ✅ đã có | Giữ vì đúng kỹ thuật, **đừng dùng làm luận điểm bán hàng** |
| **Snowpipe** | Financial, Snowflake | **0** | ❌ | Không làm — vendor-specific |
| **Auto Loader / DLT** | Databricks | **0 / 1** | ❌ | Không làm |
| **Snowpark / UDF** | Snowflake | 0,1 (4) | ❌ | Không làm |
| **RTO / RPO** | DataOps (Buổi 10) | **0** | — | **Bỏ khỏi kế hoạch.** Xem Mục 3.4 |
| **NBO / Next Best Offer** | Top-3 K22 | **0** | ❌ | Không ưu tiên |
| **Z-Order** | — | **0** | ✅ 13 bảng | Có sẵn, đừng kể như điểm mạnh chính |
| **Six Sigma / DMAIC** | Top-3 K22 (khung báo cáo) | 1 | — | Bỏ qua |

### 3.3 Nghịch lý: business use case không phải từ khoá tuyển dụng

| Use case | Vai trò trong giáo trình | JD (toàn corpus) |
|---|---|---:|
| **Customer 360** | **FINAL PROJECT của khoá Banking** | **10** |
| Churn | Top-3 K22, Databricks, Fabric | 7 |
| Cross-sell | Khoá Banking (đề bài) | 5 |
| RFM | Top-3 K22 | 2 |
| NPL / delinquency | Top-3 K22 | 1 |
| NBO | Top-3 K22 | 0 |

`Customer 360` — bài toán trung tâm của cả một khoá học — xuất hiện **10 lần trong ~490 JD**.

Các bootcamp đóng gói mọi thứ quanh cụm từ này vì nó **dạy tốt** (đủ phức tạp, đủ trực quan, có business story), không phải vì nhà tuyển dụng tìm nó.

**Hệ quả cho cách trình bày dự án:**

```text
YẾU:   "Tôi xây Customer 360 mart với 25+ KPI"
MẠNH:  "Tôi xây SCD2 dimension với data contract, reconciliation,
        và backfill an toàn theo partition"
```

### 3.4 Thị trường hỏi — không giáo trình nào dạy

| Khái niệm | JD | Dạy ở | Dự án |
|---|---:|---|---|
| **Feature store** | **81** (2,6) | chỉ nhắc như "học tiếp sau" | ❌ 2 file nhắc |
| **Data Vault** | **44** (1,3) | **0/6 khoá** | ⚠️ `DATA_VAULT_MAPPING.md` (188 dòng) |
| **Kimball / Inmon** | **31** (0,8) | **0/6 khoá** | ✅ Star schema |
| **RCA / root cause** | **64** | DataOps ✅ | ⚠️ có năng lực, chưa có tài liệu |
| **Runbook** | **37** | DataOps ✅ | ⚠️ chưa có |
| **Incident response** | **23** | DataOps ✅ | ⚠️ chưa có |
| Great Expectations | 19 (1,2) | DataOps, Financial | ❌ |

Hai điều đáng chú ý:

**① `Data Vault` được nhắc nhiều hơn `Great Expectations`** (44 vs 19) nhưng **không giáo trình nào dạy**. Dự án đã có tài liệu mapping Kimball→DV2.0 — đây là câu trả lời hợp lý mà không cần implement.

**② Khoá DataOps dạy đúng năng lực nhưng sai nhãn.** `RTO`/`RPO` = 0 lần trong toàn corpus. Thị trường hỏi cùng thứ đó bằng `runbook` (37) / `RCA` (64) / `incident response` (23). Đây là khoảng trống **tài liệu hoá**, không phải khoảng trống năng lực.

---

## 4. Ba ý tưởng kỹ thuật đáng lấy

### 4.1 Nguyên lý DataOps: `idempotency → reproducibility → reconciliation`

Trục xuyên suốt khoá Techcombank (Buổi 1, nhắc lại ở Buổi 4, 5, 11). Buổi 10 mở rộng thành chuỗi sự cố:

```text
pipeline fail → RCA → lineage truy vết tác động → runbook → backup/DR
```

Dự án đã cài đặt 3 nguyên lý; **chuỗi sự cố thì chưa có tài liệu**.

### 4.2 Bài viết schema drift (K22, dòng 149–174) — đáng đọc nguyên văn

> "Schema drift không phải lúc nào cũng làm pipeline đỏ. Nhiều lúc **job vẫn xanh, nhưng bảng downstream bắt đầu sai nghĩa** — và đó mới là kiểu lỗi nguy hiểm nhất với team Data."
>
> "Nó tạo ra **'silent data contract break'**: dashboard vẫn lên, alert hạ tầng vẫn yên, nhưng business metric bắt đầu sai từng phần."

**Checklist họ đưa ra:**

```text
□ snapshot schema theo từng run
□ alert khi có cột mới / mất cột / đổi type
□ TÁCH RÕ additive change vs breaking change
□ CHẶN PUBLISH downstream nếu contract quan trọng bị vỡ

□ count(*) ổn nhưng null_rate của field chính có tăng bất thường không?
□ Có cột nào đổi int→string hoặc timestamp→text không?
□ Model downstream có đang SELECT * không?
□ Có test contract ở staging trước khi merge vào fact/reporting không?
```

**Mindset**: *Bronze nhận linh hoạt → Silver chuẩn hoá → Gold chỉ publish khi contract ổn định.*

Đây gần như là mô tả `assert_source_snapshots()` trong [`code_etl/gold/base_job/gold_job.py:94`](../../code_etl/gold/base_job/gold_job.py) — nhưng ở **tầng schema** thay vì tầng partition. Docstring hiện tại đã nêu đúng nguyên lý cho partition:

> "Output KHÔNG rỗng, require_non_empty vẫn PASS, và Gold bị ghi đè bằng số 0 trông rất hợp lý. Đó là silent corruption, tệ hơn rỗng."

Mở rộng nguyên lý đó sang schema là bước logic tiếp theo, và **thị trường đang hỏi đúng nó** (`data contract` 2,3/100k tăng 5,8×; `schema drift` 1,3/100k tăng 4,3×).

### 4.3 Spark Real-Time Mode (K22, dòng 377–421)

Spark phá "microbatch barrier" bằng continuous data flow + concurrent stages + non-blocking operators → latency mức millisecond mà vẫn giữ throughput cao.

**Ý nghĩa cho dự án**: làm yếu đáng kể luận điểm "cần thêm Flink cho low-latency". Use case đầu tiên họ nêu là **fraud detection** — đúng domain dự án đang làm bằng Spark. Đây là lý do kỹ thuật để **không** thêm Flink, bổ sung cho lý do thị trường.

---

## 5. Nghiệp vụ ngân hàng theo giáo trình

```text
Khoá Banking (Huy)       mart_customer_360 · 25+ KPI · 1 row/khách
                         + lưu lịch sử customer/account/product/branch (SCD2)
                         + tự động phân khúc cho marketing campaign
                         + truy vấn qua Trino, không đụng production

Khoá Financial (Hùng)    4 data mart: Risk · Finance · Marketing · Operations

Top-3 K22 (Cường)        + Superset: Portfolio · Churn Watchlist
                                     · Branch performance · Campaign funnel
                         + Airflow DAG sinh báo cáo Excel/PDF định kỳ → MinIO
                         + MLflow churn + NBO propensity (3 sản phẩm)
                         + Loan portfolio risk mart (NPL ratio, delinquency)
                         + CLV mart
                         + 19/19 DQ test · PII masking qua Trino · 8 child DAG
```

### 5.1 Đối chiếu với repo

| Mart / module | Giáo trình | Repo |
|---|---|---|
| Customer 360 | ✅ trung tâm | ✅ 6 model `mart360/` |
| Segmentation (RFM, churn, cross-sell, campaign) | ✅ | ✅ 4 model `segmentation/` |
| Loan portfolio risk (NPL, delinquency) | ✅ top-3 | ✅ `risk/loan_portfolio_risk.yml` |
| Fraud / AML | — | ✅ **2 model — vượt giáo trình** |
| Branch performance | ✅ top-3 (dashboard) | ✅ `time_analytics/branch_monthly_summary.yml` |
| **CLV mart** | ✅ top-3 | ❌ **chưa có** |
| Finance mart | ✅ Financial | ❌ |
| Operations mart | ✅ Financial | ❌ |
| Superset dashboard | ✅ top-3 | ✅ |
| MLflow churn | ✅ top-3 | ✅ |
| NBO propensity serving | ✅ top-3 | ⚠️ có `cross_sell_segment`, chưa có serving API |
| Báo cáo Excel/PDF định kỳ | ✅ top-3 | ❌ (có `regulatory_reporting_dag`) |
| Iceberg maintenance (compaction, expire snapshot) | ✅ Banking Buổi 11 | ✅ |

**Dự án có 14 Gold model / 4 domain**, vượt `mart_customer_360` đơn lẻ của giáo trình. Thiếu: CLV, Finance, Operations mart.

---

## 6. Repo so với 3 final project

| Hạng mục | Banking (Huy) | DataOps (Q.Anh) | Top-3 K22 | Repo này |
|---|:-:|:-:|:-:|---|
| Medallion + Iceberg + Trino | ✅ | ○ | ✅ | ✅ |
| SCD Type 2 | ✅ | — | ✅ | ✅ 2 dim |
| CDC Kafka + Debezium | ✅ | — | — | ✅ 3 connector · 12 topic |
| Gold mart | 1 bảng | — | + risk + CLV | **14 model / 4 domain** |
| idempotency + reconciliation | — | ✅ | — | ✅ |
| Runbook + RCA | — | ✅ | — | ⚠️ chưa tài liệu hoá |
| Great Expectations | — | ✅ | — | ❌ (dbt tests) |
| Superset | — | — | ✅ | ✅ |
| MLflow | — | — | ✅ | ✅ |
| CLV mart | — | — | ✅ | ❌ |
| **Evidence manifest đo từ platform** | — | — | — | ✅ **40 metric node · 22 invariant · 18 binding** |
| **CI gate có negative test** | — | — | — | ✅ `trino-integration-gate` |
| **AML typology thật** | — | — | — | ✅ structuring · velocity · multi-channel · high-value |

Dự án đã vượt cả ba final project. **Điều không giáo trình nào dạy — và không repo học viên nào có — là lớp tự kiểm chứng.** Xem thêm [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md) Mục 4: trong 272 repo đối thủ, không repo nào nêu năng lực này.

---

## 7. Hành động đề xuất

### P1 — Có số đo thị trường hỗ trợ

**1. Schema drift: thêm ngữ nghĩa severity + quyền chặn publish**

Hiện trạng đã kiểm chứng — `governance/schema_drift.py` (231 dòng) **đã có sẵn các primitive cần thiết**:

```python
added_columns · removed_columns · type_changes
detect_drift() · detect_drift_from_contract() · get_schema_diff() · compare_two_tables()
```

Ba thứ còn thiếu so với checklist Mục 4.2:

| Thiếu | Hiện trạng |
|---|---|
| **Ngữ nghĩa severity** | `added`/`removed`/`type_changes` đang **ngang hàng**. Chưa map `added → additive (safe)` vs `removed`/`type_changes → breaking`. Từ `additive`/`breaking` xuất hiện **0 lần** trong toàn repo |
| **Quyền chặn** | `ops_schema_drift_dag` chạy 09:00 **sau** `ops_data_quality_dag` như "safety check" post-hoc — phát hiện nhưng **không chặn** Gold publish |
| **Độ phủ** | Chỉ 3 bảng Silver: `dim_customer`, `dim_account`, `dim_loan` |

Đây là mở rộng logic của `assert_source_snapshots()` từ tầng partition sang tầng schema — cùng một nguyên lý "chặn trước khi ghi", không phải "báo sau khi ghi". Công sức thấp vì primitive đã có.

*Thị trường*: `data contract` 2,3/100k (↑5,8×) · `schema drift/evolution` 1,3/100k (↑4,3×).

**2. Runbook + RCA cho 3 pipeline chính**

Khoảng trống **tài liệu hoá**, không phải năng lực. Dùng đúng từ vựng thị trường: `runbook` · `incident response` · `RCA` — **không** dùng `RTO/RPO`.

*Thị trường*: 37 · 23 · 64 lần. `RTO`/`RPO`: **0**.

**3. Feature store**

Đóng gói `ml/pipeline/*` + Gold mart thành feature view có versioning. Giáo trình chỉ nhắc như "nền tảng để học tiếp", nhưng đây là khái niệm **được JD nhắc nhiều nhất** trong nhóm dự án chưa có.

*Thị trường*: 81 lần (2,6/100k).

### P2

**4. CLV mart** — mart duy nhất trong benchmark top-3 mà repo chưa có; cho `rfm_segment` một downstream consumer thật. *Thị trường*: `CLV/LTV` 20 lần (1,2/100k).

**5. `dbt_expectations` thay vì Great Expectations đầy đủ**

Khoá Analytics Engineer dạy package `dbt_expectations` — bản port dbt-native của Great Expectations. Repo hiện có `dbt_utils`, `codegen`, `dbt_date` nhưng **chưa có `dbt_expectations`**. Thêm một dòng vào `dbt/packages.yml` rẻ hơn nhiều so với dựng framework GE riêng, và phủ được cụm từ khoá `Great Expectations` (19 lần) mà JD luôn nêu dạng *"dbt tests **hoặc** Great Expectations"*.

**6. Detection Model (ML) + Alert routing** — hoàn tất chuỗi 5 bước của JD Fraud Detection (`Data Architecture → Pipeline → Detection Engine → Detection Model → Risk Scoring → Dashboard & Alert`). Repo hiện đáp ứng 3/5, thiếu Detection Model ML-based và alert routing nghiệp vụ.

### Không làm — có số đo hỗ trợ

| Hạng mục | Dạy ở | JD | Lý do |
|---|---|---:|---|
| OBT / One Big Table | Financial (topic chính) | **0** | Không tồn tại trong ngôn ngữ tuyển dụng |
| Flink | Banking, Fabric | 8,2 | Trùng Spark Structured Streaming; Spark RTM xoá dần lý do (Mục 4.3) |
| ClickHouse | Series | 5,2 | Thêm engine OLAP không giải quyết vấn đề nào của dự án |
| Snowpipe / Auto Loader / DLT / Snowpark | Financial, Series | 0–4 | Vendor-specific, không chuyển giao |
| Kubernetes | DataOps (demo) | 9,4 | Chi phí cao; Docker Compose đã đủ kể chuyện |
| Thêm cloud vào repo này | AWS, Series | — | Nên là **dự án riêng**, không nhồi vào đây |

### P0 — Áp dụng từ Mục 3.3

Đổi **nhãn**, không đổi code. Ngừng dẫn dắt bằng "Customer 360" (10 lần trong corpus); dẫn dắt bằng `data contract` · `reconciliation` · `schema drift` · `idempotency` · `backfill` · `traceability`. Chi tiết: [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md) Mục 8.

---

## 8. Kết luận

**Giáo trình dạy đúng kỹ thuật nhưng sai từ vựng thương mại.**

- Chúng đóng gói mọi thứ quanh `Customer 360` — cụm từ xuất hiện 10 lần trong ~490 JD.
- Chúng dạy `OBT` như trade-off cốt lõi — 0 lần trong 2,77M ký tự.
- Chúng dạy `RTO/RPO` — 0 lần; thị trường gọi là `runbook`/`RCA` (37/64 lần).
- Chúng **bỏ qua** `Data Vault` (44), `Kimball/Inmon` (31), `feature store` (81).

Với dự án này: phần kỹ thuật đã vượt cả ba final project mẫu. Việc còn lại chủ yếu là **đổi nhãn** (P0) và **tài liệu hoá** (runbook/RCA), cộng ba hạng mục kỹ thuật có số đo hỗ trợ rõ (schema drift contract, feature store, CLV).

---

## 9. Ghi chú bảo trì

- Corpus giáo trình nằm ngoài repo (`thamkhao/bootcamp_class/`), không version control. Các nhận định ở đây là **`metric_type: manual`** theo chuẩn `docs/evidence/metrics-manifest.yaml`: đo một lần, nêu phương pháp và ngày, **không** đưa vào `verify_readme_metrics.py`.
- Mọi con số thị trường trích từ [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md). Nếu corpus JD đổi, tài liệu đó phải đo lại trước, rồi mới cập nhật tài liệu này.
- Nếu `thamkhao/bootcamp_class/` thay đổi (checksum Mục 0 không khớp), đọc lại toàn bộ — **không vá từng phần**.
- Không trích con số nào từ tài liệu này vào README mà không kèm ngày đo.
