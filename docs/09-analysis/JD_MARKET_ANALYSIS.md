# Phân Tích Thị Trường Data Engineer Việt Nam — Toàn Bộ Corpus JD

> **Ngày đo**: 2026-09-21
> **Nguồn**: 14 file JD + 1 file benchmark repo (`thamkhao/JD/`)
> **Quy mô**: 2.770.320 ký tự · ~490 job posting · Aug–Sep 2026
> **Mục đích**: Xác định khoảng cách giữa Banking Data Platform và nhu cầu thị trường **bằng số đo, không bằng ước lượng**
> **Thay thế**: `JD_FINAL_MARKET_ANALYSIS.md`, `JD_MARKET_ANALYSIS_REPORT.md` (cả hai chỉ phủ jd1–jd8 và dùng phần trăm ước lượng không nêu phương pháp)

---

## 0. Phương pháp đo (để tái lập được)

Mọi con số trong tài liệu này là **đếm số lần xuất hiện của regex trên văn bản đã decode UTF-8**, không phải ước lượng.

**Corpus được pin bằng checksum** tại thời điểm đo:

| File | Bytes | MD5 |
|---|---:|---|
| `jd1.md` | 325.665 | `233e2c54822cdaaed21a6d9d5fef05c9` |
| `jd2.md` | 297.841 | `f35cdce550555ca53c095cc3ccaa3ec9` |
| `jd3.md` | 347.474 | `7024a04577038b30bf0f137ef1401306` |
| `jd4.md` | 229.170 | `05b2f71b30c4e42c5db4d759c7d89462` |
| `jd5.md` | 172.803 | `279e7344d60df53a2561d79c99293046` |
| `jd6.md` | 317.559 | `65f65e5a3a1661432a8b1cb3b59f6fe6` |
| `jd7.md` | 268.405 | `8aec89077ad9f68be0c4096b294c24a5` |
| `jd8_bank.md` | 163.248 | `625c6db168874c7355462707e8eea80a` |
| `jd9.md` | 87.922 | `b407168fdddb979cee6e39460289b410` |
| `jd10.md` | 171.572 | `42784401ae01eea2db61d3ff7da5ced8` |
| `jd11.md` | 248.317 | `0caee464a9f1fddfd73692367c71ea9c` |
| `jd12.md` | 123.635 | `4d317a4c6e1f57e6517e76cca92de280` |
| `jd13.md` | 263.336 | `ea6ccc70e6f24a2a4f271c61ea622de5` |
| `jd14.md` | 76.365 | `f0908992f87282aadc91da5ce9c763f1` |
| `repo.md` | 18.906 | `6aff299111de7a07f9aa1013a505c4e7` |

**Đơn vị chuẩn hoá**: `số lần xuất hiện / 100.000 ký tự`. Dùng mật độ thay vì số tuyệt đối vì hai nhóm mẫu có kích thước khác nhau (1.902k vs 868k ký tự).

**Hai nhóm mẫu**:
- **Nhóm A** = jd1–jd8 (1.902.000 ký tự) — đã được phân tích trước đây
- **Nhóm B** = jd9–jd14 (868.000 ký tự) — mẫu mới, chưa từng phân tích

> ⚠️ **Giới hạn phương pháp cần nêu rõ**: Nhóm A và Nhóm B cùng khung thời gian (Aug–Sep 2026), nên đây là **hai mẫu độc lập**, KHÔNG phải chuỗi thời gian. Cột "bội số" nghĩa là *mật độ cao hơn ở mẫu B*, có thể do thành phần mẫu. Chỉ những khác biệt ≥2× mới nên coi là tín hiệu thật.
>
> ⚠️ Mỗi file là một trang scrape LinkedIn/ITviec: 1 JD chính ở đầu + hàng trăm "việc làm liên quan" bên dưới. Đếm regex phản ánh **cường độ nhắc đến trong toàn thị trường**, không phải "% số JD yêu cầu". Tài liệu này KHÔNG dùng cách diễn đạt "~X% JD yêu cầu" vì không đo được điều đó từ dữ liệu hiện có.

---

## 1. Tổng quan thị trường

### 1.1 Phân bố vị trí (đếm trên nhóm B)

```text
Seniority          Địa điểm                 Ngôn ngữ
  80  Senior        157  Hà Nội             127  "English"
  31  Fresher        74  TP.HCM              79  "tiếng Anh"
  17  Lead           54  Remote              21  "Fluent"
   6  Expert          4  Đà Nẵng             17  "B2"
   4  Middle
   3  Junior/Manager
```

Thị trường nghiêng hẳn về **Senior** và **Hà Nội**. Remote chiếm tỷ trọng đáng kể (54), phần lớn là client Singapore/EU.

### 1.2 Lương (trích nguyên văn từ JD)

| Mức | Dải | Ghi chú |
|---|---|---|
| Fresher / Junior | 10–20M | "FSS tìm DATA ENGINEER \| Fresher–Junior \| 10–20M" |
| Middle | 30–38M | "40-48M Gross (Senior), 30M-38M (Middle) x 14–15 tháng" |
| Senior | 40–50M | "SENIOR DATA ENGINEER \| 45–50M/tháng \| 100% Remote \| Client Singapore" |
| Senior + Cloud + English | 60–75M | "GCP DATA ENGINEER \| 4+ YOE \| UP TO 65M", "Data Engineer (Good English) - Up to 75M" |
| Chuyên biệt | 700M/năm | "DATA ENGINEER \| UP TO 700M GROSS/NĂM \| CYBER SECURITY DOMAIN" |

**Đòn bẩy lương rõ nhất là Cloud + tiếng Anh**, không phải thêm công cụ.

### 1.3 Nhà tuyển dụng khối tài chính (nhóm B)

Techcombank, VPBank, MSB, MB Bank, HDBank, ACB, VIB, TPBank, BIDV, **ABBANK**, LPBank, PVcomBank, GPBank, VietCredit, MoMo, **Binance**, SSI, Prudential, BIC.

### 1.4 Chứng chỉ được nêu tên

```text
10  Cloudera            6  AWS Certified Data Analytics
 9  DAMA (+5 CDMP)      6  Azure Data Engineer Associate
 3  SnowPro             2  Google Professional Data Engineer / Databricks Certified
```

`DAMA-CDMP` xuất hiện nhiều hơn mọi chứng chỉ cloud đơn lẻ — tín hiệu về trọng số **Data Governance** trong tuyển dụng ngân hàng.

---

## 2. Tech stack — bảng tần suất đo được

Đơn vị: lần/100k ký tự.

### 2.1 Nền tảng không đổi

| Công nghệ | Nhóm A | Nhóm B | Trạng thái dự án |
|---|---:|---:|---|
| SQL | 36,9 | 50,7 | ✅ |
| Python | 26,1 | 35,0 | ✅ |
| Spark / PySpark | 22,9 | 29,7 | ✅ |
| AWS | 21,3 | 27,8 | ⚠️ MinIO (S3-compatible) |
| Data Quality | 15,0 | 23,4 | ✅ 9 DQ check type |
| Real-time / streaming | 18,0 | 22,8 | ✅ Spark Structured Streaming |
| Airflow | 13,2 | 22,0 | ✅ 20 DAG file |
| Lakehouse | 11,1 | 17,7 | ✅ |
| CI/CD | 12,0 | 16,6 | ✅ 10 CI job |
| Kafka | 11,8 | 15,1 | ✅ 12 topic |
| Data Governance | 12,7 | 13,8 | ✅ 10 module |
| Docker | 5,5 | 6,9 | ✅ 29 service |

### 2.2 Tăng mạnh ở mẫu mới (≥2×)

| Công nghệ | Nhóm A | Nhóm B | Bội số | Dự án |
|---|---:|---:|---:|---|
| **Data Contract** | 0,4 | **2,3** | **5,8×** | ✅ 33 contract YAML (`governance/datasets/`) |
| **MCP** | 0,6 | 2,2 | 3,7× | ⚠️ 4 file nhắc |
| **Schema Drift / Evolution** | 0,3 | **1,3** | **4,3×** | ✅ `ops_schema_drift_dag` |
| **AI-assisted dev** (Cursor/Claude/Copilot) | 2,7 | **9,1** | **3,4×** | — |
| **ClickHouse** | 1,8 | 5,2 | 2,9× | ❌ (không khuyến nghị) |
| **Backfill** | 1,2 | 3,2 | 2,7× | ✅ qua `cob_dt` |
| **MS Fabric** | 2,7 | 6,8 | 2,5× | ❌ |
| **Iceberg** | 3,1 | **7,5** | **2,4×** | ✅ |
| **Snowflake** | 6,5 | 12,9 | 2,0× | ❌ |

### 2.3 Giảm ở mẫu mới

| Công nghệ | Nhóm A | Nhóm B | Bội số |
|---|---:|---:|---:|
| MLflow | 1,2 | 0,6 | 0,5× |
| Unity Catalog | 1,3 | 0,7 | 0,5× |
| MLOps | 3,8 | 2,3 | 0,6× |
| SCD | 0,7 | 0,5 | 0,7× |
| RBAC | 0,8 | 0,5 | 0,6× |

---

## 3. Phát hiện chính: thị trường đã đổi sang từ vựng xử lý lỗi

Đây là tín hiệu mạnh nhất của toàn bộ corpus. Yêu cầu tuyển dụng 2026 không còn liệt kê công cụ mà mô tả **cách hệ thống hành xử khi hỏng**.

Trích nguyên văn Binance — Financial Data Engineer, AI/LLM, Senior, Hà Nội (`jd13.md`):

> "able to design reproducible reconciliation, anomaly detection, backfill, and degradation strategies — **not just completing data development tasks**."

Một JD khác liệt kê skill tag gần như trùng khít kiến trúc của dự án này:

```text
Schema Evolution · Idempotency · CDC · retry · Alerting · incremental loading
ordering · duplicate processing · failure recovery · partitioning · logging
secrets/configuration · Monitoring · Troubleshooting · data quality · CI/CD
```

Và một JD ERP:

> "Define and enforce data quality checks using dbt tests or Great Expectations. **Define data contracts for all Oracle ERP upstream sources.**"

### 3.1 Từ vựng resilience — cách thị trường thực sự gọi tên

Đo trên **toàn bộ** 2.770.320 ký tự:

```text
RCA / root cause            64    ✅ thị trường dùng
runbook                     37    ✅
incident response           23    ✅
disaster recovery            6    ⚠️ hiếm
DR                           9    ⚠️
failover                     0    ❌
RTO                          0    ❌
RPO                          0    ❌
Recovery Time Objective      0    ❌
Recovery Point Objective     0    ❌
```

**`RTO`/`RPO` không xuất hiện một lần nào trong ~490 JD.** Đây là từ vựng của giáo trình DataOps, không phải của nhà tuyển dụng. Thị trường hỏi cùng một năng lực nhưng gọi bằng **`runbook` / `incident response` / `RCA`**.

---

## 4. Benchmark đối thủ — `repo.md`

`repo.md` là danh sách repo GitHub của các ứng viên khác. Đây là dữ liệu cạnh tranh trực tiếp.

```text
272  repo unique        255  owner unique
136  generic lakehouse
 43  ecommerce / retail
 24  banking / finance
 12  CDC pipeline
 11  taxi / transport
  9  crypto / trading
  3  fraud detection
```

**Nhóm banking (24 repo)** — bao gồm `Gokul-gok/banking-data-platform` (trùng tên dự án này), `MPGranji/retail-banking-customer360-lakehouse`, `willtran112358/bcg-vn-digital-bank-data-platform`, `tranvix0910/banking-ai-agents-aml-copilot`, `Thehy-IT/capstone-banking-fraud-detection`, `sidpanda-alt/openbank-lakehouse`, `venkateshghule24/Enterprise-Banking-Mart`.

### 4.1 Hệ quả: kiến trúc Medallion Lakehouse đã là hàng phổ thông

Chọn **Spark + Iceberg + MinIO + Trino + Airflow không còn là điểm khác biệt**. Hai kiến trúc final project trong corpus tham khảo (`thamkhao/bootcamp_class/image*.png`) gần như trùng stack này.

Điểm khác biệt còn lại của dự án nằm ở nơi khác:

| Năng lực | 272 repo đối thủ | Dự án này |
|---|---|---|
| Medallion + Iceberg + Trino | phổ biến | ✅ |
| CDC Debezium | 12 repo | ✅ |
| Evidence manifest đo từ platform | **không repo nào nêu** | ✅ 40 metric node · 22 invariant · 18 README binding |
| CI gate có negative test | **không repo nào nêu** | ✅ `trino-integration-gate` |
| AML typology thật (structuring/velocity/multi-channel) | 0 (3 repo fraud, không nêu typology) | ✅ |

### 4.2 Một minh hoạ về rủi ro số liệu không kiểm chứng

Trong `repo.md` có đoạn LaTeX résumé, cùng một thành tích viết thành hai phiên bản:

```latex
...reducing query latency by ~40--50% from ~25s to ~8--12s
...reducing query latency by ~50--60% from ~25s to ~8--12s
```

Cùng baseline 25s, cùng kết quả 8–12s, nhưng phần trăm khác nhau. Đây chính xác là loại con số mà hệ thống evidence manifest của dự án tồn tại để ngăn chặn — và là lý do tài liệu này pin checksum ở Mục 0.

---

## 5. Nghiệp vụ ngân hàng

### 5.1 ABBANK — Senior/Expert Data Architect (`jd14.md`, 10+ năm)

```text
Kiến trúc     OLAP · Data Warehousing · Data Lake · Data Mesh · object storage
Mô hình       dữ liệu có cấu trúc/phi cấu trúc ở mức vật lý–logic–khái niệm
              mô hình dữ liệu tài chính chuẩn · MDM · data federated solution
Nền tảng      Oracle · SAP · Teradata · IBM · Cloudera · Databricks · Hortonworks
ETL platform  Oracle ODI · SAP BODS · Airflow · Informatica · IBM
Streaming     Apache Storm · Kafka · Spark Streaming
Công cụ model Power Designer · SQL Developer Data Modeler · ERwin
```

### 5.2 Fraud Detection Data Engineer — ngân hàng bán lẻ (`jd14.md`)

Chuỗi trách nhiệm 5 bước:

```text
Data Architecture → Data Pipeline → Detection Engine → Detection Model
→ Risk Scoring → Dashboard & Alert
```

Yêu cầu: 3+ năm Big Data/DE · Python + SQL · Spark/Kafka/Airflow/Databricks · ưu tiên kinh nghiệm Fraud Detection, Risk Analytics, Banking/Retail.

**Dự án hiện đáp ứng 3/5 mắt xích**: Data Architecture ✅, Data Pipeline ✅, Detection Engine + Risk Scoring ✅ (`aml_monitoring.yml`, `fraud_risk_txn.yml`). Thiếu: **Detection Model** (ML-based, hiện chỉ có rule-based) và **Alert** (có Dashboard qua Superset, chưa có alert routing cho nghiệp vụ).

### 5.3 Binance — Financial Data Engineer, AI/LLM (`jd13.md`)

Domain đòi hỏi cao nhất trong corpus: trading calendar đa thị trường, múi giờ, tiền tệ, security identifier, corporate action, data correction, primary/backup source strategy, build-vs-buy trade-off, financial entity alignment cho LLM/RAG.

Liên quan trực tiếp tới dự án: **xử lý múi giờ**. Dự án đã giải bài này đúng cách (`CAST(from_utc_timestamp(txn_date,'Asia/Ho_Chi_Minh') AS DATE)` + `assert_utc_session`) — đây là điểm kể chuyện tốt cho JD dạng này.

### 5.4 Nghịch lý: use case nghiệp vụ KHÔNG phải từ khoá tuyển dụng

Đo trên toàn corpus:

```text
Customer 360      10 lần        ← final project của cả khoá banking bootcamp
churn              7
cross-sell         5
RFM                2
NPL                1
NBO / next best    0
```

`Customer 360` xuất hiện 10 lần trong ~490 JD. Các bootcamp đóng gói mọi thứ quanh cụm từ này vì nó **dạy tốt**, không phải vì nhà tuyển dụng tìm nó.

**Hệ quả cho cách trình bày dự án**: "tôi xây Customer 360 mart" yếu hơn "tôi xây SCD2 dimension với data contract, reconciliation và backfill an toàn".

### 5.5 Ngược lại: thị trường hỏi thứ không giáo trình nào dạy

```text
feature store      81 lần  (2,6/100k)   ← cao nhất nhóm này
Data Vault         44 lần  (1,3/100k)
Kimball / Inmon    31 lần  (0,8/100k)
Great Expectations 19 lần  (1,2/100k)
```

**`Data Vault` được nhắc nhiều hơn `Great Expectations`.** Dự án đã có `docs/DATA_VAULT_MAPPING.md` (mapping Kimball→DV2.0, 188 dòng) — đây là câu trả lời hợp lý mà không cần implement.

---

## 6. Đối chiếu dự án ↔ thị trường

### 6.1 Đã khớp

| Yêu cầu thị trường | Mật độ (B) | Hiện trạng dự án |
|---|---:|---|
| SQL · Python · Spark | 50,7 / 35,0 / 29,7 | ✅ |
| Airflow | 22,0 | ✅ 20 DAG file / 21 object |
| Data Quality | 23,4 | ✅ 9 DQ check type |
| Iceberg | 7,5 | ✅ REST catalog |
| dbt | 11,1 | ✅ 13 serving model |
| Trino | 5,1 | ✅ + contract test chống lẫn catalog |
| Kafka / CDC | 15,1 / 4,4 | ✅ 3 connector · 12 topic · DLQ |
| **Data Contract** | **2,3** | ✅ **33 contract YAML (`governance/datasets/`)** |
| **Schema Drift** | **1,3** | ✅ `ops_schema_drift_dag` |
| Reconciliation | 2,8 | ✅ |
| Backfill / Idempotency | 3,2 / 0,9 | ✅ `overwritePartitions` theo `cob_dt` |
| Lineage | 8,1 | ✅ OpenMetadata + module `lineage` |
| CI/CD | 16,6 | ✅ 10 job, có PR-blocking gate |
| Terraform / IaC | 4,1 | ✅ (Docker provider) |
| BI | Superset 2,9 · Power BI 10,0 | ✅ Superset · ❌ Power BI |
| PII masking · RBAC | 1,3 / 0,5 | ✅ |
| Prometheus / Grafana | 0,8 / 0,9 | ✅ |

### 6.2 Khoảng trống thật, xếp theo số đo

| # | Khoảng trống | Mật độ (B) | Toàn corpus | Ưu tiên | Lý do |
|---|---|---:|---:|---|---|
| 1 | Đổi **ngôn ngữ** README sang từ vựng thị trường | — | — | **P0** | Năng lực đã có, chỉ sai nhãn. Chi phí thấp nhất, tác động cao nhất |
| 2 | **Feature store** | 2,6 | 81 | **P1** | Cao nhất nhóm chưa có; đã có `ml/pipeline/*` + Gold mart, chỉ cần đóng gói |
| 3 | **Schema drift**: phân loại additive vs breaking + quyền chặn publish | 1,3 | 30 | **P1** | Đã có DAG phát hiện; thiếu phân loại và quyền chặn |
| 4 | **Runbook / RCA** tài liệu hoá | — | 37 / 64 | **P1** | Thị trường hỏi rõ; dự án có năng lực nhưng chưa có tài liệu |
| 5 | **CLV mart** | 1,2 | 20 | **P2** | Mart duy nhất trong benchmark top-3 mà dự án chưa có |
| 6 | **Detection Model** (ML) + Alert routing | — | — | **P2** | Hoàn tất chuỗi 5 bước của JD Fraud |
| 7 | **Great Expectations** | 1,2 | 19 | **P2** | dbt tests đã phủ; luôn xuất hiện dạng "dbt tests **hoặc** GE" |
| 8 | **Kubernetes** | 9,4 | 219 | **P3** | Chi phí cao; Docker Compose đã đủ kể chuyện |
| 9 | **Cloud** (AWS/Azure/Snowflake/Databricks) | 27,8 / 26,0 / 12,9 / 13,9 | — | **Dự án riêng** | Không nên nhồi vào repo này |

### 6.3 Không khuyến nghị (có số đo hỗ trợ)

| Hạng mục | Số đo | Lý do |
|---|---|---|
| **OBT / One Big Table** | **0 / 2.770.320 ký tự** | Giáo trình dạy như topic chính; thị trường không nhắc một lần |
| **Flink** | 8,2 | Trùng lặp với Spark Structured Streaming đã có; Spark Real-Time Mode đang xoá dần lý do tồn tại |
| **ClickHouse** | 5,2 | Thêm một engine OLAP không giải quyết vấn đề nào của dự án |
| **Snowpipe / Auto Loader / DLT** | 0 / 0 / 1 | Vendor-specific, không chuyển giao |
| **Z-Order như điểm bán hàng** | 0 | Dự án có (13 bảng) nhưng đừng dùng làm luận điểm chính |

---

## 7. Đính chính các phân tích trước

Tài liệu này sửa ba khuyến nghị sai trong `JD_FINAL_MARKET_ANALYSIS.md` và các phân tích giáo trình:

| Khuyến nghị cũ | Số đo thực tế | Kết luận |
|---|---|---|
| "Định lượng RTO/RPO để nói ngôn ngữ thị trường" | `RTO` = 0, `RPO` = 0 trên 2,77M ký tự | **Bỏ.** Dùng `runbook` / `incident response` / `RCA` (37/23/64 lần) |
| "Great Expectations là P1 gap" | 19 lần toàn corpus, thấp hơn `Data Vault` (44) | **Hạ xuống P2.** Luôn là ví dụ được nêu tên, không phải yêu cầu cứng |
| "BI/Visualization là P0 gap, ~70% banking JD" | Power BI 10,0/100k, Superset 2,9/100k — con số "70%" không tái lập được từ dữ liệu | **Đã đóng** (Superset có trong stack). Con số 70% cũ không có phương pháp đo |

Ngoài ra, **9 action item** của `JD_FINAL_MARKET_ANALYSIS.md` (2026-09-07) nay đã đóng gần hết:

```text
✅ 1. Superset BI            ✅ 6. MLflow
✅ 2. Terraform IaC          ✅ 7. Data Vault (mapping doc)
✅ 3. dbt tests              ✅ 8. Regulatory (BCBS 239 + SBV DAG)
✅ 4. Customer 360 API       ✅ 9. AI Governance framework
✅ 5. AML tables
```

---

## 8. Hành động đề xuất

### P0 — Đổi nhãn, không đổi code

Repo đã có 34 data contract, `ops_schema_drift_dag`, reconciliation, idempotent `overwritePartitions`, backfill qua `cob_dt`, DLQ, quarantine. README cần gọi đúng tên thị trường:

```text
NÊN DÙNG                          THAY VÌ
data contract                     "schema validation"
schema drift / breaking change    "kiểm tra cấu trúc"
reconciliation                    "đối chiếu số liệu"
backfill · idempotency            "chạy lại được"
traceability                      "audit log"
runbook · incident response · RCA "RTO/RPO"
degradation strategy              "xử lý lỗi"
```

### P1 — Ba hạng mục có số đo hỗ trợ

1. **Feature store** (81 lần) — đóng gói `ml/pipeline/*` + Gold mart thành feature view có versioning.
2. **Schema drift: additive vs breaking** — mở rộng `ops_schema_drift_dag` để phân loại thay đổi và **chặn publish Gold** khi contract vỡ. Theo đúng mindset: *Bronze nhận linh hoạt → Silver chuẩn hoá → Gold chỉ publish khi contract ổn định.*
3. **Runbook + RCA** — tài liệu hoá quy trình sự cố cho 3 pipeline chính.

### P2

4. **CLV mart** — cho `rfm_segment` một downstream consumer thật.
5. **Detection Model + Alert routing** — hoàn tất chuỗi 5 bước của JD Fraud Detection.

### Không làm

OBT · Flink · ClickHouse · Kubernetes · thêm cloud vào repo này.

---

## 9. Ghi chú bảo trì

- Tài liệu này đo trên corpus pin bằng MD5 ở **Mục 0**. Nếu `thamkhao/JD/` thay đổi, mọi con số phải đo lại — **không cập nhật từng phần**.
- Corpus nằm ngoài repo (`thamkhao/`), không được version control. Vì vậy các số ở đây là **`metric_type: manual`** theo chuẩn của `docs/evidence/metrics-manifest.yaml`: đo một lần, có nêu phương pháp và ngày đo, **không** được đưa vào `verify_readme_metrics.py`.
- Không trích con số nào từ tài liệu này vào README mà không kèm ngày đo.
