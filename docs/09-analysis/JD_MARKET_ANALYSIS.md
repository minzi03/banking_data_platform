# Phân Tích Thị Trường Data Engineer Việt Nam — Toàn Bộ Corpus JD

> **Ngày đo**: 2026-09-29 (thay bản 2026-09-21)
> **Nguồn**: 16 file JD + 1 file benchmark repo (`thamkhao/JD/`)
> **Quy mô**: 3.310.578 ký tự (1.842.677 sau khi bỏ dòng lặp) · Aug–Sep 2026
> **Công cụ đo**: [`scripts/measure_jd_corpus.py`](../../scripts/measure_jd_corpus.py) — mọi con số ở đây sinh lại được bằng một lệnh
> **Mục đích**: Xác định khoảng cách giữa Banking Data Platform và nhu cầu thị trường **bằng số đo, không bằng ước lượng**
> **Thay thế**: `JD_FINAL_MARKET_ANALYSIS.md`, `JD_MARKET_ANALYSIS_REPORT.md` (chỉ phủ jd1–jd8, phần trăm không nêu phương pháp)

---

## 0. Phương pháp đo (để tái lập được)

```bash
py -3 scripts/measure_jd_corpus.py ../thamkhao/JD          # bảng markdown
py -3 scripts/measure_jd_corpus.py ../thamkhao/JD --json   # JSON
```

Mọi con số là **số lần regex khớp trên văn bản đã decode UTF-8**. Regex nằm trong `TERMS` của script, kèm chú thích ở những chỗ regex trơn sẽ khớp nhầm (vd. `collection` khớp "data collection", `provisioning` khớp "provisioning services").

**Đơn vị**: `lần / 100.000 ký tự`, vì ba nhóm mẫu có kích thước khác nhau.

**Hai chế độ đếm** (mới ở bản này):

- `raw` là văn bản nguyên trạng, cùng cách đo của bản 2026-09-21.
- `dedup` giữ mỗi dòng không rỗng **một lần** trong cả nhóm. Trang scrape lặp nguyên đoạn JD và "About us" của cùng công ty; ví dụ khối giới thiệu Renova Cloud lặp 4 lần trong `jd14.md`. Bỏ lặp làm corpus co từ 3,31M xuống 1,84M ký tự (−44%). **Một số đo chỉ đáng tin khi raw và dedup kể cùng một câu chuyện.**

**Corpus được pin bằng checksum**:

| File | Bytes | MD5 | Nhóm |
|---|---:|---|---|
| `jd1.md` | 325.665 | `233e2c54822cdaaed21a6d9d5fef05c9` | A |
| `jd2.md` | 297.841 | `f35cdce550555ca53c095cc3ccaa3ec9` | A |
| `jd3.md` | 347.474 | `7024a04577038b30bf0f137ef1401306` | A |
| `jd4.md` | 229.170 | `05b2f71b30c4e42c5db4d759c7d89462` | A |
| `jd5.md` | 172.803 | `279e7344d60df53a2561d79c99293046` | A |
| `jd6.md` | 317.559 | `65f65e5a3a1661432a8b1cb3b59f6fe6` | A |
| `jd7.md` | 268.405 | `8aec89077ad9f68be0c4096b294c24a5` | A |
| `jd8_bank.md` | 163.248 | `625c6db168874c7355462707e8eea80a` | A |
| `jd9.md` | 87.922 | `b407168fdddb979cee6e39460289b410` | B |
| `jd10.md` | 171.572 | `42784401ae01eea2db61d3ff7da5ced8` | B |
| `jd11.md` | 248.317 | `0caee464a9f1fddfd73692367c71ea9c` | B |
| `jd12.md` | 123.635 | `4d317a4c6e1f57e6517e76cca92de280` | B |
| `jd13.md` | 263.336 | `ea6ccc70e6f24a2a4f271c61ea622de5` | B |
| `jd14.md` | 266.334 | `c55d3256351419db6aa93f83abb72569` | C |
| `jd15.md` | 285.665 | `aa34b1391aed1c8bcdf2b52bce36ab5c` | C |
| `jd16.md` | 113.578 | `213da2b24f99f9ac6e434d395ddb84c9` | C |
| `repo.md` | 19.635 | `332687b08a13a9305c5f7b4c3de8391c` | benchmark |

**Ba nhóm mẫu**:

| Nhóm | File | Ký tự | Sau dedup | Ghi chú |
|---|---|---:|---:|---|
| A | jd1–jd8 | 1.901.965 | 1.178.377 | Không đổi từ bản trước (MD5 khớp) |
| B | jd9–jd13 | 805.257 | 590.280 | Bản trước gồm cả `jd14.md` cũ (76k) |
| C | jd14–jd16 | 603.354 | 545.906 | **Mới**: `jd14.md` được thay bằng bản scrape 266k, cộng `jd15`, `jd16` |

Kiểm tra hiệu chỉnh: đo lại nhóm A (corpus không đổi) bằng script, so với bản 2026-09-21. SQL 37,0 so với 36,9; AWS 21,3 so với 21,3; Kafka 11,9 so với 11,8; Lakehouse, CI/CD, Docker, SCD, ClickHouse, Backfill, Data Contract, MLflow và MLOps lệch ≤0,1. Những dòng lệch nhiều hơn (Data Quality 21,2 so với 15,0; Data Governance 16,2 so với 12,7) là do script **cố ý** đếm thêm cụm tiếng Việt (`chất lượng dữ liệu`, `quản trị dữ liệu`).

> ⚠️ **Giới hạn phương pháp**
>
> Ba nhóm cùng khung Aug–Sep 2026. Chúng là **ba mẫu độc lập, KHÔNG phải chuỗi thời gian**, nên "bội số" nghĩa là *mật độ cao hơn ở mẫu đó* và có thể do thành phần mẫu. Chỉ coi là tín hiệu thật khi khác biệt **≥2× ở cả B lẫn C** so với A.
>
> Đếm regex đo **cường độ nhắc đến**, không đo "% số JD yêu cầu". Tài liệu này không dùng cách diễn đạt "~X% JD yêu cầu".

---

## 1. Tổng quan thị trường

### 1.1 Thành phần vai trò — lý do nhóm C phải đọc khác

Phân loại theo tiêu đề bài đăng (dòng ngay sau header "`<công ty> logo`"). Chỉ đếm được ở các file có header này: jd10–16.

```text
                     Nhóm B (jd10–13)   Nhóm C (jd14–16)
bài có header              153                106
DE / Platform          117  (76%)          52  (49%)
DA / BI / BA            14   (9%)          38  (36%)
Architect               10   (7%)           4   (4%)
Analytics Engineer       3   (2%)           4   (4%)
DS / ML / AI             2   (1%)           3   (3%)
Governance · DBA · khác  7   (5%)           5   (5%)
```

Nhóm C còn chứa **khoảng 50 JD lồng không có header**: khoảng 20 JD tiếng Việt trong khối ABBANK (`jd14.md` dòng 1–1025) và khoảng 30 JD LinkedIn "About the job" trong khối Binance (`jd15.md` dòng 1828–4288, trong đó có SeABank ODS/DWH).

**Hệ quả**: nhóm C có tỷ trọng Data Analyst / BI cao gấp 4 lần nhóm B. Mọi thứ tăng ở C mà gắn với vai trò DA đều phải coi là **hiệu ứng thành phần mẫu**, chưa phải xu hướng thị trường DE: Power BI, Tableau, Looker/Metabase, A/B test, và phần lớn thuật ngữ nghiệp vụ tín dụng.

### 1.2 Nhà tuyển dụng khối tài chính trong nhóm C

```text
Ngân hàng      Techcombank ×5 · PVcomBank ×4 · BIDV · ABBANK · TPBank · CIMB · NCB
               VPBank · Timo (BVBank) · SeABank · ANZ (qua HCLTech) · NAB ×2
Tài chính TD   FE CREDIT ×2 · Mcredit · Home Credit · F88 · GoTymeX · KALAPA
Ví / fintech   MoMo ×3 · Zalopay · Monee (ShopeePay) ×2 · Gimo · WeCopy Fintech
Chứng khoán    VPBankS · Binance
```

Khối tài chính chiếm khoảng 1/3 bài có header của nhóm C. Techcombank mở 5 vị trí cùng lúc: Data Engineer, Senior Data Engineer, Expert Data Engineer, Senior Data Architect và Senior Data Platform Engineer (Infrastructure).

### 1.3 Lương (trích nguyên văn)

Tuyệt đại đa số bài đăng nhóm C ghi **"Thoả thuận"**. Những điểm có số:

| Vị trí | Mức | Nguồn |
|---|---|---|
| Data Tester, Hà Nội | 22–30M/tháng | `jd14.md:1000` |
| Data Engineer (GEM, onsite) | up to 30M | `jd14.md:3520` |
| Analytics Engineer (Joon Solutions) | 20–40M | `jd14.md:3452` |
| DE/DA (SiciX) | lương cứng 20–40M | `jd16.md:795` |
| Data Engineer (Good English), FPT Software | up to 75M | `jd15.md:4512` |
| VPBank, Senior/Expert BI (thưởng) | "15–18 tháng thu nhập/năm" | `jd15.md:455` |

Kết luận của bản trước vẫn đúng: **đòn bẩy lương rõ nhất là tiếng Anh + cloud**, không phải thêm công cụ.

### 1.4 Chứng chỉ và khung chuẩn

Toàn corpus, raw (dedup trong ngoặc): `DAMA / DCAM / CDMP` = 54 (18) · `BCBS 239 / Basel` = 20 (8) · `Decree 13 / PDPL` = 50 (23).

Nhóm C thêm một yêu cầu cụ thể: **Nghị định 13 & 356** cho Data Governance (Golden Gate). Chuẩn **DAMA-DMBOK + DCAM + BCBS 239 + lưu trú dữ liệu theo SBV/NHNN** xuất hiện trong cùng một JD kiến trúc giải pháp ngân hàng (PVcomBank, `jd14.md:1921`).

---

## 2. Tech stack — bảng mật độ đo được

Đơn vị: lần/100k ký tự, raw. Cột "C dedup" để kiểm tra con số C có bị bài lặp thổi phồng không. Bảng đầy đủ 88 thuật ngữ: chạy script.

### 2.1 Nền tảng không đổi qua cả ba nhóm

| Công nghệ | A | B | C | C dedup | Trạng thái dự án |
|---|---:|---:|---:|---:|---|
| SQL | 37,0 | 50,3 | 39,9 | 43,6 | ✅ |
| Python | 26,6 | 36,1 | 29,8 | 32,6 | ✅ |
| Data Quality | 21,2 | 29,9 | 24,7 | 26,7 | ✅ 9 DQ check type |
| Spark / PySpark | 23,6 | 30,2 | 20,7 | 22,9 | ✅ |
| AWS | 21,3 | 28,6 | 23,4 | 23,4 | ⚠️ MinIO (S3-compatible) |
| Real-time / streaming | 19,1 | 23,3 | 19,2 | 21,1 | ✅ Spark Structured Streaming |
| Data Governance | 16,2 | 16,9 | 16,6 | 18,0 | ✅ |
| Airflow | 13,4 | 22,1 | 15,4 | 17,0 | ✅ |
| CI/CD | 11,9 | 16,0 | 11,4 | 12,6 | ✅ |
| Lakehouse | 11,2 | 17,6 | 11,1 | 12,3 | ✅ |
| Kafka | 11,9 | 15,8 | 8,8 | 9,7 | ✅ Debezium CDC |
| dbt | 7,1 | 12,5 | 11,6 | 12,6 | ✅ serving layer |
| Iceberg | 3,2 | 7,6 | 4,3 | 4,6 | ✅ REST catalog |
| Trino / Presto | 3,0 | 5,0 | 3,3 | 3,7 | ✅ |

### 2.2 Tín hiệu **bền**: ≥2× so với A ở **cả** B lẫn C

Đây là nhóm đáng tin nhất của tài liệu: hai mẫu độc lập cùng lệch về một hướng.

| Khái niệm | A | B | C | Bội số B / C | Dự án |
|---|---:|---:|---:|---:|---|
| **MCP** | 0,4 | 1,6 | 1,5 | 4,0× / 3,8× | ❌ |
| **Schema drift / evolution** | 0,3 | 1,4 | 1,2 | 4,7× / 4,0× | ⚠️ `ops_schema_drift_dag`: phát hiện, chưa phân loại, chưa chặn |
| **Data Contract** | 0,4 | 2,5 | 1,2 | 6,3× / 3,0× | ✅ 34 contract YAML (`governance/datasets/`) |
| **Anomaly detection** | 0,8 | 2,9 | 2,0 | 3,6× / 2,5× | ✅ DQ check `anomaly_detection` |
| **Backfill** | 1,2 | 3,2 | 3,0 | 2,7× / 2,5× | ✅ qua `cob_dt` |
| **Semantic / metric layer** | 1,5 | 3,5 | 3,8 | 2,3× / 2,5× | ❌ |
| **ClickHouse** | 1,8 | 5,8 | 3,8 | 3,2× / 2,1× | ❌ (không khuyến nghị, xem §6.3) |
| Reconciliation / đối soát *(B 1,8× — sát ngưỡng)* | 1,9 | 3,5 | 4,0 | 1,8× / 2,1× | ⚠️ có, chưa phân loại kết quả |
| **Freshness** | 0,2 | 0,6 | 1,2 | 3,0× / 6,0× | ✅ `governance/freshness_checks.py` (n nhỏ: 16 lần toàn corpus) |

Hai dòng ❌ là **MCP** và **Semantic layer**. Chúng không tách rời nhau, vì các JD nhóm C nêu chúng trong cùng một câu (xem §5.6).

### 2.3 Tăng ở B nhưng **không lặp lại** ở C — đính chính bản trước

Bản 2026-09-21 xếp các dòng dưới đây vào "tăng mạnh ≥2×". Nhóm C cho thấy đó là **đặc điểm của mẫu B**, không phải xu hướng:

| Công nghệ | A | B | C | Kết luận |
|---|---:|---:|---:|---|
| AI-assisted dev (Cursor/Claude/Copilot) | 2,3 | **9,2** | 2,3 | C về đúng mức A |
| MS Fabric | 2,8 | 7,7 | 1,8 | C thấp hơn A |
| Snowflake | 6,9 | 14,8 | 6,6 | C về mức A |
| Azure | 15,1 | 27,6 | 13,4 | C về mức A |
| Iceberg | 3,2 | 7,6 | 4,3 | C 1,3×: vẫn tăng, nhưng không phải 2,4× |

### 2.4 Giảm nhất quán ở B và C

| Công nghệ | A | B | C |
|---|---:|---:|---:|
| MLOps | 3,8 | 2,5 | 1,8 |
| MLflow | 1,2 | 0,6 | **0,0** |
| Unity Catalog | 1,3 | 0,7 | 0,5 |
| Data Vault | 1,7 | 1,0 | 0,5 |
| Flink | 5,7 | 8,9 | 3,5 |
| Databricks | 12,3 | 14,5 | 9,9 |

`MLflow` không xuất hiện lần nào trong 603k ký tự của nhóm C. Chiều ML đang chuyển từ "vận hành model" sang "dữ liệu cho AI": vector, agent, RAG (§2.5).

### 2.5 AI: từ MLOps sang "AI-ready data"

| Khái niệm | A | B | C | C dedup |
|---|---:|---:|---:|---:|
| LLM / GenAI | 9,4 | 8,8 | 9,9 | 8,8 |
| Vector DB / embeddings | 4,7 | 3,1 | **6,8** | 7,0 |
| AI agent / agentic | 4,5 | 3,8 | **6,3** | 7,0 |
| RAG | 3,3 | 2,6 | 4,0 | 4,0 |
| MCP | 0,4 | 1,6 | 1,5 | 1,6 |

LLM đi ngang. Thứ tăng là **hạ tầng dữ liệu phục vụ agent**, không phải bản thân model. Các JD nhóm C mô tả việc này bằng ngôn ngữ của Data Engineer:

> "Hỗ trợ Agentic AI Engineer bằng Data API, Search API, Retrieval Dataset và MCP-compatible Data Service." (`jd14.md:1456`)

> "Thiết kế data model và semantic layer (định nghĩa metric, entity, lineage nhất quán) phục vụ báo cáo, phân tích và AI Agent truy vấn dữ liệu." (`jd14.md:2304`)

---

## 3. Từ vựng xử lý lỗi — đo lại

Đo trên **toàn bộ** 3.310.578 ký tự (dedup trong ngoặc):

```text
RCA / root cause        90  (51)    ✅ thị trường dùng
incident                80  (47)    ✅
runbook                 46  (21)    ✅
SLA                     68  (39)
idempotency             33  (15)
RTO / RPO                0   (0)    ❌ vẫn 0 trên 3,3M ký tự
```

Kết luận của bản trước **giữ nguyên và mạnh hơn**: `RTO`/`RPO` vẫn 0 sau khi corpus tăng thêm 540k ký tự. Thị trường gọi cùng năng lực đó bằng `RCA` / `incident` / `runbook`.

Nhóm C thêm một cụm mô tả vận hành 24/7 của ngân hàng, lặp nguyên văn ở hai JD:

> "Experience operating 24/7 production data systems, handling pipelines failure, data replay, backfilling, and ensuring idempotency." (`jd14.md:190`, `:786`)

**Đính chính nội bộ**: ROADMAP §2.5 từng ghi con số `runbook` 37 của bản trước là "không tái lập được" và thay bằng 9. Script cho `runbook` = 22 (A) + 14 (B) = **36** trên jd1–jd13, tức các file của bản trước trừ `jd14.md` cũ (đã bị thay, không còn để đo lại). 36 khớp với 37; 9 thì không khớp với bất kỳ cách đếm nào. Vậy **37 đúng, 9 sai**. Nguyên nhân gốc là bản trước không lưu regex; script này sửa điều đó.

---

## 4. Benchmark đối thủ — `repo.md`

```text
                 2026-09-21    2026-09-29
repo unique          272           283   (+11)
owner unique         255           266
banking/finance       24            25
CDC pipeline          12            11   (regex tên repo khác bản trước)
fraud detection        3             3
AML                    —             1   tranvix0910/banking-ai-agents-aml-copilot
AI / agent / RAG       —             3
```

`repo.md` không có lịch sử phiên bản nên **không xác định được chính xác 11 repo nào là mới**. Hai điểm đáng chú ý trong danh sách hiện tại:

- `nandakumarvuppalapati/cdc-lakehouse-data-contracts`: đối thủ đã ghép **CDC + data contract** vào tên repo. "Data contract" không còn là điểm khác biệt riêng của dự án này.
- `repo.md` giờ trỏ tới một bản demo fraud-detection lakehouse đã deploy (vercel) và một dataset Kaggle banking transactions. Đối thủ đầu tư vào **khả năng xem được** (demo live), không chỉ code.

### 4.1 Hệ quả: Medallion Lakehouse là hàng phổ thông

137 trên 283 repo có `lakehouse / medallion / iceberg / delta` trong tên. Điểm khác biệt của dự án vẫn nằm ở những thứ **không repo nào nêu**:

| Năng lực | 283 repo đối thủ | Dự án này |
|---|---|---|
| Medallion + Iceberg + Trino | phổ biến | ✅ |
| CDC Debezium | 11 repo | ✅ |
| Data contract | ≥1 repo (có trong tên) | ✅ 34 contract |
| Evidence manifest đo từ platform | không repo nào nêu | ✅ |
| CI gate có negative test | không repo nào nêu | ✅ `trino-integration-gate` |
| AML typology (structuring/velocity/multi-channel) | 1 repo AML, không nêu typology | ✅ |
| Nghiệp vụ tín dụng: nhóm nợ / roll rate / vintage | 0 repo nêu | ❌ (§6.2 #2) |

### 4.2 Rủi ro số liệu không kiểm chứng (giữ từ bản trước)

Đoạn LaTeX résumé trong `repo.md` vẫn có cùng một thành tích viết hai cách: "~40–50%" và "~50–60%", cùng từ ~25s xuống ~8–12s. Đây chính là loại con số mà evidence manifest của dự án tồn tại để ngăn.

---

## 5. Nghiệp vụ ngân hàng

### 5.1 ABBANK: Senior/Expert Data Architect (`jd14.md`, 10+ năm)

```text
Kiến trúc     OLAP · Data Warehousing · Data Lake · Data Mesh · object storage
Mô hình       mô hình dữ liệu tài chính chuẩn · MDM · data federated solution
Nền tảng      Oracle · SAP · Teradata · IBM · Cloudera · Databricks · Hortonworks
ETL platform  Oracle ODI · SAP BODS · Airflow · Informatica · IBM
Streaming     Apache Storm · Kafka · Spark Streaming
Công cụ model Power Designer · SQL Developer Data Modeler · ERwin
```

Trong khối JD lồng cùng file có một mô tả **reconciliation cụ thể hơn mọi JD khác**:

> "Xây dựng quy trình ETL/ELT: Extract → Transform → Validate → Load → Reconcile." (`jd14.md:605`)
> "Xây dựng cơ chế Data Reconciliation, phân loại Matched / Mismatch / Missing / Duplicate / Pending." (`jd14.md:609`)

Dự án đã có reconciliation (DQ check, `reconcile_cdc.py`) nhưng kết quả là pass/fail. **Chưa có năm lớp phân loại này.**

### 5.2 Fraud Detection Data Engineer: chuỗi 5 bước (giữ từ bản trước)

```text
Data Architecture → Data Pipeline → Detection Engine → Detection Model
→ Risk Scoring → Dashboard & Alert
```

Dự án đáp ứng 3/5 mắt xích. Thiếu **Detection Model** (ML) và **Alert routing** cho nghiệp vụ. Nhóm C bổ sung F88 "Fraud Risk Data Analyst", nên `fraud` = 17 lần trong C (2,8/100k, cao nhất ba nhóm).

### 5.3 Tín dụng tiêu dùng và thu hồi nợ — **mảng nghiệp vụ mới nổi trong nhóm C**

Đây là phát hiện nghiệp vụ lớn nhất của đợt đo này. Trên toàn corpus:

```text
NPL / DPD / roll rate / vintage     11 lần   — 10 trong nhóm C, 0 trong A
collection / thu hồi nợ             12 lần   — 12 trong nhóm C, 0 trong A
CASA / NIM / CIR                     7 lần   —  4 trong nhóm C, 0 trong A
IFRS 9 / ECL / dự phòng              1 lần
```

Số tuyệt đối nhỏ và gần như toàn bộ nằm trong vai trò DA nhóm C (§1.1), nên **đây là tín hiệu nghiệp vụ, không phải tín hiệu kỹ thuật**. Nhưng từ vựng rất nhất quán giữa các nhà tuyển dụng độc lập:

> Mcredit, Collection DA: "Có hiểu biết về Credit Lifecycle, Credit Portfolio hoặc Collection và các chỉ số như DPD, Roll/Flow Rate, Cure Rate, Recovery Rate, PTP..." (`jd14.md:1835`)

> Mcredit: "Xây dựng/phát triển các mô hình phân tích và dự báo như Roll-rate, ROR, PTP Prediction, Propensity-to-pay" (`jd14.md:1815`)

> Talentnet, Credit Platform: "Track metrics such as NPL, roll rate, cure rate, redefault rate and risk migration" · "Build dashboards, Early Warning reports" (`jd14.md:2785–2787`)

> Zalopay: "funnel and approval analytics, DPD/MOB cohorts, vintage and roll-rate analysis" (`jd14.md:1588`)

> BIDV, Senior DA: "Am hiểu sản phẩm và các chỉ số ngân hàng như CASA, huy động, tín dụng, thẻ, bảo hiểm, NPL, NIM, CIR" · "Xây dựng Data Product, Data Mart, Feature Store" (`jd14.md:3236–3246`)

Cùng nhóm còn FE CREDIT (Danh mục thu hồi nợ; Data Analyst Expert), Home Credit (Senior/Lead DE), GoTymeX (DE, Personal Loans), KALAPA (credit scoring DS), Monee (BI, Credit).

**Đối chiếu dự án**: `gold.loan_portfolio_risk` chỉ có `npl_proxy` dựa trên `loan_status`. Comment trong mart ghi *"nguồn không có số ngày quá hạn theo khoản vay"*. Điều đó đúng ở mức khoản vay, nhưng **`silver.fact_loan_payment` đã có `days_late` theo từng kỳ trả**, nên DPD theo khoản vay **suy ra được**. Cái thiếu thật nằm ở generator (§6.2 #2).

### 5.4 Binance: Financial Data Engineer, AI/LLM (`jd13.md`, giữ từ bản trước)

Domain đòi hỏi cao nhất corpus: trading calendar đa thị trường, múi giờ, tiền tệ, corporate action, data correction, primary/backup source. Dự án đã giải đúng **xử lý múi giờ** (`from_utc_timestamp(...,'Asia/Ho_Chi_Minh')` + `assert_utc_session`). Đây là điểm kể chuyện tốt cho JD dạng này.

### 5.5 Thanh toán, đối soát và chuẩn hoá dữ liệu: MoMo Lead DE (`jd14.md:3538`)

Bài đăng này mô tả gần như toàn bộ triết lý của dự án, nên cũng là **bài kiểm tra tốt nhất** cho dự án:

| MoMo yêu cầu | Dự án |
|---|---|
| layered warehouse, conformed dimensions, surrogate keys, SCD | ✅ Silver dims SCD2 |
| dbt standard: sources and freshness, exposures, contracts, tests | ⚠️ có tests (`dbt build` PASS=130); `exposures.yml` cố ý để trống; **chưa có** `sources.freshness`, dbt `contract: enforced` |
| CI/CD for data: SQLFluff, slim/state-based CI trên model thay đổi | ⚠️ CI có dbt build; **chưa có** SQLFluff, slim CI |
| Airflow: SLA definition, backfill procedures, runbooks for on-call | ✅ `sla` trong `default_args` DAG Bronze/Silver/Gold · backfill qua `cob_dt` · runbook S1–S8 |
| PII: pseudonymization boundaries, data residency | ✅ PII masking + Trino ACL |
| payment: settlement, ledger, reconciliation | ⚠️ reconciliation chưa phân loại (§5.1) |
| parallel run + reconciliation against legacy outputs khi migrate | ✅ đã làm thật khi chuyển bootstrap in-process (#84): cùng count + checksum |

### 5.6 Semantic layer + MCP — nhu cầu ghép đôi

Bốn JD độc lập trong nhóm C đặt semantic layer và AI agent **trong cùng một yêu cầu**:

- Amanotes, Analytics Engineer: *"Own and evolve the enterprise semantic layer across BI platforms (e.g., Metabase) and AI agent endpoints, preventing metric drift through clear versioning, deprecation policies, and canonical definitions"* (`jd15.md:23`)
- Viettel Cyber Security, DE: *"Kinh nghiệm xây semantic layer / data model phục vụ AI Agent truy vấn (text-to-SQL, chuẩn MCP)"* (`jd14.md:2344`)
- CADDi: *"Own the core metric definitions and event taxonomy so everyone reports the same number"* (`jd14.md:2939`)
- Thế Giới Di Động, AI Data Engineer: *"Xây dựng API hoặc MCP Resource/Tool phục vụ AI Agent"* (`jd14.md:1510`)

Sau dedup, `semantic / metric layer` = **68 lần**, cao hơn `feature store` (42). Đây là khái niệm chưa-có của dự án có mật độ cao nhất sau khi bỏ bài lặp.

### 5.7 Nghịch lý use case (giữ, số đo mới)

```text
                raw   dedup
Customer 360     11     10      ← final project của cả khoá banking bootcamp
churn             9      8
CLV / LTV        26     14
```

Kết luận giữ nguyên: "tôi xây Customer 360 mart" yếu hơn "tôi xây SCD2 dimension với data contract, reconciliation và backfill an toàn".

---

## 6. Đối chiếu dự án ↔ thị trường

### 6.1 Đã khớp

| Yêu cầu | Mật độ C | Hiện trạng |
|---|---:|---|
| SQL · Python · Spark | 39,9 / 29,8 / 20,7 | ✅ |
| Data Quality · Anomaly · Freshness | 24,7 / 2,0 / 1,2 | ✅ 9 DQ check type, gồm `anomaly_detection`, freshness |
| Airflow · dbt | 15,4 / 11,6 | ✅ |
| Iceberg · Trino | 4,3 / 3,3 | ✅ |
| Kafka · CDC | 8,8 / 3,0 | ✅ Debezium + DLQ |
| Data Contract | 1,2 | ✅ 34 contract YAML |
| Backfill · Idempotency | 3,0 / 1,2 | ✅ `overwritePartitions` theo `cob_dt` |
| Lineage | 6,8 | ✅ OpenMetadata + `governance/lineage.py` |
| Runbook · RCA · Incident | 1,7 / 2,5 / 2,8 | ✅ `docs/04-operations/INCIDENT_RUNBOOK.md` S1–S8 + RCA |
| PII · RBAC | 2,5 / 3,1 | ✅ |

### 6.2 Khoảng trống thật — xếp lại theo số đo mới

| # | Khoảng trống | Căn cứ | Ưu tiên | Ghi chú |
|---|---|---|---|---|
| 1 | **Schema drift: additive vs breaking + chặn publish Gold** | tín hiệu bền 4,7× / 4,0× | **P1** | Vẫn mở từ ROADMAP 2.2. `additive`/`breaking` vẫn 0 lần trong code; DAG chỉ phủ 3 bảng |
| 2 | **Mart nợ quá hạn**: DPD → nhóm nợ, roll rate, vintage theo MOB | §5.3; 0/283 repo đối thủ có | **P1** | Xem điều kiện ngay dưới bảng |
| 3 | **Semantic layer**: định nghĩa metric chuẩn (NPL ratio, CASA ratio, late payment rate…), một nguồn cho dbt · Superset · API | tín hiệu bền 2,3× / 2,5×; dedup 68 | **P1** | dbt-core 1.12 đã có semantic models; không cần engine mới |
| 4 | **Reconciliation 5 lớp**: Matched / Mismatch / Missing / Duplicate / Pending | sát ngưỡng (B 1,8×, C 2,1×) + §5.1 | **P2** | Nâng DQ reconciliation từ pass/fail thành bảng kết quả phân loại |
| 5 | **dbt `sources.freshness` + `contract: enforced` + SQLFluff** | §5.5 (MoMo), freshness 6× ở C | **P2** | Rẻ (S). Gộp với ROADMAP 2.6 `dbt_expectations` |
| 6 | **Feature store** | 99 raw → **42 dedup** | **P2** (hạ từ P1) | Bản trước xếp cao nhất vì đếm raw; sau dedup đứng sau semantic layer |
| 7 | **MCP read-only trên semantic layer** | MCP 4,0× / 3,8× | **P3** | Chỉ làm **sau** #3; phải đi qua Trino ACL (ADR-0016), chỉ đọc serving |
| 8 | Detection Model (ML) + Alert routing | §5.2 | P2 | Giữ từ bản trước |
| 9 | CLV mart | CLV/LTV 26 raw / 14 dedup | P3 (hạ từ P2) | Mật độ thấp; phần lớn từ JD game/marketing |

**Điều kiện của #2**, vì nếu không nói rõ thì mart sẽ ra số vô nghĩa:

- Generator sinh trạng thái mỗi kỳ trả **độc lập** (`random.random()` mỗi tháng) và `days_late` tối đa 90. Mart DPD làm được ngay, nhưng **roll rate sẽ không có tín hiệu** (xác suất chuyển trạng thái = phân phối không điều kiện). Nhóm nợ 4–5 cũng không bao giờ xuất hiện.
- Muốn roll rate / vintage có nghĩa thì generator phải có **trạng thái trễ hạn nối tiếp giữa các kỳ** (xích Markov: đã trễ thì dễ trễ tiếp). Đó là việc của generator, không phải của mart.
- Nhóm nợ theo Thông tư 11/2021/TT-NHNN: nhóm 1 <10 ngày, nhóm 2 10–89, nhóm 3 90–179, nhóm 4 180–359, nhóm 5 ≥360; NPL = nhóm 3–5. Nhờ đó `npl_proxy` được thay bằng NPL đúng định nghĩa.

### 6.3 Không khuyến nghị (có số đo hỗ trợ)

| Hạng mục | Số đo | Lý do |
|---|---|---|
| **IFRS 9 / ECL** | 1 lần / 3,3M ký tự | Nghe "ngân hàng" nhưng thị trường tuyển dụng không hỏi |
| **OBT / One Big Table** | 0 / 3.310.578 ký tự | Giáo trình dạy, thị trường không nhắc một lần |
| **ClickHouse / StarRocks** | 3,8 / 0,7 (C) | Tín hiệu bền nhưng là **engine**; thêm engine OLAP không giải quyết vấn đề nào của dự án. Trino đã đóng vai serving |
| **Flink** | 5,7 → 3,5 | Giảm ở C; trùng Spark Structured Streaming |
| **MS Fabric / Snowflake** | về mức A ở C | Đỉnh ở B là đặc điểm mẫu (§2.3) |
| **MLflow / MLOps làm điểm bán** | MLflow 0 ở C | Thị trường đã chuyển sang "AI-ready data" |
| **Graph DB** | vài JD (Qode, ISB, Techcombank nêu "Graph databases" trong danh sách) | Không phải nhu cầu banking data platform chính |

---

## 7. Đính chính các phân tích trước

| Khẳng định cũ | Số đo 2026-09-29 | Kết luận |
|---|---|---|
| "AI-assisted dev tăng 3,4×", "MS Fabric 2,5×", "Snowflake 2,0×", "Iceberg 2,4×" (bản 09-21 §2.2) | C: 2,3 / 1,8 / 6,6 / 4,3, về mức A hoặc gần | **Rút lại.** Là đặc điểm mẫu B. Quy tắc mới: chỉ nhận tín hiệu ≥2× ở cả B và C |
| "Feature store 81 lần — cao nhất nhóm chưa có" | 99 raw, **42 dedup**; semantic layer 68 dedup | **Hạ P1 → P2.** Con số raw bị bài lặp thổi phồng |
| ROADMAP §2.5: "`runbook` 37 không tái lập được, đúng là 9" | Script: 36 trên đúng tập file cũ | **37 đúng, 9 sai.** Đã sửa trong ROADMAP |
| "RTO/RPO = 0" | vẫn 0 trên 3,31M ký tự | **Giữ.** |
| "`Data Vault` (44) nhiều hơn `Great Expectations` (19)" | 44 / 23 raw; 23 / 15 dedup; Data Vault giảm 1,7 → 0,5 | **Giữ thứ tự, nhưng Data Vault đang giảm.** Mapping doc hiện có là đủ |

---

## 8. Hành động đề xuất

### P1 — Ba hạng mục, mỗi cái có căn cứ độc lập

1. **Schema drift severity + quyền chặn** (ROADMAP 2.2). Tín hiệu bền nhất corpus. Primitive đã có trong `governance/schema_drift.py`.
2. **Mart nợ quá hạn** (`loan_delinquency`): DPD theo khoản vay từ `fact_loan_payment.days_late`, nhóm nợ theo TT 11/2021, roll-rate matrix theo tháng, vintage theo MOB. **Làm hai bước**: (a) generator có trạng thái trễ hạn nối tiếp; (b) mart. Làm (b) trước (a) là tạo ra con số không có nghĩa.
3. **Semantic layer**: dbt semantic models cho các metric ngân hàng đang được tính rải rác trong Gold SQL (NPL ratio, late payment rate, overdue rate…). Một định nghĩa, nhiều nơi dùng.

### P2

4. Reconciliation 5 lớp · 5. dbt freshness + contract enforced + SQLFluff + `dbt_expectations` · 6. Feature store · 7. Detection Model + Alert routing.

### P3

8. MCP read-only trên semantic layer (sau #3, qua Trino ACL) · 9. CLV mart.

### Không làm

IFRS 9/ECL · OBT · Flink · ClickHouse/StarRocks · MS Fabric/Snowflake · Kubernetes · thêm cloud vào repo này.

### Cách kể chuyện (giữ từ bản trước, bổ sung)

```text
NÊN DÙNG                                   THAY VÌ
data contract · schema drift (breaking)    "kiểm tra cấu trúc"
reconciliation (matched/mismatch/missing)  "đối soát số liệu"
backfill · idempotency · data replay       "chạy lại được"
runbook · incident · RCA                   "RTO/RPO"
semantic layer · canonical metric          "định nghĩa KPI"
DPD · nhóm nợ · roll rate · vintage        "dư nợ xấu (xấp xỉ)"
parallel run + reconciliation vs legacy    "đã test lại"
```

---

## 9. Ghi chú bảo trì

- Tài liệu đo trên corpus pin bằng MD5 ở **Mục 0**. Nếu `thamkhao/JD/` đổi, chạy lại `scripts/measure_jd_corpus.py` và cập nhật **toàn bộ** bảng, không cập nhật từng phần. Script từ chối chạy nếu có file `.md` chưa được xếp vào nhóm.
- Corpus nằm ngoài repo, không được version control. Các số ở đây là **`metric_type: manual`** theo `docs/evidence/metrics-manifest.yaml`: đo một lần, có phương pháp và ngày đo, **không** đưa vào `verify_readme_metrics.py`.
- Khi trích số từ tài liệu này: ghi ngày đo và nói rõ raw hay dedup.
