# Phân Tích Thị Trường Data Engineer Việt Nam — Toàn Bộ Corpus JD

> **Ngày đo**: 2026-10-01 (thay bản 2026-09-29)
> **Nguồn chính**: 21 file JD Data/Engineering/Analytics (`jd1–jd21`) + 1 file benchmark repo + 2 sơ đồ kiến trúc đối thủ (`thamkhao/JD/`)
> **Nguồn bổ sung**: `jd22_BA.md` — 1 JD Business Analyst/Commercial Operations, được đọc và đo riêng, không trộn vào baseline Data Engineering A–D
> **Quy mô baseline cũ**: 4.010.665 ký tự (2.282.284 sau khi bỏ dòng lặp) · Aug–Sep 2026
> **Công cụ đo**: [`scripts/measure_jd_corpus.py`](../../scripts/measure_jd_corpus.py) — script hiện nhận diện `jd22_BA.md` như nhóm supplemental `BA`; các số A–D/ALL vẫn giữ ý nghĩa baseline cũ
> **Mục đích**: Xác định khoảng cách giữa Banking Data Platform và nhu cầu thị trường **bằng số đo, không bằng ước lượng**
> **Thay thế**: bản 2026-09-29 (16 file), `JD_FINAL_MARKET_ANALYSIS.md`, `JD_MARKET_ANALYSIS_REPORT.md`
> **Bổ sung 2026-10-01**: nhóm mẫu **D** (5 file scrape mới, 166 bài đăng); `jd16.md` được thay bằng bản scrape lớn hơn (113k → 260k byte). Tên file hiện tại của bài AE/BI là `jd20_AE_BI.md`; các trích dẫn cũ dùng `jd20_AE.md` là tên trước đó.
> **Kiểm chứng**: baseline A–D là phân tích định lượng chính; `jd22_BA.md` được bổ sung như một góc nhìn consumer/business, không dùng để suy ra mật độ nhu cầu Data Engineer.

### Phạm vi bổ sung — `jd22_BA.md`

`jd22_BA.md` là JD **Commercial Business Analyst (Việc Làm Tốt), Chợ Tốt**, không phải JD Data Engineer. Nội dung bổ sung tín hiệu thị trường về lớp tiêu thụ dữ liệu:

- SQL, BigQuery và Data Studio/BI cho sales dashboard, CRM, call-center và performance reporting.
- Theo dõi performance của sales team và commercial customers, cảnh báo supervisor, payment compliance và add-on service delivery.
- Năng lực được yêu cầu là chuyển dữ liệu thành business insight, visualization và phối hợp liên phòng ban.

Ứng dụng vào project: giữ Trino + Iceberg + dbt làm analytics layer thay cho việc thêm BigQuery; dùng Grafana/Superset/Streamlit theo persona; bổ sung metric owner, threshold, severity và action vào KPI/alert contract. Không coi một JD BA đơn lẻ là bằng chứng cho nhu cầu Data Engineering.

> **Lưu ý tái lập**: corpus hiện có `jd22_BA.md` và `jd20_AE_BI.md`; khi chạy trên Windows dùng `PYTHONIOENCODING=utf-8` hoặc `python -X utf8` để tránh lỗi console encoding.

---

## 0. Phương pháp đo (để tái lập được)

```bash
py -3 scripts/measure_jd_corpus.py ../thamkhao/JD          # bảng markdown
py -3 scripts/measure_jd_corpus.py ../thamkhao/JD --json   # JSON
```

Mọi con số là **số lần regex khớp trên văn bản đã decode UTF-8**. Regex nằm trong `TERMS` của script, kèm chú thích ở những chỗ regex trơn sẽ khớp nhầm (vd. `collection` khớp "data collection", `provisioning` khớp "provisioning services").

**Đơn vị**: `lần / 100.000 ký tự`, vì bốn nhóm mẫu có kích thước khác nhau.

**Hai chế độ đếm**:

- `raw` là văn bản nguyên trạng, cùng cách đo của bản 2026-09-21.
- `dedup` giữ mỗi dòng không rỗng **một lần** trong cả nhóm. Trang scrape lặp nguyên đoạn JD và "About us" của cùng công ty; ví dụ khối giới thiệu Renova Cloud lặp 4 lần trong `jd14.md`. Bỏ lặp làm corpus co từ 4,01M xuống 2,28M ký tự (−43%). **Một số đo chỉ đáng tin khi raw và dedup kể cùng một câu chuyện.**

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
| `jd16.md` | 259.769 | `e06e0db20cbba54978b1ca2738e7af02` | C |
| `jd17.md` | 275.296 | `190564d9d20e2d436e93bc5073f58ce7` | D |
| `jd18_DS.md` | 59.932 | `6253883601437258a96c17fc7e61e901` | D |
| `jd19_DA.md` | 190.683 | `b48d3f98b42303f4ee04968e5090fd13` | D |
| `jd20_AE_BI.md` | 42.960 | `1b1f077b32e41f581a7b48c3e9965ada` | D |
| `jd21.md` | 52.675 | `ee3ccf690661f6ba31127327dee0d43b` | D |
| `repo.md` | 21.361 | `2d90698282e15b3c3020c9bafc516436` | benchmark |

> **Hai file cũ đã đổi**: `jd16.md` (113.578 → 259.769 byte) và `repo.md` (19.635 → 21.361 byte). Vì `jd16` thuộc nhóm C, **mọi số của nhóm C trong tài liệu này khác bản 2026-09-29** — không phải vì thị trường đổi, mà vì mẫu C đổi kích thước. Đây chính là lý do mục 0 tồn tại.

**Bốn nhóm mẫu**:

| Nhóm | File | Ký tự | Sau dedup | Ghi chú |
|---|---|---:|---:|---|
| A | jd1–jd8 | 1.901.965 | 1.178.377 | Không đổi từ bản 2026-09-21 (MD5 khớp) |
| B | jd9–jd13 | 805.257 | 590.280 | Không đổi từ bản 2026-09-29 |
| C | jd14–jd16 | 735.436 | 635.597 | **`jd16.md` thay bản scrape lớn hơn** (+146k ký tự) |
| D | jd17–jd21 | 568.004 | 495.707 | **Mới 2026-10-01**: 5 file bundle, 166 bài đăng |

> ⚠️ **Giới hạn phương pháp**
>
> Bốn nhóm cùng khung Aug–Sep 2026. Chúng là **bốn mẫu độc lập, KHÔNG phải chuỗi thời gian**, nên "bội số" nghĩa là *mật độ cao hơn ở mẫu đó* và có thể do thành phần mẫu. Chỉ coi là tín hiệu thật khi khác biệt **≥2× ở ít nhất HAI nhóm so với A** (nâng từ "cả B lẫn C" ở bản trước, vì giờ có bốn mẫu).
>
> Đếm regex đo **cường độ nhắc đến**, không đo "% số JD yêu cầu". Tài liệu này không dùng cách diễn đạt "~X% JD yêu cầu".

---

## 1. Tổng quan thị trường

### 1.1 Thành phần vai trò — lý do nhóm C và D phải đọc khác

Phân loại theo tiêu đề bài đăng (dòng ngay sau header "`<công ty> logo`"). Các file có header: jd10–16 (B, C) và **jd17–21 (D)**.

```text
                     Nhóm B (jd10–13)   Nhóm C (jd14–16)   Nhóm D (jd17–21)
bài có header              153                106                166
DE / Platform          117  (76%)          52  (49%)          68  (41%)
DA / BI / BA            14   (9%)          38  (36%)          60  (36%)
Architect               10   (7%)           4   (4%)           6   (4%)
Analytics Engineer       3   (2%)           4   (4%)           6   (4%)
DS / ML / AI             2   (1%)           3   (3%)          18  (11%)
Governance · DBA · khác  7   (5%)           5   (5%)           8   (5%)
```

Hai nhóm mới (C và D) đều có **tỷ trọng DA / BI cao gấp ~4 lần nhóm B**. Nhóm B (jd10–13) chủ yếu là bài DE thuần; C và D trộn lẫn nhiều vai trò phân tích. Nhóm D còn có **DS / ML / AI cao bất thường (11%)** vì `jd18_DS.md` và phần lớn `jd19_DA.md` là bài Data Scientist / Data Analyst chứ không phải DE.

**Hệ quả**: mọi thứ tăng ở C hoặc D mà gắn với vai trò DA/BI/DS đều phải coi là **hiệu ứng thành phần mẫu**, chưa phải xu hướng thị trường DE: Power BI, Tableau, Looker/Metabase, A/B test, DS/ML, và phần lớn thuật ngữ nghiệp vụ tín dụng. Đây là lý do §2 tách bảng "nền tảng" (ổn định qua cả bốn nhóm) khỏi bảng "tín hiệu" (phải kiểm tra thành phần mẫu trước khi kết luận).

### 1.2 Nhà tuyển dụng khối tài chính — nhóm C + D

```text
Ngân hàng      Techcombank ×6 · PVcomBank ×4 · VietinBank ×2 · BIDV · ABBANK · TPBank
               CIMB · NCB · VPBank ×2 · Timo (BVBank) · SeABank · Eximbank · ACB
               ANZ (qua HCLTech) · NAB ×2 · Cake Digital Bank ×3
Tài chính TD   FE CREDIT ×2 · Mcredit ×3 · Home Credit · F88 · GoTymeX · KALAPA
               Kredivo Group ×2 · Mirae Asset Finance
Ví / fintech   MoMo ×4 · Zalopay ×2 · Monee (ShopeePay) ×2 · Gimo ×2 · WeCopy Fintech
Thanh toán     NAPAS · (MoMo/Zalopay)
Chứng khoán    SSI Securities · VPBankS · Binance
```

Khối tài chính chiếm khoảng **1/3 bài có header** ở cả nhóm C và D. **VietinBank** (Data Architect, Deputy Manager BI) và **SSI Securities** (Senior DE, mô tả lakehouse stack gần như trùng dự án) là hai bài mới đáng chú ý nhất của nhóm D. **NAPAS** (Data Engineer, Hà Nội) yêu cầu đúng những gì dự án có: data dictionary, data lineage, phân quyền truy cập, data ingestion theo từng loại dữ liệu.

### 1.3 Lương (trích nguyên văn)

Tuyệt đại đa số bài đăng nhóm C ghi **"Thoả thuận"**. Những điểm có số:

| Vị trí | Mức | Nguồn |
|---|---|---|
| Data Tester, Hà Nội | 22–30M/tháng | `jd14.md:1000` |
| Data Engineer (GEM, onsite) | up to 30M | `jd14.md:3520` |
| Analytics Engineer (Joon Solutions) | 20–40M | `jd14.md:3452` |
| DE/DA (SiciX) | lương cứng 20–40M | `jd16.md:795` |
| Data Engineer (Good English), FPT Software | up to 75M | `jd15.md:4512` |
| Analytics Engineer (Amanotes) | "gói thu nhập 15–18 tháng lương/năm" | `jd15.md:453-455` |
| Senior DE (Banking project), Reco Manpower | "annual budget up to VND 960M" (~80M/tháng) | `jd17.md:1177` |
| Data Engineer, Reco Manpower (247Express) | "Data Engineer – upto 100M" | `jd17.md:209` |
| Operations Data Analyst, Tuyển Dụng 247 | "up to 15,000,000 gross/month" | `jd17.md:95` |

Kết luận của bản trước vẫn đúng và **mạnh hơn**: **đòn bẩy lương rõ nhất là tiếng Anh + cloud + ngân hàng**, không phải thêm công cụ. Nhóm D có bài DE ngân hàng thuần (Reco Manpower, "Banking project") đạt ~80M/tháng, trong khi bài DA vận hành fresher chỉ 15M — khoảng cách 5× nằm ở **domain + level**, không ở số lượng công nghệ liệt kê.

### 1.4 Chứng chỉ và khung chuẩn

Toàn corpus 4,01M ký tự, raw (dedup trong ngoặc): `DAMA / DCAM / CDMP` = 59 (20) · `BCBS 239 / Basel` = 23 (11) · `Decree 13 / PDPL` = 60 (28).

Nhóm C thêm một yêu cầu cụ thể: **Nghị định 13 & 356** cho Data Governance (Golden Gate). Chuẩn **DAMA-DMBOK + DCAM + BCBS 239 + lưu trú dữ liệu theo SBV/NHNN** xuất hiện trong cùng một JD kiến trúc giải pháp ngân hàng (PVcomBank, `jd14.md:1921`).

Nhóm D bổ sung một bài nêu **thẳng tên luật** ở cấp kỹ thuật — hiếm trong corpus: VPBank *Audit Data Analytics Specialist* (`jd17.md:2452`) yêu cầu tuân thủ **Luật An ninh mạng + Nghị định 13/2023/NĐ-CP (PDP)** khi trích xuất/xử lý dữ liệu kiểm toán, và áp dụng cụ thể **Data Masking · Anonymization · Encryption · RBAC**. Đây là mô tả gần nhất với chuỗi `pii_masking + Trino ACL + RBAC_MATRIX + audit` của dự án.

---

## 2. Tech stack — bảng mật độ đo được

Đơn vị: lần/100k ký tự, raw. Bảng đầy đủ 88 thuật ngữ: chạy script.

### 2.1 Nền tảng không đổi qua cả bốn nhóm

Đây là nhóm công nghệ có mật độ **ổn định ở cả A, B, C, D** — không phải đặc điểm của một mẫu. Sắp theo mật độ nhóm D.

| Công nghệ | A | B | C | D | Trạng thái dự án |
|---|---:|---:|---:|---:|---|
| SQL | 37,0 | 50,3 | 40,1 | 43,5 | ✅ |
| Python | 26,6 | 36,1 | 29,8 | 29,9 | ✅ |
| Data Quality | 21,2 | 29,9 | 25,7 | 22,2 | ✅ 9 DQ check type |
| Spark / PySpark | 23,6 | 30,2 | 21,2 | 20,2 | ✅ |
| AWS | 21,3 | 28,6 | 21,6 | 20,2 | ⚠️ MinIO (S3-compatible) |
| Azure | 15,1 | 27,6 | 12,2 | 16,0 | ⚠️ không có (cloud-neutral) |
| Power BI | 11,1 | 9,6 | 13,1 | 17,3 | ❌ Superset thay thế (BI layer) |
| Databricks | 12,3 | 14,5 | 10,2 | 14,3 | ❌ (Iceberg tự dựng thay thế) |
| GCP / BigQuery | 17,0 | 19,2 | 16,6 | 12,3 | ❌ |
| Airflow | 13,4 | 22,1 | 15,1 | 11,6 | ✅ |
| Data Governance | 16,2 | 16,9 | 16,2 | 9,2 | ✅ |
| Lakehouse | 11,2 | 17,6 | 12,0 | 9,7 | ✅ |
| CI/CD | 11,9 | 16,0 | 10,7 | 8,8 | ✅ |
| Tableau | 5,7 | 4,5 | 6,7 | 8,5 | ❌ Superset thay thế |
| dbt | 7,1 | 12,5 | 11,4 | 8,3 | ✅ serving layer |
| Snowflake | 6,9 | 14,8 | 5,8 | 7,7 | ❌ |
| Kafka | 11,9 | 15,8 | 8,6 | 6,0 | ✅ Debezium CDC |
| Oracle | 9,0 | 8,3 | 6,0 | 4,0 | ❌ (PostgreSQL source) |
| Docker | 5,5 | 7,2 | 4,6 | 2,8 | ✅ |
| Iceberg | 3,2 | 7,6 | 4,4 | 2,3 | ✅ REST catalog |
| Trino / Presto | 3,0 | 5,0 | 2,7 | 1,4 | ✅ |

**Đọc bảng này thế nào**: AWS/Azure/GCP, Power BI/Tableau, Databricks/Snowflake, Oracle là **nền tảng thay thế** — thị trường nhắc nhiều vì mỗi công ty chọn một bộ. Dự án chọn bộ open-source tương đương (MinIO↔S3, Superset↔Power BI, Iceberg+Spark↔Databricks, PostgreSQL↔Oracle). Đây không phải gap về năng lực, mà là **khác lựa chọn công nghệ** — cần nói được ánh xạ, không cần thay stack (§6.3).

### 2.2 Tín hiệu **bền**: ≥2× so với A ở **ít nhất hai** nhóm

Nhóm đáng tin nhất của tài liệu: nhiều mẫu độc lập cùng lệch về một hướng. Bản 2026-09-29 dùng quy tắc "cả B lẫn C"; nay có bốn mẫu nên nâng thành "≥2 nhóm".

| Khái niệm | A | B | C | D | Bội số B/C/D | Dự án |
|---|---:|---:|---:|---:|---|---|
| **Semantic / metric layer** | 1,5 | 3,5 | 3,5 | **7,6** | 2,3× / 2,3× / **5,1×** | ❌ (đã có `dbt/models/semantic/` — xem §5.6) |
| **Schema drift / evolution** | 0,3 | 1,4 | 1,2 | 0,9 | 4,7× / 4,0× / **3,0×** | ✅ nay đã phân loại additive/breaking |
| **Anomaly detection** | 0,8 | 2,9 | 1,9 | 1,9 | 3,6× / 2,4× / 2,4× | ✅ DQ check `anomaly_detection` |
| **Payment / settlement / ledger** | 0,7 | 1,7 | 2,9 | 1,6 | 2,4× / 4,1× / 2,3× | ⚠️ nghiệp vụ, chưa có mart |
| **Freshness** | 0,2 | 0,6 | 1,4 | 1,1 | 3,0× / 7,0× / 5,5× | ✅ `freshness_exporter` + `ops_schema_drift_dag` (n nhỏ) |
| **Data Contract** | 0,4 | 2,5 | 1,1 | 0,5 | 6,2× / 2,8× / 1,2× | ✅ 34 contract YAML (`governance/datasets/`) |
| **MCP** | 0,4 | 1,6 | 2,3 | 0,7 | 4,0× / 5,7× / 1,7× | ❌ (có mặt ở CẢ 4 nhóm — xem đính chính §11) |
| **ClickHouse** | 1,8 | 5,8 | 4,2 | 2,5 | 3,2× / 2,3× / 1,4× | ❌ (không khuyến nghị, xem §6.3) |
| **Backfill** | 1,2 | 3,2 | 2,7 | 1,1 | 2,7× / 2,3× / 0,9× | ✅ qua `cob_dt` |
| Reconciliation / đối soát | 1,9 | 3,5 | 4,0 | — | 1,8× / 2,1× / thấp | ⚠️ có, chưa phân loại kết quả |

**Đọc bảng này thế nào**: `Semantic / metric layer` giờ là **tín hiệu mạnh nhất và duy nhất tăng ở cả ba nhóm mới**, đặc biệt nhóm D (7,6/100k — gấp 5× nhóm A, mật độ cao nhất trong mọi khái niệm "chưa-có" của dự án). Về `MCP`: mật độ nhóm D chỉ 0,7 (thấp hơn B 1,6 và C 2,3), nhưng **kiểm chứng 2026-11 cho thấy MCP xuất hiện ở cả bốn nhóm** — `jd11.md:180` (B), `jd12.md:674` (B), `jd13.md:87` (B), `jd14.md:1360/1404/1456/1510/1859/2344` (C), `jd15.md:17/26/59` (C), `jd16.md:3829` (C), **`jd17.md:1973` và `jd17.md:4122` (D)**, **`jd20_AE.md:310` (D)**. Nên kết luận cũ "D không lặp lại" là **sai ở mức hiện diện** (chỉ đúng ở mức mật độ). Vì nhóm D ghép MCP **cùng** semantic layer trong một yêu cầu (`jd17.md:1329-1330`), hai khái niệm này vẫn nên làm liền nhau, không tách ưu tiên — xem đính chính §11.

`Data Contract` và `Backfill` là tín hiệu **của mẫu B** nhiều hơn: cả hai tụt về gần mức A ở D.

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

**Đếm lại 2026-11** trên `repo.md` hiện tại: **290** repo unique (`github.com/…`), **149** repo có `lakehouse / medallion / iceberg / delta` trong tên (≈51%), CDC **12**, banking/finance **25**, fraud **3**, AML **1**. (Con số 137/283 của bản 2026-10-01 thấp hơn một chút — lệch do cách đếm sub-path.) Điểm khác biệt của dự án vẫn nằm ở những thứ **không repo nào nêu**:

| Năng lực | 290 repo đối thủ | Dự án này |
|---|---|---|
| Medallion + Iceberg + Trino | phổ biến (149 repo) | ✅ |
| CDC Debezium | 12 repo | ✅ |
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

Trong file `jd14.md` có một mô tả **reconciliation cụ thể hơn mọi JD khác** — nhưng lưu ý đây là **một bài của nhà tuyển dụng KHÁC**, không phải ABBANK (xem đính chính §11):

> "Xây dựng quy trình ETL/ELT: Extract → Transform → Validate → Load → Reconcile." (`jd14.md:605`)
> "Xây dựng cơ chế Data Reconciliation, phân loại Matched / Mismatch / Missing / Duplicate / Pending." (`jd14.md:609`)

Khối `jd14.md:603-619` (DMS/OMS/ERP/WMS, Ecommerce, FMCG/Retail/Distribution, SQL Server, Excel/VBA/Power Query) là ad **Data Integration ngành bán lẻ/phân phối** bị **mất dòng header công ty** khi scrape — nó nằm trong vùng văn bản của ABBANK nhưng không thuộc ABBANK. Stack ABBANK thật nằm ở `jd14.md:39-41`. Giá trị dùng được: đây là biến thể yêu cầu reconciliation của **thị trường nói chung**, không phải của riêng ngân hàng.

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

| # | Khoảng trống | Căn cứ | Ưu tiên | Trạng thái kiểm chứng 2026-11 |
|---|---|---|---|---|
| 1 | **Schema drift: additive vs breaking + chặn publish Gold** | tín hiệu bền 4,7× / 4,0× | ~~P1~~ | ✅ **ĐÃ XONG** — `governance/schema_drift.py` có `NONE/ADDITIVE/BREAKING`, CLI `exit 1` khi BREAKING; `tests/governance/test_schema_drift_severity.py` |
| 2 | **Mart nợ quá hạn**: DPD → nhóm nợ, roll rate, vintage theo MOB | §5.3; 0/290 repo đối thủ có | ~~P1~~ | ✅ **ĐÃ XONG** — `serving/loan_delinquency_current.sql` (dpd_bucket/debt_group/prev_dpd_bucket/vintage_month/is_npl) + `loan_roll_rate_current.sql` + `loan_vintage_current.sql` |
| 3 | **Semantic layer**: định nghĩa metric chuẩn (NPL ratio, CASA ratio, late payment rate…), một nguồn cho dbt · Superset · API | tín hiệu bền 2,3× / 2,5×; dedup 68 | ~~P1~~ | ✅ **ĐÃ XONG** — `dbt/models/semantic/_semantic_models.yml` (MetricFlow): semantic models `loan_delinquency`, `loan_portfolio_risk`; metrics `npl_ratio`, `late_payment_rate`, `ever_30_plus_loans` |
| 4 | **Reconciliation 5 lớp**: Matched / Mismatch / Missing / Duplicate / Pending | sát ngưỡng (B 1,8×, C 2,1×) + §5.1 | **P2** | ❌ **VẪN MỞ** — chỉ có `code_etl/cdc/reconcile_cdc.py` (pass/fail) |
| 5a | **dbt `contract: enforced`** | §5.5 (MoMo) | ~~P2~~ | ✅ **ĐÃ XONG** — `contract:` trong `dbt/models/serving/_serving_models.yml` |
| 5b | **dbt `sources.freshness`** | freshness 6× ở C | P2 | ❌ **VẪN MỞ (có chủ đích)** — `_gold_sources.yml` không có block `freshness:`; comment trong serving models giải thích Trino `createView` = NOT_SUPPORTED nên đây là quyết định đã ghi lại, không phải thiếu sót |
| 5c | **SQLFluff** | §5.5 (MoMo) | P2 | ❌ **VẪN MỞ** — CI lint chỉ chạy `ruff` (Python), không có `.sqlfluff` |
| 6 | **Feature store** | 99 raw → **42 dedup** | **P2** | ❌ **VẪN MỞ** |
| 7 | **MCP read-only trên semantic layer** | MCP có ở cả 4 nhóm (§11) | **P3** | ❌ **VẪN MỞ** — nhưng nên làm **liền với #3** vì nhóm D ghép đôi (`jd17.md:1329-1330`) |
| 8 | Detection Model (ML) + Alert routing | §5.2 | P2 | ⚠️ **MỘT PHẦN** — có `serving/fraud_risk_txn_current.sql` + `serving/aml_monitoring_current.sql`; ML alert routing chưa xác nhận |
| 9 | CLV mart | CLV/LTV 26 raw / 14 dedup | P3 | ⚠️ chưa xác nhận trong serving list |

> **Ghi chú về trạng thái**: mục #1/#2/#3 và 5a của bản 2026-10-01 đã được implement trong khoảng **72 commit** sau ngày đo. Bảng trên giữ nguyên *căn cứ thị trường* (không đổi) và chỉ cập nhật cột trạng thái. Ba mục còn mở rẻ và cụ thể nhất: **#4 (reconciliation 5 lớp), #5b (freshness), #5c (SQLFluff)**.

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
| Toàn bộ tài liệu này (bản 2026-10-01) | Kiểm chứng 2026-11: 5 lỗi + trạng thái code | **Xem §11.** Sửa §1.3 (Amanotes≠VPBank), §5.1 (khối reconciliation ≠ ABBANK), §2.2 (MCP có ở cả 4 nhóm), §4 (repo 290/149), §0 (C dedup 635.597) |

---

## 8. Hành động đề xuất

> **Cập nhật 2026-11**: cả ba hạng mục P1 của bản 2026-10-01 **đã được implement** (xem §6.2 và §11). Danh sách dưới đây giữ nguyên làm hồ sơ; mục còn mở nằm ở P2/P3.

### P1 — Ba hạng mục, mỗi cái có căn cứ độc lập

1. ~~**Schema drift severity + quyền chặn** (ROADMAP 2.2)~~ — ✅ đã xong (`governance/schema_drift.py`).
2. ~~**Mart nợ quá hạn** (`loan_delinquency`)~~ — ✅ đã xong (`loan_delinquency_current` + `loan_roll_rate_current` + `loan_vintage_current`).
3. ~~**Semantic layer**~~ — ✅ đã xong (`dbt/models/semantic/`, MetricFlow).

### P2 — còn mở

4. **Reconciliation 5 lớp** · 5b. **dbt `sources.freshness`** · 5c. **SQLFluff** (+ `dbt_expectations`) · 6. **Feature store** · 7. Detection Model + Alert routing (⚠️ `fraud_risk_txn`/`aml_monitoring` đã có, ML alert chưa).
   *(5a `contract: enforced` đã xong.)*

### P3 — còn mở

8. **MCP read-only trên semantic layer** — nên làm **liền mạch với #3** (đã xong), vì nhóm D ghép đôi MCP + semantic layer trong cùng yêu cầu (`jd17.md:1329-1330`). Phải đi qua Trino ACL (ADR-0016), chỉ đọc serving · 9. CLV mart.

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

---

## 11. Đính chính và trạng thái (kiểm chứng 2026-11)

Đợt kiểm chứng #2 chạy lại `scripts/measure_jd_corpus.py` (22/22 MD5 khớp với §0) và kiểm từng trích dẫn bằng `grep` + đọc cửa sổ nhỏ. Kết quả:

### 11.1 Năm lỗi đã sửa

| # | Mục | Lỗi bản 2026-10-01 | Đã sửa thành |
|---|---|---|---|
| 1 | §1.3 | Gán "15–18 tháng thu nhập/năm" cho **VPBank** (`jd15.md:455`) | Là **Amanotes** Analytics Engineer (`jd15.md:453-455`). VPBank có trong jd15 nhưng ở `jd15.md:308-310` |
| 2 | §5.1 | Coi khối reconciliation (`jd14.md:605-609`) là "lồng trong khối ABBANK" | Là **ad bán lẻ/FMCG riêng biệt** (DMS/OMS/ERP/WMS, Excel/VBA/Power Query) mất header công ty. Stack ABBANK thật ở `jd14.md:39-41` |
| 3 | §2.2 | "MCP **không lặp lại** ở D" | MCP **có mặt ở cả 4 nhóm**: `jd17.md:1973`, `jd17.md:4122`, `jd20_AE.md:310`. Chỉ *mật độ* thấp ở D, không phải *vắng mặt* |
| 4 | §4 | 283 repo · 137 có lakehouse/iceberg | **290** repo · **149** có lakehouse/iceberg (≈51%) · CDC **12** |
| 5 | §0 | Nhóm C dedup = 595.597 | **635.597** (đảo chữ số) |

### 11.2 Trạng thái code — bản 2026-10-01 đã cũ

Giữa ngày đo (2026-10-01) và đợt kiểm chứng có **72 commit**. Nhiều "khoảng trống P1" đã được lấp. Bảng đầy đủ ở **§6.2**. Tóm tắt:

- ✅ **Đã xong**: schema drift severity (#1) · mart nợ quá hạn + roll rate + vintage (#2) · semantic layer MetricFlow (#3) · dbt `contract: enforced` (5a).
- ❌ **Còn mở**: reconciliation 5 lớp (#4) · `sources.freshness` (5b, có chủ đích vì Trino `createView` không hỗ trợ) · SQLFluff (5c) · feature store (#6) · MCP read-only (#7) · CLV mart (#9).
- ⚠️ **Một phần**: Detection Model + Alert routing (#8) — `serving/fraud_risk_txn_current.sql` và `serving/aml_monitoring_current.sql` đã có.

### 11.3 Công ty tài liệu bỏ sót

Đợt kiểm chứng cho thấy **jd12.md và jd13.md gần như chưa được phân tích** (~40 công ty vắng mặt). Đáng chú ý nhất:

- **GreenNode (VNG AI cloud)** `jd13.md:1463-1498` — charter "business ontology / metric layer from scratch / semantic layer cho BI + AI agent qua MCP", nêu tên **"dbt Semantic Layer, Cube, LookML, MCP servers"** (`jd13.md:1493`). Đây là tín hiệu semantic-layer/MCP **mạnh hơn** mọi thứ bản 2026-10-01 trích.
- **MBBank** `jd14.md:4171` — không có trong danh sách ngân hàng §1.2.
- **SalesNow** `jd13.md:2231-2289` (remote, "SalesNow MCP used from Claude/Claude-Sonnet", dbt, RAG).
- **Qode.World** `jd14.md:3276` (graph models, vector DB) · **Golden Gate** `jd14.md:1085/2599` · **Asia White Collar** `jd14.md:1840` (MCP Platform) · **Athena AI, Upskills, CADIVI, Galaxy Holdings, Central Retail, EPOS, Saint-Gobain, Joon Solutions, GEM Careers, Amaris, LG CNS, WeCopy Fintech, FPT Telecom, Viettel Post, Renova Cloud** · jd12: **PVcomBank ×2, ACB, LPBank, GPBank, SiciX, SmartOSC, MiTek, NashTech, Elfie, Maple Labs, I-Konect, FPT Software Innovation, KIMEI, HEINEKEN ×2, Starbucks, ETC Recruiter**.

### 11.4 Đã xác nhận ĐÚNG (không cần bàn lại)

- §2.3 "AI-assisted dev đỉnh ở nhóm B" — **rất mạnh**: `jd9.md` một mình 16 hits (Cursor/Claude/Copilot), `jd10.md` 4 hits. jd9 là nguồn dày nhất toàn corpus.
- Trích dẫn Mcredit (`jd14.md:1815,1835`), Talentnet (`:2787`), Zalopay (`:1588`), BIDV (`:3246`), MoMo (`:3538`), MWG/Thế Giới Di Động (`:1456`), Viettel Cyber Security (`:2344`) — **nguyên văn chính xác**, đúng tên công ty.
- SiciX "lương cứng 20–40M" (`jd16.md:795`) — chính xác.
- Mật độ theo nhóm (MCP 0,4/1,6/2,3/0,7 · semantic layer 1,5/3,5/3,5/**7,6** · feature store 3,0/2,9/2,4/0,5 · data product 3,1/2,7/3,7/8,1 · vector 4,7/3,1/8,4/6,3) — **khớp** một lần chạy JSON trực tiếp.

### 11.5 Cách chạy lại trên Windows

```bash
PYTHONIOENCODING=utf-8 PYTHONUTF8=1 py -3 scripts/measure_jd_corpus.py ../thamkhao/JD
```

Console mặc định là cp1258 nên nếu không đặt `PYTHONIOENCODING` sẽ lỗi `UnicodeEncodeError` ở dòng có tiêu đề tiếng Việt — đây là lỗi môi trường, không phải lỗi logic.
