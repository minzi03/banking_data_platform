# Kế Hoạch Hoàn Thiện — Banking Data Platform

> **Lập ngày**: 2026-09-21 · `main` @ `b787616`
> **Căn cứ**: [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md) · [`BOOTCAMP_CURRICULUM_ANALYSIS.md`](BOOTCAMP_CURRICULUM_ANALYSIS.md) · [`COURSE_BASELINE_DIFF.md`](COURSE_BASELINE_DIFF.md) · [`REFERENCE_DATASET_ANALYSIS.md`](REFERENCE_DATASET_ANALYSIS.md) · [`technical-debt.md`](technical-debt.md)
> **Nguyên tắc xuyên suốt**: *Một test xanh chỉ có giá trị khi bản thân invariant là đúng.* Không làm gate xanh bằng cách làm yếu invariant.

---

## 0. Hiện trạng đã kiểm chứng

```text
main @ b787616   worktree sạch (trừ 6 file docs mới chưa commit)
427 file tracked · 170 .py · 51 test file
647 test collected (-m "not integration") → 646 passed / 1 skipped
17 nguồn → 10 dim + 6 fact → 14 Gold / 4 domain → 13 dbt serving
29 docker service · 20 DAG file · 10 CI job · 33 data contract · 9 DQ check type
```

**Nợ kỹ thuật:**

| | Trạng thái ghi trong doc | Thực tế đã kiểm chứng |
|---|---|---|
| TD-1 · TD-3 · TD-4 · TD-7 | fixed | ✅ khớp |
| TD-2 | disabled with reason | ✅ khớp |
| TD-6 | deliberate | ✅ khớp |
| **TD-5** | **open** | ⚠️ **thực chất đã xong** — `ruff \|\| true` đã gỡ, `test_shell_failure_propagation.py` tồn tại và pass (3 test). Các `\|\| true` còn lại đều nằm trên lệnh thu log chẩn đoán |

**Evidence manifest đang lệch:**

```text
manifest sinh từ : f7ce77e  (branch fix/materialize-risk-marts-and-dbt-tests, 2026-09-19)
HEAD hiện tại    : b787616  (2026-09-21)        → chênh 2 commit
test_functions   : 476 (manifest)  vs  598 (thực tế)   → lệch 122
```

`verify_readme_metrics.py` vẫn báo 22/22 vì nó so **README ↔ manifest**, không so manifest ↔ thực tế. Vòng lặp chỉ khép khi regenerate.

---

## 1. NGAY — dọn nền trước khi xây tiếp

Mục tiêu: không còn tuyên bố nào sai trong repo. Làm hết nhóm này trước khi thêm tính năng.

### 1.1 SỬA — `customer_360` phụ thuộc giả

`code_etl/gold/mart360/customer_360.yml` khai báo `silver.fact_online_transaction` ở `source.tables`, `upstream_flags`, `require_snapshots` — nhưng chuỗi `online` **không xuất hiện trong SQL**. Hệ quả: `assert_source_snapshots()` chặn job khi bảng đó thiếu partition, dù output không phụ thuộc.

Audit toàn bộ 14 Gold model: **1/14** — lỗi đơn lẻ.

- **Quyết định cần ra**: gỡ khai báo, *hoặc* thêm lại KPI kênh số vào SQL. Tra `git log` của file để biết cái nào đã xảy ra.
- **Kèm theo**: test governance `require_snapshots ⊆ bảng trong sql` — cùng họ `test_trino_catalog_contract.py`.
- **Size**: S · Đã có task riêng.

### 1.2 CẬP NHẬT — regenerate evidence manifest

```bash
py -3 scripts/generate_metrics_manifest.py --cob-dt <ngày> --scope full
```

- **Bắt buộc**: không dùng `--allow-dirty`. Manifest phải sinh từ worktree sạch, đúng `main`.
- **Acceptance**: `build.git_commit` = HEAD, `test_functions` = 598, `verify_readme_metrics.py` vẫn 22/22 sau khi cập nhật README.
- **Size**: S

### 1.3 CẬP NHẬT — đóng TD-5 trong `technical-debt.md`

Bằng chứng đã có: `ruff` không còn `|| true` trong job `lint`; `tests/governance/test_shell_failure_propagation.py` pass. Ghi status `fixed (2026-09-21)` kèm dẫn chứng, hoặc nêu rõ instance nào còn lại và vì sao chấp nhận được.

- **Size**: S

### 1.4 CẬP NHẬT — README nói ngôn ngữ thị trường

Đây là việc **rẻ nhất, tác động cao nhất** trong toàn kế hoạch. Năng lực đã có; chỉ sai nhãn.

```text
DẪN DẮT BẰNG                      THAY VÌ
data contract (33)                "schema validation"
reconciliation                    "đối chiếu số liệu"
schema drift / breaking change    "kiểm tra cấu trúc"
backfill · idempotency            "chạy lại được"
traceability                      "audit log"
runbook · incident response · RCA "RTO/RPO"        ← 0 lần trong 490 JD
degradation strategy              "xử lý lỗi"
```

Ngừng mở đầu bằng "Customer 360" (10 lần trong ~490 JD). Mở đầu bằng năng lực vận hành.

- **Ràng buộc**: mọi số trong README phải có binding trong manifest. Làm **sau** 1.2.
- **Size**: M

### 1.5 COMMIT — 6 file docs

```text
mới:  JD_MARKET_ANALYSIS.md · BOOTCAMP_CURRICULUM_ANALYSIS.md
      COURSE_BASELINE_DIFF.md · REFERENCE_DATASET_ANALYSIS.md · ROADMAP.md
sửa:  JD_FINAL_MARKET_ANALYSIS.md · JD_MARKET_ANALYSIS_REPORT.md  (banner superseded)
```

- **Size**: S

---

## 2. SẮP TỚI — đóng vòng lặp những gì đã có

Nguyên tắc nhóm này: **không thêm công nghệ mới**. Chỉ nối lại những mắt xích đã tồn tại nhưng đứt.

### 2.1 BỔ SUNG — `geo_velocity_flag` cho `aml_monitoring`

Việc có tỷ lệ giá trị/công sức cao nhất trong toàn kế hoạch.

**Vấn đề hiện tại** — ba thứ đang đứt cùng một chỗ:

```text
silver.dim_device    → 0 consumer (gold/ · dbt/ · ml/)
silver.dim_location  → 0 consumer
is_fraud             → tới Silver rồi dừng, không Gold/ML nào dùng
```

Và generator **cố tình** tạo tín hiệu location: 35% giao dịch fraud được gán vào high-risk location (5% tổng số location) → lift ≈ **7,7×**. Tín hiệu được sinh ra rồi bị vứt.

**Việc cần làm**: thêm flag đếm `COUNT(DISTINCT state)` trong cửa sổ 24h, join `silver.dim_location`.

Một thay đổi giải quyết bốn thứ:
- `dim_location` có consumer thật
- Dùng được tương quan đã tạo sẵn
- Thêm typology **geo-velocity** — chuẩn AML quốc tế, và là câu Q10 trong brief Risk Analyst
- Không cần dữ liệu mới, không cần service mới

**Acceptance**: `aml_monitoring` có `geo_velocity_flag`; `alert_score` cập nhật trọng số; regression test cho ca ≥N state trong 24h.

- **Size**: M

### 2.2 BỔ SUNG — schema drift: severity + quyền chặn

`governance/schema_drift.py` (231 dòng) **đã có sẵn primitive**: `added_columns`, `removed_columns`, `type_changes`, `detect_drift()`, `detect_drift_from_contract()`, `get_schema_diff()`, `compare_two_tables()`.

Ba thứ thiếu:

| Thiếu | Hiện trạng |
|---|---|
| Ngữ nghĩa severity | 3 loại thay đổi đang **ngang hàng**. Từ `additive`/`breaking` xuất hiện **0 lần** trong repo |
| Quyền chặn | `ops_schema_drift_dag` chạy 09:00 **sau** DQ như safety check — phát hiện nhưng **không chặn** Gold publish |
| Độ phủ | Chỉ 3 bảng: `dim_customer`, `dim_account`, `dim_loan` |

Đây là mở rộng logic của `assert_source_snapshots()` từ tầng partition sang tầng schema — cùng nguyên lý "chặn trước khi ghi", không phải "báo sau khi ghi".

**Thị trường**: `data contract` 2,3/100k (↑5,8×) · `schema drift` 1,3/100k (↑4,3×) — hai từ khoá tăng nhanh nhất corpus.

- **Size**: M

### 2.3 BỔ SUNG — `is_fraud` làm cột đối chứng trong `fraud_risk_txn`

**Không phải để huấn luyện** — để **đo rule hiện tại**: precision/recall của các flag rule-based so với ground truth.

Phép đo này hợp lệ vì nhãn của **repo** có tín hiệu thật (lift 7,7× theo location). Nhãn của hai dataset tham khảo thì **không** — xem [`REFERENCE_DATASET_ANALYSIS.md`](REFERENCE_DATASET_ANALYSIS.md) Mục 2.

Kết quả: một con số kiểm chứng được, thay vì "rule đã chạy".

- **Size**: S

### 2.4 SỬA — `fraud_reason` trong generator

Hiện `fraud_reason = random.choice(fraud_reasons)` — gán ngẫu nhiên, không khớp điều kiện đã kích hoạt. Hệ quả: reason mang tính trang trí, không dùng để kiểm chứng rule được. Đây đúng là điểm yếu của dataset tham khảo mà repo đang lặp lại.

**Sửa**: gán theo đúng nhánh đã chạy — `UNUSUAL_LOCATION` khi đổi sang high-risk location, `HIGH_AMOUNT` khi nâng amount.

- **Size**: S

### 2.5 BỔ SUNG — runbook + RCA

Khoảng trống **tài liệu hoá**, không phải năng lực. Viết cho 3 pipeline chính: batch Gold, CDC, serving.

Dùng đúng từ vựng thị trường: `runbook` (37 lần) · `incident response` (23) · `RCA` (64). **Không** dùng `RTO`/`RPO` — 0 lần trong toàn corpus JD.

- **Size**: M

### 2.6 BỔ SUNG — `dbt_expectations`

Repo có `dbt_utils`, `codegen`, `dbt_date` — chưa có `calogica/dbt_expectations`. Thêm một dòng vào `dbt/packages.yml` rẻ hơn nhiều so với dựng framework Great Expectations riêng, mà vẫn phủ được cụm từ khoá JD luôn nêu dạng *"dbt tests **hoặc** Great Expectations"*.

- **Size**: S

---

## 3. TƯƠNG LAI GẦN — mở rộng có căn cứ thị trường

### 3.1 BỔ SUNG — Feature store

`feature store` = **81 lần** trong corpus JD (2,6/100k) — cao nhất trong nhóm khái niệm dự án chưa có, và **không giáo trình nào trong 6 khoá dạy nó**.

Đã có sẵn `ml/pipeline/churn_prediction.py`, `credit_scoring.py`, `ml/monitoring/drift_detection.py` và 14 Gold mart. Việc cần làm là đóng gói thành feature view có versioning + point-in-time correctness, không phải xây từ đầu.

- **Size**: L

### 3.2 BỔ SUNG — CLV mart

Mart duy nhất trong benchmark top-3 của khoá học mà repo chưa có. `CLV/LTV` = 20 lần trong corpus JD. Cho `rfm_segment` một downstream consumer thật.

- **Size**: M

### 3.3 BỔ SUNG — bốn KPI ngân hàng còn thiếu

Từ brief Risk Analyst (15 câu hỏi ad-hoc):

| | KPI | Kỹ thuật |
|---|---|---|
| Q8 | Credit utilization 30 ngày | aggregate-ratio |
| Q9 | Dormant cards — mở >2 năm chưa dùng | left-join-null, date-arithmetic |
| Q13 | Outlier giao dịch theo MCC | percentile, window |
| Q15 | Heatmap state × merchant category | pivot, top-n-nested |

Đều rẻ, đều là KPI ngân hàng thật.

- **Size**: M (cả bốn)

### 3.4 BỔ SUNG — Detection Model + Alert routing

JD Fraud Detection (ngân hàng bán lẻ) mô tả chuỗi 5 bước:

```text
Data Architecture → Data Pipeline → Detection Engine → Detection Model
→ Risk Scoring → Dashboard & Alert
```

Repo đáp ứng 3/5. Thiếu **Detection Model** (ML-based, không chỉ rule) và **Alert routing** nghiệp vụ. Làm sau 2.3 — khi đã có phép đo precision/recall của rule để so sánh.

- **Size**: L

### 3.5 CÂN NHẮC — `branch_monthly_summary`

Model Gold duy nhất (1/14) không có dbt serving counterpart, và chưa có nơi nào ghi lý do.

Ba lựa chọn: (a) thêm serving model cho đủ 14/14 · (b) ghi rõ lý do grain chi-nhánh-theo-tháng không hợp serving snapshot · (c) gộp vào mart khác. **Bất kể chọn gì, phải ghi lại lý do** — hiện tại nó là một điểm lệch không giải thích.

- **Size**: S

### 3.6 BỔ SUNG — phân phối dữ liệu thực tế hơn

Theo `thamkhao/` (Xóm Bank) — bộ duy nhất có hình dạng phân phối thật:

```text
lỗi tổ hợp        "Bad PIN,Insufficient Balance" — repo hiện chỉ sinh đơn nhãn
refund âm         8.184/157.224 ≈ 5,2% — kiểm tra Gold mart có SUM lẫn vào doanh số không
amount lệch phải  mean 43,72 vs median 31,14 — repo dùng random.uniform → mean ≈ median
```

Mục **refund âm** đáng kiểm tra sớm: nếu mart đang cộng lẫn refund vào doanh số thì đó là lỗi số liệu, không phải vấn đề thẩm mỹ.

- **Size**: M

---

## 4. TƯƠNG LAI XA — và những gì KHÔNG nên làm

### 4.1 Dự án riêng, không nhồi vào repo này

| Hạng mục | JD | Lý do tách |
|---|---:|---|
| Một cloud lakehouse (AWS / Azure / Snowflake / Databricks) | 27,8 / 26,0 / 12,9 / 13,9 | Là câu chuyện riêng, làm loãng repo on-prem |
| Kubernetes | 9,4 | Chi phí cao; Docker Compose đã đủ kể chuyện |

### 4.2 KHÔNG làm — có số đo hỗ trợ

| Hạng mục | Số đo | Lý do |
|---|---:|---|
| **OBT / One Big Table** | **0** lần / 2.770.320 ký tự | Giáo trình dạy như topic chính; thị trường không nhắc một lần |
| **Flink** | 8,2 | Trùng Spark Structured Streaming đã có; Spark Real-Time Mode đang xoá dần lý do |
| **ClickHouse** | 5,2 | Thêm engine OLAP không giải quyết vấn đề nào hiện có |
| **Snowpipe / Auto Loader / DLT / Snowpark** | 0–4 | Vendor-specific, không chuyển giao |
| **Great Expectations đầy đủ** | 19 | `dbt_expectations` (2.6) đã phủ, rẻ hơn nhiều |
| Import `archive (9)`/`(10)` vào Postgres | — | Repo đã có 17 nguồn tương đương; generator kiểm soát tốt hơn |
| Huấn luyện mô hình fraud trên nhãn dataset tham khảo | — | Nhãn suy biến hoặc nhiễu thuần — kết quả sẽ là con số bịa |

### 4.3 Hạng mục cân nhắc dài hạn

| | JD | Ghi chú |
|---|---:|---|
| Oracle làm nguồn thứ hai | — | Template khoá học có service `oracle`; ABBANK JD yêu cầu. Chi phí cao, giá trị vừa |
| Data Vault 2.0 implement | 44 | Đã có `DATA_VAULT_MAPPING.md` (188 dòng) — **đủ để trả lời phỏng vấn**, chưa cần implement |
| RCA agent (propose-only, human approves) | `RCA` 64 · `MCP` 2,2 (↑3,7×) · AI-assisted dev 9,1 (↑3,4×) | Pattern 2026 rõ nhất. Hợp nguyên tắc dự án: đề xuất, người duyệt — không tự sửa |

---

## 5. Thứ tự thực thi

```text
NGAY        1.1 → 1.2 → 1.3 → 1.4 → 1.5
            (sửa lỗi → regenerate → đóng TD → đổi nhãn → commit)
            Điều kiện: không còn tuyên bố sai nào trong repo

SẮP TỚI     2.1 · 2.2 · 2.3 · 2.4   (đóng vòng lặp tín hiệu + contract)
            2.5 · 2.6               (tài liệu + package)
            Điều kiện: không thêm công nghệ mới

TƯƠNG LAI   3.5 → 3.6 → 3.3 → 3.2 → 3.1 → 3.4
            (rẻ và rõ trước; feature store và ML model sau)
```

**Hai ràng buộc không đổi trong mọi giai đoạn:**

1. Mọi con số công bố phải **đo được từ platform**, không phải từ log job. Nếu chưa đo được thì ghi `not_collected` — `not_collected ≠ verified`.
2. Không bao giờ làm một gate xanh bằng cách nới invariant. Nếu gate đỏ, sửa nguyên nhân hoặc ghi nhận là nợ có tên.

---

## 6. Ghi chú bảo trì

- Hiện trạng ở Mục 0 đo ngày 2026-09-21 trên `main` @ `b787616`. Sau khi thực hiện Mục 1, **các con số này sẽ đổi** — đo lại thay vì sửa tay.
- Các số thị trường trích từ [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md), phân loại `metric_type: manual`, không đưa vào `verify_readme_metrics.py`.
- Size S/M/L là **mức tương đối**, không phải ước lượng ngày công — cố ý, vì chưa có dữ liệu throughput để ước lượng ngày công một cách trung thực.
