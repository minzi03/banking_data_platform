# Kế Hoạch Bộ Tài Liệu — Banking Data Platform

> **Lập ngày**: 2026-09-21 · `main` @ `b787616` (+ nhánh `fix/declared-sources-match-sql`)
> **Mục đích**: Dựng khung sườn tài liệu hoàn chỉnh, rồi triển khai theo khung đó
> **Liên quan**: [`ROADMAP.md`](ROADMAP.md) — kế hoạch kỹ thuật · [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md) — căn cứ thị trường

---

## 0. Hiện trạng đo được

**30 file `.md` tracked**, tổng ~9.000 dòng:

```text
gốc repo        README.md 1859 · DEMO_GUIDE.md 504 · RUNBOOK.md 313
                ARCHITECTURE.md 249 · CHANGELOG.md 135

docs/ (18 file) data-output-documentation 733 · data-input-documentation 715
                JD_MARKET_ANALYSIS_REPORT 601 · DATA_INPUT_BASE_REPORT 581
                JD_FINAL_MARKET_ANALYSIS 473 · JD_MARKET_ANALYSIS 396
                BOOTCAMP_CURRICULUM_ANALYSIS 367 · AI_GOVERNANCE_FRAMEWORK 366
                REFERENCE_DATASET_ANALYSIS 354 · architecture-image-prompt 329
                ROADMAP 309 · technical-debt 287 · DBT_DEPLOYMENT 282
                COURSE_BASELINE_DIFF 280 · p2-observability-design 279
                DATA_VAULT_MAPPING 188 · session-report-2026-08-08 188
                cdc-pipeline 109

module-level    api/ · dbt/ (README + SUMMARY) · code_etl/cdc/ · openmetadata/ · demo/
.github/        PULL_REQUEST_TEMPLATE · ISSUE_TEMPLATE/bug_report
```

### 0.1 Benchmark: 63 repo đối thủ đã clone về máy

`thamkhao/project_thamkhao*/` chứa 63 repo clone từ danh sách `repo.md`.

```text
số md mỗi repo        repo    tỷ lệ
  1 (chỉ README)       14     22%
  2–5                  22     35%
  6–20                 19     30%
  20+                   8     13%   ← dự án này nằm ở đây

có thư mục docs/       30/63  48%
```

**Kết luận: dự án KHÔNG thiếu tài liệu về số lượng — đã thuộc nhóm 13% đầu.** Vấn đề nằm ở ba chỗ khác:

| Vấn đề | Biểu hiện |
|---|---|
| **Không có bản đồ** | 30 file, 0 mục lục. Người đọc không biết bắt đầu từ đâu |
| **Thiếu theo LOẠI** | Có nhiều tài liệu *mô tả*, thiếu tài liệu *quyết định*, *tra cứu*, *quy trình sự cố* |
| **Quyết định nằm trong comment code** | 33 chỗ giải thích "Vì sao / Lý do" nằm rải trong 20 file `.py`/`.yml`/`.sql`, không ai đọc code để tìm |

### 0.2 Đính chính cho `ROADMAP.md` mục 2.5

`RUNBOOK.md` (313 dòng) **đã tồn tại** — nhưng là **runbook vận hành** (start/stop service, chạy ETL, query, troubleshoot service không lên), **không phải runbook sự cố dữ liệu**. Không có kịch bản: partition thiếu, DQ fail, schema drift, CDC lag, backfill sai.

Hai thứ khác nhau, cần tách rõ — xem §3 nhóm C.

---

## 1. Nguyên tắc thiết kế

**① Phân theo người đọc, không theo chủ đề kỹ thuật.** Mỗi tài liệu phải trả lời được: *ai mở nó, trong tình huống nào?* Tài liệu không có người đọc cụ thể sẽ không được bảo trì.

**② Bốn loại tài liệu, đừng trộn** (Diátaxis):

```text
Tutorial     học lần đầu        → DEMO_GUIDE, ONBOARDING
How-to       giải quyết 1 việc  → RUNBOOK, INCIDENT_RUNBOOK, DBT_DEPLOYMENT
Reference    tra cứu            → DATA_DICTIONARY, GLOSSARY, API, contracts
Explanation  hiểu vì sao        → ARCHITECTURE, ADR, các bản phân tích
```

README hiện 1.859 dòng đang làm cả bốn việc — đó là lý do chính khiến nó khó dùng.

**③ Một sự thật, một chỗ.** Số liệu chỉ sống trong `metrics-manifest.yaml`; tài liệu *chiếu* từ đó. Đã có cơ chế (`verify_readme_metrics.py`, 18 binding) — mở rộng cho tài liệu mới thay vì chép tay.

**④ Tài liệu quyết định phải bất biến.** ADR không sửa, chỉ superseded. Khác với tài liệu mô tả (cập nhật theo code).

**⑤ Không viết tài liệu cho thứ chưa tồn tại.** Nếu chưa đo được thì ghi `not_collected`, không viết như đã có.

---

## 2. Khung sườn đích

```text
README.md                          ← rút gọn còn ~250 dòng: cổng vào + điều hướng
CONTRIBUTING.md                    ← MỚI
SECURITY.md                        ← MỚI
LICENSE                            ← MỚI
CHANGELOG.md                       ← giữ
ARCHITECTURE.md                    ← giữ, trỏ sang ADR

docs/
├── INDEX.md                       ← MỚI · bản đồ toàn bộ tài liệu, theo người đọc
│
├── 01-getting-started/
│   ├── ONBOARDING.md              ← MỚI · dev mới: 0 → chạy được pipeline
│   ├── DEMO_GUIDE.md              ← chuyển từ gốc repo
│   └── LOCAL_SETUP.md             ← MỚI · tách phần setup khỏi README
│
├── 02-architecture/
│   ├── ARCHITECTURE.md            ← liên kết từ gốc
│   ├── DATA_FLOW.md               ← MỚI · Bronze→Silver→Gold→Serving, batch vs CDC
│   ├── CDC_PIPELINE.md            ← từ cdc-pipeline.md, mở rộng
│   ├── OBSERVABILITY.md           ← từ p2-observability-design.md
│   └── adr/                       ← MỚI · xem §3 nhóm B
│       ├── README.md
│       ├── 0001-iceberg-over-delta.md
│       └── ...
│
├── 03-data/
│   ├── DATA_DICTIONARY.md         ← MỚI · toàn bộ bảng/cột, 3 tầng
│   ├── GLOSSARY.md                ← MỚI · NPL, AUM, RFM, KYC, COB, SCD, DLQ...
│   ├── DATA_CONTRACTS.md          ← MỚI · tổng hợp 33 contract
│   ├── LINEAGE.md                 ← MỚI · bản đồ phụ thuộc
│   ├── data-input-documentation.md  ← giữ
│   ├── data-output-documentation.md ← giữ
│   └── DATA_VAULT_MAPPING.md      ← giữ
│
├── 04-operations/
│   ├── RUNBOOK.md                 ← từ gốc repo (vận hành thường ngày)
│   ├── INCIDENT_RUNBOOK.md        ← MỚI · kịch bản sự cố + RCA
│   ├── SLA_AND_FRESHNESS.md       ← MỚI
│   ├── DISASTER_RECOVERY.md       ← MỚI
│   └── DBT_DEPLOYMENT.md          ← giữ
│
├── 05-quality/
│   ├── TESTING_STRATEGY.md        ← MỚI · 695 test được tổ chức thế nào
│   ├── DATA_QUALITY.md            ← MỚI · 9 DQ check type + quarantine
│   ├── EVIDENCE_MANIFEST.md       ← MỚI · giải thích cơ chế chống drift
│   └── technical-debt.md          ← giữ
│
├── 06-security-compliance/
│   ├── PII_INVENTORY.md           ← MỚI · cột nào nhạy cảm, masking ở đâu
│   ├── RBAC_MATRIX.md             ← MỚI
│   ├── AUDIT_TRAIL.md             ← MỚI
│   ├── REGULATORY_MAPPING.md      ← MỚI · BCBS 239 / SBV → implementation
│   └── AI_GOVERNANCE_FRAMEWORK.md ← giữ
│
├── 07-ml/
│   ├── MODEL_CARD_churn.md        ← MỚI
│   ├── MODEL_CARD_credit_scoring.md ← MỚI
│   └── FEATURE_CATALOG.md         ← MỚI (sau ROADMAP 3.1)
│
├── 08-api/
│   └── API_REFERENCE.md           ← MỚI · xuất từ OpenAPI của FastAPI
│
└── 09-analysis/                   ← nghiên cứu, không phải tài liệu sản phẩm
    ├── JD_MARKET_ANALYSIS.md
    ├── BOOTCAMP_CURRICULUM_ANALYSIS.md
    ├── COURSE_BASELINE_DIFF.md
    ├── REFERENCE_DATASET_ANALYSIS.md
    ├── ROADMAP.md
    ├── DOCUMENTATION_PLAN.md      ← tài liệu này
    └── archive/                   ← 2 bản JD cũ đã superseded + session-report
```

> **Lưu ý về việc di chuyển file**: đổi cấu trúc thư mục sẽ phá mọi link hiện có. Nếu ngại rủi ro, làm **§5 Giai đoạn 0** trước (chỉ thêm `INDEX.md`, không move), rồi mới move theo từng nhóm.

---

## 3. Thiếu gì — chi tiết

### Nhóm A — Điều hướng và cổng vào (P0, rẻ nhất, tác động lớn nhất)

| Tài liệu | Người đọc | Nội dung | Nguồn dữ liệu |
|---|---|---|---|
| **`docs/INDEX.md`** | tất cả | Bảng: *"Tôi muốn… → đọc file này"*. Nhóm theo 5 vai: đánh giá dự án · dev mới · vận hành · analyst · kiểm toán | thủ công |
| **README rút gọn** | người đánh giá | 1.859 → ~250 dòng. Giữ: pitch, kiến trúc 1 hình, quickstart, số liệu có binding, liên kết. Chuyển đi: setup chi tiết, mô tả từng bảng, hướng dẫn demo | tách từ README |
| **`CONTRIBUTING.md`** | dev | Quy ước commit, branch, chạy test, chạy lint, định nghĩa "xong" | thực tế repo |
| **`SECURITY.md`** | tất cả | Báo lỗ hổng thế nào; nhắc `docker/secrets/` không commit | — |
| **`LICENSE`** | tất cả | Chưa có. Repo public không license = mặc định "all rights reserved" | — |

### Nhóm B — Tài liệu quyết định: ADR (P1, giá trị phân biệt cao nhất)

**33 chỗ giải thích "Vì sao / Lý do" đang nằm trong comment code ở 20 file.** Đây là tài sản trí tuệ lớn nhất của dự án và hiện không ai đọc được nếu không mở code.

ADR cần viết, mỗi cái ~1 trang (Context → Decision → Consequences → Status):

| # | Quyết định | Nguồn hiện tại |
|---|---|---|
| 0001 | Iceberg thay vì Delta Lake | ARCHITECTURE.md |
| 0002 | Spark catalog `lakehouse` ≠ Trino catalog `iceberg` | `technical-debt.md` TD-7 + `test_trino_catalog_contract.py` |
| 0003 | dbt `materialized: table`, không dùng view | comment trong `dbt_project.yml:32` — Iceberg REST không hỗ trợ `createView` |
| 0004 | Business date = `CAST(from_utc_timestamp(...) AS DATE)` + `assert_utc_session` | `metrics-manifest.yaml` `time_semantics` |
| 0005 | `overwritePartitions` theo `cob_dt` | `gold_job.py` docstring |
| 0006 | Fail-loud guard: `assert_source_snapshots` + `assert_non_empty` | `gold_job.py:94-145` |
| 0007 | Metadata-driven YAML thay vì code cho cả 3 tầng | kế thừa template khoá học |
| 0008 | Evidence manifest là nguồn sự thật duy nhất cho số liệu | `generate_metrics_manifest.py` |
| 0009 | `select *` trong serving model (chống drift thủ công) | comment `mart_customer_360_current.sql` |
| 0010 | Sentinel `1900-01-01` thay vì `raise_compiler_error` | comment trong cùng file |
| 0011 | CDC watermark `(timestamp, batch_id)` per table | `code_etl/cdc/` |
| 0012 | DLQ giữ Kafka partition/offset, Bronze CDC thì không | `code_etl/cdc/` |
| 0013 | Seed generator tham số hoá `--scale`, không tách mini-seeder | `technical-debt.md` TD-6 |
| 0014 | Khai báo nguồn phải khớp SQL | `test_declared_sources_match_sql.py` |

### Nhóm C — Vận hành và sự cố (P1)

| Tài liệu | Vì sao cần |
|---|---|
| **`INCIDENT_RUNBOOK.md`** | `RUNBOOK.md` hiện chỉ xử lý "service không lên". Thiếu kịch bản **dữ liệu**: partition nguồn thiếu → `assert_source_snapshots` chặn · DQ fail → quarantine · schema drift → contract vỡ · CDC lag/DLQ đầy · backfill sai ngày. Mỗi kịch bản: triệu chứng → chẩn đoán → xử lý → xác minh đã khỏi. Thị trường hỏi `runbook` 37 lần, `RCA` 64 lần, `incident response` 23 lần |
| **`SLA_AND_FRESHNESS.md`** | Có `freshness_checks` module nhưng không có cam kết được viết ra. Ghi bằng `runbook`/`incident response`, **không** dùng `RTO/RPO` (0 lần trong 490 JD) |
| **`DISASTER_RECOVERY.md`** | Iceberg time travel + snapshot = có năng lực khôi phục, chưa có quy trình |

### Nhóm D — Tra cứu dữ liệu (P1)

| Tài liệu | Nội dung |
|---|---|
| **`DATA_DICTIONARY.md`** | 17 nguồn → 10 dim + 6 fact → 14 Gold → 13 serving. Mỗi cột: kiểu, ý nghĩa, nguồn, có PII không. **Sinh tự động** từ 33 contract + DDL, không viết tay |
| **`GLOSSARY.md`** | `COB` xuất hiện 0 lần trong tài liệu dù là khái niệm trung tâm. Cần: COB, NPL, AUM, RFM, KYC, SCD1/2, DLQ, CDC, watermark, structuring, velocity, multi-channel, alert_score, risk_level |
| **`DATA_CONTRACTS.md`** | 33 contract trong `governance/datasets/` chưa có bản tổng hợp |
| **`LINEAGE.md`** | Có module `lineage` + OpenMetadata; cần bản đồ đọc được cho người |

### Nhóm E — Chất lượng (P2)

| Tài liệu | Nội dung |
|---|---|
| **`TESTING_STRATEGY.md`** | 695 test / 51 file được chia thế nào: unit · governance · integration (66 deselected). Vì sao test governance là một loại riêng. Cách chạy từng nhóm |
| **`DATA_QUALITY.md`** | 9 DQ check type, quarantine pattern, ngưỡng, ai xử lý khi fail |
| **`EVIDENCE_MANIFEST.md`** | Giải thích `declared` vs `value`, `metric_type: static/runtime/manual`, `verification_scope`, vì sao `not_collected ≠ verified`. **Đây là cơ chế đặc trưng nhất của dự án nhưng chưa có tài liệu giải thích riêng** |

### Nhóm F — Bảo mật & tuân thủ (P2 — nhưng banking thì bắt buộc)

| Tài liệu | Vì sao |
|---|---|
| **`PII_INVENTORY.md`** | Cột nào là PII, masking ở tầng nào, ai xem được bản gốc. `DAMA-CDMP` xuất hiện trong JD nhiều hơn mọi chứng chỉ cloud đơn lẻ |
| **`RBAC_MATRIX.md`** | Có module `rbac`; cần ma trận role × dataset × quyền |
| **`AUDIT_TRAIL.md`** | Có module `audit`; cần: ghi gì, giữ bao lâu, truy vấn thế nào |
| **`REGULATORY_MAPPING.md`** | Có `regulatory_reporting_dag` (BCBS 239 + SBV) nhưng không có bảng ánh xạ *yêu cầu quy định → bảng/cột/job nào đáp ứng*. Đây chính là thứ kiểm toán ngân hàng hỏi |

### Nhóm G — ML & API (P3)

| Tài liệu | Nội dung |
|---|---|
| **`MODEL_CARD_*.md`** | Model card là chuẩn ngành: mục đích, dữ liệu huấn luyện, metric, giới hạn, bias, khi nào KHÔNG nên dùng. Đã có `mlflow.log_metric` cho accuracy/f1/roc_auc — lấy số từ đó |
| **`API_REFERENCE.md`** | FastAPI đã sinh OpenAPI tại `/docs`. Xuất ra bản tĩnh để đọc không cần chạy service |
| **`FEATURE_CATALOG.md`** | Sau khi làm ROADMAP 3.1 (feature store) |

---

## 4. Ba tài liệu đáng viết nhất

Nếu chỉ làm được ba thứ, làm ba thứ này:

**① `docs/INDEX.md`** — rẻ nhất. 30 file tài liệu mà không có bản đồ thì phần lớn sẽ không bao giờ được đọc.

**② `docs/02-architecture/adr/`** — giá trị phân biệt cao nhất. Trong 63 repo đối thủ, không repo nào có ADR. 33 quyết định có lý do rõ ràng đang chôn trong comment code là tài sản lớn nhất của dự án; ADR là cách duy nhất để người ngoài thấy được.

**③ `docs/04-operations/INCIDENT_RUNBOOK.md`** — khớp thị trường nhất. `RCA` 64 lần · `runbook` 37 · `incident response` 23. Và đây là khoảng trống thật: năng lực có, quy trình chưa viết.

---

## 5. Thứ tự triển khai

```text
GIAI ĐOẠN 0 — không move file, chỉ thêm            [nửa ngày]
  docs/INDEX.md · CONTRIBUTING.md · SECURITY.md · LICENSE
  → có bản đồ ngay, không phá link nào

GIAI ĐOẠN 1 — tài liệu quyết định                   [P1]
  docs/adr/README.md + 14 ADR (bắt đầu từ 0002, 0003, 0004, 0006
  — bốn cái này đã có sẵn văn bản trong comment, chỉ cần chuyển thể)

GIAI ĐOẠN 2 — vận hành                              [P1]
  INCIDENT_RUNBOOK.md · SLA_AND_FRESHNESS.md
  (sau khi ROADMAP 2.2 xong thì bổ sung kịch bản schema drift)

GIAI ĐOẠN 3 — tra cứu                               [P1]
  GLOSSARY.md (viết tay, ~1 trang)
  DATA_DICTIONARY.md (SINH TỰ ĐỘNG từ contract + DDL — viết script,
  đừng viết tay; viết tay sẽ drift ngay commit sau)
  DATA_CONTRACTS.md · LINEAGE.md

GIAI ĐOẠN 4 — tái cấu trúc thư mục                  [P2]
  Move theo nhóm 01→09, sửa link, thêm test kiểm tra link không gãy

GIAI ĐOẠN 5 — chất lượng + tuân thủ                 [P2]
  EVIDENCE_MANIFEST.md · TESTING_STRATEGY.md · DATA_QUALITY.md
  PII_INVENTORY.md · RBAC_MATRIX.md · REGULATORY_MAPPING.md

GIAI ĐOẠN 6 — README rút gọn                        [P2 — làm CUỐI]
  Chỉ rút gọn khi đã có chỗ để chuyển nội dung sang
```

**Vì sao README rút gọn làm cuối**: rút gọn trước khi có đích đến sẽ làm mất nội dung. Và README có 18 binding tới manifest — sửa nó đòi hỏi `verify_readme_metrics.py` vẫn xanh.

---

## 6. Quy tắc chống drift cho tài liệu mới

Đây là phần dễ bị bỏ qua nhất, và là lý do phần lớn bộ tài liệu chết sau 3 tháng.

| Quy tắc | Thực thi bằng |
|---|---|
| Số liệu chỉ sống trong manifest | mở rộng `readme_bindings` cho tài liệu mới; `verify_readme_metrics.py` |
| `DATA_DICTIONARY.md` sinh tự động | script đọc `governance/datasets/*.yaml` + DDL; test so sánh file đã commit với bản sinh lại |
| Link nội bộ không gãy | test governance quét `[...](..)` trong `*.md`, kiểm tra file đích tồn tại |
| ADR bất biến | không sửa nội dung; chỉ đổi `Status: superseded by ADR-00XX` |
| Tài liệu phân tích có ngày đo | mọi file trong `09-analysis/` phải có dòng "Ngày đo"; đã áp dụng |
| Không claim thứ chưa đo | `test_docs_no_stale_claims.py` đã có — mở rộng độ phủ |

**Ba test governance nên thêm** (cùng họ với `test_declared_sources_match_sql.py`):

```text
test_docs_links_resolve.py        mọi link markdown nội bộ trỏ tới file có thật
test_docs_index_complete.py       mọi file trong docs/ phải xuất hiện trong INDEX.md
test_data_dictionary_current.py   DATA_DICTIONARY.md khớp với bản sinh từ contract
```

---

## 7. Ghi chú

- Hiện trạng §0 đo ngày 2026-09-21. Benchmark 63 repo đếm trên `thamkhao/project_thamkhao*/`, bỏ qua `node_modules`, `.venv`, `.git`, `__pycache__`.
- Kế hoạch này là **tài liệu phân tích**, thuộc `metric_type: manual` — không đưa vào `verify_readme_metrics.py`.
- Khi Giai đoạn 4 (move file) xong, cập nhật cây thư mục ở §2 cho khớp thực tế, hoặc xoá §2 nếu `INDEX.md` đã thay thế được vai trò.
