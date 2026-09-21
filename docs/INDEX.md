# Bản Đồ Tài Liệu — Banking Data Platform

> Cập nhật: 2026-09-22 · Cấu trúc đích và lý do: [`DOCUMENTATION_PLAN.md`](DOCUMENTATION_PLAN.md)

Repo có 35 tài liệu. Trang này trả lời một câu: **bạn là ai, và cần đọc gì.**

---

## Bắt đầu từ đâu

| Bạn là… | Đọc theo thứ tự này |
|---|---|
| **Người đánh giá dự án** (tuyển dụng, review) | [`README.md`](../README.md) → [`ARCHITECTURE.md`](../ARCHITECTURE.md) → [`technical-debt.md`](technical-debt.md) |
| **Dev mới vào dự án** | [`README.md`](../README.md) → [`CONTRIBUTING.md`](../CONTRIBUTING.md) → [`RUNBOOK.md`](../RUNBOOK.md) → [`data-input-documentation.md`](data-input-documentation.md) |
| **Người vận hành / on-call** | [`RUNBOOK.md`](../RUNBOOK.md) → [`p2-observability-design.md`](p2-observability-design.md) → [`technical-debt.md`](technical-debt.md) |
| **Analyst / người dùng dữ liệu** | [`data-output-documentation.md`](data-output-documentation.md) → [`DBT_DEPLOYMENT.md`](DBT_DEPLOYMENT.md) → [`DEMO_GUIDE.md`](../DEMO_GUIDE.md) |
| **Kiểm toán / tuân thủ** | [`AI_GOVERNANCE_FRAMEWORK.md`](AI_GOVERNANCE_FRAMEWORK.md) → [`DATA_VAULT_MAPPING.md`](DATA_VAULT_MAPPING.md) → [`technical-debt.md`](technical-debt.md) |
| **Muốn hiểu hướng đi của dự án** | [`ROADMAP.md`](ROADMAP.md) → [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md) |

---

## 1. Tổng quan & vào việc

| Tài liệu | Nội dung |
|---|---|
| [`README.md`](../README.md) | Cổng vào: pitch, kiến trúc, quickstart, số liệu có kiểm chứng |
| [`ARCHITECTURE.md`](../ARCHITECTURE.md) | Kiến trúc tổng thể 5 tầng |
| [`DEMO_GUIDE.md`](../DEMO_GUIDE.md) | Chạy demo end-to-end |
| [`demo/DEMO_SCRIPT.md`](../demo/DEMO_SCRIPT.md) | Kịch bản demo từng bước |
| [`CHANGELOG.md`](../CHANGELOG.md) | Lịch sử thay đổi |
| [`CONTRIBUTING.md`](../CONTRIBUTING.md) | Quy ước làm việc: branch, commit, test, lint |
| [`SECURITY.md`](../SECURITY.md) | Báo lỗ hổng · xử lý secrets và PII |

## 2. Kiến trúc & luồng dữ liệu

| Tài liệu | Nội dung |
|---|---|
| [`architecture/architecture.md`](architecture/architecture.md) | Bản chi tiết của kiến trúc |
| [`cdc-pipeline.md`](cdc-pipeline.md) | Postgres → Debezium → Kafka → Spark Streaming → Iceberg |
| [`code_etl/cdc/README.md`](../code_etl/cdc/README.md) | Cài đặt CDC ở mức code |
| [`p2-observability-design.md`](p2-observability-design.md) | Prometheus · Grafana · alerting |
| [`architecture-image-prompt.md`](architecture-image-prompt.md) | Nguồn sinh sơ đồ kiến trúc |
| [`adr/`](adr/README.md) | **Architecture Decision Records** — vì sao hệ thống được xây như vậy |

### Quyết định kiến trúc (ADR)

| # | Quyết định |
|---|---|
| [0002](adr/0002-cross-engine-catalog-naming.md) | Spark catalog `lakehouse` ≠ Trino catalog `iceberg` — và vì sao cần static check |
| [0003](adr/0003-serving-as-table-not-view.md) | Tầng serving là `table`, không phải `view` |
| [0004](adr/0004-business-date-under-utc-session.md) | Business date suy ra tường minh, Spark session bắt buộc UTC |
| [0005](adr/0005-fail-loud-before-overwrite.md) | Fail loud trước khi ghi đè partition |
| [0006](adr/0006-metadata-driven-jobs.md) | Job metadata-driven bằng YAML cho cả ba tầng |
| [0007](adr/0007-overwrite-partitions-by-cob-dt.md) | `overwritePartitions` theo `cob_dt`, không dùng MERGE |
| [0008](adr/0008-evidence-manifest-as-verifier.md) | Evidence manifest là verifier, không phải nơi dump số |
| [0010](adr/0010-cdc-event-ordering.md) | Thứ tự sự kiện CDC dùng `(timestamp_ms, batch_id)` |
| [0012](adr/0012-parameterised-seed-not-second-seeder.md) | Tham số hoá generator `--scale`, không viết seeder thứ hai |
| [0013](adr/0013-declared-sources-match-sql.md) | Khai báo nguồn phải khớp với SQL |
| [0014](adr/0014-kimball-over-data-vault.md) | Kimball star schema, Data Vault chỉ ở mức ánh xạ |

> **Chưa có**: `DATA_FLOW.md`, và **ADR-0001** (Iceberg vs Delta) — lý do chọn chưa được ghi ở đâu, cần tác giả xác nhận. Xem [`adr/README.md`](adr/README.md).

## 3. Dữ liệu

| Tài liệu | Nội dung |
|---|---|
| [`data-input-documentation.md`](data-input-documentation.md) | 17 nguồn: schema, khối lượng, cách sinh |
| [`data-output-documentation.md`](data-output-documentation.md) | Gold + serving: bảng, cột, ý nghĩa nghiệp vụ |
| [`DATA_INPUT_BASE_REPORT.md`](DATA_INPUT_BASE_REPORT.md) | Báo cáo hồ sơ dữ liệu nguồn |
| [`DATA_DICTIONARY.md`](DATA_DICTIONARY.md) | **Sinh tự động** — 85 bảng, 961 cột, kiểu dữ liệu, cờ PII, metadata contract |
| [`GLOSSARY.md`](GLOSSARY.md) | Thuật ngữ: `cob_dt`, SCD, AUM, NPL, RFM, structuring, BCBS 239… |
| [`DATA_CONTRACTS.md`](DATA_CONTRACTS.md) | **Sinh tự động** — 33 contract: grain, quality class, AI risk tier, DAG |
| [`LINEAGE.md`](LINEAGE.md) | **Sinh tự động** — đồ thị phụ thuộc, tham chiếu treo, dataset không có consumer |
| [`DATA_VAULT_MAPPING.md`](DATA_VAULT_MAPPING.md) | Ánh xạ Kimball star schema → Data Vault 2.0 |
| [`DBT_DEPLOYMENT.md`](DBT_DEPLOYMENT.md) | Tầng serving qua dbt + Trino |
| [`dbt/README.md`](../dbt/README.md) · [`dbt/SUMMARY.md`](../dbt/SUMMARY.md) | dbt project |
| [`api/README.md`](../api/README.md) | Customer 360 REST API |
| [`openmetadata/README.md`](../openmetadata/README.md) | Catalog và lineage |

> Bốn tài liệu tra cứu trên đều **sinh tự động** và có test chặn drift. Đừng sửa tay.
>
> ⚠️ `LINEAGE.md` hiện ghi nhận **4 tham chiếu treo** — bốn Gold contract trỏ tới
> `banking.dim_customer_silver` trong khi contract của `silver.dim_customer` mang
> `dataset_id` là `banking.core_customer_silver`. Lineage đứt ở dimension được
> dùng nhiều nhất của nền tảng.

## 4. Vận hành

| Tài liệu | Nội dung |
|---|---|
| [`RUNBOOK.md`](../RUNBOOK.md) | **Vận hành thường ngày**: start/stop service, chạy ETL, query, service không lên |
| [`INCIDENT_RUNBOOK.md`](INCIDENT_RUNBOOK.md) | **Sự cố dữ liệu**: 8 kịch bản theo khuôn triệu chứng → chẩn đoán → xử lý → xác minh đã khỏi |

Hai tài liệu trên trả lời hai câu hỏi khác nhau: *"chạy cái này thế nào?"* và *"nó hỏng rồi, làm gì?"*.

> **Chưa có**: `SLA_AND_FRESHNESS.md` (chưa có ngưỡng "lag bao nhiêu thì báo động"), `DISASTER_RECOVERY.md`.

## 5. Chất lượng & nợ kỹ thuật

| Tài liệu | Nội dung |
|---|---|
| [`technical-debt.md`](technical-debt.md) | TD-1…TD-7: trạng thái, bằng chứng, tiêu chí chấp nhận |
| [`evidence/p1-cdc-consolidation/README.md`](evidence/p1-cdc-consolidation/README.md) | Bằng chứng runtime: CDC consolidation |
| [`evidence/p2-observability/README.md`](evidence/p2-observability/README.md) | Bằng chứng runtime: observability |

> **Chưa có**: `TESTING_STRATEGY.md`, `DATA_QUALITY.md`, `EVIDENCE_MANIFEST.md` — cái cuối giải thích cơ chế đặc trưng nhất của dự án (`declared` vs `value`, `metric_type`, `not_collected ≠ verified`) mà hiện chưa có tài liệu riêng.

## 6. Quản trị & tuân thủ

| Tài liệu | Nội dung |
|---|---|
| [`AI_GOVERNANCE_FRAMEWORK.md`](AI_GOVERNANCE_FRAMEWORK.md) | Khung quản trị cho thành phần AI/ML |

> **Chưa có**: `PII_INVENTORY.md`, `RBAC_MATRIX.md`, `AUDIT_TRAIL.md`, `REGULATORY_MAPPING.md` (BCBS 239 / SBV → bảng, cột, job nào đáp ứng). Với ngân hàng, nhóm này là bắt buộc chứ không phải tuỳ chọn.

## 7. Nghiên cứu & định hướng

Không phải tài liệu sản phẩm — là phân tích có ngày đo, dùng để ra quyết định.

| Tài liệu | Nội dung |
|---|---|
| [`ROADMAP.md`](ROADMAP.md) | Kế hoạch kỹ thuật: loại bỏ · sửa · cập nhật · bổ sung, theo 3 mốc |
| [`DOCUMENTATION_PLAN.md`](DOCUMENTATION_PLAN.md) | Kế hoạch bộ tài liệu (trang này là sản phẩm Giai đoạn 0 của nó) |
| [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md) | 14 file JD · ~490 posting · 2,77M ký tự, đo bằng tần suất |
| [`BOOTCAMP_CURRICULUM_ANALYSIS.md`](BOOTCAMP_CURRICULUM_ANALYSIS.md) | 6 giáo trình, đối chiếu với thị trường thật |
| [`COURSE_BASELINE_DIFF.md`](COURSE_BASELINE_DIFF.md) | Dự án đã thêm gì so với template khoá học |
| [`REFERENCE_DATASET_ANALYSIS.md`](REFERENCE_DATASET_ANALYSIS.md) | 2,1 GB dataset tham khảo — và vì sao nhãn fraud của chúng không dùng được |

**Đã bị thay thế** — giữ để tra cứu lịch sử, không dùng làm căn cứ:
[`JD_FINAL_MARKET_ANALYSIS.md`](JD_FINAL_MARKET_ANALYSIS.md) · [`JD_MARKET_ANALYSIS_REPORT.md`](JD_MARKET_ANALYSIS_REPORT.md)

---

## Quy ước

- **Tài liệu phân tích** (mục 7) luôn có dòng *Ngày đo*. Số liệu trong đó là ảnh chụp tại thời điểm đó, không tự cập nhật.
- **Số liệu công bố** chỉ sống trong `docs/evidence/metrics-manifest.yaml`. README chiếu từ đó qua 18 binding và được `scripts/verify_readme_metrics.py` kiểm tra. Đừng chép số vào tài liệu mới mà không có binding.
- Mục *"Chưa có"* trong trang này là **chủ ý**: ghi rõ cái gì thiếu thì tốt hơn là để người đọc tự phát hiện.
