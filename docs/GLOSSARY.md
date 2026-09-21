# Glossary — Thuật Ngữ Dự Án

> Cập nhật: 2026-09-22 · Bản đồ tài liệu: [`INDEX.md`](INDEX.md)
>
> Tài liệu này **viết tay** (khác [`DATA_DICTIONARY.md`](DATA_DICTIONARY.md) được sinh tự động).
> Nghĩa của một thuật ngữ không suy ra được từ schema.

Sắp theo nhóm, vì tra cứu theo ngữ cảnh nhanh hơn theo bảng chữ cái.

---

## 1. Thời gian và chu kỳ

### `cob_dt` — Close of Business date

Ngày nghiệp vụ mà một lô dữ liệu thuộc về. **Cột partition của mọi bảng Bronze/Silver/Gold.**

Điểm quan trọng nhất và hay bị hiểu nhầm nhất: `cob_dt` là **ngày orchestration**, do Airflow truyền vào (`{{ ds }}`), **không** suy ra từ dữ liệu và **không** phụ thuộc múi giờ session. Nó trả lời *"lô này thuộc ngày làm việc nào"*, không phải *"giao dịch này xảy ra lúc nào"*.

Phân biệt với **business date** — xem mục dưới.

### Business date

Ngày mà một sự kiện **thực sự xảy ra** theo giờ Việt Nam, suy ra từ timestamp UTC:

```sql
CAST(from_utc_timestamp(txn_date, 'Asia/Ho_Chi_Minh') AS DATE)
```

Một giao dịch lúc `2026-09-17 23:30 ICT` có business date `2026-09-17`, nhưng timestamp UTC của nó là `2026-09-17 16:30`. Nếu dùng `CAST(ts AS DATE)` trần sẽ ra đúng trong trường hợp này nhưng sai ở các giao dịch sau 17:00 UTC.

Biểu thức trên **chỉ đúng dưới session UTC** — xem [`adr/0004`](adr/0004-business-date-under-utc-session.md).

### Watermark (CDC)

Mốc đánh dấu consolidation đã xử lý tới đâu, lưu ở `lakehouse.meta.cdc_watermark`. Cặp `(last_cdc_timestamp_ms, last_spark_batch_id)` — **không phải Kafka offset**. Xem [`adr/0010`](adr/0010-cdc-event-ordering.md).

### Freshness

Độ trễ giữa lúc dữ liệu có ở nguồn và lúc nó dùng được ở tầng tiêu thụ. Contract khai báo qua `freshness_sla_hours`.

---

## 2. Kiến trúc dữ liệu

### Medallion — Bronze / Silver / Gold

Ba tầng, mỗi tầng một trách nhiệm:

| Tầng | Trách nhiệm | Đặc điểm |
|---|---|---|
| **Bronze** | Nhận dữ liệu thô | Giữ nguyên giá trị nguồn, không sửa nghĩa |
| **Silver** | Chuẩn hoá, khử trùng, lịch sử hoá | dimension + fact, SCD1/SCD2 |
| **Gold** | Mart phục vụ nghiệp vụ | Đã tổng hợp, sẵn sàng cho BI/ML |

Nguyên tắc đi kèm: *Bronze nhận linh hoạt → Silver chuẩn hoá → Gold chỉ publish khi contract ổn định.*

### Serving

Tầng thứ tư, nằm sau Gold. Là **lát cắt hiện tại** (`cob_dt` mới nhất) của bảng Gold lịch sử, do dbt sở hữu và Trino truy vấn. Materialize thành `table` chứ không phải `view` — xem [`adr/0003`](adr/0003-serving-as-table-not-view.md).

### Dimension / Fact

Mô hình Kimball star schema:

- **Dimension** — thực thể mô tả (khách hàng, tài khoản, sản phẩm, chi nhánh). Trả lời *ai, cái gì, ở đâu*.
- **Fact** — sự kiện đo đếm được (giao dịch, khoản trả nợ, tương tác). Trả lời *bao nhiêu, bao nhiêu lần*.

### `SCD` — Slowly Changing Dimension

Cách xử lý khi thuộc tính của một dimension thay đổi theo thời gian.

| Loại | Hành vi | Dùng khi |
|---|---|---|
| **SCD Type 1** | Ghi đè giá trị cũ, không giữ lịch sử | Lịch sử không quan trọng |
| **SCD Type 2** | Tạo dòng mới, đóng dòng cũ | Cần truy vết thay đổi theo thời gian |

Dự án có 8 dimension SCD1 và 2 dimension SCD2.

Cột metadata của SCD2:

```text
effective_from   ngày bắt đầu hiệu lực
effective_to     '9999-12-31' nếu đang hiệu lực
is_current       1 = phiên bản hiện hành, 0 = lịch sử
<entity>_sk      surrogate key = sha2(business_key | cob_dt, 256)
```

### `SK` — Surrogate Key

Khoá nhân tạo, phân biệt **từng phiên bản** của một bản ghi. Khác **business key** (`customer_id`) vốn chỉ định danh thực thể. Một khách hàng có một `customer_id` nhưng nhiều `customer_sk` nếu thuộc tính đổi qua thời gian.

### Grain

Mức chi tiết của một dòng. `mart_customer_360` có grain *(customer_id, cob_dt)* — một dòng cho mỗi khách hàng mỗi ngày. Sai grain là loại lỗi làm mọi con số tổng hợp sai mà không có gì báo.

### `OBT` — One Big Table

Mô hình gộp mọi thứ vào một bảng phẳng thay vì star schema. **Dự án không dùng** — và đo được là thị trường cũng không hỏi (0 lần trong 2,77 triệu ký tự corpus JD).

---

## 3. CDC và streaming

### `CDC` — Change Data Capture

Bắt thay đổi từ PostgreSQL WAL, đẩy qua Kafka, ghi vào lakehouse. Luồng đầy đủ:

```text
PostgreSQL WAL → Debezium → Kafka → Spark Structured Streaming
  → Bronze CDC (append-only) → consolidation → Silver Current
```

### Bronze CDC vs Silver Current

- **Bronze CDC** — append-only, giữ **mọi sự kiện** kể cả đã bị ghi đè sau đó.
- **Silver Current** — trạng thái **hiện tại** sau khi MERGE, một dòng cho mỗi khoá.

### `DLQ` — Dead Letter Queue

Nơi cách ly sự kiện CDC không xử lý được, để điều tra thay vì làm chết cả luồng. **Chỉ DLQ giữ toạ độ Kafka** (`kafka_partition`, `kafka_offset`, `kafka_timestamp`) — Bronze CDC thì không. Xem [`adr/0010`](adr/0010-cdc-event-ordering.md).

### Idempotent

Chạy lại cho cùng kết quả, không nhân đôi hay làm hỏng dữ liệu. Ở dự án này đạt được bằng `overwritePartitions` theo `cob_dt` (batch) và MERGE (CDC).

### Backfill

Chạy lại pipeline cho một ngày trong quá khứ. An toàn nhờ tính idempotent — nhưng backfill **sai ngày** vẫn ghi đè dữ liệu tốt. Xem [`INCIDENT_RUNBOOK.md`](INCIDENT_RUNBOOK.md) S7.

---

## 4. Chất lượng và quản trị

### Data contract

Khai báo ràng buộc của một dataset: cột bắt buộc, cột không null, grain, số dòng tối thiểu, SLA freshness, phân loại AI risk. 33 file trong `governance/datasets/`.

### Reconciliation — đối soát

So số liệu giữa các tầng để phát hiện mất mát hoặc trùng lặp. Ví dụ: số dòng Postgres ↔ số message Kafka ↔ số dòng Bronze CDC.

### Quarantine — cách ly

Tách bản ghi vi phạm quy tắc nghiệp vụ ra `opslakehouse.quarantine_log` thay vì làm chết cả pipeline. Phần dữ liệu còn lại vẫn chảy tiếp.

### Schema drift

Schema nguồn đổi mà pipeline không biết. **Nguy hiểm vì thường không làm gì đỏ**: job vẫn xanh, dashboard vẫn lên, nhưng bảng downstream bắt đầu sai nghĩa. Phân biệt:

- **Additive** — thêm cột, thường an toàn
- **Breaking** — mất cột hoặc đổi kiểu, phải chặn publish

### Fail loud

Nguyên tắc: dừng ngay khi phát hiện điều kiện không thoả, thay vì ghi dữ liệu sai rồi báo thành công. Cài đặt bằng `assert_source_snapshots` và `assert_non_empty` — xem [`adr/0005`](adr/0005-fail-loud-before-overwrite.md).

### `not_collected ≠ verified`

Quy tắc của evidence manifest: một metric chưa đo được ghi `not_collected`, **không** ghi 0 và **không** ghi giá trị đoán. Thiếu dữ liệu khác với đã kiểm chứng.

### `PII` — Personally Identifiable Information

Dữ liệu định danh cá nhân: họ tên, email, điện thoại, địa chỉ, ngày sinh, IP. Masking áp ở tầng **Gold/serving**, không ở Bronze — xem [`../SECURITY.md`](../SECURITY.md).

### `RBAC` — Role-Based Access Control

Phân quyền theo vai trò thay vì theo từng người.

---

## 5. Nghiệp vụ ngân hàng

### `KYC` — Know Your Customer

Quy trình định danh khách hàng bắt buộc theo luật. Cột `kyc_status`: `VERIFIED` · `PENDING` · `REJECTED`.

### `AUM` — Assets Under Management

Tổng tài sản khách hàng gửi tại ngân hàng. Trong `mart_customer_360` là `aum_total`, kèm `aum_bucket` phân nhóm.

### `NPL` — Non-Performing Loan

Khoản vay quá hạn trả nợ vượt ngưỡng (thường 90 ngày). **NPL ratio** = dư nợ xấu / tổng dư nợ, là chỉ số rủi ro tín dụng cốt lõi. Có trong `loan_portfolio_risk`.

### Delinquency

Tình trạng trả nợ trễ, chưa tới mức NPL. Theo dõi qua `late_payment_flag` ở `fact_loan_payment`.

### `RFM` — Recency, Frequency, Monetary

Phân khúc khách hàng theo ba chiều: lần cuối giao dịch, tần suất, giá trị. Mỗi chiều chấm 1–5 bằng `NTILE(5)`, cửa sổ 90 ngày. Bảng `rfm_segment`.

### Churn

Khách hàng ngừng sử dụng dịch vụ. Dự án định nghĩa theo khoảng thời gian không giao dịch. Bảng `churn_prediction`.

### Cross-sell / `NBO` — Next Best Offer

Bán chéo sản phẩm cho khách hàng hiện hữu. NBO là mô hình gợi ý sản phẩm phù hợp nhất tiếp theo. Dự án có `cross_sell_segment`; **NBO serving chưa có**.

### `CLV` / `LTV` — Customer Lifetime Value

Giá trị dự kiến của một khách hàng trong toàn bộ vòng đời. **Chưa triển khai** — mục 3.2 trong [`ROADMAP.md`](ROADMAP.md).

---

## 6. Phòng chống gian lận và rửa tiền

### `AML` — Anti-Money Laundering

Phòng chống rửa tiền. Bảng `aml_monitoring` cài đặt bốn typology:

| Flag | Điều kiện | Trọng số |
|---|---|---:|
| `high_value_flag` | Giao dịch ≥ 200.000.000 | 3 |
| `structuring_flag` | ≥ 3 giao dịch trong dải 50–99,99tr cùng ngày | 4 |
| `velocity_flag` | ≥ 10 giao dịch trong ngày | 2 |
| `multi_channel_flag` | ≥ 4 kênh khác nhau | 1 |

### Structuring

Chia nhỏ một khoản tiền lớn thành nhiều giao dịch dưới ngưỡng báo cáo để né giám sát. Đây là lý do trọng số của nó cao nhất (4): nó là hành vi **cố ý né tránh**, không phải bất thường ngẫu nhiên.

### Velocity

Tần suất giao dịch bất thường trong một khoảng ngắn.

### Geo-velocity

Giao dịch ở nhiều địa điểm xa nhau trong thời gian ngắn tới mức không thể di chuyển kịp. **Chưa cài đặt** — mục 2.1 trong [`ROADMAP.md`](ROADMAP.md), và là lý do `dim_location` hiện chưa có consumer nào.

### `alert_score` và `risk_level`

`alert_score` = tổng có trọng số của các flag. `risk_level` 0–3 dựa trên **số lượng** flag bật:

```text
risk_level 3  ≥ 3 flag
risk_level 2  ≥ 2 flag
risk_level 1  ≥ 1 flag
risk_level 0  không flag nào
alert_generated = 1 khi ≥ 2 flag
```

### `MCC` — Merchant Category Code

Mã bốn chữ số phân loại ngành hàng của đơn vị chấp nhận thanh toán. Ví dụ `5411` = Grocery Stores.

---

## 7. Tuân thủ

### `BCBS 239`

Chuẩn Basel về **nguyên tắc tổng hợp dữ liệu rủi ro và báo cáo rủi ro**. Yêu cầu cốt lõi: chính xác, đầy đủ, kịp thời, có khả năng thích ứng. `regulatory_reporting_dag` sinh báo cáo theo chuẩn này.

### `SBV` / NHNN

Ngân hàng Nhà nước Việt Nam — cơ quan quản lý, đặt ra yêu cầu báo cáo định kỳ.

### `DAMA-CDMP`

Chứng chỉ quản trị dữ liệu của DAMA International. Đáng nhắc vì đo được là nó xuất hiện trong JD ngân hàng **nhiều hơn mọi chứng chỉ cloud đơn lẻ**.

### Data Vault 2.0

Phương pháp mô hình hoá dùng Hub / Link / Satellite, mạnh về audit trail và hợp nhiều nguồn. Dự án dùng Kimball; Data Vault chỉ tồn tại ở mức **tài liệu ánh xạ** — xem [`adr/0014`](adr/0014-kimball-over-data-vault.md).

---

## 8. Vận hành

### `flag_job_etl`

Bảng theo dõi trạng thái job ở `opslakehouse`. Chỉ có hai trạng thái: `R` (Running) và `S` (Success).

⚠️ **Không có trạng thái Failed.** Job chết giữa chừng nằm mãi ở `R`. Nên `R` ở một ngày đã qua nghĩa là *"đã chết"*, không phải *"đang chạy"*.

### Runbook vs Incident runbook

Hai tài liệu khác nhau, hay bị nhầm:

- [`../RUNBOOK.md`](../RUNBOOK.md) — *"chạy cái này thế nào?"*
- [`INCIDENT_RUNBOOK.md`](INCIDENT_RUNBOOK.md) — *"nó hỏng rồi, làm gì?"*

### `RCA` — Root Cause Analysis

Truy nguyên nhân gốc của sự cố. Câu hỏi quan trọng nhất không phải *"sửa thế nào"* mà *"invariant nào lẽ ra phải bắt được, và vì sao nó không bắt"*.

### `ADR` — Architecture Decision Record

Ghi lại một quyết định kiến trúc: bối cảnh, lựa chọn, hệ quả. **Bất biến** — không sửa, chỉ superseded. Xem [`adr/`](adr/README.md).

### `TD` — Technical Debt

Nợ kỹ thuật có tên và có trạng thái trong [`technical-debt.md`](technical-debt.md). Nợ được ghi nhận khác nợ bị bỏ quên.
