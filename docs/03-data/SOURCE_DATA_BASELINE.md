# Nguồn Gốc Data Input & Khoảng Trống Nguồn Dữ Liệu

> **Ngày lập**: 2026-09-29
> **Câu hỏi**: Data input của dự án có được xây trên `thamkhao/` (bootcamp_class · dataset_thamkhao · JD) không, lấy những gì, và còn thiếu nguồn nào để thành một banking data platform hoàn chỉnh?
> **Căn cứ**: DDL `docker/init_postgres/*.sql` · `data_generator/` · `code_etl/bronze/**/*.yml` · CSV header của 3 bộ dữ liệu tham khảo · template `thamkhao/code_project_cuoi/` · số đo JD từ [`scripts/measure_jd_corpus.py`](../../scripts/measure_jd_corpus.py)
> **Liên quan**: [`REFERENCE_DATASET_ANALYSIS.md`](../09-analysis/REFERENCE_DATASET_ANALYSIS.md) (chất lượng nhãn và phân phối của bộ tham khảo) · [`JD_MARKET_ANALYSIS.md`](../09-analysis/JD_MARKET_ANALYSIS.md) · [`data-input-documentation.md`](data-input-documentation.md)

---

## 0. Kết luận ngắn

1. **Có. Data input được xây trên tham khảo, theo 4 lớp** (§1): template khoá học → `archive (9)` → `archive (10)` → Xóm Bank, cộng các bảng dự án tự thêm. Dữ liệu được **sinh bằng generator**, không import CSV tham khảo. Đây là lựa chọn đúng và đã được giải thích: nhãn fraud của hai bộ archive vô dụng cho ML (`REFERENCE_DATASET_ANALYSIS.md` §2).
2. **6 trên 23 bảng nguồn được seed nhưng không bao giờ vào lakehouse** (§3). `merchant`, `standing_order` và cả 4 bảng AML (`aml_rule`, `aml_alert`, `aml_alert_transaction`, `aml_customer_risk`) không có Bronze config và không có consumer nào.
3. **81 trên 109 mã MCC là mã giả** (§4): số ngẫu nhiên 1000–9999 với mô tả chung chung. Danh sách MCC của Xóm Bank cũng không dùng thay được, vì mô tả lệch chuẩn ISO 18245.
4. **Thư mục `bootcamp_class/new1/` mới thêm (2026-09-22) không liên quan banking** (§5). Đó là khoá Data Science "Price Intelligence" (crawl giá web). Không có data input nào để lấy.
5. **Nguồn còn thiếu, xếp theo nhu cầu JD đo được** (§6): nối 6 bảng đã có vào pipeline → làm giàu cột theo bộ tham khảo → nguồn tín dụng (hồ sơ vay, tài sản bảo đảm, thu hồi nợ) → campaign/response → app event → nguồn dạng file.

---

## 1. Phả hệ 23 bảng nguồn

| Bảng nguồn | Gốc | Vào Bronze? | Ghi chú |
|---|---|:---:|---|
| `core_banking.branch` | Template | ✅ | |
| `core_banking.product` | Template | ✅ | |
| `core_banking.customer` | Template | ✅ | Việt hoá: `cccd`, `district`, `kyc_status` |
| `core_banking.account` | Template | ✅ | |
| `core_banking.deposit` | Template | ✅ | |
| `core_banking.loan` | Template | ✅ | |
| `core_banking.txn_account` | Template | ✅ | |
| `card_crm.card` | Template | ✅ | |
| `card_crm.card_txn` | Template + `archive (10)` | ✅ | `processing_time_ms`, `reference_number` lấy từ `archive (10)/transactions` |
| `card_crm.crm_interaction` | Template | ✅ | |
| `core_banking.loan_payment` | `archive (9)` | ✅ | `principal_component`, `interest_component`, `late_payment_flag` trùng `archive (9)/loan_payments`; dự án thêm `days_late`, `penalty`, `scheduled_amount` |
| `core_banking.employee` | `archive (9)` | ✅ | |
| `digital_banking.support_ticket` | `archive (9)` | ✅ | thêm `priority`, `resolution_time_hrs` |
| `digital_banking.device` | `archive (10)` | ✅ | tập con cột `archive (10)/devices` |
| `digital_banking.location` | `archive (10)` | ✅ | `latitude`, `longitude`, `is_high_risk_area` |
| `digital_banking.online_transaction` | `archive (10)` | ✅ | `is_fraud`, `fraud_reason`, `channel` |
| `digital_banking.mcc_code` | Xóm Bank (số lượng 109) | ✅ | xem §4 |
| `core_banking.standing_order` | Dự án tự thêm | ❌ | |
| `digital_banking.merchant` | Dự án tự thêm | ❌ | có FK `mcc_code` |
| `core_banking.aml_rule` | Dự án tự thêm | ❌ | |
| `core_banking.aml_alert` | Dự án tự thêm | ❌ | có `sar_filed`, `ctr_required`, `evidence_json` |
| `core_banking.aml_alert_transaction` | Dự án tự thêm | ❌ | DDL có; generator **không** sinh |
| `core_banking.aml_customer_risk` | Dự án tự thêm | ❌ | có `peps_flag`, `sanctions_flag`, `edd_required` |

**Cách xác định cột "Gốc"**:

- *Template*: `thamkhao/code_project_cuoi/` có đúng 10 bảng `core_banking.*` và `card_crm.*` này trong DDL, cùng Bronze YAML cùng tên. Đây là template mà repo được fork (`COURSE_BASELINE_DIFF.md` §0).
- *`archive (9)`*: bộ ngân hàng bán lẻ 10 bảng. Ba bảng mà template không có (`loan_payments`, `employees`, `support_tickets`) xuất hiện trong dự án với cột trùng.
- *`archive (10)`*: bộ thiên về fraud detection. Ba bảng `devices`, `locations`, `transactions` thành phân hệ `digital_banking`, với tên cột trùng nguyên văn (`device_fingerprint`, `is_trusted`, `is_high_risk_area`, `fraud_reason`).
- *Xóm Bank*: lấy **hình dạng phân phối** (log-normal cho số tiền, `data_generator/generators/amounts.py`, tham số hoá theo mean 43,72 / median 31,14 đo trên bộ này) và **số lượng MCC** (109).

`DATA_INPUT_BASE_REPORT.md` §7 còn liệt kê Data1–Data13 từ `thamkhao/project_thamkhao8_data/` (ABC Bank, BankCorp, Czech star schema…). Chúng được dùng làm ý tưởng pattern, không phải nguồn bảng.

---

## 2. Bộ tham khảo có mà dự án chưa lấy — mức cột

Chỉ liệt kê các cột **có việc dùng cụ thể** trong platform.

| Bảng dự án | Cột còn thiếu | Có trong | Mở khoá được gì |
|---|---|---|---|
| `customer` | `credit_score`, `annual_income` / `yearly_income`, `total_debt`, `occupation` | `archive (9)`, `archive (10)`, Xóm `users` | DTI (JD Talentnet), phân phối credit score (Xóm Q3, Q14), feature cho `ml/pipeline/credit_scoring.py` |
| `card` | `has_chip`, `num_cards_issued`, `year_pin_last_changed` | Xóm `cards` | Tín hiệu rủi ro thẻ (PIN cũ, thẻ phát lại nhiều lần) |
| `card_txn` | `entry_mode` (Chip/Swipe/Online), `error_reason` (kể cả **lỗi tổ hợp** "Bad PIN,Insufficient Balance") | Xóm `transactions.use_chip`, `.errors` | Xóm Q5 (tỷ lệ lỗi theo **lý do**, hiện chỉ có `status = FAILED`) |
| `card_txn` | `merchant_id` (FK → `merchant`), `merchant_state` | Xóm `transactions` | Xóm Q10 geo-velocity, Q15 heatmap state × category. Hiện `merchant_name` là text tự do |
| `device` | `os_version`, `browser` | `archive (10)` | Thấp. Không có consumer cần |

`card_txn` đã có refund âm (`REFUND`, `REVERSAL` → `amount = -amount`, `card_crm.py:181`), đúng như Xóm Bank. Các mart chi tiêu thẻ (`customer_360`, `customer_card_summary`, `customer_transaction_summary`) đều lọc `txn_type NOT IN ('REFUND','REVERSAL')`. Vì vậy hạng mục "refund âm" của ROADMAP §3.6 đã đóng.

---

## 3. Sáu bảng "chết": seed rồi dừng ở PostgreSQL

`code_etl/bronze/` có 17 YAML cho 17 trên 23 bảng. Sáu bảng ở §1 đánh ❌ được `generate_all.py` ghi vào Postgres (trừ `aml_alert_transaction`: có DDL, không có generator), nhưng không Bronze job, dbt model hay DAG nào đọc chúng.

Hệ quả cụ thể:

- **AML chạy hai lần, không gặp nhau.** `gold.aml_monitoring` tự tính cờ rule từ giao dịch. Trong khi đó `core_banking.aml_alert` đã có vòng đời case (`status`, `analyst_id`, `due_date`, `sar_filed`, `resolved_at`) mà không ai đọc. Mắt xích **Alert** trong chuỗi 5 bước của JD Fraud (`JD_MARKET_ANALYSIS.md` §5.2) chính là nằm ở đây.
- **`aml_customer_risk`** có `peps_flag`, `sanctions_flag`, `edd_required`, cũng là thứ JD KYC/AML hỏi. PEP/sanction chỉ được nhắc 3 lần sau dedup, nhưng dữ liệu đã có sẵn.
- **`merchant`** (2.000 dòng, FK `mcc_code`): JD nhắc `merchant / POS / acquiring` 34 lần sau dedup. Trong khi đó `card_txn` vẫn dùng `merchant_name` là text.
- **`standing_order`**: thanh toán định kỳ (EVN, nước, internet…). Là tín hiệu gắn bó khách hàng cho churn, hiện bị bỏ qua.

Nối các bảng này vào pipeline là **việc rẻ nhất có giá trị nhất** của tài liệu này: mỗi bảng một Bronze YAML, contract và DQ check theo khuôn đã có, không cần dữ liệu mới.

---

## 4. MCC: 28 mã thật + 81 mã giả

`seed_config.yaml` có 28 mã MCC. `generate_mcc_codes()` (`digital_banking.py:263`) **độn thêm cho đủ 109**:

```python
pool = [str(code) for code in range(1000, 10000) if str(code) not in used]
for code in random.sample(pool, 109 - existing):
    rows.append((code, random.choice(descs), random.choice(groups), ...))
```

Kết quả: 81/109 mã (74%) là số ngẫu nhiên, gắn mô tả bốc ngẫu nhiên ("Hotel", "Airline"…). `card_txn` rơi vào nhánh "random valid MCC" với xác suất 20% (`card_crm.py`, khi `random.random() >= 0.8` hoặc category không có mã), và nhánh này bốc từ **toàn bộ** 109 mã, gồm cả mã giả. Mọi phân tích theo MCC (Xóm Q13 outlier theo MCC, `mcc_code.is_high_risk`) đều chạy trên một phần mã không tồn tại.

**Không thay bằng danh sách Xóm Bank nguyên trạng.** 18/28 mã của config trùng Xóm, nhưng mô tả của Xóm lệch chuẩn. Ví dụ `3000` ghi "Steelworks", trong khi theo ISO 18245 dải 3000–3299 là mã hãng hàng không. Cách sửa: mở rộng `seed_config.yaml` bằng mã **chuẩn ISO 18245**, dùng tập mã Xóm làm danh sách **mã nào cần có** (109 mã thực sự xuất hiện trong giao dịch thẻ thật), rồi bỏ phần độn.

> Số dòng thực tế trong Postgres chưa được đo lại ở đây (stack đang dừng). Kết luận "81 mã giả" rút từ code, không từ truy vấn.

---

## 5. `bootcamp_class/` — phần mới so với lần phân tích trước

`BOOTCAMP_CURRICULUM_ANALYSIS.md` (2026-09-21) đã đọc 15 file. Sau đó thư mục có thêm:

| File | Nội dung | Data input cho dự án? |
|---|---|---|
| `new1/Buổi 0 - Giới thiệu về Bootcamp.pdf` | Data Science Bootcamp "Price Intelligence" (19 trang) | ❌ Crawl giá web, không phải banking |
| `new1/Buổi 1 - Tổng quan về dự án.pdf` | Vòng đời dự án DS (23 trang) | ❌ Chỉ lấy được khung **Business Problem → Data Problem → Data Product** |

`new/CHUẨN HÓA NGHIỆP VỤ – DỮ LIỆU.txt` là khoá Enterprise Architecture cho AI (1NF–3NF, EA). Không có dữ liệu.

Một chi tiết trong `new/DATA ENGINEER FULL-STACK K22.md` (dự án học viên K18, "Card Data Warehouse trên AWS") liên quan trực tiếp: nguồn là *"File .csv theo tháng (transactions) · File .json (MCC, category) · Nhiều nguồn khác nhau (card, user, giao dịch…)"*. Đó đúng là cấu trúc bộ Xóm Bank, và là **dạng nguồn file** mà dự án này chưa có (§6.4).

---

## 6. Nguồn còn thiếu cho platform hoàn chỉnh

Nhu cầu đo trên corpus JD pin ở `JD_MARKET_ANALYSIS.md` §0, đếm **sau dedup** (mỗi dòng một lần). Regex dùng cho từng dòng nằm trong mục này để đo lại được.

### 6.1 Tầng 0 — nối cái đã có (không dữ liệu mới)

| Việc | Căn cứ | Size |
|---|---|---|
| Bronze + contract cho `merchant`, `standing_order`, `aml_rule`, `aml_alert`, `aml_customer_risk` | §3 · merchant/POS 34 · KYC/onboarding 28 | S mỗi bảng |
| `card_txn.merchant_id` → `merchant` | Xóm Q10/Q15 | S |
| Sinh `aml_alert_transaction` hoặc bỏ DDL | DDL không có generator | S |

### 6.2 Tầng 1 — làm giàu cột theo bộ tham khảo (§2)

`customer.credit_score / annual_income / total_debt` · `card_txn.entry_mode / error_reason` (có lỗi tổ hợp) · `card.has_chip / year_pin_last_changed` · MCC chuẩn (§4).

### 6.3 Tầng 2 — nguồn nghiệp vụ mới, có nhu cầu JD

| Nguồn mới | JD (dedup) | Regex | Kết nối platform |
|---|---:|---|---|
| **`campaign` + `campaign_response`** (exposure / offer / redemption) | 33 + 23 | `campaign` · `promotion\|voucher` | `gold.campaign_target` hiện chọn đối tượng nhưng **không có phản hồi**, nên không đo được uplift / A/B |
| **`app_event`** (login, view, transfer_start/confirm, session) qua Kafka | 24 | `clickstream\|event tracking\|app events?\|user behavio\|tracking event\|event taxonomy\|sessioniz` | Nguồn **event stream** thật đầu tiên (hiện CDC là change log của bảng); sessionization, funnel (MoMo Lead DE) |
| **`loan_application`** (applied / approved / rejected, reject_reason) | 15 | `loan origination\|credit application\|loan application\|approval (?:rate\|analytics\|funnel)\|hồ sơ vay\|phê duyệt` | Funnel phê duyệt (Zalopay: *"funnel and approval analytics"*) |
| **`collateral`** (loại, giá trị định giá) | 13 | `collateral\|tài sản bảo đảm\|\bLTV\b` | LTV (Talentnet) |
| **`collection_activity`** (liên hệ, PTP, số tiền hứa, giữ/huỷ hứa) | 12 (`JD_MARKET_ANALYSIS.md` §5.3) | xem script | Cure rate, PTP kept rate (Mcredit, FE CREDIT). Đi cùng ROADMAP §3.7 |

Ba nguồn tín dụng (`loan_application`, `collateral`, `collection_activity`) cùng ROADMAP §3.7 (trạng thái trễ hạn nối tiếp) tạo thành **vòng đời tín dụng đầy đủ**: hồ sơ → giải ngân → trả nợ → quá hạn → thu hồi. Không repo nào trong 283 repo đối thủ có vòng này.

### 6.4 Tầng 3 — đa dạng **kiểu** nguồn

Cả 17 nguồn hiện tại đều là bảng PostgreSQL (JDBC hoặc CDC). JD nhắc nguồn file (`\bcsv\b|\bjson\b|excel|parquet`) 228 lần và API (`rest ?api|\bAPIs?\b`) 267 lần sau dedup. Hai con số này **rộng** (gồm cả "xuất Excel" và "xây API"), nên chỉ coi là chỉ báo hướng, không phải mức ưu tiên chính xác.

Đề xuất một nguồn file, không nhiều hơn: **file quyết toán/đối soát** (CSV/JSON theo ngày) thả vào MinIO `landing/` → Bronze. Nó phục vụ đúng bài reconciliation 5 lớp Matched / Mismatch / Missing / Duplicate / Pending (`JD_MARKET_ANALYSIS.md` §5.1) khi đối chiếu với `txn_account`/`card_txn`, và luyện được schema drift ở biên file, nơi drift hay xảy ra nhất.

### 6.5 Không làm — số đo thấp

| Nguồn | JD (dedup) | Lý do |
|---|---:|---|
| General ledger / double-entry | 1 (`general ledger\|\bGL\b\|sổ cái`) | "accounting" 35 lần gần như toàn là vai trò DA báo cáo tài chính, không phải nguồn GL |
| Sao kê thẻ / billing cycle / utilization | 0 | Xóm Q8 (credit utilization) tính được từ `card_txn` + `credit_limit`, không cần bảng sao kê |
| Treasury / trade finance | 6 | |
| Bancassurance | 4 | |
| FX rate | 1 | Có `currency` USD/VND; bảng tỷ giá chỉ cần nếu làm báo cáo quy đổi |
| CIC / credit bureau | 2 | |
| Import `archive (9)` / `(10)` vào Postgres | — | `REFERENCE_DATASET_ANALYSIS.md` §6: nhãn vô dụng, generator kiểm soát tốt hơn |

---

## 7. Thứ tự đề xuất

```text
1. Tầng 0   nối 5 bảng đã seed + merchant_id FK         (không dữ liệu mới, S×6)
2. §4       MCC chuẩn ISO 18245, bỏ phần độn             (S)
3. Tầng 1   cột làm giàu: credit_score/income/debt,       (M)
            entry_mode, error_reason (lỗi tổ hợp)
4. Tầng 2   vòng đời tín dụng: generator trễ hạn nối tiếp  (L — gộp ROADMAP §3.7)
            → loan_application → collateral → collection_activity
5. Tầng 2   campaign + campaign_response                  (M)
6. Tầng 2   app_event qua Kafka                           (M)
7. Tầng 3   file đối soát vào MinIO landing/              (M)
```

Mỗi bước là một PR riêng. Bảng mới phải đi kèm: DDL, generator, Bronze YAML, data contract, DQ check, và số dòng đo từ platform vào evidence manifest. Không ghi số dòng vào tài liệu trước khi đo được.

---

## 8. Ghi chú bảo trì

- Phả hệ §1 rút từ DDL và code tại commit lập tài liệu. Thêm hoặc bớt bảng nguồn thì cập nhật bảng §1 cùng PR.
- Số JD ở §6 là `metric_type: manual`, đo trên corpus pin MD5 ở `JD_MARKET_ANALYSIS.md` §0. Đổi corpus thì đo lại.
