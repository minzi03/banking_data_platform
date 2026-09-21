# Phân Tích Dataset Tham Khảo — Đối Chiếu Với Mô Hình Dữ Liệu Dự Án

> **Ngày đo**: 2026-09-21
> **Nguồn**: `thamkhao/dataset_thamkhao/` — 2,1 GB · 19 file CSV · 3 bộ dữ liệu độc lập
> **Mục đích**: Đánh giá dữ liệu tham khảo có dùng được cho Banking Data Platform không, và ở mức nào
> **Tài liệu liên quan**: [`JD_MARKET_ANALYSIS.md`](JD_MARKET_ANALYSIS.md) · [`COURSE_BASELINE_DIFF.md`](COURSE_BASELINE_DIFF.md)

---

## 0. Kết luận ngắn

| Bộ dữ liệu | Dùng được cho | KHÔNG dùng được cho |
|---|---|---|
| `archive (10)` | Schema reference · test tải 3M dòng | **Huấn luyện mô hình fraud** — nhãn suy biến |
| `archive (9)` | Schema reference (`support_ticket`, `loan_payment`) | **Huấn luyện mô hình fraud** — nhãn là nhiễu thuần |
| `thamkhao/` (Xóm Bank) | **Tham chiếu phân phối thực tế** · 15 câu hỏi nghiệp vụ | (không có nhãn fraud) |

Và một phát hiện về **chính repo**, quan trọng hơn cả ba bộ dữ liệu: xem Mục 4.

---

## 1. Hồ sơ dữ liệu

### 1.1 `archive (10)/banking_data/` — định hướng fraud detection

| Bảng | Dòng | Cột | Bytes |
|---|---:|---:|---:|
| `transactions` | **3.000.000** | 20 | 986.731.441 |
| `devices` | 300.000 | 14 | 68.491.215 |
| `accounts` | 250.000 | 15 | 54.220.534 |
| `customers` | 200.000 | 20 | 47.607.613 |
| `locations` | 150.000 | 12 | 23.936.988 |

Cột đáng chú ý:

```text
devices      device_fingerprint · operating_system · os_version · browser
             browser_version · ip_address · mac_address · is_trusted
locations    latitude · longitude · is_high_risk_area · zip_code · country
transactions device_id · location_id · is_fraud · fraud_reason
             processing_time_ms · reference_number · channel · transaction_status
```

### 1.2 `archive (9)/` — ngân hàng bán lẻ đầy đủ

| Bảng | Dòng | Cột |
|---|---:|---:|
| `card_transactions` | **3.000.000** | 6 |
| `transactions` | **2.000.000** | 7 |
| `loan_payments` | 600.000 | 7 |
| `accounts` | 95.000 | 7 |
| `cards` | 65.000 | 8 |
| `customers` | 60.000 | 12 |
| `support_tickets` | 25.000 | 7 |
| `loans` | 22.000 | 9 |
| `employees` | 1.800 | 6 |
| `branches` | 150 | 6 |

```text
loan_payments    principal_component · interest_component · late_payment_flag
support_tickets  issue_type · date_opened · date_resolved · satisfaction_score
```

### 1.3 `thamkhao/` — Xóm Bank (`dataset.xomdata.com/datasets/schema/banking`)

| Bảng | Dòng | Cột |
|---|---:|---:|
| `transactions` | 157.224 | 12 |
| `cards` | 6.146 | 12 |
| `users` | 2.000 | 14 |
| `mcc_codes` | 109 | 2 |

Nhỏ nhất nhưng **chất lượng phân phối tốt nhất** (Mục 3).

`note.txt` kèm theo là một bản brief Risk Analyst hoàn chỉnh: bối cảnh nghiệp vụ, 3 pain point (fraud rule chưa có · credit utilization · dormant cards), và **15 câu hỏi ad-hoc** xếp theo độ khó — xem Mục 5.

---

## 2. Nhãn fraud trong `archive (9)` và `(10)` **không dùng được cho ML**

Đây là kiểm chứng quan trọng nhất của tài liệu này. Nếu bỏ qua bước này và huấn luyện mô hình rồi báo cáo AUC, con số đó sẽ là **một phép đo bịa** — đúng loại sai lầm mà hệ thống evidence của dự án tồn tại để ngăn.

### 2.1 `archive (10)` — nhãn suy biến

```text
3.000.000 dòng · is_fraud=True: 44.953 (1,498%)
```

**Test 1 — fraud rate theo channel** (nếu nhãn có tín hiệu, các số phải khác nhau):

```text
ATM          1,484%      MOBILE_APP   1,506%
BRANCH       1,499%      POS          1,519%
WEB          1,484%
```

Chênh lệch nằm trọn trong nhiễu lấy mẫu → **`channel` mang 0 tín hiệu**.

**Test 2 — amount**:

```text
fraud  : mean = 25.183,88   median = 25.187,91   n =    44.953
normal : mean =  4.001,42   median =  4.001,83   n = 2.955.047
```

Tách biệt 6,3× và mean ≈ median → fraud amount được rút từ một phân phối riêng, gần như đều. Một ngưỡng đơn trên `amount` sẽ phân loại gần như hoàn hảo. Đây là **tín hiệu suy biến**, không phải tín hiệu học được.

**Test 3 — `fraud_reason` có khớp với đặc trưng không?**

```text
HIGH_AMOUNT          mean amount = 25.199,80   n = 7.481
UNUSUAL_LOCATION     mean amount = 25.110,07   n = 7.521
RAPID_SUCCESSION     mean amount = 25.445,81   n = 7.504
SUSPICIOUS_DEVICE    mean amount = 25.129,61   n = 7.499
BLACKLISTED_MERCHANT mean amount = 25.169,43   n = 7.448
(blank)              mean amount = 25.048,58   n = 7.500
```

Giao dịch gắn nhãn `HIGH_AMOUNT` có phân phối amount **giống hệt** `UNUSUAL_LOCATION`. Sáu nhóm chia đều ~7.500 mỗi nhóm → `fraud_reason` được gán **ngẫu nhiên**, không phản ánh nguyên nhân thật.

**Cách dữ liệu được sinh ra**: chọn ngẫu nhiên 1,5% dòng → rút `amount` từ phân phối khác → gán `fraud_reason` ngẫu nhiên.

### 2.2 `archive (9)` — nhãn là nhiễu thuần

```text
3.000.000 dòng · is_fraud: 14.954 (0,498%)

fraud rate theo merchant_category:
  Healthcare 0,521%   Fund Transfer 0,496%   Travel 0,478%   Utilities 0,498%
  Dining     0,493%   Salary Credit 0,520%   Fuel   0,502%   ATM Withdrawal 0,523%

amount:  fraud mean = 1.833,50   normal mean = 1.850,45
```

Đồng đều theo category **và** amount không tách biệt (thậm chí fraud thấp hơn một chút). **Nhãn độc lập hoàn toàn với mọi đặc trưng** — còn tệ hơn `archive (10)`.

### 2.3 Hệ quả

| Việc | Phán quyết |
|---|---|
| Huấn luyện mô hình fraud giám sát rồi báo cáo AUC/precision | ❌ **Không làm.** Kết quả sẽ vô nghĩa hoặc trivially perfect |
| Dùng `fraud_reason` để kiểm chứng rule typology | ❌ Nhãn reason ngẫu nhiên |
| Dùng làm schema reference | ✅ |
| Dùng làm test tải / benchmark 3M dòng | ✅ |

---

## 3. `thamkhao/` (Xóm Bank) — phân phối đáng học

Không có nhãn fraud, nhưng hình dạng dữ liệu **thực tế**, khác hẳn hai bộ trên.

```text
errors (157.224 giao dịch):
  154.486  (none)                      ← 98,3% thành công
    1.760  Insufficient Balance
      388  Bad PIN
      324  Technical Glitch
       93  Bad Card Number
       73  Bad Expiration
       65  Bad CVV
       17  Bad Zipcode
        6  Bad PIN,Insufficient Balance      ← LỖI TỔ HỢP
        3  Insufficient Balance,Technical Glitch

use_chip:  112.114 Chip · 27.327 Swipe · 17.783 Online

amount:  mean = 43,72   median = 31,14   min = -500,00   max = 1.911,54
         refund (âm): 8.184
MCC: 109 mã phân biệt
```

Ba đặc điểm đáng mang vào dự án:

1. **Phân phối lỗi lệch mạnh và có tổ hợp** — `"Bad PIN,Insufficient Balance"` là hai lỗi trên một giao dịch. Dữ liệu sinh của repo hiện dùng `random.choices` với weight đơn nhãn, không sinh tổ hợp.
2. **Refund là số âm** (8.184 / 157.224 ≈ 5,2%) — kiểm tra xem các Gold mart của repo có xử lý `amount < 0` đúng không, hay đang `SUM` lẫn vào doanh số.
3. **`amount` lệch phải mạnh**: mean 43,72 vs median 31,14. Repo dùng `random.uniform` → mean ≈ median, không giống chi tiêu thẻ thật.

---

## 4. Phát hiện về chính repo — quan trọng hơn cả ba bộ dữ liệu

Khi đối chiếu schema tham khảo với mô hình dữ liệu của dự án, ba vấn đề lộ ra.

### 4.1 `dim_device` và `dim_location` không có consumer nào

Repo **đã có đủ** 17 nguồn, bao gồm `device`, `location`, `mcc_code`, `support_ticket`, `loan_payment` — tức các dataset tham khảo đã được hấp thụ vào mô hình từ trước.

Nhưng:

```text
Ai đọc silver.dim_device   ?  → không model nào (gold/ · dbt/ · ml/)
Ai đọc silver.dim_location ?  → không model nào
```

Hai dimension được ingest, chuẩn hoá, bảo trì — rồi **không ai dùng**.

### 4.2 Nhãn `is_fraud` đi tới Silver rồi dừng

```text
is_fraud có mặt ở:
  bronze/digital_banking/online_transaction.yml     ✅
  cdc/config/cdc_online_transaction.yml             ✅
  silver/facts/fact_online_transaction.yml          ✅
  ──────────────────────────────────────────────────
  gold/risk/fraud_risk_txn.yml                      ❌
  gold/risk/aml_monitoring.yml                      ❌
  ml/pipeline/*.py                                  ❌
```

`fraud_risk_txn.yml` lấy nguồn từ `silver.fact_txn_account` + `dim_customer` — **không phải** `fact_online_transaction`. Nó không tham chiếu `is_fraud`, `device`, `location`, `is_high_risk_area`, hay `is_trusted`.

Nói cách khác: **dự án mang ground-truth label qua ba tầng rồi không dùng**, trong khi mô hình fraud chạy rule-based trên một bảng fact khác — bảng không có nhãn.

### 4.3 Và đây là phần đáng tiếc nhất: generator của repo **cố tình** tạo tín hiệu

`data_generator/generators/digital_banking.py`:

```python
fraud_rate = config.get("fraud_rate", 0.008)      # 0,8%
high_risk_rate = config.get("high_risk_rate", 0.05)  # 5% location là high-risk

if random.random() < fraud_rate:
    is_fraud = 1
    # Correlate fraud with high-risk locations
    if high_risk_location_ids and random.random() < 0.35:
        location_id = random.choice(high_risk_location_ids)   # ← TÍN HIỆU THẬT
    # Fraud tends to be higher amounts
    if random.random() < 0.4:
        amount = round(random.uniform(amount_range[1]*0.6, amount_range[1]), 2)
    fraud_reason = random.choice(fraud_reasons)               # ← vẫn ngẫu nhiên
```

Độ mạnh tín hiệu location:

```text
P(high-risk | fraud)     ≈ 0,35 + 0,65 × 0,05 = 0,3825
P(high-risk | non-fraud) ≈ 0,05
lift                     ≈ 7,7×
```

**Generator của repo tốt hơn cả hai bộ dataset tham khảo**: nó tạo tương quan location thật (lift ~7,7×) và tương quan amount một phần (40%, không suy biến). Nhưng:

- Tín hiệu location được tạo ra → `dim_location` **không ai đọc** (4.1)
- Nhãn `is_fraud` được tạo ra → **không model nào dùng** (4.2)
- `fraud_reason` vẫn `random.choice` → **mang tính trang trí**, giống hệt điểm yếu của dataset tham khảo

### 4.4 Defect: `customer_360` khai báo phụ thuộc thừa

`code_etl/gold/mart360/customer_360.yml` khai báo `silver.fact_online_transaction` ở **ba chỗ**:

```yaml
source.tables:                  [..., silver.fact_online_transaction]   # dòng 35
upstream_flags:                 [..., silver.fact_online_transaction]   # dòng 51
validation.require_snapshots:   [..., silver.fact_online_transaction]   # dòng 64
```

Nhưng các bảng mà **SQL thực sự tham chiếu** chỉ gồm:

```text
silver.dim_account · silver.dim_card · silver.dim_customer · silver.dim_loan
silver.fact_card_txn · silver.fact_crm_interaction
silver.fact_loan_payment · silver.fact_txn_account
```

`fact_online_transaction` **không xuất hiện trong SQL**.

**Hệ quả**: `assert_source_snapshots()` sẽ **chặn job** khi `fact_online_transaction` thiếu partition của `cob_dt` — dù output không phụ thuộc gì vào nó. Đây là một dependency giả tạo ra lỗi giả.

**Phạm vi — đã audit toàn bộ 14 Gold model:**

```text
require_snapshots thừa : 1/14   → customer_360.yml: ['silver.fact_online_transaction']
source.tables thừa     : 1/14   → customer_360.yml: ['silver.fact_online_transaction']
```

Đây là **lỗi đơn lẻ, không hệ thống** — 13 model còn lại khai báo khớp với SQL.

Đáng chú ý: đây là mặt trái của guard đã làm đúng. Guard `assert_source_snapshots` là tính năng tốt (xem [`COURSE_BASELINE_DIFF.md`](COURSE_BASELINE_DIFF.md) Mục 3), nhưng khi `require_snapshots` khai báo rộng hơn thực tế, nó biến thành nguồn lỗi giả. **Cần một test kiểm tra: mọi bảng trong `require_snapshots` và `source.tables` phải xuất hiện trong `sql`** — kiểm tra này vừa chạy thủ công và bắt được đúng 1 ca.

---

## 5. 15 câu hỏi nghiệp vụ từ `note.txt` — danh sách yêu cầu thật

Brief Xóm Bank đặt người đọc vào vai Risk Analyst nhận yêu cầu từ CRO / Head of Cards / Head of Retail. Đây là danh sách use case ngân hàng có giá trị đối chiếu:

| # | Câu hỏi | Kỹ thuật | Repo đã có? |
|---|---|---|---|
| Q1 | Tổng giao dịch theo năm | date-filter, count | ✅ |
| Q2 | Top 5 thành phố nhiều giao dịch | group-by, top-n | ✅ |
| Q3 | Phân phối credit score | case-when, bucketing | ✅ |
| Q4 | Số thẻ theo brand | percent-of-total | ✅ |
| Q5 | Tỷ lệ giao dịch lỗi | count, case-when | ⚠️ |
| Q6 | Top 10 khách chi nhiều nhất/tháng | join, top-n | ✅ |
| Q7 | Category merchant chi nhiều nhất | join, group-by | ✅ |
| Q8 | **Credit utilization 30 ngày** | aggregate-ratio | ❌ |
| Q9 | **Dormant cards — mở >2 năm chưa dùng** | left-join-null | ❌ |
| Q10 | **Fraud: 5 giao dịch nhiều state trong 24h** | date-window, fraud-pattern | ⚠️ có velocity, thiếu geo |
| Q11 | Chi tiêu tháng + running total | window, running-total | ✅ |
| Q12 | Churn — không giao dịch >90 ngày | lag, datediff | ✅ |
| Q13 | **Outlier theo MCC** | percentile, window | ❌ |
| Q14 | Credit score vs spending correlation | ntile, correlation | ⚠️ |
| Q15 | Heatmap state × category | pivot, top-n-nested | ❌ |

Bốn hạng mục thiếu (Q8, Q9, Q13, Q15) đều rẻ và đều là KPI ngân hàng thật.

**Q10 đặc biệt đáng chú ý**: *"5 giao dịch ở nhiều state khác nhau trong 24h"* là typology **geo-velocity** — chính xác là thứ cần `dim_location` (đang không ai dùng, Mục 4.1). `aml_monitoring.yml` hiện có `velocity_flag` (≥10 giao dịch/ngày) và `multi_channel_flag` (≥4 kênh), nhưng **không có chiều địa lý**.

---

## 6. Hành động đề xuất

### P1 — Đóng vòng lặp tín hiệu đã tồn tại

**1. Thêm `geo_velocity_flag` vào `aml_monitoring.yml`**

Join `silver.dim_location`, đếm `COUNT(DISTINCT state)` trong cửa sổ 24h. Việc này đồng thời:
- Cho `dim_location` một consumer thật (sửa 4.1)
- Dùng được tương quan high-risk-location mà generator đã tạo (lift ~7,7×, sửa 4.3)
- Bổ sung typology Q10 — chuẩn AML quốc tế
- Không cần dữ liệu mới

**2. Thêm test: `require_snapshots` ⊆ bảng trong `sql`**

Sửa defect 4.4 và ngăn tái diễn. Cùng họ với `test_trino_catalog_contract.py` đã có. Chi phí rất thấp, chặn được một lớp lỗi giả.

**3. Đưa `is_fraud` vào `fraud_risk_txn` làm cột đối chứng**

Không phải để huấn luyện — để **đo rule hiện tại**: precision/recall của rule-based flags so với ground truth trong dữ liệu sinh. Đây là phép đo hợp lệ vì nhãn của **repo** có tín hiệu thật (khác dataset tham khảo). Cho ra một con số kiểm chứng được thay vì "rule chạy xong".

### P2

**4. Bốn KPI còn thiếu**: credit utilization (Q8) · dormant cards (Q9) · MCC outlier percentile (Q13) · heatmap state × category (Q15).

**5. Sửa `fraud_reason` trong generator** — hiện `random.choice`, nên gán theo đúng điều kiện đã kích hoạt (`HIGH_AMOUNT` khi amount cao, `UNUSUAL_LOCATION` khi đổi sang high-risk location). Nếu không, reason mãi là trang trí và không thể dùng để kiểm chứng rule.

**6. Phân phối thực tế hơn** theo Mục 3: lỗi tổ hợp · amount lệch phải (lognormal thay uniform) · refund âm.

### Không làm

| Việc | Lý do |
|---|---|
| Import `archive (9)` / `(10)` vào Postgres | Repo đã có đủ 17 nguồn tương đương; generator kiểm soát tốt hơn |
| Huấn luyện mô hình fraud trên nhãn dataset tham khảo | Mục 2 — nhãn suy biến hoặc nhiễu thuần |
| Báo cáo AUC từ bất kỳ nguồn nào trong 3 bộ này | Sẽ là con số bịa |

---

## 7. Ghi chú bảo trì

- Corpus nằm ngoài repo (`thamkhao/dataset_thamkhao/`), không version control, 2,1 GB.
- Mọi thống kê ở Mục 2–3 tính bằng cách **quét toàn bộ** file CSV (không sampling), ngày 2026-09-21. Script đo là ad-hoc; để tái lập, đọc lại toàn bộ file và đếm — số dòng và kích thước byte ghi ở Mục 1 dùng để xác minh file không đổi.
- Các số phía repo đo bằng `git grep` / parse YAML trên `main` @ `b787616`, ngày 2026-09-21.
- Phân loại **`metric_type: manual`** theo chuẩn `docs/evidence/metrics-manifest.yaml` — **không** đưa vào `verify_readme_metrics.py`.
- Không trích con số nào từ tài liệu này vào README mà không kèm ngày đo.
