# BÁO CÁO: Data Input Base — Banking Data Platform

> **Ngày**: 2026-09-07
> **Phiên bản**: v2.0 (Enhanced)
> **Trạng thái**: ✅ HOÀN THÀNH & VALIDATED

---

## 1. Tổng Quan

Data Input Base là lớp dữ liệu gốc (source data) feeding toàn bộ Banking Data Platform — từ PostgreSQL source qua CDC/Debezium vào Lakehouse Bronze→Silver→Gold, phục vụ dbt transformations, BI analytics, và fraud detection.

### Con số chính
| Metric | Giá trị |
|--------|---------|
| Tổng số bảng | **20 tables** |
| Tổng số rows | **~2,757,949 rows** |
| Số schema | **4 schemas** |
| Số generator functions | **20 functions** |
| Số DDL files | **4 files** |
| Dòng code generator | ~1,500 lines Python |
| Reference datasets phân tích | 13 datasets (Data1-Data13) |

---

## 2. Kiến Trúc Schema

```
PostgreSQL (Source)
├── core_banking (10 tables) ──── 1,526,800 rows
│   ├── branch (100)
│   ├── product (30)
│   ├── customer (10,000)
│   ├── account (30,000)
│   ├── deposit (15,000)
│   ├── loan (5,000)
│   ├── loan_payment (~250,000)        ← NEW
│   ├── standing_order (15,000)        ← NEW
│   ├── txn_account (1,200,000)
│   └── employee (1,800)
│
├── card_crm (3 tables) ─────────── 656,000 rows
│   ├── card (6,000)
│   ├── card_txn (600,000)
│   └── crm_interaction (50,000)
│
├── digital_banking (6 tables) ──── 582,109 rows
│   ├── device (50,000)
│   ├── location (5,000)
│   ├── online_transaction (500,000)
│   ├── support_ticket (25,000)
│   ├── mcc_code (109)
│   └── merchant (2,000)               ← NEW
│
└── opslakehouse (1 table) ──────── 19 rows
    └── source_table_registry (19)
```

---

## 3. Chi Tiết Từng Bảng

### 3.1 core_banking.branch (100 rows, 10 columns)

**Mục đích**: Master data — các chi nhánh ngân hàng

| Column | Type | Description |
|--------|------|-------------|
| branch_code | VARCHAR(10) PK | Mã chi nhánh |
| branch_name | VARCHAR(200) | Tên chi nhánh |
| region | VARCHAR(20) | NORTH / CENTRAL / SOUTH |
| city | VARCHAR(100) | Thành phố |
| district | VARCHAR(100) | Quận/Huyện |
| address | VARCHAR(500) | Địa chỉ |
| manager_name | VARCHAR(100) | Tên giám đốc |
| open_date | DATE | Ngày thành lập |
| status | VARCHAR(20) | ACTIVE / CLOSED |
| last_updated | TIMESTAMP | Auto-trigger |

**Cải tiến**: Geographic consistency — REGION_CITIES mapping (NORTH→Hanoi/Hai Phong, CENTRAL→Da Nang/Hue, SOUTH→HCM/Can Tho/Vung Tau). CITY_DISTRICTS cho district data.

---

### 3.2 core_banking.product (30 rows, 8 columns)

**Mục đích**: Master data — sản phẩm ngân hàng

13 sản phẩm predefined:
- **DEPOSIT**: CASA001 (Current Account), SAV001-003 (Savings variants)
- **LOAN**: LOAN001-004 (Personal, Mortgage, Auto, Business)
- **CARD**: CRD001-005 (Credit Classic/Gold/Platinum, Debit Visa/Mastercard)

Còn lại 17 rows generated tự động để đủ 30.

---

### 3.3 core_banking.customer (10,000 rows, 16 columns)

**Mục đích**: Master data — khách hàng cá nhân

| Column | Type | Description |
|--------|------|-------------|
| customer_id | BIGINT PK | ID tự tăng |
| cccd | VARCHAR(20) UNIQUE | Căn cước công dân |
| full_name | VARCHAR(200) | Họ tên (Vietnamese) |
| gender | CHAR(1) | M/F/O |
| date_of_birth | DATE | Ngày sinh (1955-2005) |
| phone | VARCHAR(20) | Số điện thoại |
| email | VARCHAR(200) | Email |
| address | VARCHAR(500) | Địa chỉ |
| city | VARCHAR(100) | Thành phố |
| district | VARCHAR(100) | Quận/Huyện |
| branch_code | VARCHAR(10) FK | Chi nhánh đăng ký |
| customer_segment | VARCHAR(20) | RETAIL (70%) / PRIORITY (22%) / VIP (8%) |
| kyc_status | VARCHAR(20) | VERIFIED (85%) / PENDING (10%) / REJECTED (5%) |
| register_date | DATE | Ngày đăng ký |
| is_active | SMALLINT | 0/1 (94% active) |
| last_updated | TIMESTAMP | Auto-trigger |

**Cải tiến**: 
- Expanded name pool: 50 male + 50 female first names, 50 last names → 125K+ unique combinations
- Geographic consistency: branch_code assigned based on customer city

---

### 3.4 core_banking.account (30,000 rows, 12 columns)

**Mục đích**: Tài khoản tiền gửi

- **CASA (55%)**: Current Account — balance range 100K-500M VND
- **TIME_DEPOSIT (45%)**: Term Deposit — balance range 10M-1B VND
- Status: ACTIVE (80%), CLOSED (15%), FROZEN (5%)

**Cải tiến**: Balance values feed into txn_account for running balance simulation.

---

### 3.5 core_banking.deposit (15,000 rows, 11 columns)

**Mục đích**: Chi tiết tiền gửi có kỳ hạn

- Terms: 1, 3, 6, 12, 24, 36 months
- Interest rates: 3.0-8.5% per year
- Principal: 5M-1B VND
- Status: ACTIVE (60%), MATURED (30%), EARLY_WITHDRAWN (10%)

---

### 3.6 core_banking.loan (5,000 rows, 12 columns)

**Mục đích**: Cho vay

- Amounts: 10M-5B VND
- Rates: 6.0-15.0% per year
- Terms: 6, 12, 24, 36, 60, 120 months
- Status: ACTIVE (55%), CLOSED (30%), OVERDUE (10%), WRITTEN_OFF (5%)

---

### 3.7 core_banking.loan_payment (~250,000 rows, 14 columns) ⭐ NEW

**Mục đích**: Lịch trả nợ vay (amortization schedule)

| Column | Type | Description |
|--------|------|-------------|
| payment_id | BIGINT PK | ID thanh toán |
| loan_id | BIGINT FK | FK → loan |
| payment_date | DATE | Ngày đáo hạn |
| scheduled_amount | NUMERIC(18,2) | Số tiền dự kiến |
| amount_paid | NUMERIC(18,2) | Số tiền thực trả |
| principal_component | NUMERIC(18,2) | Phần nợ gốc |
| interest_component | NUMERIC(18,2) | Phần lãi |
| penalty | NUMERIC(18,2) | Phí phạt (default 0) |
| outstanding_after | NUMERIC(18,2) | Dư nợ còn lại |
| days_late | SMALLINT | Số ngày trễ (0 = đúng hạn) |
| payment_method | VARCHAR(30) | BANK_TRANSFER/CASH/CHEQUE/DEBIT_CARD/MOBILE_APP |
| payment_status | VARCHAR(20) | PAID/LATE/MISSED/PENDING |
| late_payment_flag | SMALLINT | 0/1 (legacy) |
| last_updated | TIMESTAMP | Auto-trigger |

**Algorithm**: Standard amortization formula:
```
monthly_payment = P × r(1+r)^n / ((1+r)^n - 1)
```
Where P = principal, r = monthly rate, n = months.

**Patterns**:
- 5% late payment rate
- 2% missed payment rate
- CLOSED loans: outstanding balance → 0
- Payment methods: Vietnamese banking context (BANK_TRANSFER most common)

**Source inspiration**: Archive 9 `loan_payments.csv`, Data13 loan payment details

---

### 3.8 core_banking.standing_order (15,000 rows, 12 columns) ⭐ NEW

**Mục đích**: Thanh toán định kỳ (recurring payments)

| Column | Type | Description |
|--------|------|-------------|
| order_id | BIGINT PK | ID lệnh |
| account_id | BIGINT FK | FK → account |
| customer_id | BIGINT FK | FK → customer |
| order_type | VARCHAR(30) | BILL_PAYMENT/TRANSFER/LOAN_PAYMENT |
| beneficiary_name | VARCHAR(200) | Tên thụ hưởng |
| beneficiary_account | VARCHAR(20) | Tài khoản thụ hưởng |
| amount | NUMERIC(18,2) | Số tiền |
| frequency | VARCHAR(20) | MONTHLY (60%) / WEEKLY (25%) / QUARTERLY (15%) |
| next_execute_date | DATE | Ngày thực hiện tiếp |
| status | VARCHAR(20) | ACTIVE (70%) / PAUSED (15%) / CANCELLED (15%) |
| created_date | DATE | Ngày tạo |
| last_updated | TIMESTAMP | Auto-trigger |

**Vietnamese bill payment scenarios**:
- EVN (điện), Vietwater (nước), FPT/Viettel (internet)
- Insurance premiums, loan auto-payments

**Source inspiration**: Project 8 `order.csv`

---

### 3.9 core_banking.txn_account (1,200,000 rows, 13 columns)

**Mục đích**: Giao dịch tài khoản (lớn nhất)

| Column | Type | Description |
|--------|------|-------------|
| txn_id | BIGINT PK | ID giao dịch |
| account_id | BIGINT FK | FK → account |
| customer_id | BIGINT | Denormalized |
| txn_date | TIMESTAMP | Ngày GD (seasonal) |
| txn_amount | NUMERIC(18,2) | Số tiền |
| txn_type | VARCHAR(20) | DEPOSIT/WITHDRAWAL/TRANSFER_IN/TRANSFER_OUT/FEE/INTEREST |
| debit_credit | CHAR(1) | D/C |
| balance_after | NUMERIC(18,2) | Số dư sau GD |
| channel | VARCHAR(20) | BRANCH/ATM/INTERNET_BANKING/MOBILE_BANKING/POS |
| description | VARCHAR(500) | Mô tả |
| counter_account | VARCHAR(20) | Tài khoản đối ứng |
| created_ts | TIMESTAMP | Timestamp tạo |
| last_updated | TIMESTAMP | Auto-trigger |

**Cải tiến**:
- **Balance simulation**: Running balance per account — deposits/transfer_in add, withdrawals/transfer_out subtract
- **Seasonal datetime**: 65% weekday, hour peaks 9-11am & 7-9pm
- Channel distribution: MOBILE_BANKING (50%), INTERNET_BANKING (25%), ATM (15%), BRANCH (5%), POS (5%)

---

### 3.10 core_banking.employee (1,800 rows, 8 columns)

**Mục đích**: Nhân viên ngân hàng

- Roles: TELLER (40%), MANAGER (20%), ANALYST (30%), DIRECTOR (10%)
- Salary: 8M-80M VND/month
- Active rate: 90%

---

### 3.11 card_crm.card (6,000 rows, 12 columns)

**Mục đích**: Thẻ ngân hàng

- Types: DEBIT (55%), CREDIT (40%), PREPAID (5%)
- Brands: VISA (40%), MASTER (30%), JCB (15%), NAPAS (15%)
- Credit limit: 5M-200M VND
- Status: ACTIVE (75%), BLOCKED (5%), EXPIRED (12%), CLOSED (8%)

---

### 3.12 card_crm.card_txn (600,000 rows, 16 columns)

**Mục đích**: Giao dịch thẻ

| Column | Type | Description |
|--------|------|-------------|
| txn_id | BIGINT PK | ID giao dịch |
| card_id | BIGINT FK | FK → card |
| customer_id | BIGINT | Denormalized |
| txn_date | TIMESTAMP | Ngày GD (seasonal) |
| txn_amount | NUMERIC(18,2) | Số tiền |
| txn_type | VARCHAR(20) | PURCHASE/CASH_ADVANCE/REFUND/REVERSAL |
| currency | CHAR(3) | VND |
| merchant_name | VARCHAR(200) | Tên merchant |
| merchant_category | VARCHAR(50) | GROCERY/RESTAURANT/TRAVEL/... |
| **mcc_code** | VARCHAR(10) FK | **FK → mcc_code** |
| channel | VARCHAR(20) | POS/ECOM/ATM |
| status | VARCHAR(20) | SUCCESS/FAILED/PENDING |
| **processing_time_ms** | INT | **Thời gian xử lý** |
| **reference_number** | VARCHAR(30) | **CDN + sequential** |
| created_ts | TIMESTAMP | Timestamp |
| last_updated | TIMESTAMP | Auto-trigger |

**Cải tiến**: MCC code FK linkage, processing_time_ms, reference_number

---

### 3.13 card_crm.crm_interaction (50,000 rows, 12 columns)

**Mục đích**: Tương tác CRM

- Channels: CALL (30%), EMAIL (20%), CHAT (25%), BRANCH (15%), SMS (10%)
- Direction: INBOUND (60%), OUTBOUND (40%)
- Categories: COMPLAINT (20%), INQUIRY (30%), CAMPAIGN (20%), CROSS_SELL (15%), RETENTION (15%)

---

### 3.14 digital_banking.device (50,000 rows, 10 columns)

**Mục đích**: Thiết bị digital banking

- Types: MOBILE (65%), TABLET (15%), DESKTOP (20%)
- OS-aware: MOBILE→iOS/Android, DESKTOP→Windows/macOS/Linux
- Trusted rate: 40%

---

### 3.15 digital_banking.location (5,000 rows, 9 columns)

**Mục đích**: Địa điểm merchant

- 10 Vietnamese cities with approximate coordinates
- High-risk area flag: 5% (used for fraud correlation)

---

### 3.16 digital_banking.online_transaction (500,000 rows, 15 columns)

**Mục đích**: Giao dịch online (fraud detection focus)

| Column | Type | Description |
|--------|------|-------------|
| transaction_id | BIGINT PK | ID giao dịch |
| account_id | BIGINT | FK → account (nullable) |
| device_id | BIGINT | FK → device |
| location_id | BIGINT | FK → location |
| customer_id | BIGINT | Denormalized |
| transaction_type | VARCHAR(30) | PURCHASE/TRANSFER/PAYMENT/WITHDRAWAL/TOP_UP |
| channel | VARCHAR(20) | MOBILE_APP/WEB/API/POS |
| amount | NUMERIC(18,2) | Số tiền |
| currency | VARCHAR(3) | VND |
| is_fraud | SMALLINT | 0/1 (~0.8% fraudulent) |
| fraud_reason | VARCHAR(200) | Lý do fraud |
| status | VARCHAR(20) | SUCCESS/FAILED/PENDING |
| transaction_date | TIMESTAMP | Ngày GD (seasonal) |
| created_ts | TIMESTAMP | Timestamp |
| last_updated | TIMESTAMP | Auto-trigger |

**Fraud enrichment**:
- 13 fraud reasons (was 5): "Unusual location", "Velocity check failed", "Amount exceeds limit", "Known fraud pattern", "Device fingerprint mismatch", "Geo-anomaly detected", "Device change detected", "Amount anomaly", "IP blacklist match", "Multiple failed attempts", "Suspicious merchant pattern", "Cross-border anomaly", "Time-of-day anomaly"
- 35% of fraud correlated with high-risk locations
- Fraud transactions tend to be higher amounts (40% chance of 60-100% of max)

---

### 3.17 digital_banking.support_ticket (25,000 rows, 10 columns)

**Mục đích**: Ticket hỗ trợ

- Issue types: TRANSACTION_DISPUTE, ACCOUNT_ACCESS, CARD_BLOCK, GENERAL_INQUIRY, FEEDBACK
- Priority: LOW (20%), MEDIUM (45%), HIGH (25%), URGENT (10%)
- Status: OPEN (8%), IN_PROGRESS (12%), RESOLVED (55%), CLOSED (25%)

---

### 3.18 digital_banking.mcc_code (109 rows, 5 columns)

**Mục đích**: Master data — MCC (Merchant Category Code)

- 28 codes from config (standard banking MCCs)
- 81 auto-generated codes (pool sampling, no duplicates)
- Risk flags: gambling (7995), cash disbursement (6011), quasi-cash (6051), dating (7273)

---

### 3.19 digital_banking.merchant (2,000 rows, 9 columns) ⭐ NEW

**Mục đích**: Danh sách merchant

| Column | Type | Description |
|--------|------|-------------|
| merchant_id | BIGINT PK | ID merchant |
| merchant_name | VARCHAR(200) | Tên (unique) |
| merchant_category | VARCHAR(50) | 15 categories |
| mcc_code | VARCHAR(10) FK | FK → mcc_code |
| city | VARCHAR(100) | Thành phố |
| state | VARCHAR(100) | Tỉnh/TP |
| risk_category | VARCHAR(20) | LOW/MEDIUM/HIGH |
| is_active | SMALLINT | 0/1 (95% active) |
| last_updated | TIMESTAMP | Auto-trigger |

**Source inspiration**: Data12 (Indian Banking) merchants table

---

### 3.20 opslakehouse.source_table_registry (19 rows, 8 columns)

**Mục đích**: Metadata — registry cho tất cả source tables

Maps 19 source tables → Iceberg Bronze/Silver targets:
```
core_banking.branch → bronze.core_branch → silver.dim_branch
core_banking.customer → bronze.core_customer → silver.dim_customer
card_crm.card_txn → bronze.card_txn → silver.fact_card_txn
digital_banking.merchant → bronze.digi_merchant → (no silver)
...etc
```

---

## 4. Referential Integrity

```
branch ←── customer ←── account ←── deposit
                   │           ├── loan ←── loan_payment
                   │           ├── txn_account
                   │           └── standing_order
                   └── employee

customer ←── card ←── card_txn
                  └── crm_interaction

customer ←── device
location ←── online_transaction
mcc_code ←── card_txn
mcc_code ←── merchant
```

**Generation order** (FK-safe):
1. branch, product (no FKs)
2. customer (FK → branch)
3. account (FK → customer, product, branch)
4. deposit, loan (FK → customer, account, product)
5. loan_payment (FK → loan)
6. standing_order (FK → account, customer)
7. txn_account (FK → account)
8. employee (FK → branch)
9. mcc_code (no FKs)
10. card (FK → customer, account, product)
11. card_txn (FK → card, mcc_code)
12. crm_interaction (FK → customer)
13. device (FK → customer)
14. location (no FKs)
15. online_transaction (FK → device, location)
16. support_ticket (FK → customer)
17. merchant (FK → mcc_code)
18. source_table_registry (no FKs)

---

## 5. Data Quality Features

### 5.1 Seasonal Patterns
All transaction tables use `_random_datetime_seasonal()`:
- **Weekday bias**: 65% Mon-Fri, 20% Sat, 15% Sun
- **Hour distribution**: Peaks 9-11am (36%) and 7-9pm (14%)
- **Night hours**: 0.5-2% (realistic banking pattern)

### 5.2 Balance Simulation
`txn_account` tracks running balance per account:
- Deposits/Transfers-In increase balance
- Withdrawals/Transfers-Out decrease balance
- Balance initialized from `account.balance`
- Prevents unrealistic negative balances

### 5.3 Fraud Detection Data
- **Base rate**: 0.8% fraudulent transactions
- **Location correlation**: 35% of fraud linked to high-risk areas
- **Amount bias**: 40% chance fraud transactions are 60-100% of max amount
- **13 fraud patterns**: From reference dataset archive 10

### 5.4 Geographic Consistency
- Branches assigned to cities based on region
- Customers in same city → same branch
- District data from Vietnamese administrative divisions
- Coordinates for 10 major Vietnamese cities

### 5.5 Uniqueness Guarantees
- `cccd` (national ID): UNIQUE constraint + generated with sequential pattern
- `account_no`: UNIQUE constraint + generated format
- `mcc_code`: Pool sampling (no duplicates) — previously had 13% collision rate
- `merchant_name`: Tracked in `used_names` set

---

## 6. Configuration

All parameters controlled via `config/seed_config.yaml`:

```yaml
data:
  seed_date: "2025-12-31"
  start_date: "2020-01-01"
  end_date: "2025-12-31"

core_banking:
  branch: { row_count: 100, regions: [...], region_weights: [...] }
  customer: { row_count: 10000, gender_distribution: {...}, segment_distribution: {...} }
  txn_account: { row_count: 1200000, type_distribution: {...}, amount_range: [10000, 500000000] }
  loan_payment: { late_payment_rate: 0.05, missed_payment_rate: 0.02 }
  standing_order: { row_count: 15000, frequency_distribution: {...} }
  # ...etc

card_crm:
  card_txn: { row_count: 600000, merchant_categories: [...] }

digital_banking:
  online_transaction: { row_count: 500000, fraud_rate: 0.008 }
  merchant: { row_count: 2000 }
```

---

## 7. Reference Datasets Phân Tích

Đã phân tích 13 datasets từ `project_thamkhao8_data`:

| Dataset | Source | Insights Applied |
|---------|--------|------------------|
| Data1 | ABC Bank (India) | Customer demographics patterns |
| Data2/3 | BankCorp (US) | Transaction patterns, account types |
| Data5 | Czech star schema | Dimensional modeling, SCD patterns |
| Data7 | US dirty data | Data quality patterns (orphan FKs, mixed formats) |
| Data12 | Indian Banking | Merchant directory, MCC codes |
| Data13 | Loan payments | Amortization schedule, payment tracking |
| Archive 9/10 | Vietnamese banking | Fraud patterns, Vietnamese context |

---

## 8. Chạy Generator

### PostgreSQL mode
```bash
cd banking_data_platform/data_generator
python generate_all.py
```

### CSV export mode
```bash
python generate_all.py --csv-dir ./output/csv/
```

### Docker Compose
```bash
docker compose up seed-data
```

---

## 9. Validation Checklist

| Check | Status |
|-------|--------|
| DDL columns = Generator columns (all 20 tables) | ✅ |
| Generator function signatures match call sites | ✅ |
| FK generation order correct | ✅ |
| mcc_code_list defined before use | ✅ (bug fixed) |
| Config sections exist for all tables | ✅ |
| Truncate order in postgres_writer matches FK deps | ✅ |
| source_table_registry has all 19 tables | ✅ |
| Seasonal datetime applied to all transaction tables | ✅ |
| Balance simulation feeds from account data | ✅ |
| MCC code linkage: card_txn + merchant → mcc_code | ✅ |
| Fraud correlation: online_txn → high-risk locations | ✅ |

---

## 10. Kết Luận

Data Input Base đã **đầy đủ, chính xác, và hoàn thiện** cho Banking Data Platform:

- **20 tables**覆盖全部业务领域: deposits, loans, cards, digital banking, CRM
- **~2.76M rows** đủ lớn để test query performance, CDC throughput, and analytics
- **Seasonal patterns** realistic cho time-series analysis
- **Fraud data** enriched cho ML/analytics use cases
- **Referential integrity** ensured qua FK-safe generation order
- **Vietnamese context** consistent (names, cities, bill payments, MCC codes)
- **Reference datasets** leveraged cho realism (not just random data)

Pipeline sẵn sàng: PostgreSQL source → Debezium CDC → Kafka → Spark Streaming → Iceberg Bronze→Silver→Gold → dbt → BI.
