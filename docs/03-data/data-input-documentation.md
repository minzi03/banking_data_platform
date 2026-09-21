# Data Input Layer — Banking Data Platform

> Mô tả toàn diện về Data Input: nguồn dữ liệu, cấu trúc, nghiệp vụ banking, 
> phân hệ, và so sánh với banking thực tế.
> Cập nhật: 2026-09-09

---

## 1. Tổng Quan

Dự án mô phỏng hệ thống OLTP (Online Transaction Processing) của ngân hàng Việt Nam với **4 phân hệ** PostgreSQL, **20 bảng nguồn**, tổng cộng **~2.76M rows** dữ liệu được sinh ra bởi Python data generator.

```
PostgreSQL 15 (OLTP Source)
├── core_banking    — 10 bảng  — Giao dịch cốt lõi
├── card_crm        — 3 bảng   — Thẻ & CRM
├── digital_banking — 6 bảng   — Kênh số & giao dịch online
└── opslakehouse    — 4 bảng   — Metadata vận hành
```

**Dữ liệu Việt Nam hóa:**
- Họ tên: Vietnamese names (Faker `vi_VN`)
- Số CCCD: 12 digits (Căn cước công dân)
- Số điện thoại: 0xxxxxxxxx format
- Đơn vị tiền tệ: VND
- Địa danh: Hanoi, HCM, Da Nang, Hai Phong, Can Tho, v.v.
- MCC codes: Standard ISO 18245 merchant category codes

**Trường `last_updated`:** Mỗi bảng có trigger `BEFORE UPDATE` tự cập nhật `last_updated = NOW()`, 
cho phép incremental JDBC ingestion dựa trên watermark timestamp.

---

## 2. Tổng Quan Phân Hệ & Bảng Nguồn

| # | Phân Hệ | Schema | Số Bảng | Tổng Rows | Vai Trò |
|---|---------|--------|---------|-----------|---------|
| 1 | Core Banking | core_banking | 10 | ~1,530,930 | Giao dịch cốt lõi: tài khoản, tiết kiệm, vay, chuyển khoản |
| 2 | Card & CRM | card_crm | 3 | ~656,000 | Thẻ ngân hàng, giao dịch thẻ, tương tác CRM |
| 3 | Digital Banking | digital_banking | 6 | ~582,109 | Kênh số: giao dịch online, thiết bị, MCC, merchant |
| 4 | Ops Metadata | opslakehouse | 1 | 19 | Registry mapping source → lakehouse |
| **Tổng** | | **20 bảng** | **~2,769,058 rows** | |

**Verified count**: 2,300,000 distinct curated transactions (account 1.2M + card 600K + online 500K)
## 3. Core Banking (10 Tables)

Schema `core_banking` — 10 bảng, ~1,530,930 rows. Trung tâm của toàn bộ dữ liệu OLTP, mô phỏng hệ thống core banking tại các ngân hàng Việt Nam.

### 3.1. branch — Chi nhánh ngân hàng

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 100 |
| **Columns** | 10 (branch_code, branch_name, region, city, district, address, manager_name, open_date, status, last_updated) |
| **PK** | `branch_code` VARCHAR(10) |
| **CHECK** | region IN ('NORTH','CENTRAL','SOUTH'), status IN ('ACTIVE','CLOSED') |

**Phân bố vùng miền (region_weights):**
- NORTH: 35% (Hanoi, Hai Phong)
- CENTRAL: 20% (Da Nang, Hue)
- SOUTH: 45% (HCM, Can Tho, Bien Hoa, Nha Trang, Vung Tau, Quy Nhon)

**Trạng thái:** ACTIVE 92%, CLOSED 8%. Branches_CLOSEd mô phỏng ngân hàng thu gọn mạng lưới.

### 3.2. product — Sản phẩm ngân hàng

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 30 (13 predefined + generated variants) |
| **Columns** | 8 (product_code, product_name, product_group, product_type, currency, is_active, launch_date, last_updated) |
| **PK** | `product_code` VARCHAR(20) |
| **CHECK** | product_group IN ('DEPOSIT','LOAN','CARD'), product_type IN ('CASA','SAVINGS','PERSONAL_LOAN','MORTGAGE','CREDIT_CARD','DEBIT_CARD') |

**13 sản phẩm predefined:**

| Mã | Tên | Nhóm | Loại |
|----|-----|------|------|
| CASA001 | Current Account | DEPOSIT | CASA |
| SAV001 | Standard Savings | DEPOSIT | SAVINGS |
| SAV002 | High-Yield Savings | DEPOSIT | SAVINGS |
| SAV003 | Online Savings | DEPOSIT | SAVINGS |
| LOAN001 | Personal Loan | LOAN | PERSONAL_LOAN |
| LOAN002 | Home Mortgage | LOAN | MORTGAGE |
| LOAN003 | Auto Loan | LOAN | PERSONAL_LOAN |
| LOAN004 | Business Loan | LOAN | PERSONAL_LOAN |
| CRD001 | Classic Credit Card | CARD | CREDIT_CARD |
| CRD002 | Gold Credit Card | CARD | CREDIT_CARD |
| CRD003 | Platinum Credit Card | CARD | CREDIT_CARD |
| CRD004 | Visa Debit Card | CARD | DEBIT_CARD |
| CRD005 | Mastercard Debit | CARD | DEBIT_CARD |

### 3.3. customer — Khách hàng

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 10,000 |
| **Columns** | 16 (customer_id, cccd, full_name, gender, date_of_birth, phone, email, address, city, district, branch_code, customer_segment, kyc_status, register_date, is_active, last_updated) |
| **PK** | `customer_id` BIGINT |
| **UNIQUE** | `cccd` — Căn cước công dân 12 digits |
| **FK** | branch_code → branch(branch_code) |
| **Indexes** | idx_customer_branch, idx_customer_segment, idx_customer_upd |

**Phân bố:**
- Gender: M 52%, F 46%, O 2%
- Segment: RETAIL 70%, PRIORITY 22%, VIP 8%
- KYC: VERIFIED 85%, PENDING 10%, REJECTED 5%
- Active rate: 94%
- Sinh từ 1955-01-01 đến 2005-12-31

**Việt Nam hóa:** 50+50 first names (M+F), 50 last names → 125K+ unique combos. Geographic consistency: customer được assign branch theo thành phố (branch_city_map).

### 3.4. account — Tài khoản

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 30,000 |
| **Columns** | 12 (account_id, account_no, customer_id, product_code, branch_code, account_type, currency, balance, open_date, close_date, status, last_updated) |
| **PK** | `account_id` BIGINT |
| **UNIQUE** | `account_no` VARCHAR(20) |
| **FK** | customer_id → customer, product_code → product, branch_code → branch |
| **CHECK** | account_type IN ('CASA','TIME_DEPOSIT'), status IN ('ACTIVE','CLOSED','FROZEN') |

**Phân bố:**
- Type: CASA 55%, TIME_DEPOSIT 45%
- Status: ACTIVE 80%, CLOSED 15%, FROZEN 5%
- Balance CASA: 100K–500M VND
- Balance TIME_DEPOSIT: 10M–1B VND

**3 accounts per customer** (trung bình). Balance được simulate chạy parallel với txn_account.

### 3.5. deposit — Giấy tờ tiết kiệm

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 15,000 |
| **Columns** | 11 (deposit_id, account_id, customer_id, product_code, principal_amount, interest_rate, term_months, open_date, maturity_date, status, last_updated) |
| **PK** | `deposit_id` BIGINT |
| **FK** | account_id → account (nullable), customer_id → customer, product_code → product |
| **CHECK** | term_months IN (1,3,6,12,24,36), interest_rate > 0 |

**Phân bố:**
- Term: 1, 3, 6, 12, 24, 36 tháng
- Rate: 3.0–8.5%/năm
- Principal: 5M–1B VND
- Status: ACTIVE 60%, MATURED 30%, EARLY_WITHDRAWN 10%

### 3.6. loan — Khoản vay

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 5,000 |
| **Columns** | 12 (loan_id, customer_id, product_code, branch_code, loan_amount, outstanding_balance, interest_rate, term_months, disbursement_date, maturity_date, loan_status, last_updated) |
| **PK** | `loan_id` BIGINT |
| **FK** | customer_id → customer, product_code → product, branch_code → branch |
| **CHECK** | loan_status IN ('ACTIVE','CLOSED','OVERDUE','WRITTEN_OFF'), loan_amount > 0 |

**Phân bố:**
- Amount: 10M–5B VND
- Rate: 6.0–15.0%/năm
- Term: 6, 12, 24, 36, 60, 120 tháng
- Status: ACTIVE 55%, CLOSED 30%, OVERDUE 10%, WRITTEN_OFF 5%

**WRITTEN_OFF (5%)** mô phỏng nợ xấu — feed vào Gold layer cho risk analytics.

### 3.7. loan_payment — Lịch trả nợ

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | ~250,000 |
| **Columns** | 14 (payment_id, loan_id, payment_date, scheduled_amount, amount_paid, principal_component, interest_component, penalty, outstanding_after, days_late, payment_method, payment_status, late_payment_flag, last_updated) |
| **PK** | `payment_id` BIGINT |
| **FK** | loan_id → loan |
| **CHECK** | payment_status IN ('PAID','LATE','MISSED','PENDING'), payment_method IN ('BANK_TRANSFER','CASH','CHEQUE','DEBIT_CARD','MOBILE_APP') |

**Business rules:**
- Standard amortization schedule: principal + interest components
- Late payment rate: 5% (days_late > 0, penalty > 0)
- Missed payment rate: 2% (amount_paid = 0)
- outstanding_after = running balance sau mỗi kỳ thanh toán

### 3.8. standing_order — Thanh toán định kỳ

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 15,000 |
| **Columns** | 12 (order_id, account_id, customer_id, order_type, beneficiary_name, beneficiary_account, amount, frequency, next_execute_date, status, created_date, last_updated) |
| **PK** | `order_id` BIGINT |
| **FK** | account_id → account, customer_id → customer |
| **CHECK** | order_type IN ('BILL_PAYMENT','TRANSFER','LOAN_PAYMENT'), frequency IN ('MONTHLY','WEEKLY','QUARTERLY') |

**Phân bố:**
- Frequency: MONTHLY 60%, WEEKLY 25%, QUARTERLY 15%
- Status: ACTIVE 70%, PAUSED 15%, CANCELLED 15%
- Mô phỏng: hóa đơn tiền điện (EVN), viễn thông (Viettel, FPT), vay tự động

### 3.9. txn_account — Giao dịch tài khoản

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 1,200,000 (bảng lớn nhất) |
| **Columns** | 13 (txn_id, account_id, customer_id, txn_date, txn_amount, txn_type, debit_credit, balance_after, channel, description, counter_account, created_ts, last_updated) |
| **PK** | `txn_id` BIGINT |
| **FK** | account_id → account |
| **CHECK** | txn_type IN ('DEPOSIT','WITHDRAWAL','TRANSFER_IN','TRANSFER_OUT','FEE','INTEREST'), debit_credit IN ('D','C'), channel IN ('BRANCH','ATM','INTERNET_BANKING','MOBILE_BANKING','POS') |
| **Indexes** | idx_txn_acct_date, idx_txn_cust_date, idx_txn_upd |

**Phân bố:**
- Type: DEPOSIT 15%, WITHDRAWAL 15%, TRANSFER_IN 25%, TRANSFER_OUT 25%, FEE 10%, INTEREST 10%
- Debit/Credit: D 55%, C 45%
- Channel: BRANCH 5%, ATM 15%, INTERNET_BANKING 25%, MOBILE_BANKING 50%, POS 5%
- Amount: 10K–500M VND

**Seasonality:** 65% weekday, giờ cao điểm 9–11h sáng và 19–21h tối. Balance simulation chạy parallel theo account.

### 3.10. employee — Nhân viên

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 1,800 |
| **Columns** | 8 (employee_id, full_name, branch_code, role, hire_date, salary, status, last_updated) |
| **PK** | `employee_id` BIGINT |
| **FK** | branch_code → branch |
| **CHECK** | role IN ('TELLER','MANAGER','ANALYST','DIRECTOR'), status IN ('ACTIVE','TERMINATED') |

**Phân bố:**
- Role: TELLER 40%, MANAGER 20%, ANALYST 30%, DIRECTOR 10%
- Salary: 8M–80M VND/tháng
- Active rate: 90%

### 3.11. ER Diagram — Core Banking

```
branch (100) ──────────┐
  │                     │
  ├── customer (10K)    │ FK branch_code
  │     │               │
  │     ├── account (30K)
  │     │     │
  │     │     ├── deposit (15K)
  │     │     ├── loan (5K) ──── loan_payment (~250K)
  │     │     ├── standing_order (15K)
  │     │     └── txn_account (1.2M)
  │     │
  │     └── loan (FK customer_id)
  │     └── deposit (FK customer_id)
  │
  └── employee (1.8K)

product (30) ── FK ──→ account, deposit, loan
```

---

## 4. Card & CRM (3 Tables)

Schema `card_crm` — 3 bảng, ~656,000 rows. Quản lý thẻ ngân hàng và tương tác khách hàng (CRM).

### 4.1. card — Thẻ ngân hàng

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 6,000 |
| **Columns** | 12 (card_id, card_no_masked, customer_id, account_id, product_code, card_type, card_brand, credit_limit, issue_date, expiry_date, status, last_updated) |
| **PK** | `card_id` BIGINT |
| **UNIQUE** | `card_no_masked` — masked format: 4111****1234 |
| **Logical FK** | customer_id → core_banking.customer, account_id → core_banking.account (debit only) |
| **CHECK** | card_type IN ('DEBIT','CREDIT','PREPAID'), card_brand IN ('VISA','MASTER','JCB','NAPAS') |

**Conditional CHECK — credit_limit:**
```sql
(card_type = 'CREDIT' AND credit_limit IS NOT NULL AND credit_limit > 0)
OR (card_type <> 'CREDIT' AND credit_limit IS NULL)
```
→ Chỉ CREDIT card mới có credit_limit, DEBIT/PREPAID must be NULL.

**Phân bố:**
- Type: DEBIT 55%, CREDIT 40%, PREPAID 5%
- Brand: VISA 40%, MASTER 30%, JCB 15%, NAPAS 15%
- Credit limit: 5M–200M VND
- Status: ACTIVE 75%, BLOCKED 5%, EXPIRED 12%, CLOSED 8%
- Expiry: 12–60 tháng từ ngày phát hành

### 4.2. card_txn — Giao dịch thẻ

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 600,000 |
| **Columns** | 16 (txn_id, card_id, customer_id, txn_date, txn_amount, txn_type, currency, merchant_name, merchant_category, mcc_code, channel, status, processing_time_ms, reference_number, created_ts, last_updated) |
| **PK** | `txn_id` BIGINT |
| **FK** | card_id → card |
| **CHECK** | txn_type IN ('PURCHASE','CASH_ADVANCE','REFUND','REVERSAL'), channel IN ('POS','ECOM','ATM') |
| **Indexes** | idx_card_txn_card_date, idx_card_txn_cust_date, idx_card_txn_last_upd |

**Phân bố:**
- Type: PURCHASE 70%, CASH_ADVANCE 15%, REFUND 10%, REVERSAL 5%
- Channel: POS 45%, ECOM 40%, ATM 15%
- Status: SUCCESS 90%, FAILED 7%, PENDING 3%
- Amount: 50K–50M VND
- Merchant categories: GROCERY, RESTAURANT, TRAVEL, ECOM, FUEL, EDUCATION, HEALTHCARE, ENTERTAINMENT, UTILITIES

**Cross-schema FK:** mcc_code → digital_banking.mcc_code (nullable). reference_number format: CDN + sequential number.

### 4.3. crm_interaction — Tương tác CRM

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 50,000 |
| **Columns** | 12 (interaction_id, customer_id, interaction_date, channel, direction, subject, category, status, assigned_to, satisfaction_score, created_ts, last_updated) |
| **PK** | `interaction_id` BIGINT |
| **CHECK** | channel IN ('CALL','EMAIL','CHAT','BRANCH','SMS'), direction IN ('INBOUND','OUTBOUND'), satisfaction_score BETWEEN 1 AND 5 (nullable) |

**Phân bố:**
- Channel: CALL 30%, EMAIL 20%, CHAT 25%, BRANCH 15%, SMS 10%
- Direction: INBOUND 60%, OUTBOUND 40%
- Category: COMPLAINT 20%, INQUIRY 30%, CAMPAIGN 20%, CROSS_SELL 15%, RETENTION 15%
- Status: OPEN 10%, RESOLVED 75%, PENDING 15%

### 4.4. ER Diagram — Card & CRM

```
core_banking.customer ──────────────┐
  │                                  │ (logical FK)
  ├── card (6K)                      │
  │     └── card_txn (600K)          │
  │           └── FK mcc_code ──────→ digital_banking.mcc_code
  │
  └── crm_interaction (50K)
```

---

## 5. Digital Banking (6 Tables)

Schema `digital_banking` — 6 bảng, ~582,109 rows. Kênh số: giao dịch online, thiết bị, MCC codes, merchant directory.

### 5.1. device — Thiết bị khách hàng

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 50,000 |
| **Columns** | 10 (device_id, customer_id, device_type, device_fingerprint, operating_system, ip_address, is_trusted, first_seen, last_seen, last_updated) |
| **PK** | `device_id` BIGINT |
| **CHECK** | device_type IN ('MOBILE','TABLET','DESKTOP'), is_trusted IN (0,1) |

**Phân bố:**
- Type: MOBILE 65%, TABLET 15%, DESKTOP 20%
- OS: iOS, Android, Windows, macOS, Linux (OS-aware: mobile → iOS/Android)
- Trusted rate: 40%
- Average ~5 devices per customer (50K/10K customers)

### 5.2. location — Địa điểm merchant

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 5,000 |
| **Columns** | 9 (location_id, merchant_name, merchant_category, city, state, latitude, longitude, is_high_risk_area, last_updated) |
| **PK** | `location_id` BIGINT |
| **CHECK** | is_high_risk_area IN (0,1) |

**Phân bố:**
- 10 thành phố Việt Nam: Hanoi, HCM, Da Nang, Hai Phong, Can Tho, Bien Hoa, Nha Trang, Vung Tau, Hue, Quy Nhon
- High-risk areas: 5% (correlated với fraud in online_transaction — 35% fraud发生在 high-risk areas)
- 12 merchant categories: grocery, restaurant, travel, ecom, fuel, education, healthcare, entertainment, utilities, fashion, electronics, home_improvement
- Coordinates: NUMERIC(10,7) — latitude/longitude for geo analytics

### 5.3. online_transaction — Giao dịch online

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 500,000 |
| **Columns** | 15 (transaction_id, account_id, device_id, location_id, customer_id, transaction_type, channel, amount, currency, is_fraud, fraud_reason, status, transaction_date, created_ts, last_updated) |
| **PK** | `transaction_id` BIGINT |
| **FK** | account_id → core_banking.account (nullable), device_id → device, location_id → location |
| **CHECK** | transaction_type IN ('PURCHASE','TRANSFER','PAYMENT','WITHDRAWAL','TOP_UP'), channel IN ('MOBILE_APP','WEB','API','POS') |
| **Indexes** | idx_otxn_customer_date, idx_otxn_account_date, idx_otxn_fraud |

**Phân bố:**
- Type: PURCHASE 35%, TRANSFER 30%, PAYMENT 20%, WITHDRAWAL 10%, TOP_UP 5%
- Channel: MOBILE_APP 55%, WEB 25%, API 10%, POS 10%
- Status: SUCCESS 92%, FAILED 5%, PENDING 3%
- Amount: 10K–100M VND
- **Fraud rate: 0.8%** (~4,000 fraudulent transactions)
- Fraud reasons: 13 loại (correlated với high-risk locations, amount bias towards higher amounts)
- Seasonal datetime pattern matching txn_account

### 5.4. support_ticket — Ticket hỗ trợ

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 25,000 |
| **Columns** | 10 (ticket_id, customer_id, issue_type, priority, status, date_opened, date_resolved, resolution_time_hrs, satisfaction_score, last_updated) |
| **PK** | `ticket_id` BIGINT |
| **CHECK** | issue_type IN ('TRANSACTION_DISPUTE','ACCOUNT_ACCESS','CARD_BLOCK','GENERAL_INQUIRY','FEEDBACK'), priority IN ('LOW','MEDIUM','HIGH','URGENT') |

**Phân bố:**
- Priority: LOW 20%, MEDIUM 45%, HIGH 25%, URGENT 10%
- Status: OPEN 8%, IN_PROGRESS 12%, RESOLVED 55%, CLOSED 25%
- Satisfaction: 1–5 (nullable)
- resolution_time_hrs: auto-calculated từ date_opened → date_resolved

### 5.5. mcc_code — Mã danh mục thương hiệu

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 109 |
| **Columns** | 5 (mcc_code, description, category_group, is_high_risk, last_updated) |
| **PK** | `mcc_code` VARCHAR(10) |
| **CHECK** | is_high_risk IN (0,1) |

**Phân bố:**
- 28 MCC codes defined in config + 81 auto-generated (pool sampling, no duplicates)
- Category groups: RETAIL, FOOD, TRAVEL, SERVICES, UTILITIES
- High-risk MCCs: 7995 (Gambling), 6011 (Cash Disbursement), 6051 (Quasi-Cash), 7273 (Dating Services)

**MCC codes được config trong seed_config.yaml:**

| MCC | Mô tả | Nhóm | High-risk |
|-----|--------|------|-----------|
| 5411 | Grocery Stores | RETAIL | No |
| 5812 | Eating Places, Restaurants | FOOD | No |
| 5814 | Fast Food Restaurants | FOOD | No |
| 3000 | Airlines | TRAVEL | No |
| 3351 | Car Rental | TRAVEL | No |
| 3501 | Hotels | TRAVEL | No |
| 4121 | Taxi | TRAVEL | No |
| 5541 | Gas Stations | RETAIL | No |
| 5732 | Electronics Stores | RETAIL | No |
| 5999 | Misc Retail Stores | RETAIL | No |
| 7995 | Gambling | SERVICES | **Yes** |
| 6011 | Cash Disbursement | SERVICES | **Yes** |
| 6051 | Quasi-Cash | SERVICES | **Yes** |
| 7273 | Dating Services | SERVICES | **Yes** |
| 4814 | Telecom Services | UTILITIES | No |
| 8062 | Hospitals | SERVICES | No |
| ... | +71 codes (auto-generated) | ... | ... |

### 5.6. merchant — Danh sách merchant

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 2,000 |
| **Columns** | 9 (merchant_id, merchant_name, merchant_category, mcc_code, city, state, risk_category, is_active, last_updated) |
| **PK** | `merchant_id` BIGINT |
| **FK** | mcc_code → digital_banking.mcc_code |
| **CHECK** | risk_category IN ('LOW','MEDIUM','HIGH'), is_active IN (0,1) |

**Cross-schema linkage:** merchant.mcc_code → mcc_code.mcc_code. Risk categories correlated với MCC high-risk flags.

### 5.7. ER Diagram — Digital Banking

```
digital_banking (6 tables):

device (50K) ──────────────────────┐
  │ FK customer_id → customer       │
  │                                 │
online_transaction (500K) ─────────┤
  │ FK device_id → device           │
  │ FK location_id → location       │
  │ FK account_id → account         │
  │                                 │
location (5K) ─────────────────────┘

mcc_code (109) ←── FK ── merchant (2K)
    ↑
    └── FK ── card_txn (card_crm schema)

support_ticket (25K) ── FK customer_id → customer
```

---

## 6. Ops Metadata (opslakehouse Schema)

Schema `opslakehouse` — 4 tables trong DDL, nhưng data generator chỉ populate 1 bảng (source_table_registry, 19 rows). 3 bảng còn lại là infrastructure cho ETL pipeline.

### 6.1. source_table_registry — Registry source → lakehouse (DATA)

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 19 (mỗi source table = 1 row) |
| **Columns** | 8 (id, schema_name, table_name, source_type, jdbc_conn_id, bronze_table, silver_table, is_active, last_updated) |
| **PK** | `id` SERIAL |
| **UNIQUE** | (schema_name, table_name) |
| **CHECK** | source_type IN ('postgresql','oracle','mysql'), is_active IN (0,1) |

**Vai trò:** Single source of truth cho pipeline config. Map 19 PostgreSQL source tables → Bronze Iceberg → Silver Iceberg targets. Dùng bởi Airflow DAGs để dynamically discover tables cho JDBC ingestion.

**Mapping example:**
```
core_banking.customer → lakehouse.bronze.core_banking_customer → lakehouse.silver.dim_customer
card_crm.card_txn    → lakehouse.bronze.card_crm_card_txn    → lakehouse.silver.fact_card_txn
```

### 6.2. flag_job_etl — ETL Pipeline Control Flags (DDL only)

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 0 (runtime only) |
| **Columns** | 9 (id, job_name, schema_name, table_name, status, start_time, end_time, cob_dt, created_at) |
| **CHECK** | status IN ('R','S') — R=Running, S=Success |

**Pattern:** INSERT-only. Mỗi DAG chạy = 1 cặp flags (R khi bắt đầu, S khi xong). Dùng cho dependency resolution: downstream DAG chỉ chạy khi upstream flag = 'S'.

### 6.3. data_quality_log — Data Quality Checks (DDL only)

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 0 (runtime only) |
| **Columns** | 9 (id, check_name, table_name, check_status, expected_value, actual_value, details, cob_dt, checked_at) |
| **CHECK** | check_status IN ('PASS','FAIL','WARN') |

**Vai trò:** Audit trail cho data quality checks. Kiểm tra: row_count, null_check, fk_integrity trên Silver/Gold layers.

### 6.4. pipeline_run_log — Pipeline Execution Log (DDL only)

| Thuộc tính | Chi tiết |
|-----------|---------|
| **Rows** | 0 (runtime only) |
| **Columns** | 10 (id, dag_id, task_id, status, cob_dt, rows_processed, execution_time_s, error_message, started_at, completed_at) |
| **CHECK** | status IN ('RUNNING','SUCCESS','FAILED','SKIPPED') |

**Vai trò:** Chi tiết execution log cho monitoring và debugging. Tracks rows_processed, execution_time_s per task.

### 6.5. Summary

```
opslakehouse schema (4 tables):
├── source_table_registry (19 rows)  ← DATA: registry mapping
├── flag_job_etl (runtime)           ← ETL control flags
├── data_quality_log (runtime)       ← DQ audit trail
└── pipeline_run_log (runtime)       ← Execution monitoring
```

---

## 7. So Sánh với Banking Thực Tế

### 7.1. Tổng quan — Mức độ phủ coverage

| Phân hệ | Trong dự án | Ngân hàng thực tế | Coverage |
|---------|------------|-------------------|----------|
| Core Banking | branch, customer, account, deposit, loan, loan_payment, standing_order, txn_account, employee, product | Core system + CIF + deposit core + loan core + clearing | ~60% |
| Card Management | card, card_txn | Card issuance + embossing + authorization + settlement + statement | ~30% |
| CRM | crm_interaction | Omnichannel CRM + campaign management + lead tracking | ~25% |
| Digital Banking | device, location, online_transaction, support_ticket, mcc_code, merchant | Mobile banking + internet banking + API gateway + fraud engine + KYC digital | ~40% |
| Operations | source_table_registry | Core-ops + reconciliation + treasury ops + payment gateway | ~10% |

### 7.2. Chi tiết — Những gì đã cover

**Đã model tốt (≥80% fidelity):**
- **Customer lifecycle:** registration → KYC → segmentation (RETAIL/PRIORITY/VIP) → active/inactive
- **Account management:** CASA + TIME_DEPOSIT, balance tracking, multi-currency (VND/USD)
- **Deposit products:** term deposits với interest rates, maturity, early withdrawal penalty
- **Loan lifecycle:** disbursement → amortization → late/missed payments → overdue → written_off
- **Card management:** DEBIT/CREDIT/PREPAID, brand distribution, credit limit conditional logic
- **Transaction channels:** Branch, ATM, Internet Banking, Mobile Banking, POS — reflection真实 channel usage
- **Fraud detection basics:** is_fraud flag, fraud_reason, high-risk location correlation, amount bias
- **Geographic consistency:** Region→City→District mapping consistent across all tables
- **Time-series realism:** Seasonal patterns, weekday bias, hour peaks

### 7.3. Chi tiết — Những gì simplified/thiếu

**Simplified (model exists but reduced complexity):**
- **Loan:** Chỉ có 4 loan_status (thiếu: RESTRUCTURED, PARTIAL_DISBURSE, GUARANTEE)
- **Card:** Không có card statement, billing cycle, minimum payment, rewards points
- **Deposit:** Không có auto-renewal, interest calculation method (simple vs compound)
- **Customer:** Không có SCD Type 2 fields (no effective_date/expire_date trong source)

**Missing (không model):**
- **Treasury & Payments:** SWIFT messages, RTGS, interbank clearing, NAPAS gateway
- **Trade Finance:** L/C, bank guarantees, documentary collections
- **Risk Management:** Credit scoring models, Basel III capital adequacy, IFRS 9 ECL
- **AML/CFT:** Transaction monitoring rules, STR/SAR reporting, PEP screening
- **Regulatory Reporting:** SBV reports (Circular 13/2023), consolidated statements
- **General Ledger:** Double-entry bookkeeping, chart of accounts, balance sheet
- **Interest Calculation:** Actual daily accrual, compounding logic, penalty rate tables
- **Multi-entity:** Consortium banks, branch-level P&L, inter-branch transactions

### 7.4..scale Comparison

| Metric | Dự án | Ngân hàng TMCP VN (top 5) | Tỷ lệ |
|--------|------|--------------------------|-------|
| Customers | 10,000 | 10–50 million | 0.01–0.1% |
| Accounts | 30,000 | 30–150 million | 0.02% |
| Transactions/day | ~1,600 (1.2M/6yr×365) | 5–20 million | 0.008–0.03% |
| Cards | 6,000 | 10–30 million | 0.02–0.06% |
| Branches | 100 | 300–1,000 | 10–33% |
| Employees | 1,800 | 10,000–50,000 | 3.6–18% |

### 7.5. Design Decisions — Tại sao simplified?

1. **Portfolio scope:** Dự án tập trung vào data engineering pipeline (Bronze→Silver→Gold), không phải banking domain depth
2. **Data volume:** ~2.76M rows đủ để demonstrate partitioning, incremental ingest, CDC, performance tuning
3. **Query variety:** 20 tables tạo ra multi-table JOINs, aggregations, window functions đủ phong phú
4. **Realism balance:** Fraud detection, loan_payment, standing_order thêm business complexity mà không cần full core banking
5. **Interview readiness:** 39 JDs banking yêu cầu tech stack (Spark/Iceberg/Airflow/Kafka), không yêu cầu full domain model

---

## 8. Data Distribution & Business Rules

### 8.1. Data Volume Summary

```
┌─────────────────────────────────────────────────────┐
│ Total: ~2,769,058 rows across 20 tables, 4 schemas │
├─────────────────────────────────────────────────────┤
│ core_banking:    1,530,930 rows  (55.3%)            │
│   └── txn_account: 1,200,000  (43.3% of total)     │
│ card_crm:          656,000 rows  (23.7%)            │
│   └── card_txn:    600,000  (21.7% of total)        │
│ digital_banking:   582,109 rows  (21.0%)            │
│   └── online_txn:  500,000  (18.1% of total)        │
│ opslakehouse:           19 rows  (0.001%)            │
└─────────────────────────────────────────────────────┘

Transaction-heavy: 2,300,000 rows (83.1%) across
  txn_account + card_txn + online_transaction
```

### 8.2. Foreign Key Dependency Chain

Data generator respects FK ordering:

```
Step 1: branch (100) ─── seed first, no FK
   │
Step 2: product (30) ─── seed second, no FK
   │
Step 3: customer (10K) ── FK → branch
   │
Step 4: account (30K) ── FK → customer, product, branch
   │
Step 5: deposit (15K) ── FK → account*, customer, product
   │    loan (5K) ────── FK → customer, product, branch
   │
Step 6: loan_payment (~250K) ─── FK → loan
   │    standing_order (15K) ─── FK → account, customer
   │    txn_account (1.2M) ───── FK → account
   │
Step 7: mcc_code (109) ── pre-gen, no FK (used by card & digital)
   │
Step 8: card (6K) ──── FK → product (logical: customer, account)
   │    card_txn (600K) ─── FK → card, mcc_code*
   │    crm_interaction (50K) ── FK → customer*
   │
Step 9: device (50K) ── FK → customer*
   │    location (5K) ── no FK
   │
Step 10: online_txn (500K) ── FK → account*, device, location
   │     support_ticket (25K) ── FK → customer*
   │     merchant (2K) ── FK → mcc_code
   │
Step 11: source_table_registry (19) ── manual seed
```

(*) Logical FK — không enforce trong DDL do cross-schema limitation

### 8.3. Business Rules Summary

| Rule | Mô tả | Bảng ảnh hưởng |
|------|-------|---------------|
| **Balance simulation** | Running balance_per_account, cập nhật sau mỗi txn | txn_account.balance_after |
| **Seasonal datetime** | 65% weekday, hour peaks 9–11h/19–21h | txn_account, card_txn, online_transaction |
| **Credit limit conditional** | CHỈ credit card mới có credit_limit | card |
| **Fraud correlation** | 35% fraud发生在 high-risk locations, amount bias | online_transaction |
| **Late payment pattern** | 5% late (penalty + days_late), 2% missed (amount=0) | loan_payment |
| **MCC linkage** | card_txn.mcc_code → mcc_code, merchant.mcc_code → mcc_code | card_txn, merchant, mcc_code |
| **Geographic consistency** | Customer城市的branch được assign bởi city→branch mapping | customer |
| **Watermark ingestion** | last_updated trigger enables JDBC incremental extract | ALL tables (20/20) |
| **Cross-schema FK** | card/digital tables reference core_banking IDs (logical, not enforced) | card, card_txn, online_transaction, device, support_ticket |
| **INSERT-only flags** | flag_job_etl chỉ INSERT, không UPDATE — Append-only pattern | flag_job_etl |

### 8.4. Date Range & Time Distribution

```
Date range: 2020-01-01 → 2025-12-31 (6 years of data)
Seed date:  2025-12-31 (cob_dt for all generated data)

Time distribution (transaction tables):
├── 65% weekday (Mon–Fri)
├── 35% weekend (Sat–Sun)
├── Peak hours: 09:00–11:00, 19:00–21:00
├── Low hours: 00:00–06:00
└── Uniform: 06:00–09:00, 11:00–19:00, 21:00–24:00
```

### 8.5. Money Distribution (VND)

| Table | Min | Max | Notes |
|-------|-----|-----|-------|
| account (CASA) | 100K | 500M | Range balance CASA |
| account (TIME_DEPOSIT) | 10M | 1B | Range balance tiết kiệm |
| deposit.principal | 5M | 1B | Gửi tiết kiệm |
| loan.loan_amount | 10M | 5B | Khoản vay最大值 |
| txn_account.txn_amount | 10K | 500M | Giao dịch tài khoản |
| card_txn.txn_amount | 50K | 50M | Giao dịch thẻ |
| online_transaction.amount | 10K | 100M | Giao dịch online |
| employee.salary | 8M | 80M | Lương tháng |
| card.credit_limit | 5M | 200M | Hạn mức thẻ tín dụng |

### 8.6. Vietnamese Localization Details

| Element | Value | Source |
|---------|-------|--------|
| Names | 50M + 50F first names, 50 last names | Faker vi_VN + custom pool |
| National ID | CCCD 12 digits | Regulation 2024 |
| Phone | 0xxxxxxxxx (10 digits) | VN mobile format |
| Currency | VND (no decimal) | National currency |
| Cities | Hanoi, HCM, Da Nang, Hai Phong, Can Tho, Bien Hoa, Nha Trang, Vung Tau, Hue, Quy Nhon | Top 10 VN cities by population |
| Regions | NORTH (35%), CENTRAL (20%), SOUTH (45%) | Population-weighted distribution |
| Bill payments | EVN (electricity), Viettel, FPT (telecom) | Major VN billers |
| Card brands | VISA, MASTER, JCB, NAPAS | VN market: NAPAS domestic + international brands |
| MCC codes | ISO 18245 standard | International standard |
