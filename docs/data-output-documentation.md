# Data Output Layer — Banking Data Platform

> Mô tả toàn diện về Data Output: cấu trúc lakehouse (Bronze → Silver → Gold → Serving),
> transformation logic, ETL pipelines, business rules, và so sánh với thực tế.
> Cập nhật: 2026-09-09

---

## 1. Tổng Quan

Dữ liệu OLTP từ PostgreSQL được ingest vào **lakehouse kiến trúc Medallion** gồm 4 tầng:

```
PostgreSQL (OLTP Source)
    │  JDBC Batch + Debezium CDC
    ▼
┌─────────────────────────────────────────────────┐
│  BRONZE — Raw ingestion (Iceberg)               │
│  22 tables (16 batch + 6 CDC)                   │
│  Load: overwritePartitions by cob_dt             │
├─────────────────────────────────────────────────┤
│  SILVER — Cleansed + Conformed (Iceberg)        │
│  15 tables (13 batch + 2 CDC current)           │
│  Load: SCD1 UPSERT / SCD2 history / fact append │
├─────────────────────────────────────────────────┤
│  GOLD — Business-ready marts (Iceberg)          │
│  10 tables                                      │
│  Load: overwritePartitions by cob_dt             │
├─────────────────────────────────────────────────┤
│  SERVING — Current snapshot (dbt + Trino)       │
│  9 tables (1 row / customer)                    │
│  Materialized: table (not view)                  │
└─────────────────────────────────────────────────┘
```

**Kỹ thuật lưu trữ:**
- Catalog: `lakehouse` (Iceberg REST catalog)
- Format: Apache Iceberg on MinIO (S3-compatible)
- Partition: `cob_dt` (Cut-Off Business Date) trên các fact/Gold tables
- Compression: ZSTD (default Iceberg)
- Timezone: UTC storage, Asia/Ho_Chi_Minh khi derive business date

**ETL Orchestration:** Apache Airflow 2.10 với flag-based dependency (`flag_job_etl`)

---

## 2. Tổng Quan Tầng & Bảng Nguồn

| # | Tầng | Schema | Số Bảng | Strategy | Schedule |
|---|------|--------|---------|----------|----------|
| 1 | Bronze | lakehouse.bronze | 22 | overwritePartitions | 02:00 AM |
| 2 | Silver | lakehouse.silver | 15 | SCD1 UPSERT / SCD2 / fact | 04:00 AM |
| 3 | Gold | lakehouse.gold | 10 | overwritePartitions | 06:00 AM |
| 4 | Serving | dbt (Trino) | 9 | materialized=table | Sau Gold |
| **Tổng** | | | **56 tables** | | |

**Pipeline Flow:**
```
02:00 — Bronze (3 DAGs: core_banking + card_crm + digital_banking)
04:00 — Silver (1 DAG: 8 dims + 5 facts)
06:00 — Gold (1 DAG: 5 mart360 + 4 segments + 1 time_analytics)
Sau Gold — dbt serving (9 tables, runs with Gold DAG)
```

---

## 3. Bronze Layer — Raw Ingestion

Schema `lakehouse.bronze` — 22 bảng. Tầng giữ nguyên cấu trúc nguồn PostgreSQL, thêm `cob_dt` partition. Dữ liệu dạng full snapshot: mỗi cob_dt chứa toàn bộ records.

**Naming convention:** `{source_schema_prefix}_{table_name}`
- `core_banking.*` → `core_{table}` (e.g., core_customer, core_account)
- `card_crm.*` → `core_{table}` (e.g., core_card, core_card_txn)
- `digital_banking.*` → `core_{table}` (e.g., core_device, core_online_transaction)

### 3.1. Batch Tables (16 tables)

#### 3.1.1. core_banking Domain (8 tables)

| Bronze Table | Source | Rows | Columns | Type |
|-------------|--------|------|---------|------|
| core_branch | core_banking.branch | 100 | 10 | Dimension |
| core_product | core_banking.product | 30 | 8 | Dimension |
| core_customer | core_banking.customer | 10,000 | 16 | Dimension |
| core_account | core_banking.account | 30,000 | 12 | Dimension |
| core_deposit | core_banking.deposit | 15,000 | 11 | Dimension |
| core_loan | core_banking.loan | 5,000 | 12 | Dimension |
| core_employee | core_banking.employee | 1,800 | 8 | Dimension |
| core_txn_account | core_banking.txn_account | 1,200,000 | 13 | **Fact** |

**core_customer — Cross-schema FK:** branch_code → core_branch. PII columns: cccd, full_name, phone, email, address. Indexed on branch_code, customer_segment, last_updated.

**core_account — Cross-schema FKs:** customer_id → core_customer, product_code → core_product, branch_code → core_branch. Partitioned by cob_dt.

**core_deposit — Nullable FK:** account_id can be NULL (standalone savings). FKs: customer_id → core_customer, product_code → core_product.

**core_loan — Multi-FK:** customer_id → core_customer, product_code → core_product, branch_code → core_branch.

**core_txn_account — Largest Bronze table (1.2M rows):** Contains denormalized customer_id for query performance. Indexed on (account_id, txn_date) and (customer_id, txn_date) for incremental ingestion and customer-level queries.

#### 3.1.2. card_crm Domain (3 tables)

| Bronze Table | Source | Rows | Columns | Type |
|-------------|--------|------|---------|------|
| core_card | card_crm.card | 6,000 | 12 | Dimension |
| core_card_txn | card_crm.card_txn | 600,000 | 16 | **Fact** |
| core_crm_interaction | card_crm.crm_interaction | 50,000 | 12 | **Fact** |

**core_card:** Debit/Credit/Prepaid with conditional credit_limit CHECK constraint preserved from source.

**core_card_txn:** Contains mcc_code FK → digital_banking.mcc_code (cross-schema). Reference_number format: CDN + sequential. Indexed on (card_id, txn_date).

**core_crm_interaction:** 5 channels (CALL/EMAIL/CHAT/BRANCH/SMS), 5 categories. Indexed on (customer_id, interaction_date).

#### 3.1.3. digital_banking Domain (5 tables)

| Bronze Table | Source | Rows | Columns | Type |
|-------------|--------|------|---------|------|
| core_device | digital_banking.device | 50,000 | 10 | Dimension |
| core_location | digital_banking.location | 5,000 | 9 | Dimension |
| core_online_transaction | digital_banking.online_transaction | 500,000 | 15 | **Fact** |
| core_support_ticket | digital_banking.support_ticket | 25,000 | 10 | Dimension |
| core_mcc_code | digital_banking.mcc_code | 109 | 5 | Dimension |

**core_online_transaction:** Contains is_fraud flag (0.8% rate), fraud_reason, high-risk location correlation. Indexed on (customer_id, transaction_date), (account_id, transaction_date), is_fraud.

### 3.2. CDC Tables (6 tables)

Mô hình Change Data Capture từ Debezium → Kafka → Spark Streaming → Bronze CDC.

| CDC Table | Source | Rows | Partition |
|-----------|--------|------|-----------|
| core_customer_cdc | core_banking.customer | ~90,014 | DATE(__cdc_timestamp) |
| core_account_cdc | core_banking.account | ~30,000 | DATE(__cdc_timestamp) |
| core_transaction_cdc | core_banking.txn_account | ~1,200,000 | DATE(__cdc_timestamp) |
| card_account_cdc | card_crm.card | ~6,000 | DATE(__cdc_timestamp) |
| card_transaction_cdc | card_crm.card_txn | ~600,000 | DATE(__cdc_timestamp) |
| online_transaction_cdc | digital_banking.online_transaction | ~500,000 | DATE(__cdc_timestamp) |

**CDC columns on each table:**
- `__cdc_operation`: INSERT / UPDATE / DELETE
- `__cdc_timestamp`: Wall-clock time of change
- `__spark_batch_id`: Spark Structured Streaming batch identifier

**Pattern:** Append-only. Mỗi row = 1 change event (không ghi đè). CDC consolidation job tạo `dim_customer_current` và `dim_account_current` (Silver) bằng latest-state logic.

### 3.3. Bronze Load Strategy

```yaml
# Mỗi Bronze table có YAML config:
load:
  strategy: full_snapshot     # Full load từ PostgreSQL

# Transformation khi load:
# 1. JDBC fetch → Spark DataFrame
# 2. Thêm column: cob_dt = '{{ ds }}'
# 3. Ghi vào Iceberg: overwritePartitions by cob_dt
```

**Partitioning:** Mọi Bronze table đều partitioned by `cob_dt` — mỗi DAG run tạo 1 partition mới.

### 3.4. Bronze Naming Convention Summary

| Source Schema | Source Table | Bronze Table Name |
|--------------|-------------|-------------------|
| core_banking | branch | core_branch |
| core_banking | product | core_product |
| core_banking | customer | core_customer |
| core_banking | account | core_account |
| core_banking | deposit | core_deposit |
| core_banking | loan | core_loan |
| core_banking | employee | core_employee |
| core_banking | txn_account | core_txn_account |
| card_crm | card | core_card |
| card_crm | card_txn | core_card_txn |
| card_crm | crm_interaction | core_crm_interaction |
| digital_banking | device | core_device |
| digital_banking | location | core_location |
| digital_banking | online_transaction | core_online_transaction |
| digital_banking | support_ticket | core_support_ticket |
| digital_banking | mcc_code | core_mcc_code |

---

## 4. Silver Layer — Cleansed & Conformed

Schema `lakehouse.silver` — 15 bảng (8 dims + 5 facts + 2 CDC current). Tầng làm sạch, deduplicate, và conform dữ liệu. Dimensions dùng surrogate key (SCD2) hoặc UPSERT (SCD1). Facts được join với dimensions để lấy surrogate keys.

### 4.1. Dimensions — SCD Type 1 (6 tables)

SCD Type 1 = UPSERT: nếu record đã tồn tại → cập nhật tại chỗ, nếu chưa → chèn mới. Không lưu lịch sử.

| Silver Table | Source (Bronze) | Business Key | Columns | Rows |
|-------------|----------------|--------------|---------|------|
| dim_branch | core_branch | branch_code | 10 | 100 |
| dim_product | core_product | product_code | 8 | 30 |
| dim_card | core_card | card_id | 12 | 6,000 |
| dim_employee | core_employee | employee_id | 8 | 1,800 |
| dim_device | core_device | device_id | 10 | 50,000 |
| dim_location | core_location | location_id | 9 | 5,000 |

**SCD Type 1 MERGE pattern:**
```sql
MERGE INTO lakehouse.silver.dim_branch t
USING source_view s
ON t.branch_code = s.branch_code          -- business key
WHEN MATCHED THEN UPDATE SET t.* = s.*    -- overwrite all non-key cols
WHEN NOT MATCHED THEN INSERT VALUES s.*   -- insert new
```

**Logic:** YAML-driven. Engine `scd_type1.py` tự动生成 MERGE từ business_key list.

### 4.2. Dimensions — SCD Type 2 (2 tables)

SCD Type 2 = Full history tracking: mỗi thay đổi tạo row mới với effective_from/to flags.

| Silver Table | Source (Bronze) | Business Key | Tracked Columns | SK Column |
|-------------|----------------|--------------|-----------------|-----------|
| dim_customer | core_customer | customer_id | phone, email, address, city, district, customer_segment, kyc_status, branch_code | customer_sk |
| dim_account | core_account | account_id | balance, status, close_date | account_sk |

**SCD Type 2 columns (thêm so với source):**
| Column | Type | Mô tả |
|--------|------|-------|
| effective_from | TIMESTAMP | Thời điểm record trở thành current |
| effective_to | TIMESTAMP | Thời điểm record bị expire (NULL = current) |
| is_current | SMALLINT | 1 = current, 0 = expired |
| customer_sk / account_sk | VARCHAR(64) | SHA-256 surrogate key |

**SCD Type 2 MERGE pattern:**
```sql
-- 1. Expire existing current rows where tracked columns changed
-- 2. Insert new current rows with updated effective_from
-- Engine handles idempotent rerun (cleanup previous run first)
```

**dim_customer tracked columns (8 fields):**
phone, email, address, city, district, customer_segment, kyc_status, branch_code

**dim_account tracked columns (3 fields):**
balance, status, close_date

**Rows:** dim_customer ~10,000 (1x source, each cob_dt snapshots current), dim_account ~30,000

### 4.3. Facts (5 tables)

Facts dùng `overwritePartitions by cob_dt`: mỗi cob_dt = 1 partition mới, overwrite nếu tồn tại.

| Silver Table | Source (Bronze) | Dimensions Joined | Rows/Day | Business Key |
|-------------|----------------|-------------------|----------|--------------|
| fact_txn_account | core_txn_account | dim_account (account_sk) + dim_customer (customer_sk) | ~1,200,000 | txn_id |
| fact_card_txn | core_card_txn | dim_customer (customer_sk) | ~600,000 | txn_id |
| fact_crm_interaction | core_crm_interaction | dim_customer (customer_sk) | ~50,000 | interaction_id |
| fact_online_transaction | core_online_transaction | dim_customer (customer_sk) + device + location | ~500,000 | transaction_id |
| fact_support_ticket | core_support_ticket | dim_customer (customer_sk) | ~25,000 | ticket_id |

**Fact JOIN pattern:**
```sql
-- LEFT JOIN với dim, chỉ lấy is_current = 1
FROM lakehouse.bronze.core_txn_account t
LEFT JOIN lakehouse.silver.dim_account a
    ON t.account_id = a.account_id AND a.is_current = 1
LEFT JOIN lakehouse.silver.dim_customer c
    ON t.customer_id = c.customer_id AND c.is_current = 1
WHERE t.cob_dt = DATE '{{ cob_dt }}'
```

**Surrogate keys trong facts:** fact_txn_account chứa cả natural keys (account_id, customer_id) VÀ surrogate keys (account_sk, customer_sk) — denormalized cho query performance.

### 4.4. Silver CDC Current-State (2 tables)

CDC streaming jobs tạo 2 bảng mutable current-state từ CDC events.

| Silver Table | Source (CDC) | Rows | Pattern |
|-------------|-------------|------|---------|
| dim_customer_current | core_customer_cdc | 10,000 | Latest state per customer_id |
| dim_account_current | core_account_cdc | 30,000 | Latest state per account_id |

**Additional columns:**
- `__cdc_operation`: Last operation (INSERT/UPDATE/DELETE)
- `__cdc_timestamp`: Last change timestamp
- `__consolidated_at`: When consolidation job ran

### 4.5. Silver DAG Execution Order

```
silver_all_dag (04:00 AM):
  1. Check Bronze DAGs complete (3 sensors)
  2. Run 8 dim jobs parallel:
     ├── SCD1: dim_branch, dim_product, dim_card, dim_employee, dim_device, dim_location
     └── SCD2: dim_customer, dim_account
  3. Run 5 fact jobs parallel:
     ├── fact_txn_account, fact_card_txn, fact_crm_interaction
     └── fact_online_transaction, fact_support_ticket
```

---

## 5. Gold Layer — Business-Ready Marts

Schema `lakehouse.gold` — 10 bảng. Tầng aggregates và business logic: Customer 360, RFM segmentation, churn prediction, cross-sell, campaign targeting, branch analytics.

**Grain:** Mỗi Gold table = daily snapshot partitioned by `cob_dt`. Queries current chỉ dùng `*_current` serving tables.

### 5.1. Mart360 — Customer-Level Aggregations (5 tables)

| Gold Table | Sources | Grain | Key Metrics |
|-----------|---------|-------|-------------|
| mart_customer_360 | dim_customer + dim_account + dim_card + 4 facts | 1 row/customer/cob_dt | 28+ KPIs: profile, holdings, transactions, RFM, churn_flag, AUM bucket |
| customer_balance_summary | dim_customer + dim_account | 1 row/customer | total_balance, avg_balance, aum_total, aum_bucket |
| customer_transaction_summary | dim_customer + fact_txn_account + fact_card_txn | 1 row/customer | acct/card txn 30d counts/amounts, combined totals, last_txn_date |
| customer_product_summary | dim_customer + dim_account + dim_card | 1 row/customer | account/card counts, has_credit_card, has_savings, has_loan |
| customer_card_summary | dim_customer + dim_card + fact_card_txn | 1 row/customer | card holding + card txn 30d metrics, max_credit_limit |

#### 5.1.1. mart_customer_360 — Detailed Column Map

| Column | Type | Source | Mô tả |
|--------|------|--------|-------|
| customer_id | BIGINT | dim_customer | Natural key |
| customer_sk | VARCHAR(64) | dim_customer | SCD2 surrogate key |
| full_name_masked | STRING | dim_customer | First name + ** (PII masked) |
| age | INT | dim_customer | Derived from date_of_birth |
| gender | CHAR(1) | dim_customer | M/F/O |
| primary_branch_code | VARCHAR(10) | dim_customer | Branch from dim |
| customer_segment | VARCHAR(20) | dim_customer | RETAIL/PRIORITY/VIP |
| kyc_status | VARCHAR(20) | dim_customer | VERIFIED/PENDING/REJECTED |
| register_date | DATE | dim_customer | Registration date |
| total_accounts | INT | dim_account | COUNT(DISTINCT account_id) |
| total_cards | INT | dim_card | COUNT(DISTINCT card_id) |
| total_loans | INT | Hardcoded 0 | Placeholder (no Silver loan fact yet) |
| has_credit_card | INT | dim_card | 0/1 flag |
| has_savings | INT | Hardcoded 1 | Assumption: all customers have savings |
| has_loan | INT | Hardcoded 0 | Placeholder |
| total_deposit_balance | NUMERIC(18,2) | dim_account | SUM(balance) where ACTIVE |
| total_loan_outstanding | NUMERIC(18,2) | Hardcoded 0 | Placeholder |
| aum_total | NUMERIC(18,2) | dim_account | = total_deposit_balance |
| aum_bucket | VARCHAR(20) | Derived | VIP / PRIORITY / AFFLUENT / MASS |
| txn_count_30d | INT | facts | Account + card txn count last 30 days |
| txn_amount_30d | NUMERIC(18,2) | facts | Account + card txn amount last 30 days |
| last_txn_date | TIMESTAMP | facts | GREATEST(last_acct, last_card) |
| days_since_last_txn | INT | Derived | cob_dt - last_txn_date |
| primary_channel | VARCHAR(20) | fact_txn_account | Most-used channel last 30d |
| interaction_count_90d | INT | fact_crm_interaction | CRM interactions last 90 days |
| last_interaction_date | TIMESTAMP | fact_crm_interaction | Most recent CRM interaction |
| rfm_recency_score | INT | NTILE(5) | 1-5, lower = more recent |
| rfm_frequency_score | INT | NTILE(5) | 1-5, higher = more frequent |
| rfm_monetary_score | INT | NTILE(5) | 1-5, higher = more spending |
| rfm_segment | VARCHAR(20) | Derived | Champions/Loyal/Potential/New/AtRisk/Hibernating/Lost |
| churn_flag | INT | Derived | 1 if no txn > 90 days |
| cross_sell_credit_card_flag | INT | Derived | 1 if AUM >= 100M and no credit card |
| cob_dt | DATE | Static | Partition date |

**AUM Bucket Thresholds:**

| Bucket | Threshold (VND) | Equivalent |
|--------|----------------|-----------|
| VIP | >= 5,000,000,000 | >= 5 ty |
| PRIORITY | >= 1,000,000,000 | >= 1 ty |
| AFFLUENT | >= 100,000,000 | >= 100 trieu |
| MASS | < 100,000,000 | < 100 trieu |

**RFM Segment Logic:**

| Segment | Score Range (R+F+M) | Mo ta |
|---------|---------------------|-------|
| Champions | >= 13 | Khach hang tot nhat |
| Loyal Customers | 10-12 | Khach trung thanh |
| Potential Loyalists | 7-9 | Tiem nang trung thanh |
| New Customers | 5-6 | Khach moi |
| At Risk | 3-4 | Co nguy roi bo |
| Hibernating | 2 | Dang ngu dong |
| Lost | 1 | Da mat |

### 5.2. Segmentation (4 tables)

| Gold Table | Sources | Grain | Key Logic |
|-----------|---------|-------|-----------|
| rfm_segment | dim_customer + fact_txn_account + fact_card_txn | 1 row/customer/cob_dt | NTILE(5) RFM scoring, 90-day lookback |
| churn_prediction | dim_customer + fact_txn_account + fact_card_txn | 1 row/customer/cob_dt | Rule-based: High/Medium/Low/Active |
| cross_sell_segment | dim_customer + dim_card | 1 row/customer/cob_dt | Product gap detection |
| campaign_target | gold.rfm_segment + gold.churn_prediction + gold.cross_sell_segment + gold.mart_customer_360 | 1 row/customer/cob_dt | Campaign type assignment |

#### 5.2.1. rfm_segment — RFM Scoring

**Columns:** customer_id, customer_sk, recency_days, frequency, monetary, r_score, f_score, m_score, rfm_score, rfm_segment, cob_dt

**Scoring:** NTILE(5) over recency (ASC), frequency (DESC), monetary (DESC)

**Lookback window:** 90 days from cob_dt. Grain rule: each fact self-aggregates to grain customer_id in separate CTE before join — eliminates Cartesian amplification.

#### 5.2.2. churn_prediction — Churn Risk

**Churn Risk Levels:**

| Risk | Condition |
|------|-----------|
| High | No txn in 90+ days OR never transacted |
| Medium | Last txn 61-90 days ago |
| Low | Last txn 31-60 days ago |
| Active | Last txn <= 30 days ago |

**Lookback windows:** 30d, 90d, 365d for transaction counts/amounts (both account + card).

#### 5.2.3. cross_sell_segment — Product Gap

**Columns:** customer_id, customer_sk, customer_segment, no_credit_card, no_debit_card, primary_opportunity, cob_dt

**Opportunity Logic:**

| Condition | primary_opportunity |
|-----------|-------------------|
| No active credit card | Credit Card |
| No active debit card (has credit) | Debit Card |
| Both cards present | None |

#### 5.2.4. campaign_target — Campaign Assignment

**Phase 2 dependency:** Depends on Phase 1 completion (rfm_segment + churn_prediction + cross_sell_segment + mart_customer_360).

**Campaign Type Logic:**

| Condition | campaign_type |
|-----------|--------------|
| churn_candidate=1 AND rfm IN (At Risk, Hibernating) | Retention |
| no_credit_card=1 AND aum_bucket IN (AFFLUENT/PRIORITY/VIP) | Cross_Sell_CC |
| primary_opportunity != None | Cross_Sell |
| rfm IN (Champions, Loyal Customers) | Upsell |
| Default | Awareness |

### 5.3. Time Analytics (1 table)

| Gold Table | Sources | Grain | Key Metrics |
|-----------|---------|-------|-------------|
| mart_branch_monthly_summary | dim_branch + dim_account + fact_txn_account | 1 row/branch/month | active_customers, txn_count, amounts, top_channel |

**Columns:** branch_code, branch_name, region, city, txn_year, txn_month, txn_quarter, active_customers, txn_count, total_txn_amount, avg_txn_amount, total_credit_amount, total_debit_amount, top_channel, cob_dt

### 5.4. Gold Validation Rules

Each Gold table has `require_non_empty: true` — job fails if query returns 0 rows.

Snapshot-backed sources have additional `require_snapshots` — checks partition `cob_dt` exists before transform. Missing partition = silent corruption (metrics = 0 instead of fail).

### 5.5. Gold DAG Execution

```
gold_all_dag (06:00 AM):
  1. Check silver_all_dag complete
  2. Phase 1 — Independent (parallel):
     ├── mart_customer_360, customer_balance_summary, customer_transaction_summary
     ├── customer_product_summary, customer_card_summary
     ├── rfm_segment, churn_prediction, cross_sell_segment
     └── branch_monthly_summary
  3. Phase 2 — Dependent (sequential after Phase 1):
     └── campaign_target
  4. GOLD_COMPLETE flag (contract for downstream consumers)
```

---

## 6. Serving Layer — Current Snapshot (dbt + Trino)

Schema `dbt (iceberg.serving)` — 9 tables. Tầng current-serving: mỗi Gold historical table có phiên bản `*_current` filter by `var('cob_dt')`. Grain: exactly 1 row per customer.

### 6.1. Serving Tables

| Serving Model | Gold Source | Wave | Materialized |
|--------------|------------|------|-------------|
| rfm_segment_current | gold.rfm_segment | Wave 1 | table |
| churn_prediction_current | gold.churn_prediction | Wave 1 | table |
| customer_transaction_summary_current | gold.customer_transaction_summary | Wave 1 | table |
| customer_balance_summary_current | gold.customer_balance_summary | Wave 2 | table |
| customer_card_summary_current | gold.customer_card_summary | Wave 2 | table |
| customer_product_summary_current | gold.customer_product_summary | Wave 2 | table |
| cross_sell_segment_current | gold.cross_sell_segment | Wave 2 | table |
| campaign_target_current | gold.campaign_target | Wave 2 | table |
| mart_customer_360_current | gold.mart_customer_360 | Wave 3 | table |

### 6.2. Serving Model Pattern

Every model follows the same pattern:

```sql
-- materialized=table (not view: Trino + Iceberg REST doesn't support createView)
-- Grain: 1 row per customer_id
-- cob_dt comes from var('cob_dt'), NOT MAX(cob_dt)
select *
from {{ source('gold', '<gold_table>') }}
where cob_dt = date '{{ cob_dt }}'
```

**Why table not view:** Trino + Iceberg REST catalog does not support `createView` (NOT_SUPPORTED, verified in pilot). Freshness depends on dbt DAG running each cob_dt alongside Gold DAG.

**Why var not MAX:** MAX silently serves D1 when D2 pipeline fails — "latest available" vs intended "current verified processing date".

### 6.3. Serving Data Quality Contract

Defined in `_serving_models.yml`:

| Test | Scope | Rule |
|------|-------|------|
| unique | customer_id | No duplicate customers |
| not_null | customer_id | Every row has customer |
| not_null | cob_dt | Partition date required |
| accepted_values | churn_risk | High/Medium/Low/Active |
| accepted_values | rfm_segment | Champions/Loyal/.../Lost |
| accepted_values | campaign_type | Retention/Cross_Sell/Upsell/Awareness |

### 6.4. dbt Execution

```bash
dbt build --select serving --vars '{"cob_dt": "YYYY-MM-DD"}'
```

Runs after Gold DAG completes. Custom `generate_schema_name` macro routes to `serving` schema directly.

### 6.5. dbt Tests (6 singular tests)

| Test | Purpose |
|------|---------|
| assert_gold_source_reachable | Smoke test: Trino can read gold.rfm_segment and it is non-empty |
| assert_serving_snapshot_alignment | All 4 core serving tables share same cob_dt |
| assert_customer_360_completeness | Customer 360 completeness check |
| assert_rfm_scores_valid | RFM scores within valid range |
| assert_no_duplicate_customers | Grain integrity: 1 row per customer |
| assert_balances_non_negative | Business rule: balances >= 0 |

---

## 7. So Sánh với Banking Thực Tế

### 7.1. Medallion Architecture Comparison

| Component | Dự án | Ngân hàng thực tế | Coverage |
|-----------|------|-------------------|----------|
| Bronze (Raw) | Full snapshot JDBC + CDC streaming | CDC + micro-batch + real-time streaming | ~70% |
| Silver (Cleansed) | SCD1 UPSERT + SCD2 history + facts | SCD + dedup + data quality + quarantine | ~60% |
| Gold (Business) | 5 mart360 + 4 segments + 1 time_analytics | Hundreds of marts, cubes, aggregate tables | ~20% |
| Serving | 9 current tables via dbt | Data services layer (APIs, materialized views, OLAP cubes) | ~30% |

### 7.2. What the Project Models Well

**Strong fidelity (>=80%):**
- **Bronze raw ingestion:** Full snapshot pattern is standard for initial load. CDC for incremental is production-grade.
- **SCD Type 2:** Industry-standard slowly changing dimension with surrogate keys — directly applicable to real banking.
- **RFM segmentation:** Standard customer analytics methodology used by all retail banks.
- **Churn prediction:** Rule-based approach mirrors early-stage bank analytics before ML adoption.
- **AUM bucketing:** VIP/PRIORITY/AFFLUENT/MASS classification is standard in Vietnamese banking.
- **ETL orchestration:** Airflow DAGs with flag-based dependency is production-grade pattern.
- **Data quality gates:** require_non_empty + require_snapshots prevents silent corruption.

### 7.3. Simplified/Missing vs Real Banking

**Simplified:**
- **Gold layer:** 10 marts vs hundreds in real bank. Missing: regulatory marts (SBV Circular 13), financial marts (P&L, balance sheet), operational marts (ATM utilization, channel performance).
- **Serving:** 9 tables vs 100+ materialized views. Missing: real-time serving (Kafka→Redis), API caching, role-based data access.
- **Data quality:** Basic row-count/null checks vs statistical profiling, anomaly detection, data contracts.

**Missing entirely:**
- **Data Vault:** No raw vault, business vault, or point-in-time tables
- **Incremental:** Bronze uses full_snapshot, not CDC-based incremental (CDC exists but separate pipeline)
- **Multi-tenancy:** Single-tenant, no bank/entity partitioning
- **Lineage automation:** OpenMetadata lineage is manual, not auto-captured from Spark
- **Cost optimization:** No compaction, small file optimization, or partition pruning strategy
- **Disaster recovery:** No cross-region replication, no backup/restore strategy
- **Schema evolution:** No schema registry, no backward/forward compatibility handling
- **Late-arriving data:** No watermark management for out-of-order events

### 7.4. Scale Comparison

| Metric | Dự án | Ngân hàng TMCP VN (top 5) | Tỷ lệ |
|--------|------|--------------------------|-------|
| Daily transactions | ~1.2M | 50-200M | 0.6-2.4% |
| Bronze tables | 22 | 200-500 | 4-11% |
| Silver tables | 15 | 100-300 | 5-15% |
| Gold marts | 10 | 100-500 | 2-10% |
| Serving tables | 9 | 100-300 | 3-9% |
| ETL latency | ~4 hours (batch) | <1 hour (batch), <5 min (realtime) | N/A |
| Data retention | 6 years | 10-20 years | 30-60% |

### 7.5. Design Decisions

1. **Full snapshot Bronze:** Simplifies initial load; real banks use CDC for incremental (already implemented separately).
2. **SCD2 on customer/account only:** These are the only dimensions that change meaningfully; others are effectively static.
3. **customer_id as grain:** Most Gold tables grain to customer — aligns with retail banking's customer-centric analytics.
4. **Hardcoded placeholders (total_loans, has_loan):** Loan fact not yet in Silver; fields reserved for future expansion.
5. **campaign_target as Phase 2:** Depends on all other Gold tables; ensures campaign targets are based on complete data.

---

## 8. Data Distribution & Business Rules

### 8.1. Data Volume Across Layers

```
Layer           Tables    Rows (approx)    Storage Pattern
─────────────   ──────    ────────────     ──────────────────────
Bronze (batch)  16        ~1,932,830       Full snapshot per cob_dt
Bronze (CDC)    6         ~2,426,014       Append-only events
Silver (dims)   8         ~102,930         UPSERT / SCD2
Silver (facts)  5         ~2,375,000       OverwritePartitions
Silver (CDC)    2         ~40,000          Mutable current-state
Gold            10        ~10,000+         OverwritePartitions
Serving         9         ~90,000          Table (refreshed daily)
```

### 8.2. Foreign Key Relationships (Cross-Layer)

```
Bronze Layer:
  core_branch ←── core_customer (branch_code)
  core_branch ←── core_account (branch_code)
  core_product ←── core_account (product_code)
  core_customer ←── core_account (customer_id)
  core_customer ←── core_deposit (customer_id)
  core_account ←── core_deposit (account_id, nullable)
  core_customer ←── core_loan (customer_id)
  core_product ←── core_loan (product_code)
  core_branch ←── core_loan (branch_code)
  core_loan ←── (no Silver fact yet)
  core_account ←── core_txn_account (account_id)
  core_card ←── core_card_txn (card_id)
  core_mcc_code ←── core_card_txn (mcc_code, nullable)

Silver Layer:
  dim_customer (is_current=1) ←── fact_txn_account (customer_sk)
  dim_account (is_current=1) ←── fact_txn_account (account_sk)
  dim_customer (is_current=1) ←── fact_card_txn (customer_sk)
  dim_customer (is_current=1) ←── fact_crm_interaction (customer_sk)
  dim_customer (is_current=1) ←── fact_online_transaction (customer_sk)
  dim_customer (is_current=1) ←── fact_support_ticket (customer_sk)

Gold Layer:
  rfm_segment + churn_prediction + cross_sell_segment + mart_customer_360
      └── campaign_target (Phase 2 dependency)
```

### 8.3. Time Semantics

| Term | Definition | Example |
|------|-----------|---------|
| cob_dt | Cut-Off Business Date — orchestration/snapshot date | 2025-12-31 |
| txn_date | Business event timestamp (UTC storage) | 2025-12-31 14:30:00 UTC |
| last_updated | PostgreSQL trigger-generated, used for JDBC watermark | 2025-12-31 14:30:05 ICT |
| effective_from | SCD2 record start timestamp | 2025-12-31 04:00:00 UTC |
| effective_to | SCD2 record end timestamp (NULL = current) | 2026-01-01 04:00:00 UTC |

**Business date derivation:**
```sql
-- CORRECT: explicit timezone conversion for business date
CAST(from_utc_timestamp(txn_date, 'Asia/Ho_Chi_Minh') AS DATE)

-- WRONG: depends on session timezone (Spark vs Trino can differ 29.2%)
CAST(txn_date AS DATE)
```

### 8.4. Partitioning Strategy

| Layer | Partition Column | Strategy | Notes |
|-------|-----------------|----------|-------|
| Bronze | cob_dt | overwritePartitions | Each DAG run = 1 partition |
| Silver (dims) | is_current | N/A (SCD2) | Current + historical rows |
| Silver (facts) | cob_dt | overwritePartitions | Each DAG run = 1 partition |
| Gold | cob_dt | overwritePartitions | Each DAG run = 1 partition |
| Serving | None | Full table refresh | materialized=table |

### 8.5. DAG Dependency Chain

```
Phase A — Infrastructure:
  docker-compose up → PostgreSQL, Spark, Iceberg, MinIO, Trino, Airflow

Phase B — Data Seeding:
  generate_all.py → 20 PostgreSQL tables (~2.76M rows)

Phase C — Batch ETL:
  02:00  bronze_core_banking_dag (8 tables)
  02:00  bronze_card_crm_dag (3 tables)
  02:00  bronze_digital_banking_dag (5 tables)
  04:00  silver_all_dag → dims (8) → facts (5)
  06:00  gold_all_dag → Phase1 (9) → Phase2 (1) → GOLD_COMPLETE

Phase D — CDC Pipeline:
  Debezium → Kafka → Spark Streaming → Bronze CDC (6 tables)
  cdc_consolidation_dag → Silver CDC current (2 tables)

Phase E — Serving:
  dbt build --select serving → 9 current tables

Phase F — Monitoring:
  Prometheus + Grafana + Freshness Exporter
  OpenMetadata lineage + tags + glossary
```

### 8.6. ETL Engine Architecture

| Component | File | Role |
|-----------|------|------|
| Bronze JDBC | code_etl/bronze/base_job/ingestion_jdbc.py | PostgreSQL → Iceberg full snapshot |
| Silver SCD1 | code_etl/silver/base_job/scd_type1.py | YAML-driven UPSERT MERGE |
| Silver SCD2 | code_etl/silver/base_job/scd_type2.py | Full history with SHA-256 surrogate keys |
| Silver Fact | code_etl/silver/base_job/fact_txn.py | Incremental fact with dim joins |
| Gold | code_etl/gold/base_job/gold_job.py | SQL from YAML, overwritePartitions |
| CDC Streaming | code_etl/cdc/base_job/cdc_streaming.py | Debezium → Spark Structured Streaming |
| CDC Consolidation | code_etl/cdc/consolidation/cdc_consolidation.py | Latest-state from append-only CDC |

**Metadata-driven:** All transformations defined in YAML configs, not hardcoded. Engine reads YAML, generates MERGE/INSERT dynamically.

### 8.7. Naming Convention Summary

| Layer | Pattern | Example |
|-------|---------|---------|
| Bronze | `core_{source_table}` | core_customer, core_card_txn |
| Silver dims | `dim_{entity}` | dim_customer, dim_account, dim_branch |
| Silver facts | `fact_{domain}_{entity}` | fact_txn_account, fact_card_txn |
| Silver CDC | `dim_{entity}_current` | dim_customer_current, dim_account_current |
| Gold mart | `mart_{scope}` or `customer_{topic}_summary` | mart_customer_360, customer_balance_summary |
| Gold segment | `{type}_segment` or `{type}_prediction` | rfm_segment, churn_prediction, cross_sell_segment |
| Gold campaign | `campaign_target` | campaign_target |
| Gold time | `mart_branch_monthly_summary` | mart_branch_monthly_summary |
| Serving | `{gold_table}_current` | mart_customer_360_current, rfm_segment_current |
