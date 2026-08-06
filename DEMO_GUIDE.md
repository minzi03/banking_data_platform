# =============================================================================
# DEMO CHI TIẾT — Banking Data Platform
# Lakehouse Architecture: PostgreSQL → Iceberg (Bronze/Silver/Gold) → dbt Semantic
# =============================================================================

## Kiến trúc tổng thể

```
┌──────────┐    ┌───────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│   Data   │───▶│ PostgreSQL│───▶│  Bronze  │───▶│  Silver  │───▶│   Gold   │
│Generator │    │  (Source) │    │  (Raw)   │    │ (Clean)  │    │  (Mart)  │
└──────────┘    └───────────┘    └──────────┘    └──────────┘    └──────────┘
                                        │                            │
                              CDC (Debezium)                         │
                              ┌──────────┘                           │
                              ▼                                      ▼
                         ┌──────────┐                          ┌──────────┐
                         │  Kafka   │                          │   dbt    │
                         │(Streaming│                          │(Semantic)│
                         └──────────┘                          └──────────┘
                                                                  │
                                                                  ▼
                                                           ┌──────────┐
                                                           │  Trino   │
                                                           │ (Query)  │
                                                           └──────────┘
```

**Stack:**
- **20 Docker containers** — ~18GB memory budget
- **PostgreSQL 15** — Source database (3 schemas: core_banking, card_crm, digital_banking)
- **MinIO** — S3-compatible object storage
- **Iceberg REST 1.6** — Table catalog (JDBC backend)
- **Spark 3.5.3** — All ETL transformations (Bronze → Silver → Gold)
- **dbt 1.12.0** — Semantic layer on Gold (3 ephemeral models)
- **Trino 443** — Query engine
- **Airflow 2.10.0** — Orchestration (19+ DAGs)
- **Debezium + Kafka** — CDC pipeline
- **OpenMetadata 1.5.6** — Data Catalog & Lineage tracking

---

## BƯỚC 1: Khởi động Infrastructure

### 1.1 Cấu hình Environment

```bash
cd banking_data_platform

# Tạo .env từ template
cp docker/.env.example docker/.env

# Chỉnh sửa mật khẩu an toàn
# docker/.env → POSTGRES_PASSWORD, MINIO_ROOT_PASSWORD, AIRFLOW_FERNET_KEY, ...
```

### 1.2 Build & Start Docker

```bash
cd docker

# Start ALL services (17 containers)
docker compose up -d

# Hoặc start nhẹ hơn (không có CDC)
docker compose up -d --scale debezium=0
```

### 1.3 Kiểm tra trạng thái

```bash
docker compose ps
```

**Docker Compose sẽ tự động:**
1. Tạo PostgreSQL database + DDL (6 SQL files trong `init_postgres/`)
2. Tạo MinIO buckets (`lakehouse`, `airflow-logs`, `spark-logs`)
3. Khởi động Iceberg REST catalog
4. Khởi động Spark cluster (1 master + 1 worker)
5. Khởi động Trino + Iceberg catalog
6. Khởi动 Airflow (init → webserver → scheduler)

### 1.4 Services & Ports

| Service | URL | Đăng nhập |
|---------|-----|-----------|
| Airflow | http://localhost:8080 | admin / admin123 |
| MinIO Console | http://localhost:9001 | minioadmin / Minioadmin123 |
| Spark Master | http://localhost:9090 | — |
| Spark Worker | http://localhost:9091 | — |
| Trino | http://localhost:8085 | — |
| Kafka UI | http://localhost:8081 | — |
| Debezium | http://localhost:8083 | — |

---

## BƯỚC 2: Tạo Seed Data (PostgreSQL)

### 2.1 Chạy data generator

```bash
# Option 1: Trong container
docker compose exec postgres python /opt/project/data_generator/generate_all.py \
    --host postgres --port 5432

# Option 2: Từ host (PostgreSQL must be running)
python data_generator/generate_all.py --host localhost --port 5432
```

### 2.2 Kiểm tra dữ liệu PostgreSQL

```bash
docker compose exec postgres psql -U banking_admin -d banking_db
```

```sql
-- Kiểm tra số dòng
SELECT schemaname, tablename,
       (SELECT reltuples::bigint FROM pg_class WHERE relname = pg_tables.tablename) as estimated_rows
FROM pg_tables
WHERE schemaname IN ('core_banking', 'card_crm', 'digital_banking')
ORDER BY schemaname, tablename;
```

**Dữ liệu seed (~2.6M rows):**

| Schema | Table | Rows (approx) |
|--------|-------|---------------|
| core_banking | branch | 100 |
| core_banking | product | 13 |
| core_banking | customer | 10,000 |
| core_banking | account | 25,000 |
| core_banking | deposit | 15,000 |
| core_banking | loan | 8,000 |
| core_banking | txn_account | 2,000,000 |
| core_banking | employee | 500 |
| card_crm | card | 15,000 |
| card_crm | card_txn | 500,000 |
| card_crm | crm_interaction | 20,000 |
| digital_banking | device | 20,000 |
| digital_banking | location | 500 |
| digital_banking | online_transaction | 300,000 |
| digital_banking | support_ticket | 10,000 |
| digital_banking | mcc_code | 200 |

---

## BƯỚC 3: Bronze Layer (Ingestion từ PostgreSQL → Iceberg)

### 3.1 Trigger Bronze DAGs từ Airflow UI

1. Mở http://localhost:8080
2. Tìm các DAG: `bronze_core_banking_dag`, `bronze_card_crm_dag`, `bronze_digital_banking_dag`
3. Bật toggle → Trigger DAG

**Bronze layer sẽ:**
1. Đọc dữ liệu từ PostgreSQL via JDBC
2. Thêm column `cob_dt` (ngày business process = `{{ ds }}`)
3. Ghi vào Iceberg tables (full snapshot, overwritePartitions)

### 3.2 Verify Bronze data

```bash
docker compose exec trino trino --catalog lakehouse
```

```sql
-- Kiểm tra Bronze tables
SELECT table_schema, table_name
FROM lakehouse.information_schema.tables
WHERE table_schema = 'bronze'
ORDER BY table_name;

-- Đếm rows
SELECT 'core_customer' as tbl, COUNT(*) as cnt FROM lakehouse.bronze.core_customer
UNION ALL
SELECT 'core_account', COUNT(*) FROM lakehouse.bronze.core_account
UNION ALL
SELECT 'core_txn_account', COUNT(*) FROM lakehouse.bronze.core_txn_account
UNION ALL
SELECT 'card_card', COUNT(*) FROM lakehouse.bronze.card_card
UNION ALL
SELECT 'digital_online_txn', COUNT(*) FROM lakehouse.bronze.digital_online_txn;
```

---

## BƯỚC 4: Silver Layer (Transformations)

### 4.1 Trigger Silver DAG

1. Mở Airflow UI
2. Tìm DAG: `silver_all_dag`
3. Bật toggle → Trigger DAG
4. DAG tự động chờ 3 bronze DAGs hoàn thành (SqlSensor)

**Silver layer sẽ thực hiện:**
- **SCD Type 1** (overwrite): branch, product, card, employee, device, location
- **SCD Type 2** (history): customer, account (theo dõi thay đổi)
- **Facts**: fact_txn_account, fact_card_txn, fact_crm_interaction, fact_online_transaction, fact_support_ticket

### 4.2 Verify Silver data

```sql
-- Kiểm tra Silver tables
SELECT table_schema, table_name
FROM lakehouse.information_schema.tables
WHERE table_schema = 'silver'
ORDER BY table_name;

-- Sample dimension (SCD Type 2)
SELECT customer_id, full_name, effective_from, effective_to, is_current
FROM lakehouse.silver.dim_customer
WHERE is_current = true
LIMIT 5;

-- Sample fact
SELECT * FROM lakehouse.silver.fact_txn_account LIMIT 5;
```

---

## BƯỚC 5: Gold Layer (Analytics Marts)

### 5.1 Trigger Gold DAG

1. Mở Airflow UI
2. Tìm DAG: `gold_all_dag`
3. Bật toggle → Trigger DAG
4. DAG tự động chờ silver_all_dag hoàn thành (SqlSensor)

**Gold layer tạo 10 analytics tables (2 phases):**

Phase 1 (parallel, independent):
| Table | Mô tả |
|-------|-------|
| `mart_customer_360` | Customer 360° view (28+ KPIs) |
| `customer_balance_summary` | Tổng hợp số dư theo khách hàng |
| `customer_transaction_summary` | Tổng hợp giao dịch theo khách hàng |
| `customer_product_summary` | Tổng hợp sản phẩm theo khách hàng |
| `customer_card_summary` | Tổng hợp thẻ theo khách hàng |
| `rfm_segment` | Phân khúc RFM |
| `churn_prediction` | Dự đoán churn |
| `cross_sell_segment` | Phân khúc cross-sell |
| `branch_monthly_summary` | Tổng hợp chi nhánh theo tháng |

Phase 2 (depends on Phase 1):
| Table | Mô tả |
|-------|-------|
| `campaign_target` | Đối tượng chiến dịch (phụ thuộc rfm + churn + cross_sell + mart360) |

### 5.2 Verify Gold data

```sql
-- Customer 360° view (current serving: exactly 1 row/customer)
SELECT customer_id, full_name_masked, customer_segment,
       total_accounts, aum_total, rfm_segment,
       primary_channel, churn_flag
FROM lakehouse.gold.mart_customer_360_current
LIMIT 10;

-- Revenue by segment (current serving)
SELECT rfm_segment, COUNT(*) as cnt
FROM lakehouse.gold.rfm_segment_current
GROUP BY rfm_segment;

-- Branch performance (snapshot/time-series table remains history-oriented)
SELECT * FROM lakehouse.gold.mart_branch_monthly_summary LIMIT 5;
```

---

## BƯỚC 6: dbt Semantic Layer

### 6.1 Trigger dbt DAG

```
Enable DAG: dbt_run → Trigger
Schedule: Daily at 6 AM

Flow: start → dbt_deps → dbt_run_semantic → dbt_test → dbt_docs_generate → end
```

**dbt sẽ tạo 3 ephemeral models:**

| Model | Nguồn | Mô tả |
|-------|--------|-------|
| `sm_customer_current` | `mart_customer_360_current` | Customer semantic model |
| `sm_transaction_current` | `customer_transaction_summary_current` + `customer_card_summary_current` | Transaction semantic model |
| `sm_balance_current` | `customer_balance_summary_current` | Balance semantic model |
| `sm_product_current` | `customer_product_summary_current` | Product semantic model |
| `sm_card_current` | `customer_card_summary_current` | Card semantic model |
| `sm_rfm_current` | `rfm_segment_current` | RFM semantic model |
| `sm_churn_current` | `churn_prediction_current` | Churn semantic model |
| `sm_cross_sell_current` | `cross_sell_segment_current` | Cross-sell semantic model |
| `sm_campaign_current` | `campaign_target_current` | Campaign semantic model |

### 6.2 Hoặc chạy manual

```bash
cd dbt
dbt deps
dbt debug
dbt run --select semantic
dbt test
dbt docs generate
dbt docs serve --port 8081
```

---

## BƯỚC 7: Data Quality & Ops

### 7.1 Data Quality Checks

```
Enable DAG: ops_data_quality_dag → Trigger
```

Chạy DQ checks trên Silver, Gold, và Bronze CDC tables.
Kết quả ghi vào `opslakehouse.data_quality_log`.

### 7.2 PII Masking

```
Enable DAG: ops_pii_masking_daily_dag → Trigger
```

Tạo masked tables trong schema `sandbox`:
- `dim_customer_masked` — Name, phone, email masking
- `mart_customer_360_masked` — Full PII masking

### 7.3 Iceberg Maintenance (Weekly)

```
Schedule: ops_maintenance_weekly_dag → Chủ nhật 02:00
```

Compact data files, expire old snapshots, remove orphan files.

---

## BƯỚC 8: CDC Pipeline (Optional)

### 8.1 Start CDC services

```bash
docker compose up -d zookeeper kafka debezium
```

### 8.2 Register Debezium connectors

```
Enable DAG: cdc_register_connectors → Trigger
```

Đăng ký 3 connectors:
- `banking-core-banking` (6 tables)
- `banking-card-crm` (3 tables)
- `banking-digital-banking` (3 tables)

CDC credentials đọc từ env vars: `CDC_DB_USER`, `CDC_DB_PASSWORD`

### 8.3 Start streaming jobs

```
Enable DAG: cdc_streaming_pipeline → Trigger
```

6 Spark Structured Streaming jobs chạy liên tục:
- core_account, core_customer, core_transaction
- card_account, card_transaction, online_transaction

### 8.4 Test CDC

```sql
-- Insert test data vào PostgreSQL
INSERT INTO core_banking.customer (customer_id, cccd, full_name, gender, date_of_birth,
    phone, email, address, city, district, branch_code, customer_segment, kyc_status,
    register_date, is_active, last_updated)
VALUES (999999, '000000000000', 'CDC Test Customer', 'M', '1990-01-01',
    '0900000000', 'cdc@test.com', '123 Test St', 'HCMC', 'District 1', 'BR001',
    'PREMIUM', 'VERIFIED', '2025-01-15', true, NOW());

-- Kiểm tra Bronze CDC table (sau vài giây)
SELECT * FROM lakehouse.bronze.core_customer_cdc
WHERE customer_id = 999999
ORDER BY __cdc_timestamp;
```

---

## BƯỚC 9: Query Data với Trino

### 9.1 Kết nối Trino

```bash
docker compose exec trino trino --catalog lakehouse
```

### 9.2 Sample Queries

```sql
-- ═══════════════════════════════════════════════════════════════
-- 1. Customer 360° View (current serving, 1 row/customer)
-- ═══════════════════════════════════════════════════════════════
SELECT
    customer_id, full_name_masked, customer_segment,
    total_accounts, total_cards, aum_total,
    txn_count_30d, txn_amount_30d, rfm_segment, primary_channel
FROM lakehouse.gold.mart_customer_360_current
ORDER BY aum_total DESC
LIMIT 20;

-- ═══════════════════════════════════════════════════════════════
-- 2. Top customers by transaction volume (current serving)
-- ═══════════════════════════════════════════════════════════════
SELECT
    customer_id, customer_sk,
    total_txn_count_30d, total_txn_amount_30d,
    acct_credit_count_30d, acct_debit_count_30d,
    card_txn_count_30d, card_txn_amount_30d
FROM lakehouse.gold.customer_transaction_summary_current
ORDER BY total_txn_amount_30d DESC
LIMIT 10;

-- ═══════════════════════════════════════════════════════════════
-- 3. RFM Segmentation (current serving)
-- ═══════════════════════════════════════════════════════════════
SELECT rfm_segment, COUNT(*) as customer_count
FROM lakehouse.gold.rfm_segment_current
GROUP BY rfm_segment
ORDER BY customer_count DESC;

-- ═══════════════════════════════════════════════════════════════
-- 4. Campaign targeting (current serving)
-- ═══════════════════════════════════════════════════════════════
SELECT campaign_type, COUNT(*) AS customer_count
FROM lakehouse.gold.campaign_target_current
GROUP BY campaign_type
ORDER BY customer_count DESC;

-- ═══════════════════════════════════════════════════════════════
-- 5. Branch monthly summary (history/snapshot table)
-- ═══════════════════════════════════════════════════════════════
SELECT *
FROM lakehouse.gold.mart_branch_monthly_summary
ORDER BY cob_dt DESC, branch_code
LIMIT 20;
```

---

## BƯỚC 10: Monitoring

### 10.1 Docker logs

```bash
docker compose logs -f --tail=50
docker compose logs -f spark-master spark-worker-1 --tail=30
docker compose logs -f airflow-webserver airflow-scheduler --tail=30
```

### 10.2 Spark Master UI
- URL: http://localhost:9090

### 10.3 MinIO Console
- URL: http://localhost:9001

### 10.4 Airflow
- URL: http://localhost:8080

---

## Troubleshooting

### Docker build fails
```bash
docker compose build --no-cache
docker compose down --rmi local
docker compose up -d
```

### Spark OOM
```bash
# Kiểm tra spark-worker mem_limit (9000m) và SPARK_WORKER_MEMORY (8192m)
# Giảm row_count trong data generator nếu cần
```

### CDC not working
```bash
# Kiểm tra CDC credentials trong .env
CDC_DB_USER=cdc_user
CDC_DB_PASSWORD=CDCPassword123

# Kiểm tra Debezium health
curl http://localhost:8083/connectors
```

### dbt connection failed
```bash
docker compose exec trino trino --catalog lakehouse -e "SELECT 1"
cd dbt && dbt debug
```
