# Banking Data Platform — Demo Script

## Tổng quan Demo
Thời lượng: 15-20 phút

## Mục tiêu
- Trình bày kiến trúc Medallion Architecture (Bronze→Silver→Gold)
- Demonstrate ETL Pipeline hoạt động
- Showcase OpenMetadata Catalog & Lineage
- Demo Real-time CDC Pipeline
- Trình bày Business Analytics với Streamlit

---

## PHẦN 1: Giới thiệu (2 phút)

### 1.1 Mục tiêu dự án
- Xây dựng Data Platform hoàn chỉnh cho ngân hàng
- Kết hợp Batch ETL + Real-time CDC
- Medallion Architecture: Bronze (Raw) → Silver (Cleaned) → Gold (Analytics)

### 1.2 Tech Stack
| Layer | Technology |
|-------|------------|
| Source | PostgreSQL (OLTP) |
| Storage | MinIO (S3) + Apache Iceberg |
| Compute | Apache Spark 3.5.3 |
| Orchestration | Apache Airflow 2.10.0 |
| Real-time | Debezium CDC + Kafka |
| Query | Trino 443 |
| Catalog | OpenMetadata 1.5.6 |
| BI | Streamlit |

---

## PHẦN 2: Architecture Demo (3 phút)

### 2.1 Truy cập OpenMetadata
- URL: http://localhost:8585
- Login: `admin` / `admin`

### 2.2 Shows
1. **Data Assets** → Browse 53 tables across Bronze/Silver/Gold
2. **Lineage** → Visualize data flow từ PostgreSQL đến Gold marts
3. **Tags** → Tier1/2/3 (importance), PII.Sensitive (sensitivity)
4. **Glossary** → Banking terms (KYC, AML, RFM, etc.)

---

## PHẦN 3: ETL Pipeline Demo (5 phút)

### 3.1 Bronze Layer (Raw)
```sql
-- Query raw data
SELECT COUNT(*) FROM bronze.core_customer;  -- 30,000 customers
SELECT COUNT(*) FROM bronze.core_account;   -- 90,000 accounts
SELECT COUNT(*) FROM bronze.core_card_txn;  -- 1.8M card transactions
SELECT COUNT(*) FROM bronze.core_online_transaction; -- 1.5M online transactions
```

### 3.2 Silver Layer (Cleaned)
```sql
-- SCD Type 2 dimensions
SELECT COUNT(*) FROM silver.dim_customer;  -- 10,000 (current)
SELECT COUNT(*) FROM silver.dim_account;   -- 30,000 (current)

-- Facts
SELECT COUNT(*) FROM silver.fact_txn_account;  -- 2.4M transactions
SELECT COUNT(*) FROM silver.fact_card_txn;     -- 1.2M card transactions
```

### 3.3 Gold Layer (Analytics)
```sql
-- Customer 360
SELECT COUNT(*) FROM gold.mart_customer_360;  -- 20,000 customers

-- RFM Segmentation
SELECT rfm_segment, COUNT(*) as cnt
FROM gold.rfm_segment
GROUP BY rfm_segment
ORDER BY cnt DESC;
-- Results:
-- Potential Loyalists: 5,094 (25.5%)
-- Champions: 4,428 (22.1%)
-- Loyal Customers: 4,414 (22.1%)
-- At Risk: 3,088 (15.4%)
-- New Customers: 1,772 (8.9%)
-- Hibernating: 1,204 (6.0%)

-- Churn Prediction
SELECT churn_risk, COUNT(*) as cnt
FROM gold.churn_prediction
GROUP BY churn_risk
ORDER BY cnt DESC;
-- Results:
-- Active: 19,116 (95.6%)
-- High: 682 (3.4%)
-- Low: 195 (1.0%)
-- Medium: 7 (0.0%)

-- AUM Buckets
SELECT aum_bucket, COUNT(*) as cnt
FROM gold.customer_balance_summary
GROUP BY aum_bucket
ORDER BY cnt DESC;
-- Results:
-- AFFLUENT: 10,130 (50.7%)
-- PRIORITY: 7,310 (36.6%)
-- MASS: 2,560 (12.8%)

-- Customer Segments
SELECT customer_segment, COUNT(*) as cnt
FROM gold.mart_customer_360
GROUP BY customer_segment
ORDER BY cnt DESC;
-- Results:
-- RETAIL: 14,108 (70.5%)
-- PRIORITY: 4,314 (21.6%)
-- VIP: 1,578 (7.9%)
```

---

## PHẦN 4: Real-time CDC Demo (3 phút)

### 4.1 CDC Architecture
- Debezium → Kafka → Spark Streaming → Iceberg

### 4.2 Demo Steps
1. **Kafka UI**: http://localhost:8081
   - Show 12 Kafka topics
   - Show message flow

2. **Insert new customer in PostgreSQL**
```sql
INSERT INTO core_banking.customer (cccd, full_name, gender, date_of_birth, phone, email, address, city, district, branch_code, customer_segment, kyc_status, register_date, is_active)
VALUES ('001234567890', 'Demo Customer', 'Nam', '1990-01-05', '0901234567', 'demo@test.com', '123 Demo Street', 'Ho Chi Minh', 'Quan 1', 'BR001', 'VIP', 'APPROVED', '2026-08-06', 1);
```

3. **Show real-time update in Iceberg**
```sql
-- Check CDC table (should have new record)
SELECT * FROM silver_cdc.core_customer_cdc
ORDER BY __cdc_timestamp DESC
LIMIT 5;
```

---

## PHẦN 5: Streamlit Dashboard Demo (3 phút)

### 5.1 Truy cập Dashboard
- URL: http://localhost:8501

### 5.2 Shows
1. **Overview** → Tổng quan 20,000 khách hàng
2. **Customer 360** → Chi tiết khách hàng
3. **RFM Analysis** → Phân khúc khách hàng
4. **Churn Risk** → Dự báo rời bỏ
5. **Campaign Target** → Chiến lược marketing

---

## PHẦN 6: Airflow Orchestration (2 phút)

### 6.1 Truy cập Airflow
- URL: http://localhost:8080
- Login: `admin` / `admin`

### 6.2 Shows
1. **DAGs** → 19+ DAGs đang chạy
2. **Production Schedule**:
   - Bronze: 2:00 AM daily
   - Silver: 4:00 AM daily
   - Gold: 6:00 AM daily
   - dbt: 7:00 AM daily
   - Ops: 8:00-9:00 AM daily

---

## PHẦN 7: Kết luận (2 phút)

### 7.1 Kết quả đạt được
- ✅ 53 tables registered in OpenMetadata
- ✅ 22 lineage relationships
- ✅ 30,000 customers, 90,000 accounts
- ✅ 2.4M+ transactions processed
- ✅ Real-time CDC pipeline operational
- ✅ Production-ready scheduling

### 7.2 Business Value
- **Customer Segmentation**: 6 RFM segments
- **Churn Prediction**: 3.4% high risk customers
- **AUM Analysis**: 50.7% affluent segment
- **Cross-sell Opportunities**: Identified from product ownership

### 7.3 Next Steps
- Machine Learning models
- Advanced fraud detection
- Real-time dashboards
- Data quality monitoring

---

## Quick Commands Reference

```bash
# Start all services
cd banking_data_platform/docker
docker compose up -d

# Check status
docker compose ps

# Query data
docker exec banking-trino trino --catalog lakehouse --execute "SELECT COUNT(*) FROM gold.mart_customer_360"

# Check CDC
curl -s http://localhost:8083/connectors | jq

# Check Kafka topics
docker exec banking-kafka kafka-topics --bootstrap-server localhost:9092 --list
```
