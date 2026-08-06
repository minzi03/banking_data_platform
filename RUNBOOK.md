# Runbook — Banking Data Platform

## 🚀 Quick Reference

### Start All Services
```bash
cd banking_data_platform/docker
docker compose up -d
```

### Stop All Services
```bash
docker compose down
```

### Check Status
```bash
docker compose ps
```

### View Logs
```bash
docker compose logs -f [service_name]
```

---

## 🔧 Common Operations

### 1. Start Infrastructure
```bash
cd banking_data_platform/docker
docker compose up -d
# Wait 2 minutes for services to become healthy
docker compose ps  # Verify all services are healthy
```

### 2. Generate Seed Data
```bash
# From host (recommended)
cd banking_data_platform
python data_generator/generate_all.py --host localhost --port 5432
```

### 3. Run ETL Pipeline

**Via Airflow UI (http://localhost:8080):**
1. Login: admin / admin123
2. Enable DAGs
3. Trigger manually

**Via CLI:**
```bash
docker compose exec airflow-scheduler airflow dags trigger bronze_core_banking_dag
docker compose exec airflow-scheduler airflow dags trigger bronze_card_crm_dag
docker compose exec airflow-scheduler airflow dags trigger bronze_digital_banking_dag
docker compose exec airflow-scheduler airflow dags trigger silver_all_dag
docker compose exec airflow-scheduler airflow dags trigger gold_all_dag
docker compose exec airflow-scheduler airflow dags trigger ops_data_quality_dag
```

### 4. Query Data

**Via Trino (port 8085):**
```bash
docker compose exec trino trino --catalog lakehouse

# History snapshot table
SELECT COUNT(*) FROM lakehouse.gold.mart_customer_360;

# Current serving table (1 row/customer)
SELECT COUNT(*) FROM lakehouse.gold.mart_customer_360_current;
SELECT customer_segment, COUNT(*) FROM lakehouse.gold.mart_customer_360_current GROUP BY 1;

# Other current-serving customer-grain Gold tables
SELECT COUNT(*) FROM lakehouse.gold.customer_balance_summary_current;
SELECT COUNT(*) FROM lakehouse.gold.customer_transaction_summary_current;
SELECT COUNT(*) FROM lakehouse.gold.customer_product_summary_current;
SELECT COUNT(*) FROM lakehouse.gold.customer_card_summary_current;
SELECT COUNT(*) FROM lakehouse.gold.rfm_segment_current;
SELECT COUNT(*) FROM lakehouse.gold.churn_prediction_current;
SELECT COUNT(*) FROM lakehouse.gold.cross_sell_segment_current;
SELECT COUNT(*) FROM lakehouse.gold.campaign_target_current;
```


### 5. Check Data Quality
```bash
docker compose exec trino trino --catalog lakehouse

SELECT * FROM opslakehouse.data_quality_log ORDER BY checked_at DESC LIMIT 10;
```

### 6. Check Lineage
```bash
docker compose exec trino trino --catalog lakehouse

SELECT * FROM opslakehouse.lineage_log ORDER BY created_at DESC LIMIT 10;
```

### 7. dbt semantic layer (dbt-core + dbt-trino)

```bash
cd dbt
pip install dbt-core dbt-trino
dbt deps
dbt parse
dbt run --select semantic
dbt test
dbt docs generate
dbt docs serve --port 8081
```

---

## 🐛 Troubleshooting

### Service Won't Start
```bash
# Check logs
docker compose logs [service_name]

# Check health
docker compose ps

# Restart specific service
docker compose restart [service_name]
```

### Iceberg REST Connection Error
```bash
# Check if iceberg_catalog database exists
docker exec banking-postgres psql -U banking_admin -d banking_db -c "\l"

# Create if missing
docker exec banking-postgres psql -U banking_admin -d banking_db -c "CREATE DATABASE iceberg_catalog;"
docker compose restart iceberg-rest
```

### Airflow DAG Import Errors
```bash
# Check DAG import errors
docker compose exec airflow-scheduler airflow dags list-import-errors

# Add missing connections
docker compose exec airflow-scheduler airflow connections add 'spark_default' \
  --conn-type 'spark' --conn-host 'spark://spark-master' --conn-port '7077'
```

### SparkSubmit Failed
```bash
# Check Spark worker logs
docker compose logs spark-worker-1

# Verify Iceberg jars
docker compose exec spark-worker-1 ls /opt/spark/jars/ | grep iceberg
```

---

## 📊 Monitoring

### Check Pipeline Status
```bash
# Via Airflow CLI
docker compose exec airflow-scheduler airflow dags list-runs -d bronze_core_banking_dag

# Check task states
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
  silver_all_dag "manual__2026-08-04T18:18:11+00:00"
```

### Check Data Volume
```bash
docker compose exec trino trino --catalog lakehouse

SELECT 'bronze' as layer, COUNT(*) FROM lakehouse.bronze.core_customer
UNION ALL
SELECT 'silver', COUNT(*) FROM lakehouse.silver.dim_customer
UNION ALL
SELECT 'gold', COUNT(*) FROM lakehouse.gold.mart_customer_360;
```

---

## 🔄 Backup & Restore

### Backup PostgreSQL
```bash
docker exec banking-postgres pg_dump -U banking_admin banking_db > backup.sql
```

### Restore PostgreSQL
```bash
cat backup.sql | docker exec -i banking-postgres psql -U banking_admin banking_db
```

---

## 📚 Related

- [README.md](README.md) — Quick start
- [ARCHITECTURE.md](ARCHITECTURE.md) — Architecture details
- [DEMO_GUIDE.md](DEMO_GUIDE.md) — Demo walkthrough
