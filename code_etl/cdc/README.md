# CDC Streaming Pipeline — Banking Data Platform

## Overview

This module implements real-time Change Data Capture (CDC) using:
- **Debezium**: Captures changes from PostgreSQL databases
- **Apache Kafka**: Message broker for CDC events
- **Spark Structured Streaming**: Processes CDC events and writes to Iceberg

## Architecture

```
┌─────────────────┐     ┌──────────┐     ┌─────────────────┐     ┌─────────────────┐
│   PostgreSQL    │────▶│ Debezium │────▶│     Kafka       │────▶│ Spark Streaming │
│   (Source DB)   │     │   CDC    │     │   (Broker)      │     │   (Consumer)    │
└─────────────────┘     └──────────┘     └─────────────────┘     └────────┬────────┘
                                                                          │
                                                                          ▼
                                                                 ┌─────────────────┐
                                                                 │  Iceberg Bronze │
                                                                 │   (CDC Tables)  │
                                                                 └─────────────────┘
                                                                          │
                                                                          ▼
                                                                 ┌─────────────────┐
                                                                 │  Spark Silver   │
                                                                 │  (Transform)    │
                                                                 └─────────────────┘
```

## Components

### 1. PostgreSQL Configuration (`docker/init_postgres/05-cdc-setup.sql`)

Enables CDC on PostgreSQL:
- Sets `wal_level = logical`
- Creates `cdc_user` with replication privileges
- Creates publication for Debezium

### 2. Debezium Connector (`code_etl/cdc/register_connectors.py`)

Registers CDC connectors for 3 schemas:
- `core_banking` (6 tables)
- `card_crm` (3 tables)
- `digital_banking` (3 tables)

### 3. Spark Streaming Job (`code_etl/cdc/base_job/cdc_streaming.py`)

Reads CDC events from Kafka and writes to Iceberg Bronze tables.

**Features:**
- Micro-batch processing (configurable trigger interval)
- Idempotent writes via Iceberg
- Checkpoint management for fault tolerance
- Schema-aware parsing for each table

### 4. YAML Configurations (`code_etl/cdc/config/`)

Table-specific configurations:
- `cdc_core_account.yml`
- `cdc_core_customer.yml`
- `cdc_core_transaction.yml`
- `cdc_card_account.yml`
- `cdc_card_transaction.yml`
- `cdc_online_transaction.yml`

### 5. Airflow DAGs (`airflow/dags/cdc/`)

- `cdc_register_connectors_dag.py`: Setup DAG for registering connectors
- `cdc_streaming_dag.py`: Start/stop streaming jobs

**Note:** Silver layer transformations are handled by Spark (`code_etl/silver/`), not dbt.

## Setup Instructions

### Step 1: Start CDC Infrastructure

```bash
cd banking_data_platform/docker
docker compose up -d debezium kafka zookeeper
```

### Step 2: Restart PostgreSQL (for CDC config)

```bash
docker compose restart postgres
```

Wait for PostgreSQL to be healthy, then verify CDC user exists:
```bash
docker exec banking-postgres psql -U banking_admin -d banking -c "\du cdc_user"
```

### Step 3: Register Debezium Connectors

**Via Airflow:**
1. Open Airflow UI (http://localhost:8080)
2. Enable and trigger `cdc_register_connectors` DAG
3. Wait for completion

**Or manually:**
```bash
docker exec banking-airflow-scheduler python /opt/project/code_etl/cdc/register_connectors.py
```

### Step 4: Verify Connectors

```bash
# Check connector status
curl http://localhost:8083/connectors | python3 -m json.tool

# Check Kafka topics
docker exec banking-kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Step 5: Start Streaming Jobs

**Via Airflow:**
1. Open Airflow UI
2. Enable and trigger `cdc_streaming_pipeline` DAG

**Or manually (for each table):**
```bash
docker exec banking-spark-worker-1 /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.memory=512m \
    --conf spark.executor.memory=768m \
    /opt/project/code_etl/cdc/base_job/cdc_streaming.py \
    --config /opt/project/code_etl/cdc/config/cdc_core_account.yml \
    --kafka_bootstrap kafka:9092
```

### Step 6: Test CDC

**Insert test data:**
```sql
INSERT INTO core_banking.account (account_id, account_name, account_type, balance, status, open_date, customer_id, branch_id, currency_code)
VALUES (999999, 'Test CDC Account', 'SAVINGS', 10000.00, 'ACTIVE', '2025-01-01', 1, 1, 'VND');
```

**Verify CDC data:**
```sql
-- Check Bronze CDC table
SELECT * FROM lakehouse.bronze.core_account_cdc
WHERE account_id = 999999
ORDER BY __cdc_timestamp;

-- Check operation types
SELECT __cdc_operation, COUNT(*)
FROM lakehouse.bronze.core_account_cdc
GROUP BY __cdc_operation;
```

## Monitoring

### Kafka UI
- URL: http://localhost:8081
- View topics, consumer lag, message rates

### Spark Master UI
- URL: http://localhost:9090
- View streaming queries, batch progress

### Debezium
- URL: http://localhost:8083
- View connector status, tasks, errors

## Troubleshooting

### Issue: Connector fails to start

Check PostgreSQL logs:
```bash
docker logs banking-postgres --tail 50
```

Common causes:
- `wal_level` not set to `logical`
- `cdc_user` doesn't have replication privileges
- Replication slots limit reached

### Issue: No data in Kafka topics

Check Debezium connector status:
```bash
curl http://localhost:8083/connectors/banking-core-banking/status
```

Check Kafka consumer:
```bash
docker exec banking-kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic postgresql.banking.core_banking.account \
    --from-beginning \
    --max-messages 5
```

### Issue: Spark streaming job fails

Check Spark logs:
```bash
docker logs banking-spark-worker-1 --tail 100
```

Common causes:
- Kafka not reachable
- Iceberg REST catalog not ready
- Checkpoint location issues

### Issue: Duplicate data in Bronze tables

The streaming job uses micro-batch processing. Duplicates can occur if:
- Checkpoint is corrupted
- Job restarts without proper checkpoint

Solution: The dbt staging models handle deduplication by keeping only the latest event per business key.

## Configuration

### YAML Config Options

```yaml
kafka:
  topic: postgresql.banking.core_banking.account  # Kafka topic
  starting_offsets: latest                          # earliest/latest
  checkpoint_location: s3a://lakehouse/checkpoints/cdc/core_account
  trigger_interval: 30 seconds                      # Micro-batch interval
  max_offsets_per_trigger: 100000                   # Max events per batch

target:
  catalog: lakehouse
  schema: bronze
  table: core_account_cdc                          # Target Iceberg table
```

### Adjusting Trigger Interval

For higher throughput (more real-time):
```yaml
trigger_interval: 10 seconds
```

For lower resource usage:
```yaml
trigger_interval: 60 seconds
```

## Performance Tuning

### Kafka
- Increase partitions for high-volume topics
- Monitor consumer lag in Kafka UI

### Spark Streaming
- Adjust `spark.sql.shuffle.partitions`
- Increase executor memory for large batches
- Use `maxOffsetsPerTrigger` to limit batch size

### Iceberg
- Run compaction regularly on CDC tables
- Set appropriate partitioning (by date)

## Cleanup

### Stop Streaming Jobs

Via Airflow: Trigger `cdc_streaming_stop_all` DAG

### Remove Connectors

```bash
curl -X DELETE http://localhost:8083/connectors/banking-core-banking
curl -X DELETE http://localhost:8083/connectors/banking-card-crm
curl -X DELETE http://localhost:8083/connectors/banking-digital-banking
```

### Drop CDC Tables

```sql
DROP TABLE IF EXISTS lakehouse.bronze.core_account_cdc;
DROP TABLE IF EXISTS lakehouse.bronze.core_customer_cdc;
DROP TABLE IF EXISTS lakehouse.bronze.core_transaction_cdc;
DROP TABLE IF EXISTS lakehouse.bronze.card_account_cdc;
DROP TABLE IF EXISTS lakehouse.bronze.card_transaction_cdc;
DROP TABLE IF EXISTS lakehouse.bronze.online_transaction_cdc;
```
