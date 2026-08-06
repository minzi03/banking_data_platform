# CDC Pipeline — Banking Data Platform

## Overview

The CDC (Change Data Capture) pipeline captures real-time changes from PostgreSQL databases using Debezium and streams them to Iceberg Bronze tables via Apache Kafka.

## Architecture

```
PostgreSQL → Debezium → Kafka → Spark Streaming → Iceberg Bronze CDC Tables
```

## Components

### 1. Debezium Connectors
- **Core Banking**: customer, account, branch, employee, loan, txn_account
- **Card CRM**: card, card_txn, crm_interaction
- **Digital Banking**: online_transaction, device, support_ticket

### 2. Kafka Topics
- `postgresql.banking.core_banking.customer`
- `postgresql.banking.core_banking.account`
- `postgresql.banking.core_banking.txn_account`
- `postgresql.banking.card_crm.card`
- `postgresql.banking.card_crm.card_txn`
- `postgresql.banking.digital_banking.online_transaction`

### 3. Spark Streaming Jobs
- 6 streaming jobs consuming from Kafka
- Processing interval: 30 seconds
- Checkpoint location: `s3a://lakehouse/checkpoints/cdc/`

### 4. Bronze CDC Tables
| Table | Source | Rows |
|-------|--------|------|
| core_customer_cdc | PostgreSQL core_banking.customer | 10,004 |
| core_account_cdc | PostgreSQL core_banking.account | 30,000 |
| core_transaction_cdc | PostgreSQL core_banking.txn_account | 1,200,000 |
| card_account_cdc | PostgreSQL card_crm.card | 6,000 |
| card_transaction_cdc | PostgreSQL card_crm.card_txn | 600,000 |
| online_transaction_cdc | PostgreSQL digital_banking.online_transaction | 500,000 |

## CDC Operations

- **SNAPSHOT**: Initial data load (op: 'r')
- **INSERT**: New records (op: 'c')
- **UPDATE**: Updated records (op: 'u')
- **DELETE**: Deleted records (op: 'd')

## Configuration

### PostgreSQL Requirements
- `wal_level = logical` ✓
- Publications created for each connector ✓
- CDC user with REPLICATION privilege ✓

### Debezium Configuration
- Plugin: pgoutput
- Snapshot mode: initial
- Transform: ExtractNewRecordState (flattened JSON)

## Usage

### Start CDC Pipeline
```bash
# Start Debezium
docker compose up -d debezium

# Start streaming jobs (via Airflow or manually)
# Manual start:
docker exec banking-spark-worker-1 bash /tmp/run_cdc.sh
```

### Stop CDC Pipeline
```bash
# Stop streaming jobs
docker exec banking-spark-worker-1 pkill -f cdc_

# Stop Debezium
docker compose stop debezium
```

### Monitor CDC
- **Kafka UI**: http://localhost:8081
- **Spark Master**: http://localhost:9090
- **Debezium REST**: http://localhost:8083

## Troubleshooting

### Common Issues

1. **Kafka connector not found**
   - Ensure Kafka JARs are in Spark classpath
   - Download: `spark-sql-kafka-0-10_2.12-3.5.3.jar`

2. **Schema mismatch errors**
   - Verify DDL matches YAML config columns
   - Use `04_ddl_bronze_cdc_v3.sql` for correct schema

3. **Checkpoint conflicts**
   - Use unique checkpoint locations for each job
   - Clear old checkpoints: `rm -rf /tmp/spark-*`

## Files

- **DDL**: `docker/init_iceberg/04_ddl_bronze_cdc_v3.sql`
- **Config**: `code_etl/cdc/config/cdc_*.yml`
- **Streaming Job**: `code_etl/cdc/base_job/cdc_streaming.py`
- **Airflow DAG**: `airflow/dags/cdc/cdc_streaming_dag.py`
