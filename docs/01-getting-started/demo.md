# Demo Script — 5 Minutes

## Overview

This demo walks through the Banking Data Platform in 5 minutes, showing the dual-path architecture (batch + CDC), medallion layers, and key outputs.

---

## Timeline

### 00:00–00:40 — Architecture + Docker Services

**Show**: `docker compose ps` output

```bash
cd banking_data_platform/docker
docker compose ps
```

**Say**:
> "This is a production-like banking data platform with 24 Docker containers. It implements dual-path ingestion: batch processing through JDBC and near-real-time CDC using Debezium, Kafka, and Spark Structured Streaming."

**Point out**: PostgreSQL, Spark, Kafka, Debezium, Trino, Airflow, OpenMetadata

---

### 00:40–01:30 — PostgreSQL Source Data

**Show**: Source tables in PostgreSQL

```bash
docker exec banking-postgres psql -U banking_admin -d banking_db -c \
  "SELECT customer_id, full_name, email, kyc_status, customer_segment 
   FROM core_banking.customer 
   LIMIT 5;"
```

**Show**: One customer record with details

**Say**:
> "The source is a PostgreSQL database modeling core banking, card, CRM, and digital banking domains. We have 16 source tables across these domains."

---

### 01:30–02:20 — Debezium + Kafka Events

**Show**: Kafka UI at http://localhost:8081

**Navigate to**: Kafka topics → `postgresql.banking.core_banking.customer`

**Show**: Recent CDC events (Avro or JSON format)

**Say**:
> "Debezium captures changes from PostgreSQL WAL and streams them to Kafka. Each event contains the operation type (INSERT/UPDATE/DELETE), the full before/after payload, and Kafka metadata (topic, partition, offset)."

**Optional**: Show Debezium connector status at http://localhost:8083

---

### 02:20–03:00 — Bronze CDC via Trino

**Show**: Query Bronze CDC tables

```bash
docker exec banking-trino trino \
  --catalog lakehouse \
  --execute "
    SELECT __cdc_operation, __cdc_timestamp, customer_id, full_name, email
    FROM lakehouse.bronze.core_customer_cdc
    ORDER BY __cdc_timestamp DESC
    LIMIT 5;
  "
```

**Say**:
> "Bronze CDC stores append-only change events with full Kafka metadata. This is the audit/replay boundary — we can trace any record back to its exact Kafka offset."

**Show**: Batch vs CDC side-by-side

```bash
# Batch (source-aligned)
docker exec banking-trino trino \
  --catalog lakehouse \
  --execute "SELECT COUNT(*) FROM lakehouse.bronze.core_customer;"

# CDC (append-only events)
docker exec banking-trino trino \
  --catalog lakehouse \
  --execute "SELECT COUNT(*) FROM lakehouse.bronze.core_customer_cdc;"
```

---

### 03:00–03:40 — Silver + Gold Tables

**Show**: Silver dimensions

```bash
docker exec banking-trino trino \
  --catalog lakehouse \
  --execute "
    SELECT customer_id, full_name, kyc_status, is_current
    FROM lakehouse.silver.dim_customer
    WHERE is_current = true
    LIMIT 5;
  "
```

**Show**: Gold Customer 360

```bash
docker exec banking-trino trino \
  --catalog lakehouse \
  --execute "
    SELECT customer_id, full_name, total_balance, rfm_segment, churn_risk
    FROM lakehouse.gold.mart_customer_360
    LIMIT 5;
  "
```

**Say**:
> "Silver applies SCD Type 1/2 processing and fact enrichment. Gold produces business-ready marts like Customer 360, RFM segmentation, and churn risk scoring."

---

### 03:40–04:20 — Airflow Orchestration

**Show**: Airflow UI at http://localhost:8080

**Navigate to**: DAGs list

**Point out**:
- Bronze DAGs (3 domain groups)
- Silver DAG
- Gold DAG
- CDC DAGs
- Ops DAGs (DQ, PII, lineage)

**Say**:
> "Airflow orchestrates 17 DAGs with a production-like schedule: Bronze at 2 AM, Silver at 4 AM, Gold at 6 AM, dbt at 7 AM, ops at 8 AM."

---

### 04:20–04:45 — OpenMetadata Lineage

**Show**: OpenMetadata UI at http://localhost:8585

**Navigate to**: Lineage view

**Show**: Bronze → Silver → Gold lineage graph

**Say**:
> "OpenMetadata catalogs 53 tables with 22 lineage edges. We can trace any Gold mart back to its source tables in PostgreSQL."

**Show**: Tags, glossary terms (KYC, PCI DSS, SCD Type 2, etc.)

---

### 04:45–05:00 — Closing

The dashboard step is intentionally omitted. The Streamlit application in
`streamlit/` connects to Trino with the Spark catalog name `lakehouse`, while
the Trino catalog is `iceberg`, so its queries do not currently run. Demoing it
would mean demoing a broken path. Tracked as TD-7.

**Show instead**: the Trino SQL examples from the README, executed live.

---

### Closing Statement

> "The batch path produces curated analytics through Bronze, Silver, and Gold, while the CDC path currently preserves append-only near-real-time changes in Bronze. Consolidating selected CDC entities into Silver current-state tables is the next targeted enhancement."

---

## Key Talking Points

### Architecture Decisions

1. **Why dual-path?**
   > "Batch handles daily analytics workloads with full ACID guarantees. CDC enables near-real-time operational reporting. Different use cases, different trade-offs."

2. **Why Iceberg + MinIO?**
   > "Iceberg provides ACID transactions, schema evolution, and time travel on top of S3-compatible storage. MinIO gives us cloud-native storage semantics locally."

3. **Why CDC only reaches Bronze?**
   > "I intentionally keep CDC append-only in Bronze as an audit/replay boundary. Silver and Gold are currently produced by scheduled batch pipelines. The next enhancement is current-state consolidation for selected entities rather than making every Gold mart real-time."

---

## Troubleshooting

If services are not running:

```bash
cd banking_data_platform/docker
docker compose up -d
# Wait 2-3 minutes for initialization
docker compose ps
```

If Trino queries fail:

```bash
# Check Trino is ready
docker exec banking-trino trino --execute "SELECT 1"
```

If Airflow DAGs are paused:

```bash
# Unpause all DAGs
docker exec banking-airflow airflow dags unpause --all
```
