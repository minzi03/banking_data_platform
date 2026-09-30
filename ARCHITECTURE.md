# Architecture — Banking Data Platform

## 🏗️ Kiến Trúc Tổng Quan

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ Data Generator│  │ PostgreSQL 15│  │   Debezium   │  │    Kafka     │   │
│  │   (Python)   │  │   (Source)   │  │    (CDC)     │  │  (Streaming) │   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │
└─────────┼─────────────────┼─────────────────┼─────────────────┼───────────┘
          │                 │                 │                 │
          │    JDBC Ingest  │    CDC Stream   │                 │
          ▼                 ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER (MinIO + Iceberg)                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    BRONZE LAYER (Raw)                                │   │
│  │  22 tables: core_banking(13) + card_crm(3) + digital_banking(6)    │   │
│  │  Format: Parquet + Iceberg metadata                                 │   │
│  │  Strategy: full snapshot per cob_dt, every table PARTITIONED BY cob_dt│  │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    SILVER LAYER (Cleaned)                            │   │
│  │  10 Dims: SCD2 customer/account/product/branch + 6 SCD1            │   │
│  │  6 Facts: fact_txn_account, fact_card_txn, fact_*, ...             │   │
│  │  Facts: overwritePartitions(cob_dt), dims joined as-of cob_dt      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │           HISTORICAL GOLD LAYER — 15 tables (Spark)                  │   │
│  │  6 Mart360: mart_customer_360, customer_*_summary, ...             │   │
│  │  4 Segments: rfm, churn, cross_sell, campaign_target               │   │
│  │  4 Risk: loan_portfolio, loan_delinquency, fraud_risk, aml         │   │
│  │  1 Time analytics: mart_branch_monthly_summary                     │   │
│  │  Strategy: overwritePartitions by cob_dt                            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │  GOLD_COMPLETE(cob_dt)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              CURRENT-SERVING LAYER — 16 tables (dbt via Trino)              │
│  iceberg.serving.*  —  one row per customer, one cob_dt                    │
│  Owned by dbt, materialized as Iceberg tables, published per cob_dt         │
│  SERVING_COMPLETE(cob_dt) written only after dbt build + tests pass         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         QUERY & GOVERNANCE                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                      │
│  │    Trino     │  │  Governance  │  │ OpenMetadata │                      │
│  │   (Query)    │  │ (Contracts,  │  │  (Catalog)   │                      │
│  │  Port 8085   │  │  DQ, Lineage)│  │              │                      │
│  │              │  │              │  │              │                      │
│  └──────────────┘  └──────────────┘  └──────────────┘                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 🔄 Data Flow

### 1. Ingestion Flow (Bronze)
```
PostgreSQL ──JDBC──▶ Spark ──writeTo──▶ Iceberg (Bronze)
                     │
                     └── Config: YAML files in code_etl/bronze/
```

### 2. Transformation Flow (Silver)
```
Bronze ──Spark SQL──▶ Silver (SCD1/SCD2)
                       │
                       └── Config: YAML files in code_etl/silver/
```

### 2b. CDC Consolidation Flow (Silver Current-State)
```
Bronze CDC ──CDC Consolidation──▶ Silver Current-State
(append-only)    (config-driven)    (mutable, latest state)
                  │
                  ├── dim_customer_current
                  └── dim_account_current

Key features:
- Progress = last merged Iceberg snapshot of Bronze CDC; each run reads (watermark, end]
- Deterministic deduplication (timestamp + batch_id)
- INSERT/UPDATE/DELETE via Iceberg MERGE, never overwriting newer state with an older event
- Idempotent reprocessing (re-run = no change)
```

### 3. Aggregation Flow (Gold)
```
Silver ──Spark SQL──▶ Gold (Marts)
                       │
                       └── Config: YAML files in code_etl/gold/
```

### 4. Serving Flow

```
Historical Gold ──dbt via Trino──▶ iceberg.serving.* ──▶ Trino / SQL consumers
```

Spark owns Bronze, Silver and historical Gold. dbt, executed through Trino,
publishes the current-serving layer. Serving objects are created by the engine
that serves them — a Spark-created view is not visible through Trino.

Note the catalog naming: Spark addresses the warehouse as `lakehouse`, Trino as
`iceberg` (the catalog name comes from
`docker/init_trino/catalog/iceberg.properties`). Same data, two engine-local
names.

### 5. Time Semantics

```
Storage / engine timezone : UTC
Business timezone         : Asia/Ho_Chi_Minh
Processing date           : explicit cob_dt
Business date             : explicitly derived from UTC timestamps
```

Spark runs with a UTC session timezone, enforced at runtime by
`assert_utc_session()`. Banking calendar dates are derived explicitly:

```sql
-- Spark
CAST(from_utc_timestamp(txn_date, 'Asia/Ho_Chi_Minh') AS DATE)
-- Trino
CAST(txn_date AT TIME ZONE 'Asia/Ho_Chi_Minh' AS DATE)
```

Timestamps stay UTC instants; only calendar dates and months are converted.

## 🛡️ Governance Flow

### Data Contracts
```
YAML Contract ──▶ ContractRegistry ──▶ ContractEnforcer ──▶ Pipeline
(governance/datasets/)    (load)           (validate)        (block/pass)
```

### Data Quality
```
DQ Rules (dq_rules.yml) ──▶ data_quality.py ──▶ PostgreSQL (data_quality_log)
                              │
                              └── 9 check types
```

### Lineage Tracking
```
Pipeline Run ──▶ LineageTracker ──▶ PostgreSQL (lineage_log)
                     │
                     └── Bronze→Silver (16 transforms)
                         Silver→Gold (15 transforms)
```

## 📊 Schema Mapping

### Bronze → Silver Transforms
| Bronze Table | Silver Table | Transform |
|--------------|--------------|-----------|
| core_customer | dim_customer | SCD2 (tracked columns + Type 1 overwrite, close missing keys) |
| core_account | dim_account | SCD2 |
| core_product | dim_product | SCD2 |
| core_branch | dim_branch | SCD2 |
| core_card | dim_card | SCD1 (MERGE upsert) |
| core_employee | dim_employee | SCD1 |
| core_device | dim_device | SCD1 |
| core_location | dim_location | SCD1 |
| core_deposit | dim_deposit | SCD1 |
| core_loan | dim_loan | SCD1 |
| core_txn_account | fact_txn_account | overwritePartitions(cob_dt), full snapshot |
| core_card_txn | fact_card_txn | overwritePartitions(cob_dt) |
| core_crm_interaction | fact_crm_interaction | overwritePartitions(cob_dt) |
| core_online_transaction | fact_online_transaction | overwritePartitions(cob_dt) |
| core_support_ticket | fact_support_ticket | overwritePartitions(cob_dt) |
| core_loan_payment | fact_loan_payment | overwritePartitions(cob_dt) |

Facts và Gold chọn version dim **hiệu lực tại cob_dt** (`cob_dt BETWEEN effective_from AND effective_to`),
không phải `is_current = 1`, nên chạy lại một ngày cũ dùng đúng dim của ngày đó.

### Silver → Gold Transforms
| Silver Tables | Gold Table | Transform |
|---------------|------------|-----------|
| dim_customer + dim_account + dim_card + fact_* | mart_customer_360 | Aggregation |
| dim_customer + dim_account | customer_balance_summary | Balance agg |
| dim_customer + fact_txn_account + fact_card_txn | customer_transaction_summary | Txn agg |
| dim_customer + dim_account + dim_card + dim_loan | customer_product_summary | Product holding |
| dim_customer + dim_card + fact_card_txn | customer_card_summary | Card agg |
| dim_customer + dim_loan + fact_loan_payment | customer_loan_summary | Loan agg |
| dim_customer + fact_txn_account + fact_card_txn | rfm_segment | RFM scoring |
| dim_customer + fact_txn_account + fact_card_txn | churn_prediction | Churn risk |
| mart_customer_360 + dim_customer | cross_sell_segment | Cross-sell |
| dim_branch + dim_account + fact_txn_account | mart_branch_monthly_summary | Monthly agg |
| dim_loan + fact_loan_payment | loan_delinquency | DPD buckets |
| dim_loan + dim_branch + fact_loan_payment | loan_portfolio_risk | Portfolio risk |
| dim_customer + fact_txn_account + fact_online_transaction | fraud_risk_txn | Rule-based fraud scoring |
| dim_customer + fact_txn_account | aml_monitoring | AML alert scoring |
| rfm_segment + churn_prediction + cross_sell_segment | campaign_target | Campaign |

## 🔧 Tech Stack Details

### Docker Services (29)

25 long-running + 4 one-shot initialization/migration jobs. Counted from the
`services:` keys in `docker/docker-compose.yml`.

| Service | Port | Purpose |
|---------|------|---------|
| **Storage & catalog** | | |
| postgres | 5432 | Source database |
| minio | 9000, 9001 | Object storage |
| mc | — | MinIO bucket initialization (one-shot) |
| iceberg-rest | 8181 | Iceberg REST catalog |
| iceberg-init | — | Namespace creation (one-shot) |
| **Processing** | | |
| spark-master | 7077, 9090 | Spark coordinator |
| spark-worker-1 | 9091 | Spark compute |
| **Streaming & CDC** | | |
| zookeeper | 2181 | Kafka coordination |
| kafka | 9092 | Event streaming |
| kafka-ui | 8089 | Kafka inspection UI |
| debezium | 8083 | CDC connector |
| **Query & publication** | | |
| trino | 8453 (HTTPS), 8085 (HTTP → 403) | Query engine |
| dbt | — | dbt runner |
| **Orchestration** | | |
| airflow-init | — | DB migration (one-shot) |
| airflow-webserver | 8080 | Airflow UI |
| airflow-scheduler | 8793 | Airflow scheduler |
| **Serving & analytics** | | |
| api | 8000 | FastAPI Customer 360 |
| streamlit | 8501 | Streamlit dashboard |
| superset | 8088 | Superset BI |
| superset-init | — | Superset bootstrap |
| mlflow | 5000 | Experiment tracking |
| **Observability** | | |
| prometheus | 9095 | Metrics collection |
| grafana | 3000 | Dashboards |
| alertmanager | 9093 | Alert routing |
| freshness-exporter | 9119 | CDC freshness metrics |
| **Governance** | | |
| om-mysql | 3308 | OpenMetadata DB |
| om-elasticsearch | 9200 | OpenMetadata search |
| om-migrate | — | OpenMetadata migration (one-shot) |
| openmetadata | 8585 | Data catalog |

### CDC freshness

| Measure | Value |
|---------|------:|
| Median local source→Silver | 409.8s |
| Range | 65.9–576.2s |
| Trials | 5 |
| Consolidation cadence | 600s (`*/10 * * * *`) |

Measured end to end: `t0` is the PostgreSQL `COMMIT`, `t1` is the moment the
value becomes readable in `silver.dim_customer_current` through Trino. The
figure therefore includes waiting for the next scheduled consolidation run,
which dominates it — deliberately, because that is the delay a consumer
experiences.

## 📈 Data Volume

| Layer | Tables | Rows (approx, one snapshot) |
|-------|--------|---------------|
| Source | 22 ingested (3 schemas) | ~2.6M |
| Bronze | 22 (batch, per cob_dt) + 6 (CDC) | ~2.6M per snapshot + CDC events |
| Silver | 16 (10 dims + 6 facts) + 2 (CDC current) | 2.3M distinct txns + 40K current rows |
| Historical Gold | 15 | ~100K per cob_dt |
| Current serving | 16 (dbt/Trino) + 1 time spine | ~90K |

Transaction counts are distinct `(domain, transaction_id)` within one verified
snapshot. Silver facts are full snapshots per `cob_dt`, so `COUNT(*)` across
partitions counts the same transaction more than once.

## 🔗 Related

- [README.md](README.md) — Quick start
- [RUNBOOK.md](RUNBOOK.md) — Operations guide
- [DEMO_GUIDE.md](DEMO_GUIDE.md) — Demo walkthrough

---

## 📐 Verified figures

Every count in this document is generated and checked against
[`docs/evidence/metrics-manifest.yaml`](docs/evidence/metrics-manifest.yaml).
See [README.md](README.md#verified-portfolio-snapshot) for the full table with
metric definitions — counts are ambiguous without them.

## ⚠️ Not implemented

Stated explicitly so the architecture is not read as claiming more than it does:

- CDC consolidation progress is an Iceberg snapshot id per target table; event order
  is `(CDC timestamp, Spark batch id)`. It is **not** partition-aware, and Kafka
  topic/partition/offset are **not** persisted on the valid Bronze CDC path (only on the DLQ path).
- The 2026-09-30 fixes have been verified by unit/Spark regression tests and Airflow
  rendering, not yet on the running Docker stack (see README §Current Limitations).
- Gold analytics remain batch-derived; Silver Current does not feed Gold.
- The platform does not claim end-to-end exactly-once semantics; it provides
  checkpointed, replay-safe, idempotent processing.
