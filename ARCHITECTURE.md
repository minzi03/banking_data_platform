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
│  │  16 tables: core_banking(8) + card_crm(3) + digital_banking(5)     │   │
│  │  Format: Parquet + Iceberg metadata                                 │   │
│  │  Strategy: full_snapshot (dims) + incremental (facts)               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    SILVER LAYER (Cleaned)                            │   │
│  │  8 Dimensions: dim_customer(SCD2), dim_account(SCD2), dim_*, ...   │   │
│  │  5 Facts: fact_txn_account, fact_card_txn, fact_*, ...             │   │
│  │  Strategy: SCD1/SCD2 + incremental                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    GOLD LAYER (Analytics)                            │   │
│  │  5 Mart360: mart_customer_360, customer_*_summary, ...             │   │
│  │  3 Segments: rfm_segment, churn_prediction, cross_sell_segment     │   │
│  │  2 Analytics: branch_monthly_summary, campaign_target              │   │
│  │  Strategy: Aggregated marts                                         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
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

### 3. Aggregation Flow (Gold)
```
Silver ──Spark SQL──▶ Gold (Marts)
                       │
                       └── Config: YAML files in code_etl/gold/
```

### 4. Query Flow
```
Gold ──Trino──▶ SQL Query ──▶ Results
```

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
                              └── 8 check types
```

### Lineage Tracking
```
Pipeline Run ──▶ LineageTracker ──▶ PostgreSQL (lineage_log)
                     │
                     └── Bronze→Silver (13 transforms)
                         Silver→Gold (11 transforms)
```

## 📊 Schema Mapping

### Bronze → Silver Transforms
| Bronze Table | Silver Table | Transform |
|--------------|--------------|-----------|
| core_customer | dim_customer | SCD2 (merge) |
| core_account | dim_account | SCD2 (merge) |
| core_product | dim_product | SCD1 (upsert) |
| core_branch | dim_branch | SCD1 (upsert) |
| core_card | dim_card | SCD1 (upsert) |
| core_employee | dim_employee | SCD1 (upsert) |
| core_device | dim_device | SCD1 (upsert) |
| core_location | dim_location | SCD1 (upsert) |
| core_txn_account | fact_txn_account | Incremental |
| core_card_txn | fact_card_txn | Incremental |
| core_crm_interaction | fact_crm_interaction | Incremental |
| core_online_transaction | fact_online_transaction | Incremental |
| core_support_ticket | fact_support_ticket | Incremental |

### Silver → Gold Transforms
| Silver Tables | Gold Table | Transform |
|---------------|------------|-----------|
| dim_customer + dim_account + dim_card + fact_* | mart_customer_360 | Aggregation |
| mart_customer_360 | rfm_segment | RFM scoring |
| mart_customer_360 | churn_prediction | Churn risk |
| mart_customer_360 | cross_sell_segment | Cross-sell |
| dim_branch + dim_account + fact_txn_account | branch_monthly_summary | Monthly agg |
| rfm_segment + churn_prediction + cross_sell_segment | campaign_target | Campaign |

## 🔧 Tech Stack Details

### Docker Services (20)
| Service | Port | Purpose |
|---------|------|---------|
| postgres | 5432 | Source database |
| minio | 9000, 9001 | Object storage |
| iceberg-rest | 8181 | Catalog service |
| spark-master | 7077, 9090 | Spark coordinator |
| spark-worker-1 | 9091 | Spark compute |
| zookeeper | 2181 | Kafka coordination |
| kafka | 9092 | Event streaming |
| debezium | 8083 | CDC connector |
| trino | 8085 | Query engine |
| airflow-init | — | DB migration |
| airflow-webserver | 8080 | Airflow UI |
| airflow-scheduler | 8793 | Airflow scheduler |
| om-mysql | 3307 | OpenMetadata DB |
| om-elasticsearch | 9200 | OpenMetadata search |
| openmetadata | 8585 | Data catalog |

## 📈 Data Volume

| Layer | Tables | Rows (approx) |
|-------|--------|---------------|
| Source | 16 | ~2.6M |
| Bronze | 16 | ~2.6M |
| Silver | 13 | ~2.5M |
| Gold | 10 | ~100K |

## 🔗 Related

- [README.md](README.md) — Quick start
- [RUNBOOK.md](RUNBOOK.md) — Operations guide
- [DEMO_GUIDE.md](DEMO_GUIDE.md) — Demo walkthrough
