# Banking Data Platform

> End-to-end data platform for banking analytics — Medallion Architecture on Apache Iceberg + MinIO, orchestrated by Apache Airflow, with Real-time CDC via Debezium + Kafka.

## 🎯 Mục tiêu
Xây dựng data platform hoàn chỉnh cho ngân hàng với:
- **Medallion Architecture** (Bronze → Silver → Gold)
- **YAML-driven ETL** trên Apache Spark
- **Real-time CDC** với Debezium + Kafka + Spark Streaming
- **Data Governance** (Data Contracts, DQ, Lineage)
- **Data Catalog** với OpenMetadata (53 tables, lineage, tags, glossary)
- **Analytics Dashboard** với Streamlit (10 pages)
- **Production Scheduling** với Airflow (daily/weekly)

## 🏗️ Kiến trúc

```
┌──────────────┐    ┌───────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ Data Generator│───▶│ PostgreSQL│───▶│  Bronze  │───▶│  Silver  │───▶│   Gold   │
│  (Python)    │    │  (Source) │    │  (Raw)   │    │ (Clean)  │    │  (Mart)  │
└──────────────┘    └───────────┘    └──────────┘    └──────────┘    └──────────┘
                                        │                                │
                              CDC (Debezium + Kafka)                     │
                              ┌──────────┘                               │
                              ▼                                          ▼
                         ┌──────────┐                            ┌──────────────┐
                         │  Kafka   │                            │  Governance  │
                         │(Streaming)│                           │  (Contracts, │
                         └──────────┘                            │   DQ, Lineage)│
                                                                 └──────────────┘
                                                                        │
                                                                        ▼
                                                                 ┌──────────┐
                                                                 │  Trino   │
                                                                 │ (Query)  │
                                                                 └──────────┘
                                                                        │
                                                                        ▼
                                                                 ┌──────────────┐
                                                                 │ OpenMetadata │
                                                                 │  (Catalog)   │
                                                                 └──────────────┘
```

## 📊 Tech Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| PostgreSQL | 15 | Source database (4 schemas, 16 tables) |
| Apache Spark | 3.5.3 | ETL compute (Standalone: 1 master + 1 worker) |
| Apache Iceberg | 1.6 | Table format (ACID, time travel) |
| MinIO | latest | S3-compatible object storage |
| Iceberg REST | 1.6 | Catalog service (JDBC backend) |
| Trino | 443 | Interactive query engine |
| Apache Airflow | 2.10.0 | Pipeline orchestration (19+ DAGs) |
| Debezium | 2.6 | Change Data Capture (CDC) |
| Kafka | 7.6 | Event streaming (12 topics) |
| OpenMetadata | 1.5.6 | Data Catalog & Lineage (53 tables) |
| dbt | 1.12.0 | Semantic layer (12 models) |
| Streamlit | 1.40.0 | Analytics dashboard (10 pages) |

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (14GB+ RAM)
- Git

### 1. Clone & Configure
```bash
git clone https://github.com/your-org/banking_data_platform.git
cd banking_data_platform
cp docker/.env.example docker/.env
```

### 2. Start Infrastructure
```bash
cd docker && docker compose up -d
# Wait ~2 minutes for services to become healthy
```

### 3. Check Status
```bash
docker compose ps
```

### 4. Access UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **MinIO** | http://localhost:9001 | minioadmin / Minioadmin123 |
| **Spark** | http://localhost:9090 | — |
| **Trino** | http://localhost:8085 | — |
| **PostgreSQL** | localhost:5432 | banking_admin / BankingAdmin123 |
| **OpenMetadata** | http://localhost:8585 | admin / admin |
| **Streamlit** | http://localhost:8501 | — |
| **Kafka UI** | http://localhost:8081 | — |
| **Debezium** | http://localhost:8083 | — |

### 5. Generate Seed Data
```bash
python data_generator/generate_all.py --host localhost --port 5432
```

### 6. Run ETL Pipeline

**Airflow DAG order:**
1. `cdc_register_connectors` → `cdc_streaming_pipeline`
2. `bronze_core_banking_dag` → `bronze_card_crm_dag` → `bronze_digital_banking_dag`
3. `silver_all_dag` (waits for bronze)
4. `gold_all_dag` (waits for silver)
5. `ops_data_quality_dag` → `ops_pii_masking_daily_dag`
6. `dbt_run`

### 7. Query Data
```bash
docker exec banking-trino trino --catalog lakehouse --execute "SELECT COUNT(*) FROM gold.mart_customer_360"

# RFM Segments
docker exec banking-trino trino --catalog lakehouse --execute "SELECT rfm_segment, COUNT(*) FROM gold.rfm_segment GROUP BY rfm_segment ORDER BY 2 DESC"

# Churn Risk
docker exec banking-trino trino --catalog lakehouse --execute "SELECT churn_risk, COUNT(*) FROM gold.churn_prediction GROUP BY churn_risk ORDER BY 2 DESC"
```

## 📁 Project Structure

```
banking_data_platform/
├── docker/                    # Docker Compose + init scripts (21 services)
│   ├── docker-compose.yml     # Full stack configuration
│   ├── .env / .env.example    # Environment variables
│   ├── Dockerfile.airflow     # Airflow image
│   ├── Dockerfile.spark       # Spark with Kafka JARs
│   ├── init_postgres/         # PostgreSQL DDL (6 SQL files)
│   ├── init_trino/            # Trino catalog config
│   ├── init_iceberg/          # Iceberg schema DDL (5 SQL files)
│   └── spark/conf/            # spark-defaults.conf
├── code_etl/                  # YAML-driven ETL jobs
│   ├── shared/                # SparkSession, utils, ops
│   ├── bronze/                # 16 YAML configs + JDBC ingestion
│   ├── silver/                # 13 YAML configs + SCD/fact jobs
│   ├── gold/                  # 10 YAML configs + mart jobs
│   └── cdc/                   # CDC (6 YAML configs + streaming job)
├── airflow/
│   ├── dags/                  # 19+ Airflow DAGs
│   │   ├── bronze/            # Bronze layer DAGs
│   │   ├── silver/            # Silver layer DAG
│   │   ├── gold/              # Gold layer DAG
│   │   ├── dbt/               # dbt DAG
│   │   ├── ops/               # Operations DAGs
│   │   └── cdc/               # CDC DAGs (register + streaming)
│   └── plugins/               # ETL flag, JDBC connection utils
├── governance/                # Data Governance module
│   ├── contracts.py           # Pydantic models
│   ├── enforcement.py         # Contract validation
│   ├── lineage.py             # Lineage tracking
│   ├── audit.py               # Audit trail
│   └── datasets/              # 33 YAML contract files
├── dbt/                       # dbt project (semantic layer)
├── streamlit/                 # Streamlit dashboard (10 pages)
├── openmetadata/              # OpenMetadata registration scripts
│   ├── register_all_tables.sh # Register all 53 tables
│   ├── register_tables.py     # Python version
│   └── README.md              # Documentation
├── demo/                      # Demo materials
│   └── DEMO_SCRIPT.md         # Demo script with real data
├── data_generator/            # Seed data generator
├── tests/                     # 262 unit tests
├── .github/workflows/         # CI/CD pipelines
├── scripts/                   # Utility scripts
└── README.md                  # This file
```

## 📊 Data Layers

### Bronze Layer (Raw)
- **14 batch tables**: core_banking (8), card_crm (3), digital_banking (3)
- **6 CDC tables**: Real-time updates from PostgreSQL
- **Data**: 30K customers, 90K accounts, 1.8M card txns, 1.5M online txns

### Silver Layer (Cleaned)
- **8 dimension tables**: SCD Type 1/2 tracking
- **5 fact tables**: Partitioned by cob_dt
- **Data**: 10K customers, 30K accounts, 2.4M+ transactions

### Gold Layer (Analytics)
- **9 history tables**: Daily snapshots for trend analysis
- **9 current tables**: Latest state for serving
- **Data**: 20K customers with full analytics

## 📈 Business Analytics

### RFM Segmentation
| Segment | Count | Percentage |
|---------|-------|------------|
| Potential Loyalists | 5,094 | 25.5% |
| Champions | 4,428 | 22.1% |
| Loyal Customers | 4,414 | 22.1% |
| At Risk | 3,088 | 15.4% |
| New Customers | 1,772 | 8.9% |
| Hibernating | 1,204 | 6.0% |

### Churn Prediction
| Risk Level | Count | Percentage |
|------------|-------|------------|
| Active | 19,116 | 95.6% |
| High | 682 | 3.4% |
| Low | 195 | 1.0% |
| Medium | 7 | 0.0% |

### AUM Buckets
| Bucket | Count | Percentage |
|--------|-------|------------|
| AFFLUENT | 10,130 | 50.7% |
| PRIORITY | 7,310 | 36.6% |
| MASS | 2,560 | 12.8% |

### Customer Segments
| Segment | Count | Percentage |
|---------|-------|------------|
| RETAIL | 14,108 | 70.5% |
| PRIORITY | 4,314 | 21.6% |
| VIP | 1,578 | 7.9% |

## 🛡️ Data Governance

### Data Contracts (33 YAML files — 24 history + 9 current-serving)
- Validates schema, nullability, uniqueness before write
- Enforces business rules at each layer
- Freshness SLA enforcement via contract-driven checks

### Data Quality (8 check types)
- row_count, null_check, unique_check, range_check
- referential_integrity, anomaly_detection, freshness_check, schema_drift

### Lineage Tracking
- Bronze → Silver: 13 transforms (SCD1/SCD2/Fact)
- Silver → Gold: 11 transforms (Mart aggregations)
- OpenMetadata: 22 lineage edges visualized

## 📚 OpenMetadata Catalog

### Tables Registered: 53
| Layer | Tables | Status |
|-------|--------|--------|
| Bronze (Batch) | 14 | ✅ |
| Bronze (CDC) | 6 | ✅ |
| Silver (Dims) | 8 | ✅ |
| Silver (Facts) | 5 | ✅ |
| Gold (History) | 9 | ✅ |
| Gold (Current) | 9 | ✅ |

### Tags Applied
- **Tier.Tier1** (4 tables): core_banking_customer, core_banking_account, dim_customer, dim_account
- **Tier.Tier2** (13 tables): Transactions, cards, gold tables
- **Tier.Tier3** (5 tables): branch, employee, product
- **PII.Sensitive** (14 tables): All tables with customer PII

### Glossary
- **Banking_Glossary**: 8 terms (KYC, PCI_DSS, AML, SCD_Type1, SCD_Type2, RFM, Churn_Risk, AUM)

## 🔄 Production Schedule

| Time | DAG | Description |
|------|-----|-------------|
| 02:00 | Bronze DAGs | Ingest raw data from PostgreSQL |
| 04:00 | Silver DAGs | Clean, deduplicate, SCD tracking |
| 06:00 | Gold DAGs | Build analytics marts |
| 07:00 | dbt DAGs | Run semantic layer models |
| 08:00 | Ops DAGs | Data quality, PII masking |
| 09:00 | Ops DAGs | Contract validation, quarantine |
| 03:00 (Sun) | Maintenance | Vacuum, optimize, cleanup |

## 🧪 Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ -v --cov=governance --cov-report=term-missing
```

**262 tests** covering governance, ETL, and integration.

## 🔄 CI/CD

### GitHub Actions
- **CI**: Lint → Validate YAML → Test → Coverage
- **CD**: Build Docker → Push to GHCR

### Validate Locally
```bash
# Lint
ruff check governance/ code_etl/ tests/

# Validate YAML
./scripts/validate_yaml.sh

# Run tests
python -m pytest tests/ -v
```

## 📚 Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — Kiến trúc chi tiết
- [RUNBOOK.md](RUNBOOK.md) — Hướng dẫn vận hành
- [DEMO_GUIDE.md](DEMO_GUIDE.md) — Hướng dẫn demo
- [demo/DEMO_SCRIPT.md](demo/DEMO_SCRIPT.md) — Demo script với data thực tế
- [openmetadata/README.md](openmetadata/README.md) — OpenMetadata documentation

## 🔗 Quick Links

| Service | URL |
|---------|-----|
| Airflow | http://localhost:8080 |
| MinIO | http://localhost:9001 |
| Spark | http://localhost:9090 |
| Trino | http://localhost:8085 |
| OpenMetadata | http://localhost:8585 |
| Streamlit | http://localhost:8501 |
| Kafka UI | http://localhost:8081 |
| Debezium | http://localhost:8083 |

## 📝 License

MIT
