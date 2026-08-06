# Banking Data Platform

> End-to-end data platform for banking analytics — Medallion Architecture on Apache Iceberg + MinIO, orchestrated by Apache Airflow.

## 🎯 Mục tiêu
Xây dựng data platform hoàn chỉnh cho ngân hàng với:
- **Medallion Architecture** (Bronze → Silver → Gold)
- **YAML-driven ETL** trên Apache Spark
- **Data Governance** (Data Contracts, DQ, Lineage)
- **CI/CD** với GitHub Actions

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
| Kafka | 7.6 | Event streaming |
| OpenMetadata | 1.5.6 | Data Catalog & Lineage |

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (10GB+ RAM)
- Git

### 1. Clone & Configure
```bash
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
| **Airflow** | http://localhost:8080 | admin / admin123 |
| **MinIO** | http://localhost:9001 | minioadmin / Minioadmin123 |
| **Spark** | http://localhost:9090 | — |
| **Trino** | http://localhost:8085 | — |
| **PostgreSQL** | localhost:5432 | banking_admin / BankingAdmin123 |
| **OpenMetadata** | http://localhost:8585 | — |

### 5. Generate Seed Data
```bash
python data_generator/generate_all.py --host localhost --port 5432
```

### 6. Run ETL Pipeline

**Airflow DAG order:**
1. `bronze_core_banking_dag` → `bronze_card_crm_dag` → `bronze_digital_banking_dag`
2. `silver_all_dag` (waits for bronze)
3. `gold_all_dag` (waits for silver)
4. `ops_data_quality_dag` (waits for silver + gold)

### 7. Query Data
```bash
docker compose exec trino trino --catalog lakehouse

-- History snapshot table (can contain multiple rows per customer across dates)
SELECT COUNT(*) FROM lakehouse.gold.mart_customer_360;

-- Current serving table (exactly 1 row per customer)
SELECT COUNT(*) FROM lakehouse.gold.mart_customer_360_current;

-- Other current-serving customer-grain Gold tables
SELECT COUNT(*) FROM lakehouse.gold.customer_balance_summary_current;
SELECT COUNT(*) FROM lakehouse.gold.rfm_segment_current;
SELECT COUNT(*) FROM lakehouse.gold.campaign_target_current;

SELECT rfm_segment, COUNT(*) FROM lakehouse.gold.rfm_segment_current GROUP BY 1;
```

### 8. dbt semantic layer (dbt-core + dbt-trino)

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

## 📁 Project Structure

```
banking_data_platform/
├── docker/                    # Docker Compose + init scripts (20 services)
├── code_etl/                  # YAML-driven ETL jobs
│   ├── shared/                # SparkSession, utils, ops
│   ├── bronze/                # 16 YAML configs + JDBC ingestion
│   ├── silver/                # 13 YAML configs + SCD/fact jobs
│   ├── gold/                  # 10 YAML configs + mart jobs
│   └── cdc/                   # CDC streaming configs
├── governance/                # Data Governance module
│   ├── contracts.py           # Pydantic models
│   ├── enforcement.py         # Contract validation
│   ├── lineage.py             # Lineage tracking
│   ├── audit.py               # Audit trail
│   └── datasets/              # 33 YAML contract files
├── airflow/dags/              # 19+ Airflow DAGs
├── tests/                     # 262 unit tests
├── .github/workflows/         # CI/CD pipelines
├── scripts/                   # Utility scripts
└── README.md                  # This file
```

## 📊 Data Layers

| Layer | Tables | Description |
|-------|--------|-------------|
| **Bronze** | 16 | Raw data from PostgreSQL (16 batch + 6 CDC) |
| **Silver** | 13 | Cleaned, SCD-tracked (8 dims + 5 facts) |
| **Gold** | 19 | Analytics marts (10 history + 9 current-serving) |

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

## 📝 License

MIT
