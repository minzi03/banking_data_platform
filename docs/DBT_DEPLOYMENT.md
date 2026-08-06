# =============================================================================
# DBT Deployment Guide — Banking Data Platform
# =============================================================================
# Architecture: Lakehouse 2.0
# dbt Role: Semantic layer on top of Gold tables (Spark handles Bronze→Silver→Gold)
# =============================================================================

## Overview

This guide covers deploying dbt for the Banking Data Platform.

**Architecture:**
- **Spark** handles ALL data transformations: Bronze → Silver → Gold
- **dbt** handles ONLY the semantic layer on top of Gold tables (12 ephemeral models + 19 sources)
- **Airflow** orchestrates both Spark ETL and dbt semantic layer

## Prerequisites

- Python 3.11+
- Trino instance running (localhost:8085 in Docker)
- Iceberg catalog configured (`lakehouse`)
- Gold tables created by Spark ETL

## Installation

### Option 1: Local Installation

```bash
# Navigate to scripts directory
cd banking_data_platform/scripts

# Run installation script
# Linux/Mac:
chmod +x install_dbt.sh
./install_dbt.sh

# Windows:
install_dbt.bat
```

### Option 2: Docker Installation

```bash
# Navigate to docker directory
cd banking_data_platform/docker/dbt

# Build and run dbt
docker-compose -f docker-compose.dbt.yml up dbt

# Run docs server
docker-compose -f docker-compose.dbt.yml up dbt-docs
```

### Option 3: Manual Installation

```bash
# Install dbt-core and trino adapter
pip install dbt-core==1.12.0
pip install dbt-trino==1.8.0

# Navigate to dbt project
cd banking_data_platform/dbt

# Install packages
dbt deps
```

## Configuration

### 1. Profile Configuration

Edit `dbt/profiles.yml`:

```yaml
banking:
  target: dev

  outputs:
    dev:
      type: trino
      method: none
      host: trino          # Your Trino host
      port: 8080           # Your Trino port
      catalog: lakehouse   # Your Iceberg catalog
      schema: semantic     # Default schema for dbt
      user: admin          # Your username
      password: ""         # Your password
      threads: 4
```

### 2. Project Configuration

`dbt/dbt_project.yml`:

```yaml
name: 'banking'
version: '1.0.0'
profile: 'banking'
model-paths: ["models"]
macro-paths: ["macros"]
models:
  banking:
    semantic:
      +materialized: ephemeral
      +schema: semantic
      +tags: ["semantic"]
```

## Architecture

### Sources (19 Gold tables from Spark)

dbt reads Gold tables created by Spark as sources:

**History Tables (10):**

| Source Table | Description |
|-------------|-------------|
| `mart_customer_360` | Customer 360° view (snapshot by cob_dt) |
| `customer_transaction_summary` | Transaction aggregations per customer |
| `customer_card_summary` | Card usage aggregations per customer |
| `customer_balance_summary` | Balance aggregations per customer |
| `customer_product_summary` | Product holdings per customer |
| `rfm_segment` | RFM segmentation |
| `churn_prediction` | Churn risk prediction |
| `cross_sell_segment` | Cross-sell opportunity detection |
| `campaign_target` | Campaign targeting |
| `branch_monthly_summary` | Branch performance by month |

**Current-Serving Tables (9):**

| Source Table | Description |
|-------------|-------------|
| `mart_customer_360_current` | Latest customer 360 (1 row/customer) |
| `customer_transaction_summary_current` | Latest transaction summary |
| `customer_card_summary_current` | Latest card summary |
| `customer_balance_summary_current` | Latest balance summary |
| `customer_product_summary_current` | Latest product summary |
| `rfm_segment_current` | Latest RFM segment |
| `churn_prediction_current` | Latest churn prediction |
| `cross_sell_segment_current` | Latest cross-sell segment |
| `campaign_target_current` | Latest campaign target |

### Semantic Models (12 ephemeral models)

| Model | Description | Sources Used |
|-------|-------------|-------------|
| `sm_customer` | Customer semantic (history) | `mart_customer_360` |
| `sm_customer_current` | Customer semantic (current) | `mart_customer_360_current` |
| `sm_account` | Account semantic | `customer_balance_summary` |
| `sm_balance_current` | Balance semantic (current) | `customer_balance_summary_current` |
| `sm_transaction` | Transaction semantic (history) | `customer_transaction_summary` |
| `sm_transaction_current` | Transaction semantic (current) | `customer_transaction_summary_current` |
| `sm_product_current` | Product semantic (current) | `customer_product_summary_current` |
| `sm_card_current` | Card semantic (current) | `customer_card_summary_current` |
| `sm_rfm_current` | RFM semantic (current) | `rfm_segment_current` |
| `sm_churn_current` | Churn semantic (current) | `churn_prediction_current` |
| `sm_cross_sell_current` | Cross-sell semantic (current) | `cross_sell_segment_current` |
| `sm_campaign_current` | Campaign semantic (current) | `campaign_target_current` |

### Metrics

Metrics are defined in `dbt/models/semantic/metrics.yml` (currently empty, waiting for MetricFlow integration).

## Usage

### Run Semantic Layer

```bash
cd banking_data_platform/dbt

# Install packages
dbt deps

# Run semantic models
dbt run --select semantic

# Run all tests (source tests on Gold tables)
dbt test

# Generate documentation
dbt docs generate

# Serve docs locally
dbt docs serve --port 8081
# Access docs at: http://localhost:8081
```

### Run Tests

```bash
# Run all tests
dbt test

# Run tests for specific source
dbt test --select source:gold
```

## Airflow Integration

### DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `dbt_run` | Daily 6 AM | Run semantic models + tests + docs |
| `dbt_seed` | Once | Load seed data |

### Task Flow (dbt_run DAG)

```
start → dbt_deps → dbt_run_semantic → dbt_test → dbt_docs_generate → end
```

## Docker Deployment

### Build Image

```bash
cd banking_data_platform
docker build -f docker/dbt/Dockerfile -t banking-dbt .
```

### Run Container

```bash
docker run --rm \
  --network banking-network \
  -v $(pwd)/dbt:/opt/dbt \
  banking-dbt \
  dbt run
```

### Docker Compose

```bash
cd banking_data_platform/docker/dbt
docker-compose -f docker-compose.dbt.yml up
```

**Note:** dbt-docs runs on port 8082 (port 8081 is used by Kafka UI).

## Troubleshooting

### Connection Issues

```bash
# Test Trino connection
dbt debug

# Check profiles
dbt debug --profiles-dir .
```

### Package Issues

```bash
# Clean and reinstall packages
rm -rf dbt_packages
dbt deps
```

### Gold Tables Not Found

Ensure Spark ETL has run and created Gold tables:

```sql
-- Check Gold tables exist
SELECT table_name FROM lakehouse.information_schema.tables
WHERE table_schema = 'gold';
```

### Schema Mismatch

Gold tables are created by Spark. If columns don't match dbt sources:
1. Query actual table schema: `DESCRIBE lakehouse.gold.mart_customer_360`
2. Update `dbt/models/gold/_gold_sources.yml` to match

## Support

- dbt documentation: https://docs.getdbt.com
- Trino documentation: https://trino.io/docs
- Project README: `dbt/README.md`
