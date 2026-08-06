# dbt — Semantic Layer (Banking Data Platform)

## Architecture

```
Spark (PySpark) ──→ Bronze → Silver → Gold     (Airflow DAGs orchestrate)
dbt ──→ Semantic layer TRÊN Gold                (metrics, docs, lineage)
```

- **Spark** xử lý toàn bộ transform: Bronze → Silver → Gold
- **dbt** chỉ làm semantic layer trên Gold tables

## Components

### Gold Sources (19 tables)
Trỏ vào Spark-created Iceberg tables trong `lakehouse.gold`:

| Table | Description |
|-------|-------------|
| `mart_customer_360` | Customer 360° view — 28+ KPIs |
| `mart_customer_360_current` | Current-serving Customer 360 — 1 row/customer |
| `customer_balance_summary` | Balance aggregations |
| `customer_balance_summary_current` | Current-serving balance summary |
| `customer_transaction_summary` | Transaction aggregations |
| `customer_transaction_summary_current` | Current-serving transaction summary |
| `customer_product_summary` | Product ownership |
| `customer_product_summary_current` | Current-serving product summary |
| `customer_card_summary` | Card portfolio |
| `customer_card_summary_current` | Current-serving card summary |
| `rfm_segment` | RFM segmentation |
| `rfm_segment_current` | Current-serving RFM segment |
| `churn_prediction` | Churn risk scoring |
| `churn_prediction_current` | Current-serving churn prediction |
| `cross_sell_segment` | Cross-sell opportunities |
| `cross_sell_segment_current` | Current-serving cross-sell segment |
| `campaign_target` | Campaign targeting |
| `campaign_target_current` | Current-serving campaign target |
| `mart_branch_monthly_summary` | Branch monthly performance |

### Semantic Models (12 models)
Ephemeral models trên Gold tables:

| Model | Source Tables | Purpose |
|-------|---------------|---------|
| `sm_customer` | `mart_customer_360` | Customer dimension for metrics |
| `sm_customer_current` | `mart_customer_360_current` | Current-serving customer 360 |
| `sm_account` | `customer_balance_summary_current` | Account dimension for metrics |
| `sm_balance_current` | `customer_balance_summary_current` | Current-serving balance summary |
| `sm_transaction_current` | `customer_transaction_summary_current` | Current-serving transaction summary |
| `sm_product_current` | `customer_product_summary_current` | Current-serving product summary |
| `sm_card_current` | `customer_card_summary_current` | Current-serving card summary |
| `sm_rfm_current` | `rfm_segment_current` | Current-serving RFM segment |
| `sm_churn_current` | `churn_prediction_current` | Current-serving churn prediction |
| `sm_cross_sell_current` | `cross_sell_segment_current` | Current-serving cross-sell segment |
| `sm_campaign_current` | `campaign_target_current` | Current-serving campaign target |

### Exposures (4 exposures)
| Exposure | Type | Description |
|----------|------|-------------|
| `superset_customer_360` | dashboard | Superset dashboard |
| `powerbi_customer_360` | dashboard | Power BI dataset |
| `notebook_customer_analytics` | notebook | Notebook analytics |
| `ai_serving_customer` | ml | AI serving |

### Macros (1 macro)
- `generate_schema_name.sql` — Custom schema naming

## Usage

```bash
# Set alias for dbt-core (not dbt-fusion)
alias dbt="C:/Users/miynzi/AppData/Local/Python/pythoncore-3.14-64/Scripts/dbt.exe"

# Debug connection
dbt debug

# Install dependencies
dbt deps

# Parse models
dbt parse

# Compile models
dbt compile

# Run models (ephemeral, no materialization)
dbt run

# Test data quality (52 tests)
dbt test

# Generate docs
dbt docs generate

# Serve docs (port 8082)
dbt docs serve --port 8082
```

## Connection

- **Catalog**: `lakehouse`
- **Schema**: `semantic` (for dbt models)
- **Host**: `localhost:8085` (Trino)

## Test Results

```
dbt compile: Found 12 models, 52 data tests, 19 sources, 4 exposures
dbt test:    52/52 PASS
dbt docs:    Generated successfully
```
