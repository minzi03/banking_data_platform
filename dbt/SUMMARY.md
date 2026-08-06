# dbt Implementation Summary — Banking Data Platform

## Architecture

Spark handles Bronze → Silver → Gold transforms.
dbt handles Semantic Layer on Gold (metrics, docs, lineage).

## Components

| Component | Count | Description |
|-----------|-------|-------------|
| Gold Sources | 19 | Spark-created Iceberg tables in `lakehouse.gold` |
| Semantic Models | 12 | Ephemeral models on Gold tables |
| Exposures | 4 | Dashboard, Notebook, ML serving |
| Macros | 1 | `generate_schema_name.sql` |
| Data Tests | 52 | Source not_null + unique tests on Gold tables |

## Files

```
dbt/
├── dbt_project.yml              # Project config
├── profiles.yml                 # Trino connection
├── packages.yml                 # dbt packages
├── models/
│   ├── gold/
│   │   └── _gold_sources.yml    # 19 Gold sources with docs + tests
│   └── semantic/
│       ├── sm_customer.sql      # Customer semantic model
│       ├── sm_customer_current.sql
│       ├── sm_account.sql       # Account semantic model
│       ├── sm_balance_current.sql
│       ├── sm_transaction_current.sql
│       ├── sm_product_current.sql
│       ├── sm_card_current.sql
│       ├── sm_rfm_current.sql
│       ├── sm_churn_current.sql
│       ├── sm_cross_sell_current.sql
│       ├── sm_campaign_current.sql
│       ├── exposures.yml        # 4 exposures
│       ├── metrics.yml          # Metrics definitions
│       └── semantic_contracts.yml
└── macros/
    └── generate_schema_name.sql # Schema naming override
```

## Test Results

```
dbt compile: Found 12 models, 52 data tests, 19 sources, 4 exposures
dbt test:    52/52 PASS
dbt docs:    Generated successfully
```

## Gold Table Row Counts

| Table | Rows |
|-------|------|
| mart_customer_360 | 20,000 |
| mart_customer_360_current | 10,000 |
| rfm_segment | 20,000 |
| rfm_segment_current | 10,000 |
| churn_prediction | 20,000 |
| churn_prediction_current | 10,000 |
| cross_sell_segment | 20,000 |
| cross_sell_segment_current | 10,000 |
| campaign_target | 170,000 |
| campaign_target_current | 10,000 |
| customer_balance_summary | 20,000 |
| customer_balance_summary_current | 10,000 |
| customer_transaction_summary | 20,000 |
| customer_transaction_summary_current | 10,000 |
| customer_product_summary | 20,000 |
| customer_product_summary_current | 10,000 |
| customer_card_summary | 20,000 |
| customer_card_summary_current | 10,000 |
| mart_branch_monthly_summary | 7,200 |
