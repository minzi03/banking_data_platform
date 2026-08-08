# P1 — CDC Consolidation Evidence

## Architecture

```
PostgreSQL
    ↓
Debezium (WAL CDC)
    ↓
Kafka
    ↓
Spark Structured Streaming
    ↓
Bronze CDC (append-only history)
    ↓
CDC Consolidation Engine (config-driven)
    ↓
Silver Current-State Tables
├── dim_customer_current
└── dim_account_current
```

- Bronze CDC remains append-only for audit and replay
- Silver Current provides latest derived operational state
- Gold analytics remain batch-produced (separate path)

## Verification Summary

### Initial Load (P1.1)
| Table | Rows | Distinct Keys | Status |
|-------|------|---------------|--------|
| dim_customer_current | 10,000 | 10,000 | ✅ |
| dim_account_current | 30,000 | 30,000 | ✅ |

### CRUD Operations (P1.2)
| Operation | Status | Evidence |
|-----------|--------|----------|
| INSERT | ✅ | Customer 999999 created, visible in Silver |
| UPDATE | ✅ | Customer 1001 email updated, reflected in Silver |
| DELETE | ✅ | Customer 999999 removed from Silver, Bronze append-only |
| Deduplication | ✅ | rows = distinct keys, no duplicates |

### Idempotency / Restart (P1.3)
| Check | Status |
|-------|--------|
| Re-run with no events = no state change | ✅ |
| Watermark persists across Spark sessions | ✅ |
| No duplicate business keys after restart | ✅ |
| Watermark never regresses | ✅ |

### Latency (P1.4)
5 local trials, no manual intervention:

| Metric | Min | Median | Average | Max |
|--------|-----|--------|---------|-----|
| E2E (source → Silver) | 22.4s | 49.8s | 41.1s | 54.0s |

All trials completed within one minute.

> Measured source-to-Silver freshness: 22.4–54.0s, median 49.8s,
> all trials < 1 minute in the local test environment.

## Scope Boundary

- P1 consolidates **customer** and **account** CDC entities into Silver current-state tables
- Gold analytics remain batch-produced (Bronze → Silver SCD2 → Gold marts)
- Engine is config-driven: adding new tables requires only a new YAML config

## Files

| File | Description |
|------|-------------|
| 01-initial-load.txt | Initial load evidence (customer + account) |
| 02-update.txt | UPDATE handling evidence |
| 03-delete.txt | DELETE handling evidence |
| 04-dedup-idempotency.txt | Deduplication and idempotency evidence |
| 05-restart-watermark.txt | Restart recovery and watermark persistence |
| 06-account-generic-engine.txt | Generic/config-driven engine proof |
| 07-latency-results.txt | Latency measurement raw data and statistics |
