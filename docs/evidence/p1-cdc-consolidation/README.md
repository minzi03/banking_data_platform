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

### Streaming Checkpoint Restart Smoke Test (P1.5)

Docker-backed verification run on 2026-10-17 using the isolated test channel
`qr.payment.restart_test`:

| Check | Result |
|-------|--------|
| Phase A: 12 events reached Bronze | ✅ 12/12 |
| Forced streaming-job kill | ✅ |
| Phase C: 8 new events + 1 replay | ✅ |
| Final accepted rows / distinct transaction IDs | ✅ 20 / 20 |
| Restart offset | ✅ Kafka partition 0, offset 12 |
| Exit code | ✅ 0 |

The restart log resumed from the committed checkpoint at offset 12 rather than
Kafka offset 0. The replayed event was removed by the configured watermark/dedup
state, so the append-only Bronze test table ended with 20 rows and 20 distinct
business keys. This proves checkpoint recovery and bounded watermark
deduplication; it is not an unbounded exactly-once claim.

### Merchant EOD three-way status (2026-10-19)

The implementation and read-only verifier are present, but a Docker-backed
`source = batch = CDC current` PASS has **not** been claimed. An isolated CDC
fixture proved Postgres UPDATE → Debezium → Kafka → Spark → Bronze (1 valid event,
0 DLQ), while the existing Silver batch partition represented a different source
snapshot. The verifier therefore correctly returned `FAIL` rather than publishing
misleading evidence. The independent synthetic QR producer and SFTP settlement
file are separate populations and are not included in this claim.

The remaining work is TD-22: a fully isolated Postgres + Kafka + Debezium + Spark +
Iceberg stack with one deterministic cohort, an injected mismatch case, and
verified cleanup. Until that exists, Merchant EOD is **implemented and unit-tested,
but not runtime-verified end-to-end**.

Evidence verifier: `scripts/verify_merchant_eod_reconciliation.py`.

### Latency (P1.4)
5 local trials, no manual intervention:

Evidence script: `scripts/verify_checkpoint_restart.py`.
Raw logs:
- `logs/restart_stream_1792253768.log` (phase A)
- `logs/restart_stream_1792253827.log` (phase D restart)

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
