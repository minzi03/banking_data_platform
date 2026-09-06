# Banking Data Platform

<p align="center">
  <strong>End-to-End Batch & Near-Real-Time Lakehouse Architecture for Banking Analytics</strong>
</p>

<p align="center">
  Apache Spark · Apache Iceberg · MinIO · Apache Airflow · Debezium · Kafka · Trino · dbt · OpenMetadata · Apache Superset
</p>

<p align="center">
  <a href="https://github.com/minzi03/banking_data_platform/actions">
    <img
      alt="CI"
      src="https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white"
    >
  </a>
  <img
    alt="Python"
    src="https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white"
  >
  <img
    alt="Spark"
    src="https://img.shields.io/badge/Apache%20Spark-3.5.3-E25A1C?logo=apachespark&logoColor=white"
  >
  <img
    alt="Iceberg"
    src="https://img.shields.io/badge/Apache%20Iceberg-1.6.0-2D6CDF"
  >
  <img
    alt="Airflow"
    src="https://img.shields.io/badge/Apache%20Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white"
  >
  <img
    alt="License"
    src="https://img.shields.io/badge/License-MIT-green"
  >
</p>

---

## Executive summary

An end-to-end, production-like banking lakehouse built to show how batch, CDC, data quality, governance, analytical serving and CI/CD fit together as **one platform** rather than a collection of isolated pipelines.

Operational banking data is ingested from PostgreSQL through both a batch and a CDC path, processed with Apache Spark, stored in Apache Iceberg, orchestrated by Airflow, and served through Trino and dbt.

The verified dataset holds **2.3 million distinct financial transactions per verified snapshot** across the account, card and online domains — a figure that was itself corrected after the original count summed the same logical transactions across several physical snapshots.

Spark owns history: **10 historical Gold models**, partitioned by close-of-business date. dbt, executed through Trino, owns current-serving publication: **9 dbt-managed current-serving tables** built from one explicit snapshot, so consumers never have to choose a historical partition.

Correctness is verified from platform state rather than inferred from successful job logs. Snapshot alignment, join grain, CDC current state, business-date semantics, and published metrics are each guarded by executable checks.

CI goes past unit tests. A reproducible Docker-based Trino/Iceberg topology runs **34 Trino-backed integration tests** against real pipeline output, including a real SCD Type 2 transition. The gate is path-aware, blocks the pull request when it is relevant, and was deliberately negative-tested: an intentional data assertion failure was pushed, the pull request went red, and the revert restored it.

Two principles shaped that work.

> **A green test is only valuable if the invariant itself is correct.**
>
> **A fallback that looks defensive can be a fabricated measurement.**

Several defects surfaced not because a job crashed, but because a previously green path was validating the wrong thing or suppressing the real failure signal. They are written up in [Engineering Decisions & Failures Found](#engineering-decisions--failures-found).

---

## What is this project?

**Banking Data Platform** is a production-like data engineering project that implements two complementary ingestion paths for banking workloads:

- **Scheduled batch processing** through a Bronze → Silver → Gold Medallion Lakehouse.
- **Near-real-time Change Data Capture (CDC)** using PostgreSQL WAL, Debezium, Kafka, and Spark Structured Streaming.

The platform combines:

- **Apache Spark** for batch and streaming processing
- **Apache Iceberg + MinIO** for lakehouse storage
- **Apache Airflow** for orchestration
- **Trino + dbt + Apache Superset** for analytical serving
- **OpenMetadata** for catalog, lineage, and governance
- **Prometheus + Grafana** for CDC freshness observability
- **Dead Letter Queue (DLQ)** handling for malformed CDC events

PostgreSQL remains the **operational source of truth**.

The batch and CDC paths produce different derived representations:

```text
Silver SCD1/SCD2
= historical analytical representation

Silver Current
= latest CDC-derived representation
```

---

# Architecture

The platform is easier to understand as five cooperating planes than as a list
of technologies. The first four move and shape data; the fifth controls and
verifies all of them.

```text
┌──────────────────────────────────────────────┐
│ 1. Operational sources                       │
│    PostgreSQL — core banking, cards, digital │
└───────────────────────┬──────────────────────┘
                        │  batch path  +  change-data-capture path
                        ▼
┌──────────────────────────────────────────────┐
│ 2. Data movement                             │
│    Spark JDBC · Debezium → Kafka             │
│    Spark Structured Streaming                │
└───────────────────────┬──────────────────────┘
                        ▼
┌──────────────────────────────────────────────┐
│ 3. Lakehouse transformation                  │
│    Bronze → Silver → historical Gold         │
│    Spark on Apache Iceberg                   │
└───────────────────────┬──────────────────────┘
                        ▼
┌──────────────────────────────────────────────┐
│ 4. Analytical serving                        │
│    dbt, executed through Trino               │
│    current-serving Iceberg tables            │
└──────────────────────────────────────────────┘

╔══════════════════════════════════════════════╗
║ 5. Control, governance and evidence          ║
║    Airflow · data contracts · data quality   ║
║    OpenMetadata · CI · evidence manifest     ║
║    — spans planes 1–4, not a stage after them║
╚══════════════════════════════════════════════╝
```

### 1. Operational sources

PostgreSQL is the operational source of truth across three banking domains:
core banking, cards and CRM, and digital banking. Nothing downstream writes
back to it.

### 2. Data movement

Two independent paths leave the source. A scheduled batch path reads over JDBC.
A change-data-capture path streams the write-ahead log through Debezium and
Kafka into Spark Structured Streaming, where malformed events are diverted to a
dead-letter queue instead of failing the micro-batch.

### 3. Lakehouse transformation

Persistent state begins here. Bronze holds a full snapshot per close-of-business
date; Silver builds SCD Type 1 and Type 2 dimensions and fact tables; historical
Gold holds the business marts. Every layer is partitioned by an explicit
`cob_dt`, and business dates are derived explicitly from UTC storage rather than
inherited from a session timezone.

### 4. Analytical serving

Spark owns history. dbt, executed through Trino, owns current-serving
publication: it reads one explicit snapshot from historical Gold and publishes
the current-serving Iceberg tables consumers actually query. A consumer never
chooses a partition, and a missing snapshot fails the build rather than serving
stale data quietly.

### 5. Control, governance and evidence

This plane runs across the other four rather than after them. Airflow
orchestrates and enforces completion contracts between layers; data contracts
and data-quality checks constrain what each layer may contain; OpenMetadata
carries catalog and lineage. CI stands up the whole topology and tests against
real pipeline output. Published metrics come from a versioned evidence manifest
and are drift-checked, so a number in this README cannot diverge from what was
measured.

---

For component relationships and major flows, see
**[ARCHITECTURE.md](ARCHITECTURE.md)**.

For implementation detail — orchestration, CDC semantics, serving mechanics and
time semantics — see
**[docs/architecture/architecture.md](docs/architecture/architecture.md)**.

---

# Key Capabilities

| Capability                             | Status         |
| -------------------------------------- | -------------- |
| PostgreSQL batch ingestion             | ✅ Implemented |
| YAML-driven Spark ETL                  | ✅ Implemented |
| Bronze → Silver → Gold processing      | ✅ Implemented |
| SCD Type 1 / Type 2 modeling           | ✅ Implemented |
| Debezium + Kafka CDC                   | ✅ Implemented |
| Spark Structured Streaming             | ✅ Implemented |
| Append-only Bronze CDC                 | ✅ Implemented |
| CDC consolidation into Silver Current  | ✅ Implemented |
| Persisted CDC watermarks               | ✅ Implemented (timestamp + batch id) |
| Idempotent Iceberg MERGE               | ✅ Implemented |
| Dead Letter Queue for malformed events | ✅ Implemented |
| Data contracts and Data Quality        | ✅ Implemented |
| OpenMetadata catalog and lineage       | ✅ Implemented |
| Trino + dbt serving layer              | ✅ Implemented |
| Apache Superset analytics              | ✅ Implemented |
| Prometheus + Grafana monitoring        | ✅ Implemented |
| GitHub Actions CI/CD                   | ✅ Implemented |

---

# Verified Portfolio Snapshot

Every runtime value below is generated and checked against
[`docs/evidence/metrics-manifest.yaml`](docs/evidence/metrics-manifest.yaml) by
`scripts/generate_metrics_manifest.py`, then re-checked against this table by
`scripts/verify_readme_metrics.py`. Definitions are part of the contract —
counts are ambiguous without them.

| Metric                     | Verified value | Definition                                                                     |
| -------------------------- | -------------: | ------------------------------------------------------------------------------ |
| Source workloads           |             16 | Executable Bronze ingestion configurations; templates and registries excluded   |
| Bronze batch tables        |             16 | One per ingestion workload                                                      |
| Bronze CDC tables          |              6 | Append-only change-history tables                                               |
| Silver SCD Type 2 dims     |              2 | `dim_customer`, `dim_account`                                                   |
| Silver SCD Type 1 dims     |              6 | Branch, product, card, employee, device, location                               |
| Silver fact tables         |              5 | Transactional and interaction facts                                             |
| Silver CDC current-state   |              2 | `dim_customer_current`, `dim_account_current`                                   |
| Historical Gold tables     |             10 | Spark-managed Gold history, partitioned by `cob_dt`                             |
| Current-serving tables     |              9 | dbt-managed Iceberg tables in `serving`, queryable through Trino                |
| Curated transactions       |      2,300,000 | Distinct domain-qualified transactions in one verified Silver snapshot          |
| Debezium connectors        |              3 | Runtime connector definitions                                                   |
| Kafka CDC topics           |             12 | One per captured source table (6 + 3 + 3)                                       |
| Data contracts             |             33 | Governance contract YAMLs                                                       |
| Data-quality check types   |              8 | Supported DQ rule categories                                                    |
| Airflow DAG files          |             16 | Files defining at least one DAG (17 DAG objects — one file defines two)         |
| Automated tests            |            472 | Python `def test_*` functions                                                   |
| Trino integration tests    |             34 | `def test_*` in the two modules the PR-blocking gate executes                    |
| Docker Compose services    |             24 | 20 long-running + 4 one-shot initialization/migration jobs                      |
| CDC current-state rows     | 10,000 / 30,000 | Customer / account rows after consolidation                                    |

**Curated transactions** replaces the previous `4.6M+` claim. That figure counted
`COUNT(*)` across accumulated full-snapshot fact partitions, so the same
transactions were counted once per `cob_dt`. The verified figure counts distinct
`(domain, transaction_id)` pairs within a single snapshot:

```text
account  1,200,000
card       600,000
online     500,000
─────────────────
total    2,300,000  distinct curated transaction records
```

**Automated tests** counts source-level test functions. The pytest node count
after parametrization is larger and is tracked separately in the evidence
manifest, so the two never have to be reconciled by hand.

## CDC freshness

| Measure | Value |
| ------- | ----: |
| Median local source→Silver | 409.8s |
| Range | 65.9–576.2s |
| Trials | 5 |
| Consolidation cadence | 600s (`*/10 * * * *`) |

Measured end to end: `t0` is the PostgreSQL `COMMIT`, `t1` is the moment the new
value is readable in `silver.dim_customer_current` **through Trino**. The number
therefore includes waiting for the next scheduled consolidation run, which
dominates it — that is deliberate, because it is the delay a consumer actually
experiences. Re-measured on `portfolio-v1.1`; see
[CDC Freshness Verification](#cdc-freshness-verification) for methodology and
for why it is not comparable to the figure published in `v1.0`.

---

# Batch Pipeline

## Data Flow

```text
PostgreSQL
    ↓
Spark JDBC
    ↓
Bronze Batch
    ↓
Spark Transformations
    ↓
Silver Dimensions + Facts
    ↓
Spark Business Transformations
    ↓
Gold Analytics
    ↓
Trino / dbt / Superset
```

Batch ingestion is driven by reusable YAML configuration and Spark ETL jobs.

---

## Bronze Batch

The Bronze Batch layer stores source-aligned data ingested through PostgreSQL JDBC.

It provides a stable landing layer for:

- validation
- schema enforcement
- downstream transformation
- backfills
- reprocessing

```text
PostgreSQL
    ↓
Spark JDBC
    ↓
Bronze Batch
```

---

## Silver Analytical Model

The batch Silver layer contains:

- **8 dimensions**
  - 2 SCD Type 2
  - 6 SCD Type 1
- **5 fact tables**

### SCD Type 2

- `dim_customer`
- `dim_account`

### SCD Type 1

- `dim_branch`
- `dim_product`
- `dim_card`
- `dim_employee`
- `dim_device`
- `dim_location`

### Fact Tables

- `fact_txn_account`
- `fact_card_txn`
- `fact_online_transaction`
- `fact_crm_interaction`
- `fact_support_ticket`

Within one verified Silver snapshot the transaction facts hold:

```text
2,300,000 distinct curated transaction records
account 1,200,000 · card 600,000 · online 500,000
```

Counted as distinct `(domain, transaction_id)` pairs. Silver facts are full
snapshots per `cob_dt`, so a plain `COUNT(*)` across all partitions counts the
same transaction once per snapshot — that is what the retired `4.6M+` figure
was measuring.

---

## SCD Type 2 Semantics

SCD Type 2 preserves historical versions of business entities.

Example:

```text
customer_id = 1001

Version 1
segment = MASS
valid_from = Jan
valid_to = Jun

Version 2
segment = PRIORITY
valid_from = Jul
valid_to = current
```

This enables questions such as:

> What customer state was valid when a historical business event occurred?

In short:

```text
Silver SCD2
= What was true then?
```

---

# Gold Analytics

Gold is produced from the **batch Silver analytical model**.

Current portfolio baseline:

```text
10 historical Gold tables   (Spark, partitioned by cob_dt)
 9 current-serving tables   (dbt via Trino, iceberg.serving.*)
```

The 8 `gold.*_current` CTAS tables and the Spark-only
`mart_customer_360_current` view were retired: the CTAS tables were created once
at initialization and never refreshed, and the Spark view was not visible to
Trino at all. Both are replaced by dbt-managed serving tables.

Representative outputs include:

- Customer 360
- RFM Segmentation
- Rule-Based Churn-Risk Scoring
- Customer Balance / AUM Analytics
- Cross-Sell Analytics
- Campaign Analytics
- Customer Product Summaries
- Customer Transaction Summaries
- Branch-Level Analytics

> Gold remains batch-derived in the current architecture.

---

# CDC Pipeline

## Data Flow

```text
PostgreSQL WAL
    ↓
Debezium
    ↓
Kafka
    ↓
Spark Structured Streaming
    ↓
CDC Validation
   /             \
valid           invalid
  ↓                ↓
Bronze CDC         DLQ
  ↓
CDC Consolidation
  ↓
Silver Current
```

CDC provides a near-real-time change stream without repeatedly scanning entire PostgreSQL tables.

---

## CDC Components

- PostgreSQL WAL
- Debezium 2.6
- 3 Debezium connectors
- 12 Kafka CDC topics
- Spark Structured Streaming
- 6 append-only Bronze CDC tables
- CDC validation
- CDC Dead Letter Queue
- Config-driven Silver Current consolidation

---

# Bronze CDC

Bronze CDC preserves source changes as **append-only change history**.

Representative datasets:

- `core_customer_cdc`
- `core_account_cdc`
- `core_transaction_cdc`
- `card_account_cdc`
- `card_transaction_cdc`
- `online_transaction_cdc`

CDC records retain:

- normalized CDC operation (`INSERT` / `UPDATE` / `DELETE` / `SNAPSHOT`)
- event timestamp (epoch ms and derived timestamp)
- Spark batch ID
- ingestion timestamp

Kafka topic, partition and offset are **not** persisted on the valid-event path.
They are captured only for events routed to the dead-letter queue, where they
are needed for triage. Replay of valid events is therefore driven by event
timestamp and batch id, not by Kafka offsets.

### Change Operations

```text
SNAPSHOT
INSERT
UPDATE
DELETE
```

Example:

```text
customer_id = 1001

09:00 SNAPSHOT email=a@example.com
10:00 UPDATE   email=b@example.com
11:00 UPDATE   email=c@example.com
```

All events remain available in Bronze CDC.

> **Bronze CDC = audit + replay boundary**

---

# CDC Current-State Consolidation

Selected CDC entities are incrementally consolidated into Silver latest-state representations.

```text
bronze.core_customer_cdc
        ↓
cdc_consolidation.py
        ↓
silver.dim_customer_current
```

```text
bronze.core_account_cdc
        ↓
cdc_consolidation.py
        ↓
silver.dim_account_current
```

The same generic engine is reused with different YAML configurations.

---

## Consolidation Responsibilities

The engine performs:

- incremental CDC reads
- business-key deduplication
- persisted progress tracking per target table (timestamp + batch id)
- INSERT handling
- UPDATE handling
- DELETE handling
- SNAPSHOT handling
- idempotent Iceberg MERGE
- restart-safe reprocessing

---

## Verified Current-State Tables

| Bronze CDC Source          | Silver Current Target         |   Rows |
| -------------------------- | ----------------------------- | -----: |
| `bronze.core_customer_cdc` | `silver.dim_customer_current` | 10,000 |
| `bronze.core_account_cdc`  | `silver.dim_account_current`  | 30,000 |

Current-state semantics:

```text
1 business key
      ↓
1 latest row
```

In short:

```text
Silver Current
= What is true now?
```

---

# Silver SCD2 vs Silver Current

The two representations intentionally serve different requirements.

| Property              | Silver SCD2           | Silver Current            |
| --------------------- | --------------------- | ------------------------- |
| Source path           | Batch                 | CDC                       |
| Purpose               | Historical analytics  | Latest consolidated state |
| Rows per business key | Multiple versions     | One                       |
| History               | Preserved             | Latest state only         |
| Freshness             | Batch-cycle dependent | CDC-derived               |
| Example               | `dim_customer`        | `dim_customer_current`    |
| Main question         | What was true then?   | What is true now?         |

Example:

### SCD Type 2

```text
customer_sk | customer_id | segment  | valid_from | valid_to | current
------------|-------------|----------|------------|----------|--------
SK01        | 1001        | MASS     | Jan        | Jun      | 0
SK02        | 1001        | PRIORITY | Jul        | NULL     | 1
```

### Silver Current

```text
customer_id | segment
------------|---------
1001        | PRIORITY
```

Both are derived representations.

PostgreSQL remains the operational source of truth.

---

# Watermark & Restart Recovery

CDC consolidation stores processing progress per target table:

```text
table_name
+ last_cdc_timestamp_ms
+ last_spark_batch_id
```

Incremental reads select events after that pair, ordered by
`(__cdc_timestamp_ms, __spark_batch_id)`.

### Watermark Benefits

- incremental processing
- restart recovery
- reduced rescanning
- replay-safe failure handling

### Current limitation — stated plainly

The watermark is **not** partition-aware, and Kafka offsets are not persisted on
the valid-event path. Progress is tracked per table, not per
`(topic, partition)`:

```text
implemented today   : watermark = CDC timestamp + Spark batch id
not implemented yet : per-partition Kafka offset watermark
```

This is sufficient for restart-safe, idempotent replay — reprocessing the same
events produces the same Silver state — but it does not provide per-partition
progress or offset-level replay. Those require persisting Kafka metadata into
the valid Bronze CDC path first. The evidence manifest tracks this explicitly:

```yaml
consolidation_watermark:
  implementation: timestamp_plus_spark_batch_id
  partition_aware: false
  kafka_offsets_persisted_in_valid_bronze: false
```

---

## Failure Recovery

Processing order:

```text
Read CDC
    ↓
Deduplicate
    ↓
MERGE Silver Current
    ↓
Successful?
    ↓
Advance Watermark
```

Consider:

```text
Silver MERGE succeeds
        ↓
process crashes
        ↓
watermark not updated
```

After restart:

```text
old watermark
    ↓
same events read again
    ↓
idempotent MERGE
    ↓
same final Silver state
    ↓
watermark advances
```

The system therefore supports:

> **checkpointed, replay-safe, idempotent processing**

It does **not** claim end-to-end exactly-once semantics.

---

# CDC Freshness Verification

Five local runtime trials measured the time from a PostgreSQL source change until the resulting state became visible in Silver Current.

**Methodology**

```text
t0 = PostgreSQL COMMIT of an UPDATE to core_banking.customer
t1 = the new value is readable in silver.dim_customer_current, queried via Trino
freshness = t1 - t0
```

`t1` is deliberately taken at the consumer-facing table through the query
engine, not at Kafka or Bronze arrival. The measurement therefore includes the
wait for the next scheduled consolidation run.

**Phase sampling.** Consolidation runs on `*/10 * * * *`, so freshness is
dominated by *where in the 600s window the commit lands*. Running trials
back to back does not sample that offset randomly: each trial finishes the
instant a consolidation run completes, so the next trial commits at offset ~0
and waits nearly a full window. Measured that way the series locks onto the
ceiling — observed directly as 355s, 596s, 599s against a 600s cadence. Trials
below are therefore separated by a uniform random sleep over `[0, cadence)`,
which restores the offset distribution a real source change would see.

|      Trial | Source → Silver |
| ---------: | --------------: |
|          1 |          546.9s |
|          2 |           65.9s |
|          3 |          409.8s |
|          4 |          255.0s |
|          5 |          576.2s |
| **Median** |      **409.8s** |

Summary:

```text
Trials  : 5 (all observed, none timed out)
Range   : 65.9–576.2 seconds
Median  : 409.8 seconds
Cadence : 600 seconds (*/10 * * * *)
```

> **Measured local PostgreSQL-to-Silver-Current CDC freshness: median 409.8s
> across 5 trials (range 65.9–576.2s) at a 600s consolidation cadence.**

This is a local verification benchmark, not a production SLA. Latency here is a
scheduling choice, not a pipeline limit: the consolidation job itself completes
in roughly 30 seconds, so a shorter cadence — or event-driven triggering —
moves the number, and the measurement would have to be repeated.

**Why this is not comparable to the `v1.0` figure.** `v1.0` reported a 49.8s
median. That measurement ran consolidation by hand immediately after the source
`UPDATE`, so it timed *processing* and excluded scheduling entirely. It also
predates the discovery that `cdc_consolidation_pipeline` submitted Spark from
inside the Airflow container, which carries no Iceberg jars — consolidation had
never actually run from Airflow, so the deployed cadence was never part of the
number. The two figures answer different questions; the older one is retained in
the evidence manifest under `superseded_claim`.

Evidence:

**[docs/evidence/p1-cdc-consolidation/](docs/evidence/p1-cdc-consolidation/)**

---

# Dead Letter Queue

Malformed CDC events are isolated instead of failing the entire micro-batch.

```text
Kafka Events
     ↓
Validation
   /          \
valid        invalid
  ↓             ↓
Bronze CDC     CDC DLQ
```

## Validation Criteria

1. `__op` exists and is one of `c`, `u`, `d`, `r`
2. `__ts_ms` is numeric / parseable
3. payload is non-null

---

## DLQ Metadata

The Iceberg `cdc_dead_letter` table stores fields such as:

- `source_topic`
- `entity`
- `raw_payload`
- `error_type`
- `error_message`
- `event_timestamp`
- `kafka_partition`
- `kafka_offset`
- `kafka_timestamp`
- `failed_at`
- `spark_batch_id`

---

## Runtime Verification

Test input:

```text
5 valid events
2 invalid events
```

Result:

```text
5 valid   → Bronze CDC ✅
2 invalid → CDC DLQ    ✅
Streaming job completed without crash ✅
```

Verified invalid examples:

```text
__op is null
Unknown __op = x
```

---

# Medallion Data Model

## Bronze

| Type  | Count | Description                 |
| ----- | ----: | --------------------------- |
| Batch |    16 | Source-aligned batch tables |
| CDC   |     6 | Append-only change history  |

## Silver

| Type              | Count | Description                                       |
| ----------------- | ----: | ------------------------------------------------- |
| SCD Type 2        |     2 | `dim_customer`, `dim_account`                     |
| SCD Type 1        |     6 | Branch, product, card, employee, device, location |
| Facts             |     5 | Transactional and interaction facts               |
| CDC Current-State |     2 | `dim_customer_current`, `dim_account_current`     |

## Gold

| Type                   | Count | Description                                        |
| ---------------------- | ----: | -------------------------------------------------- |
| Historical Gold tables |    10 | Spark-managed marts, partitioned by `cob_dt`       |
| Current-serving tables |     9 | dbt-managed, in `serving` schema, served via Trino |

---

# Analytics Outputs

The Gold layer is organized around banking customer analytics.

---

## Customer 360

Customer 360 combines information such as:

- customer profile
- KYC context
- account portfolio
- card portfolio
- deposits and loans
- recent transaction behavior
- customer value indicators
- RFM segmentation
- churn-risk signals
- cross-sell indicators

It acts as a foundation for downstream customer analytics.

---

## RFM Segmentation

Behavioral segmentation based on:

- **Recency**
- **Frequency**
- **Monetary Value**

Representative segments include:

- Champions
- Loyal Customers
- Potential Loyalists
- At Risk
- New Customers
- Hibernating

The implementation uses transparent business rules rather than claiming an ML segmentation model.

---

## Rule-Based Churn-Risk Scoring

The project identifies customers with reduced transaction activity using explainable rules.

The implementation is intentionally described as:

```text
rule-based churn-risk scoring
```

rather than an ML churn prediction model.

---

## Customer Balance / AUM Analytics

Aggregates customer balance and financial-value indicators from the available portfolio datasets.

> The project uses a simplified AUM / customer-value representation and does not claim a complete wealth-management AUM calculation.

---

## Cross-Sell Analytics

Identifies product opportunities using signals such as:

- product ownership
- customer balances
- account portfolio
- transaction behavior

The current implementation is rule-based.

---

## Campaign Analytics

Supports analytics such as:

- target population
- response
- conversion
- revenue
- campaign cost
- ROI-oriented reporting

---

# Serving & Analytics

## Ownership boundary

Spark owns the Bronze, Silver, and historical Gold transformation layers. dbt,
executed through Trino, publishes the current-serving layer as materialized
Iceberg tables for downstream consumers.

```text
Spark
│
├── Bronze
├── Silver
└── Historical Gold (10 tables, partitioned by cob_dt)
          │
          ▼
      dbt via Trino
          │
          ▼
Current Serving Layer (9 materialized Iceberg tables)
          │
          ├── Trino
          ├── Superset
          ├── Streamlit
          └── downstream SQL consumers
```

This boundary matters because objects created by one engine are not
automatically usable by another. Serving objects are created by the engine that
actually serves them.

---

## Trino

Interactive SQL access to Iceberg. Trino is the serving engine: dbt, Superset,
Streamlit and the SQL examples in this README all go through it.

The Trino catalog is named `iceberg` (Trino derives the catalog name from
`docker/init_trino/catalog/iceberg.properties`). Spark addresses the same
warehouse as `lakehouse`. Same data, two engine-local names.

---

## dbt

dbt acts as the **serving publisher**, not the primary transformation engine.
Historical analytical transformations remain in Spark Gold, while dbt
materializes 9 current-serving tables through Trino for a requested `cob_dt`.

```text
9 dbt models  →  iceberg.serving.*
```

Serving models are `materialized: table`, not views: the Trino Iceberg REST
catalog does not support `createView`. Freshness therefore comes from the
orchestration schedule, not from view semantics — the serving DAG rebuilds them
per `cob_dt` alongside Gold.

Each model filters on an explicit `cob_dt` passed as a dbt var rather than
`MAX(cob_dt)`. `MAX` would silently serve yesterday's snapshot whenever today's
pipeline failed; an explicit date makes a missing snapshot a build failure
instead of stale data.

```text
GOLD_COMPLETE(cob_dt)
        ↓
Airflow serving DAG
        ↓
dbt build --select serving --vars cob_dt
        ↓
snapshot + grain tests
        ↓
SERVING_COMPLETE(cob_dt)
```

`dbt build` — not `dbt run` — so model creation and tests are one gate. Tests
assert one row per customer, exactly one `cob_dt` per serving table, and that
the served `cob_dt` equals the requested one.

---

## Apache Superset

Superset provides analytical dashboards over `iceberg.serving.*` through Trino.

---

# Governance & Security

Governance is implemented as a cross-cutting platform capability.

---

## Data Contracts

```text
33 YAML data contracts
```

Contracts define expectations including:

- schema
- field constraints
- ownership
- quality rules
- freshness expectations

---

## Data Quality

```text
8 data-quality check types
```

Examples:

- row-count checks
- null checks
- uniqueness checks
- range checks
- referential integrity
- anomaly checks
- freshness checks
- schema-drift checks

---

## OpenMetadata

Cataloged assets:

```text
53 production data tables
22 lineage edges
```

Capabilities include:

- searchable data catalog
- technical metadata
- lineage
- ownership
- glossary / business metadata

---

## Security Controls

Portfolio security/governance patterns include:

- RBAC
- column masking
- PII classification and controls
- audit trails

These demonstrate governance patterns without claiming full banking regulatory compliance.

---

# Orchestration

Apache Airflow coordinates scheduled and job-oriented workflows.

```text
Apache Airflow
16 DAGs
```

Representative responsibilities:

- Bronze ingestion
- Silver transformations
- Gold transformations
- CDC consolidation
- dbt jobs
- Data Quality
- Iceberg maintenance
- supporting operational workflows

Example batch schedule:

| Time         | Workflow    | Description                           |
| ------------ | ----------- | ------------------------------------- |
| 02:00        | Bronze      | PostgreSQL batch ingestion            |
| 04:00        | Silver      | Clean, conform, SCD / fact processing |
| 06:00        | Gold        | Build analytical marts                |
| 07:00        | dbt         | Gold models and tests                 |
| 08:00        | Ops         | Data Quality / governance workflows   |
| Sunday 03:00 | Maintenance | Iceberg cleanup and optimization      |

## Cross-DAG contract

Downstream DAGs depend on a **data-level completion flag**, not on a producer
DAG name:

```text
Gold producer DAG(s)
        ↓
GOLD_COMPLETE(cob_dt)        ← written only after every Gold job succeeds
        ↓
serving publisher DAG        ← waits for the flag for the SAME cob_dt
        ↓
dbt build --select serving --vars cob_dt
        ↓
serving snapshot + grain tests
        ↓
SERVING_COMPLETE(cob_dt)
```

The serving DAG waits for a `GOLD_COMPLETE` flag for the same `cob_dt`. Only
after `dbt build` and the serving tests succeed is `SERVING_COMPLETE` recorded.
Both failure paths are enforced rather than assumed:

```text
no GOLD_COMPLETE for cob_dt   → serving publisher does not run
dbt serving build fails       → no SERVING_COMPLETE
```

Using a flag rather than a sensor on `gold_mart360_dag` keeps consumers
decoupled from producer topology: if Gold is later split across several DAGs,
only the final producer writes the flag and no consumer changes.

> Airflow orchestrates jobs.  
> Spark Structured Streaming processes the continuous Kafka stream.

---

# Time Semantics

```text
Storage / engine timezone : UTC
Business timezone         : Asia/Ho_Chi_Minh
Processing date           : explicit cob_dt
Business date             : explicitly derived from UTC timestamps
```

Spark is enforced to run with a UTC session timezone. Banking calendar dates are
explicitly derived in `Asia/Ho_Chi_Minh`; they do not rely on an implicit local
Spark session.

```sql
-- Spark
CAST(from_utc_timestamp(txn_date, 'Asia/Ho_Chi_Minh') AS DATE)

-- Trino
CAST(txn_date AT TIME ZONE 'Asia/Ho_Chi_Minh' AS DATE)
```

Timestamps are never bulk-converted to local time — they stay UTC instants.
Only calendar dates and months are derived. `cob_dt` is an orchestration date
and is independent of both.

Because Spark `TIMESTAMP` is an instant and `CAST(... AS DATE)` renders it in the
session timezone, the derivation above is only correct under a UTC session.
That precondition is enforced at runtime by `assert_utc_session()` rather than
assumed, so a misconfigured session fails loudly instead of silently shifting
every daily metric. Details and measurements:
[`docs/evidence/`](docs/evidence/).

---

# Observability

The project includes lightweight CDC freshness observability.

```text
Silver Current / Trino
        ↓
CDC Freshness Exporter
        ↓
Prometheus
        ↓
Grafana
```

---

## CDC Freshness Exporter

A custom Python HTTP exporter queries Trino and exposes Prometheus-compatible metrics.

| Metric                               | Description                                  |
| ------------------------------------ | -------------------------------------------- |
| `cdc_freshness_seconds{table="..."}` | Age of latest consolidated CDC-derived state |
| `cdc_row_count{table="..."}`         | Current-state table row count                |
| `cdc_recent_events`                  | Recent Bronze CDC activity                   |
| `exporter_up`                        | Trino / exporter reachability                |

Prometheus scrape interval:

```text
15 seconds
```

---

## Grafana Dashboard

The CDC dashboard includes eight panels.

|   # | Panel                       |
| --: | --------------------------- |
|   1 | Trino Up / Down             |
|   2 | Customer Current-State Rows |
|   3 | Account Current-State Rows  |
|   4 | Recent CDC Events           |
|   5 | CDC Freshness Over Time     |
|   6 | Customer Freshness Now      |
|   7 | Account Freshness Now       |
|   8 | Row Count Over Time         |

Freshness threshold:

```text
3600 seconds = 1 hour
```

Crossing the threshold indicates **stale CDC-derived data requiring investigation**.

It does not by itself prove the pipeline has failed, because the source may simply have had no recent events.

A stronger production implementation could additionally monitor:

```text
latest Bronze event time
-
latest Silver processed event time
```

plus Kafka consumer lag.

Evidence:

**[docs/evidence/p2-observability/](docs/evidence/p2-observability/)**

---

# CI/CD & Testing

GitHub Actions provides automated validation.

```text
Developer
    ↓
GitHub
    ↓
GitHub Actions
    ↓
Tests / Validation
    ↓
Docker Build / Deployment
```

Workflow categories include:

- CI / lint / unit tests
- integration tests
- performance / benchmark validation

Verified baseline:

```text
312 automated tests
```

covering areas such as:

- ETL
- transformation logic
- Data Quality
- governance
- pipeline behavior

---

# Demo

A 5-minute walkthrough is available at:

**[docs/demo/demo.md](docs/demo/demo.md)**

Typical demo flow:

```text
Architecture
    ↓
Docker Services
    ↓
PostgreSQL UPDATE
    ↓
Kafka CDC Event
    ↓
Bronze CDC
    ↓
Silver Current
    ↓
Silver / Gold Analytics
    ↓
Airflow
    ↓
OpenMetadata
    ↓
Superset / Analytics
```

Runtime evidence:

**[docs/evidence/](docs/evidence/)**

---

# Quick Query Examples

## Customer 360

```bash
docker exec banking-trino trino \
  --catalog lakehouse \
  --execute "
    SELECT COUNT(*)
    FROM gold.mart_customer_360
  "
```

---

## RFM Distribution

```bash
docker exec banking-trino trino \
  --catalog lakehouse \
  --execute "
    SELECT
        rfm_segment,
        COUNT(*) AS customer_count
    FROM gold.rfm_segment
    GROUP BY rfm_segment
    ORDER BY customer_count DESC
  "
```

---

## Silver Current Customer

```bash
docker exec banking-trino trino \
  --catalog lakehouse \
  --execute "
    SELECT
        customer_id,
        email,
        __cdc_operation,
        __cdc_timestamp_ms
    FROM silver.dim_customer_current
    WHERE customer_id = 1001
  "
```

---

# How to Run

## Prerequisites

Recommended local environment:

- Docker Desktop
- 16 GB RAM or more
- Git
- Python 3.11+

Because the platform runs multiple services locally, sufficient Docker memory allocation is recommended.

---

## Quick Start

```bash
# Clone repository
git clone https://github.com/minzi03/banking_data_platform.git
cd banking_data_platform

# Configure environment
cp docker/.env.example docker/.env

# Start infrastructure
cd docker
docker compose up -d
```

Generate seed data if required:

```bash
python data_generator/generate_all.py \
  --host localhost \
  --port 5432
```

Then open Airflow and run the required workflows according to their dependencies.

---

# Service Endpoints

| Service                  | URL / Address                 | Role                             |
| ------------------------ | ----------------------------- | -------------------------------- |
| Airflow                  | http://localhost:8080         | Workflow orchestration           |
| MinIO Console            | http://localhost:9001         | Object storage UI                |
| Spark Master UI          | http://localhost:9090         | Spark cluster UI                 |
| Spark Worker UI          | http://localhost:9091         | Spark worker UI                  |
| Kafka UI                 | http://localhost:8081         | Kafka inspection                 |
| Kafka Connect / Debezium | http://localhost:8083         | CDC connector API                |
| Trino                    | http://localhost:8085         | SQL query engine                 |
| Iceberg REST Catalog     | http://localhost:8181         | Iceberg catalog                  |
| OpenMetadata             | http://localhost:8585         | Catalog / lineage                |
| Apache Superset          | http://localhost:8088         | BI dashboards                    |
| Streamlit                | http://localhost:8501         | Additional analytics application |
| Prometheus               | http://localhost:9095         | Metrics                          |
| Grafana                  | http://localhost:3000         | Monitoring dashboard             |
| Freshness Exporter       | http://localhost:9119/metrics | Prometheus metrics               |
| PostgreSQL               | localhost:5432                | Source database                  |
| Kafka                    | localhost:9092                | Event streaming                  |

Local development credentials are configured through:

```text
docker/.env
```

Development defaults should not be reused in a real production environment.

---

# Current Limitations

This repository demonstrates production-oriented data engineering patterns but intentionally remains a local portfolio environment.

Current boundaries include:

- Spark runs as a small local cluster rather than a large distributed deployment.
- Kafka is not deployed as a production HA multi-broker cluster.
- Airflow uses a local production-like configuration rather than distributed HA.
- CDC consolidation currently targets only **customer** and **account**.
- Other CDC datasets remain append-only Bronze history.
- Gold analytics remain batch-derived.
- Churn-risk logic is rule-based rather than an ML prediction model.
- Customer Balance / AUM analytics use a simplified portfolio representation.
- Observability is intentionally lightweight.
- The freshness metric primarily measures data age rather than complete source-to-target processing lag.
- CDC current-state freshness is bounded by the consolidation cron (`*/10`), not by processing time; there is no event-driven trigger from streaming ingestion to consolidation.
- The platform does not claim end-to-end exactly-once semantics.
- Enterprise mTLS, KMS, Kubernetes, multi-region DR, and production on-call/SLO systems are outside the current portfolio scope.

---

# Portfolio Baseline

Feature-frozen release:

**[`portfolio-v1.0`](https://github.com/minzi03/banking_data_platform/releases/tag/portfolio-v1.0)**

```text
P0 ✅ Portfolio Polish
P1 ✅ CDC Consolidation
P2 ✅ Basic Observability
P3 ✅ DLQ / Error Handling
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FEATURE FREEZE
```

---

## P0 — Portfolio Polish ✅

- architecture documentation
- architecture image
- README
- 5-minute demo
- interview talking points
- runtime evidence

---

## P1 — CDC Consolidation ✅

- Bronze CDC → Silver Current for customer and account
- generic YAML-configured consolidation engine
- persisted watermarks (timestamp + Spark batch id)
- business-key deduplication
- INSERT / UPDATE / DELETE handling
- restart-safe processing
- idempotent Iceberg MERGE
- measured local source-to-Silver freshness: median 409.8s at a 600s cadence

---

## P2 — Basic Observability ✅

- custom CDC freshness exporter
- Prometheus
- Grafana
- Silver Current freshness metrics
- row-count metrics
- freshness visualization and thresholds
- runtime evidence

---

## P3 — DLQ / Error Handling ✅

- CDC event validation
- Iceberg dead-letter table
- malformed-event isolation
- valid-event continuation
- runtime verification

---

# Technology Stack

| Component            | Version / Distribution | Role                         |
| -------------------- | ---------------------- | ---------------------------- |
| PostgreSQL           | 15                     | Operational source database  |
| Apache Spark         | 3.5.3                  | Batch + streaming processing |
| Apache Iceberg       | 1.6.0                  | Lakehouse table format       |
| MinIO                | Repository image       | S3-compatible object storage |
| Iceberg REST Catalog | 1.6.x                  | Shared Spark / Trino catalog |
| Apache Airflow       | 2.10.0                 | Workflow orchestration       |
| Debezium             | 2.6                    | Change Data Capture          |
| Confluent Platform   | 7.6                    | Kafka distribution           |
| Apache Kafka Broker  | 3.6.x                  | Event streaming              |
| Trino                | 443                    | Interactive SQL query engine |
| dbt Core             | Repository-pinned      | Gold models and tests        |
| OpenMetadata         | 1.5.6                  | Catalog, lineage, governance |
| Apache Superset      | Repository-pinned      | Analytics dashboards         |
| Prometheus           | Repository-pinned      | Metrics collection           |
| Grafana              | Repository-pinned      | Metrics visualization        |
| Docker Compose       | —                      | Local platform deployment    |

> `Confluent Platform 7.6` is the distribution version and should not be described as `Apache Kafka 7.6`.

---

# Documentation

| Document                                                                   | Purpose                            |
| -------------------------------------------------------------------------- | ---------------------------------- |
| [Architecture](docs/architecture/architecture.md)                          | Detailed technical architecture    |
| [Demo](docs/demo/demo.md)                                                  | 5-minute project walkthrough       |
| [Interview Talking Points](docs/interview/talking-points.md)               | Architecture and design discussion |
| [P1 Interview Prep](docs/interview/p1-interview-prep.md)                   | CDC deep-dive questions            |
| [Evidence](docs/evidence/)                                                 | Runtime screenshots                |
| [P1 CDC Evidence](docs/evidence/p1-cdc-consolidation/)                     | CDC consolidation verification     |
| [P2 Observability Evidence](docs/evidence/p2-observability/)               | Observability verification         |

---

# Architecture Principles

### PostgreSQL remains the source of truth

```text
PostgreSQL
= operational source of truth

Silver SCD2
= derived historical representation

Silver Current
= derived latest-state representation
```

---

### Bronze CDC remains append-only

```text
Bronze CDC
= audit + replay boundary
```

---

### Historical and current-state semantics are separated

```text
Silver SCD2
= What was true then?

Silver Current
= What is true now?
```

---

### Spark owns Medallion transformations

```text
Spark
Bronze → Silver → Gold
```

dbt operates downstream on curated Gold analytical models.

---

### Watermarks advance after successful target processing

```text
Read
→ Deduplicate
→ MERGE
→ Success
→ Update Watermark
```

---

### Replay-safe processing over unsupported exactly-once claims

The platform uses:

- Spark checkpoints
- persisted watermarks
- deduplication
- idempotent Iceberg MERGE

It does not claim one globally coordinated exactly-once transaction across PostgreSQL, Debezium, Kafka, Spark, Bronze, Silver, and metadata state.

---

### Control planes stay separate from the data path

Airflow, OpenMetadata, governance, CI/CD, and observability coordinate or inspect the platform.

They do not replace the physical processing flow.

---

# Interview-Ready Summary

> I built a production-like banking data platform with separate scheduled batch and near-real-time CDC paths. The batch pipeline uses Spark, Iceberg, and MinIO to build SCD1/SCD2 dimensions, fact tables, and Gold analytical marts. In parallel, Debezium captures PostgreSQL WAL changes into Kafka, Spark Structured Streaming validates and persists append-only Bronze CDC events, and a config-driven consolidation engine derives customer and account current-state tables using timestamp+batch-id watermarks, deduplication, and idempotent Iceberg MERGE. The platform also includes Airflow orchestration, Trino/dbt/Superset analytical serving, OpenMetadata governance, Data Quality, DLQ-based error isolation, CI/CD, and lightweight CDC freshness observability.

---

# Release

Portfolio release:

**[`portfolio-v1.0`](https://github.com/minzi03/banking_data_platform/releases/tag/portfolio-v1.0)**

Repository:

**https://github.com/minzi03/banking_data_platform**

---

# License

MIT
