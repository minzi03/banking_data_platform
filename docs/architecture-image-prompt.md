# Banking Data Platform — Architecture Image Prompt

> File này chứa prompt chi tiết để gen hình ảnh kiến trúc dự án bằng AI image generator
> (DALL-E, Midjourney, GPT-4o, Claude, etc.)
> Cập nhật: 2026-09-08 — phản ánh trạng thái hiện tại của dự án

---

## Phiên Bản Tổng Quan (Short Prompt)

```
Professional enterprise data architecture diagram: Banking Data Platform medallion lakehouse.
Dark navy background. Horizontal left-to-right flow:

[PostgreSQL 15] → [Spark JDBC + Debezium/Kafka CDC] → [Bronze/Silver/Gold Iceberg on MinIO] → [dbt + Trino serving] → [Superset BI, FastAPI, MLflow]

Top banner: Airflow orchestration (16 DAGs), OpenMetadata catalog (53 tables), GitHub Actions CI/CD (472 tests).
Bottom strip: Prometheus + Grafana monitoring, CDC Freshness Exporter, DLQ.

Tech icons: PostgreSQL, Spark 3.5, Iceberg 1.6, Kafka, Debezium 2.6, Airflow 2.10, Trino 443, dbt, Superset, OpenMetadata, MLflow, Docker.

Style: Flat design, enterprise-grade, clean lines, dark theme.
Title: "Banking Data Platform — End-to-End Batch & Near-Real-Time Lakehouse Architecture"
```

---

## Prompt Chi Tiết (Full Prompt)

### Section 1: Overall Layout

```
Layout: 5 horizontal planes flowing left to right, with a governance banner spanning across the top.

Color scheme:
- Sources: Steel Blue (#4A90D9)
- Batch path: Light Blue (#5C6BC0)
- CDC path: Teal (#00897B)
- Lakehouse: Green gradient — Bronze(#8D6E63), Silver(#90A4AE), Gold(#FFB300)
- Serving: Orange (#FF9800)
- Governance: Crimson (#C62828)
- Background: Dark Navy (#1A1A2E)
- Text: White, Arrows: Light Gray

Title at top: "Banking Data Platform — Medallion Lakehouse Architecture"
Subtitle: "PostgreSQL → Bronze → Silver → Gold → Serving"
```

### Section 2: Plane 1 — Sources (Far Left)

```
PLANE 1 — OPERATIONAL SOURCES (far left, Steel Blue section):

PostgreSQL 15 icon (large database icon) with label:
"PostgreSQL 15 — Operational Source of Truth"

Below it, 4 grouped sub-boxes with small database icons:

Box 1: "Core Banking" (bold)
  - Customer (10,000 rows)
  - Account (30,000 rows)
  - Transaction (1,200,000 rows)
  - Loan (5,000 rows)
  - Deposit, Branch, Product, Employee

Box 2: "Card & CRM"
  - Card (6,000 rows)
  - Card Transaction (600,000 rows)
  - CRM Interaction (50,000 rows)

Box 3: "Digital Banking"
  - Online Transaction (500,000 rows)
  - Device (50,000 rows)
  - Support Ticket, Location, Merchant, MCC Code

Box 4: "Ops Metadata"
  - ETL Flags, Audit Log, Lineage Log, Data Quality Log
```

### Section 3: Plane 2 — Data Movement

```
PLANE 2 — DATA MOVEMENT (two parallel paths, arrows from Plane 1):

TOP PATH — Batch Processing (Light Blue):
  Arrow → "Spark JDBC" → Apache Spark icon labeled "Spark 3.5.3"
  Subtitle: "Scheduled Batch Ingestion, YAML-Driven ETL"
  Arrow → Bronze Batch (16 tables)

BOTTOM PATH — CDC Streaming (Teal):
  Arrow → PostgreSQL WAL icon
  Arrow → Debezium icon labeled "Debezium 2.6"
  Arrow → Kafka icon labeled "Apache Kafka — 12 CDC Topics"
  Arrow → Spark icon labeled "Spark Structured Streaming"

  Branch DOWN from streaming:
    → "CDC Validation" checkpoint icon
    → Valid path → Bronze CDC (6 tables)
    → Invalid path → Red "DLQ" box labeled "Dead Letter Queue"
      Subtitle: "Malformed events isolated, raw payload preserved"

  After Bronze CDC:
    → Arrow → "CDC Consolidation Engine" box
      Subtitle: "YAML Config, Dedup, Watermarks, Idempotent MERGE"
    → Arrow → Silver Current-State (2 tables)
```

### Section 4: Plane 3 — Lakehouse (Center)

```
PLANE 3 — LAKEHOUSE (center, large section with 3 stacked layers):

TOP — BRONZE LAYER (Brown/Bronze color #8D6E63):
  Label: "Bronze — Raw / Append-Only"
  Subtitle: "16 Batch Tables + 6 CDC Tables = 22 Tables"
  Iceberg icon + MinIO icon labeled "S3-Compatible Object Storage"
  Left side: "Full snapshots per cob_dt"
  Right side: "CDC append-only change history"
  Annotation: "Audit + Replay Boundary"

MIDDLE — SILVER LAYER (Silver/Gray color #90A4AE):
  Label: "Silver — Cleansed / Analytical"
  Subtitle: "Historical Analytical Model"
  Left column "SCD Dimensions":
    - "SCD2: dim_customer, dim_account (history preserved)"
    - "SCD1: branch, product, card, employee, device, location"
  Center: "5 Fact Tables: txn_account, card_txn, online_transaction, crm_interaction, support_ticket"
  Far right: "CDC Current-State: dim_customer_current 10K, dim_account_current 30K"
  Annotation top: "What was true then? (SCD2)"
  Annotation bottom: "What is true now? (CDC Current)"

BOTTOM — GOLD LAYER (Gold #FFB300):
  Label: "Gold — Analytics / Business Marts"
  10 tables listed:
    "Customer 360 | RFM Segmentation | Churn Risk | Cross-Sell | Campaign Target"
    "Balance Summary | Transaction Summary | Product Summary | Card Summary | Branch Monthly"
  Annotation: "2,300,000 distinct curated transactions"
```

### Section 5: Plane 4 — Serving

```
SERVING (Orange #FF9800):
  dbt icon → "dbt — 9 Current-Serving Tables"
  Subtitle: "Materialized via Trino, 1 row per customer, per cob_dt"
  Arrow to Trino icon → "Trino — SQL Query Engine (port 8085)"

  Three output boxes:
  1. Superset icon → "Apache Superset — BI Dashboards (port 8088)"
  2. FastAPI icon → "REST API — Customer 360 (port 8000)"
  3. MLflow icon → "MLflow — ML Model Registry (port 5000)"

  Small box: "Streamlit — Interactive Dashboard (port 8501)"
```

### Section 6: Governance & Observability

```
TOP BANNER — GOVERNANCE (Crimson #C62828, spans full width):

Left: Airflow icon → "Apache Airflow — 16 DAGs"
  Subtitle: "Bronze → Silver → Gold → dbt orchestration"

Center-left: Shield icon → "Governance"
  Subtitle: "RBAC • Column Masking • PII Controls • Audit Trails"

Center: Contract icon → "Data Contracts — 33 YAML"
  Subtitle: "Schema enforcement • Quality rules • Ownership"

Center-right: OpenMetadata icon → "OpenMetadata"
  Subtitle: "53 Tables Cataloged • 22 Lineage Edges"

Right: CI/CD badge → "GitHub Actions"
  Subtitle: "472 Tests • 34 Trino Integration • Security Scan"

BOTTOM STRIP — OBSERVABILITY (Dark Teal #004D40):

Left: Prometheus icon → "Prometheus — 15s Scrape"
Center: Grafana icon → "Grafana Dashboard"
Right: Exporter icon → "CDC Freshness Exporter"
  Subtitle: "cdc_freshness_seconds • row_count • exporter_up"

Far right red box: "Failure Isolation (DLQ)"
  Subtitle: "Invalid events routed to DLQ, raw payload preserved"
```

### Section 7: Data Flow Arrows

```
DATA FLOW ARROWS:

Solid arrows (data movement):
  PostgreSQL ──→ Spark JDBC ──→ Bronze Batch (16 tables)
  PostgreSQL ──→ Debezium ──→ Kafka (12 topics) ──→ Spark Streaming ──→ Bronze CDC (6 tables)
  Bronze Batch ──→ Silver (SCD2 dims + SCD1 dims + 5 facts)
  Bronze CDC ──→ CDC Consolidation ──→ Silver Current (2 tables)
  Silver ──→ Gold (10 analytics marts)
  Gold ──→ dbt ──→ Serving (9 current-serving tables via Trino)
  Serving ──→ Trino ──→ Superset / REST API / MLflow

Dashed arrows (governance):
  Airflow ──→ all layers (orchestration)
  Data Contracts ──→ Bronze, Silver, Gold (validation)
  OpenMetadata ──→ all tables (catalog + lineage)
  GitHub Actions ──→ full pipeline (CI/CD gate)
```

### Section 8: Bottom Stats Bar

```
BOTTOM STATS BAR (dark gray, white text, evenly spaced):

"20 Source Tables" | "22 Bronze (16 Batch + 6 CDC)" | "15 Silver (8 SCD + 5 Facts + 2 Current)" | "10 Gold Marts" | "9 dbt Serving Tables" | "472 Automated Tests" | "34 Trino Integration Tests" | "24 Docker Containers"
```

### Section 9: Key Annotations (Callout Boxes)

```
CALLOUT BOXES (floating annotations with arrows):

1. Near Bronze CDC:
   "Audit + Replay Boundary — append-only, immutable change history"

2. Near Silver SCD2:
   "What was true then? — SCD2 preserves historical versions"

3. Near Silver Current:
   "What is true now? — CDC-derived latest consolidated state"

4. Near Gold:
   "2,300,000 distinct curated transactions (account + card + online)"

5. Near Serving:
   "1 row per customer — dbt materialized via Trino"

6. Near CDC Freshness:
   "Measured: median 409.8s at 600s cadence"
```

### Section 10: Legend

```
LEGEND (bottom-left corner, small):

  ━━━  Data Flow (solid arrow)
  ┄┄┄  Orchestration / Governance (dashed arrow)

  Color key:
  ■ Blue     = Sources (PostgreSQL)
  ■ Teal     = CDC Streaming (Debezium/Kafka)
  ■ Brown    = Bronze Layer (Raw/Append-Only)
  ■ Silver   = Silver Layer (Cleansed/SCD)
  ■ Gold     = Gold Layer (Analytics Marts)
  ■ Orange   = Serving Layer (dbt/Trino)
  ■ Crimson  = Governance (top banner)
  ■ Dark Teal= Observability (bottom strip)
```

---

## Verified Metrics Reference

```
METRIC                          VALUE           DEFINITION
─────────────────────────────────────────────────────────────────────
Source workloads                16              Executable Bronze ingestion configs
Bronze batch tables             16              One per ingestion workload
Bronze CDC tables               6               Append-only change-history tables
Silver SCD Type 2 dims          2               dim_customer, dim_account
Silver SCD Type 1 dims          6               Branch, product, card, employee, device, location
Silver fact tables              5               Transactional and interaction facts
Silver CDC current-state        2               dim_customer_current, dim_account_current
Historical Gold tables          10              Spark-managed Gold, partitioned by cob_dt
Current-serving tables          9               dbt-managed Iceberg in serving schema
Curated transactions            2,300,000       Distinct domain-qualified transactions
Debezium connectors             3               Runtime connector definitions
Kafka CDC topics                12              One per captured source table
Data contracts                  33              Governance contract YAMLs
DQ check types                  8               Supported DQ rule categories
Airflow DAG files               16              DAG definition files
Automated tests                 472             Python test functions
Trino integration tests         34              PR-blocking gate tests
Docker Compose services         24              20 long-running + 4 init jobs
CDC freshness (median)          409.8s          Source to Silver Current via Trino
```

---

## Technology Stack

```
COMPONENT                   VERSION         ROLE
─────────────────────────────────────────────────────────────────────
PostgreSQL                  15              Operational source database
Apache Spark                3.5.3           Batch + streaming processing
Apache Iceberg              1.6.0           Lakehouse table format
MinIO                       (latest)        S3-compatible object storage
Iceberg REST Catalog        1.6.x           Shared Spark/Trino catalog
Apache Airflow              2.10.0          Workflow orchestration
Debezium                    2.6             Change Data Capture
Apache Kafka                3.6.x           Event streaming (Confluent 7.6)
Trino                       443             Interactive SQL query engine
dbt Core                    (pinned)        Serving layer materialization
OpenMetadata                1.5.6           Catalog, lineage, governance
Prometheus                  (latest)        Metrics collection
Grafana                     (latest)        Metrics visualization
MLflow                      2.16.0          ML model registry
FastAPI                     (latest)        REST API serving
Streamlit                   (latest)        Interactive dashboard
Superset                    (latest)        BI dashboards
Docker Compose              —               Local platform deployment
```

---

## Key Callout Texts for Image

Use these as prominent text labels or speech bubbles in the diagram:

```
1. "Two ingestion paths — Batch (scheduled) + CDC (near-real-time)"
2. "Medallion Architecture — Bronze → Silver → Gold"
3. "Spark owns history. dbt owns current serving."
4. "SCD2 = What was true then? | CDC Current = What is true now?"
5. "472 automated tests + 34 Trino integration tests"
6. "Production-like governance: RBAC, masking, audit, 33 data contracts"
7. "CDC Freshness: median 409.8s at 600s cadence"
8. "2,300,000 distinct curated financial transactions"
```
