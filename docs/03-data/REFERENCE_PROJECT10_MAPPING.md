# Reference Mapping — `project_thamkhao10/bank360-data`

> **Status:** reference-only; no code, CSV/SQLite data, Docker stack or screenshots
> from this repository are imported into the canonical production path.
> **Reviewed:** 2026-10-19

## 1. Reference project scope

`bank360-data` is a compact French-documented Bank360 portfolio project:

```text
PostgreSQL → Spark JDBC → Iceberg/MinIO Bronze
  → Spark Silver cleansing → Great Expectations gate
  → dbt Star Schema/Gold → Streamlit

Kafka/Debezium → Spark Structured Streaming → Iceberg streaming table
Airflow orchestrates; Prometheus/Grafana/Alertmanager monitor.
```

The reference README describes roughly 15 source tables and 150K+ rows, three
facts, a small set of dimensions/KPIs and seven Streamlit pages. Those counts,
screenshots and runtime claims belong to the reference project and are not
metrics of this platform.

## 2. Business/domain mapping

| Project10 domain | Current platform mapping | Decision |
|---|---|---|
| `clients` | `core_banking.customer` | Already covered with Vietnamese PII/KYC/segment contracts. |
| `comptes` | `core_banking.account` | Already covered; current generator is larger and FK-aware. |
| `transactions` | `core_banking.txn_account`, `digital_banking.online_transaction` | Split by business channel/grain; do not flatten into one table. |
| `paiements` / `virements` | account/card/digital facts and standing orders | Covered by richer fact domains; map by business meaning, not name. |
| `mobile_banking` | `digital_banking.device`, `online_transaction`, support data | Already covered with device/channel metadata. |
| `operations_atm` | transaction channel/ATM patterns | Use as a reference scenario; no duplicate source table without a contract. |
| `cartes` | `card_crm.card`, `card_crm.card_txn` | Already covered with card lifecycle and transaction grain. |
| `beneficiaires` | counterparty/customer/transaction fields | Reference concept; add only if a current use case requires a beneficiary contract. |
| `alertes_fraude` | AML/fraud Gold marts and alert sources | Current platform has richer AML/fraud rule domains; reference KPI ideas only. |
| `credits` | `core_banking.loan`, `loan_payment`, delinquency/risk marts | Current platform is richer in loan lifecycle/payment analytics. |
| `agences` / `employes` | `core_banking.branch` / `employee` | Already covered with Vietnamese regional semantics. |
| `devises` / `taux_change` | currency fields and reference/config patterns | Currency is present; FX should be added only with a defined reporting use case. |
| MCC | `digital_banking.mcc_code`, merchant/MCC Gold | Current Merchant QR/POS path is broader. |

## 3. Capability comparison

| Capability | Project10 reference | Current Banking Data Platform |
|---|---|---|
| Medallion batch | Implemented | Implemented, larger and YAML metadata-driven |
| Iceberg/MinIO | Implemented | Implemented with REST catalog, Trino and maintenance DAG |
| dbt star schema | Implemented | Implemented with more serving models and contracts |
| Kafka/Debezium/Spark streaming | Implemented | Implemented with event-time dedup, DLQ and CDC current-state consolidation |
| SCD history | Limited/reference | 5 SCD2 + 6 SCD1 dimensions |
| Merchant QR/POS | Not the central domain | Merchant Gold, real-time serving and advisory marts |
| Governance | Great Expectations examples | DQ, data contracts, RBAC, masking, lineage and evidence manifest |
| Metadata/catalogue | Limited | OpenMetadata tables/lineage/glossary |
| CI/integration | Basic project CI | Ruff, governance, security and 34 Trino integration gate tests |
| Serving | Streamlit over Gold/Spark Thrift | Trino/dbt plus PostgreSQL serving upserts |
| EOD Merchant reconciliation | Not demonstrated | Implementation/verifier exists; synchronized three-way runtime PASS is TD-22 |

The reference architecture confirms the market-relevant backbone, but it does
not justify replacing the current generator, schemas or Iceberg catalog.

## 4. Patterns worth reusing

### Quality gate before publication

Project10's intended flow is:

```text
Silver → Great Expectations → PASS → dbt Gold
                         └→ FAIL → quarantine/keep previous Gold
```

The current platform already has DQ, contract validation, quarantine and fail-loud
Gold validation. The reusable improvement is documentation and evidence that the
quality gate blocks publication, not importing Great Expectations wholesale.

### Clear star-schema grain

Project10 separates dimensions, facts and KPI marts. Keep that discipline in the
current richer model: every fact/mart must state its grain, business date, keys,
status/currency rules and source. The existing Gold fan-out regression tests are
stronger than the reference's static model alone.

### Persona-based consumers

Project10's dashboard pages (direction, finance, risk/fraud, customers,
transactions and cards) suggest consumer groupings. The current platform can tell
the same story through its API/Streamlit/Superset and Merchant Grafana surfaces
without copying the reference UI.

### Checkpoint, retry, DLQ and observability

Project10 documents source ingestion checkpoint validation, Spark checkpoints,
retry, rejected-record DLQ, Airflow retries, Prometheus/Grafana and Alertmanager.
The current platform already has stronger CDC checkpoint/dedup/DLQ and governance;
use the reference as a checklist for runbook/metric naming, not as proof of current
runtime behavior.

## 5. Explicit non-reuse boundaries

- Do not import `bank360-data` source code into `data_generator/`, `code_etl/`,
  `airflow/`, or production Docker Compose.
- Do not import its PostgreSQL data, SQLite database, CSVs, screenshots or expected
  row counts into the current seed path or evidence manifest.
- Do not copy Nessie/Snowflake assumptions into the current Iceberg REST/Trino
  architecture without a new ADR.
- Do not repeat the reference project's exactly-once wording as a global guarantee;
  current claims remain bounded by checkpoint, watermark/dedup and idempotent sink
  semantics.
- Treat its `Great Expectations 51/53` and dashboard screenshots as reference
  evidence only, not current-platform evidence.

## 6. Recommended application to this project

1. Keep `data_generator/` as the sole canonical seed implementation; its 20-table,
   four-schema input base is larger and aligned to Vietnamese banking/ICT semantics.
2. Use this mapping as a review checklist for source domains and consumer personas.
3. Reuse only compatible patterns: quality gates, explicit grains, checkpoint/retry
   provenance, DLQ taxonomy, dashboard personas and monitoring alerts.
4. Keep the unresolved Merchant EOD three-way runtime proof in TD-22; do not use the
   reference project's claims to close it.
5. Add new tables only for a declared business need and a complete DDL → generator →
   Bronze → Silver/Gold contract. Similar names in a reference repo are not a reason
   to create parallel tables.
