# Runbook — Banking Data Platform

## 🚀 Quick Reference

### Start All Services
```bash
cd banking_data_platform/docker
docker compose up -d
```

### Stop All Services
```bash
docker compose down
```

### Check Status
```bash
docker compose ps
```

### View Logs
```bash
docker compose logs -f [service_name]
```

---

## 🔧 Common Operations

### 1. Start Infrastructure
```bash
cd banking_data_platform/docker
docker compose up -d
# Wait 2 minutes for services to become healthy
docker compose ps  # Verify all services are healthy
```

### 2. Generate Seed Data
```bash
# From host (recommended)
cd banking_data_platform
python data_generator/generate_all.py --host localhost --port 5432
```

### 3. Run ETL Pipeline

**Via Airflow UI (http://localhost:8080):**
1. Login: admin / admin123
2. Enable DAGs
3. Trigger manually

**Via CLI:**
```bash
docker compose exec airflow-scheduler airflow dags trigger bronze_core_banking_dag
docker compose exec airflow-scheduler airflow dags trigger bronze_card_crm_dag
docker compose exec airflow-scheduler airflow dags trigger bronze_digital_banking_dag
docker compose exec airflow-scheduler airflow dags trigger silver_all_dag
docker compose exec airflow-scheduler airflow dags trigger gold_all_dag
docker compose exec airflow-scheduler airflow dags trigger ops_data_quality_dag
```

### 4. Query Data

**Via Trino (port 8085):**
```bash
docker compose exec trino trino --catalog lakehouse

# History snapshot table
SELECT COUNT(*) FROM lakehouse.gold.mart_customer_360;

# Current serving table (1 row/customer)
SELECT COUNT(*) FROM lakehouse.gold.mart_customer_360_current;
SELECT customer_segment, COUNT(*) FROM lakehouse.gold.mart_customer_360_current GROUP BY 1;

# Other current-serving customer-grain Gold tables
SELECT COUNT(*) FROM lakehouse.gold.customer_balance_summary_current;
SELECT COUNT(*) FROM lakehouse.gold.customer_transaction_summary_current;
SELECT COUNT(*) FROM lakehouse.gold.customer_product_summary_current;
SELECT COUNT(*) FROM lakehouse.gold.customer_card_summary_current;
SELECT COUNT(*) FROM lakehouse.gold.rfm_segment_current;
SELECT COUNT(*) FROM lakehouse.gold.churn_prediction_current;
SELECT COUNT(*) FROM lakehouse.gold.cross_sell_segment_current;
SELECT COUNT(*) FROM lakehouse.gold.campaign_target_current;
```


### 5. Check Data Quality

The DQ log lives in **PostgreSQL** (`banking_db`, schema `opslakehouse`), not in
the lakehouse — so query it with `psql`, not Trino. Trino has no `lakehouse`
catalog either: Spark calls the Iceberg catalog `lakehouse`, Trino calls it
`iceberg` (ADR-0002).

```bash
docker exec banking-postgres sh -c 'psql -U "$POSTGRES_USER" -d banking_db -c "SELECT cob_dt, table_name, check_name, check_status, details FROM opslakehouse.data_quality_log ORDER BY checked_at DESC LIMIT 20"'
```

Only what did not pass:

```bash
docker exec banking-postgres sh -c 'psql -U "$POSTGRES_USER" -d banking_db -c "SELECT cob_dt, table_name, check_name, check_status, details FROM opslakehouse.data_quality_log WHERE check_status <> '"'"'PASS'"'"' ORDER BY checked_at DESC LIMIT 20"'
```

### 5b. Contract validation

Checks one day's snapshot of every contract in a layer against its
`quality_rules` (`code_etl/shared/ops/contract_validation.py`, run daily by
`ops_contract_validation_dag`). Exit 1 means at least one contract failed:

```bash
docker exec banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/project/code_etl/shared/ops/contract_validation.py --cob_dt 2026-09-22 --layer silver
```

Results land in PostgreSQL, not the lakehouse — query them with `psql`:

```bash
docker exec banking-postgres sh -c 'psql -U "$POSTGRES_USER" -d banking_db -c "SELECT dataset_id, check_name, check_status, details FROM opslakehouse.contract_validation_log WHERE check_status <> '"'"'PASS'"'"' ORDER BY checked_at DESC LIMIT 20"'
```

**Stacks created before 2026-09-23 need two one-time steps.** The log table used
to be defined only in `docker/init_openmetadata/` (never run, removed 2026-09-24), and the
worker had no database credentials:

```bash
sed -n '/^-- Table: contract_validation_log/,/^COMMENT ON TABLE opslakehouse.contract_validation_log/p' docker/init_postgres/00_extensions.sql | docker exec -i banking-postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d banking_db'
```

```bash
cd docker && docker compose up -d spark-worker-1
```

The first is idempotent (`CREATE ... IF NOT EXISTS`). The second recreates the
worker so it picks up `POSTGRES_USER`/`POSTGRES_PASSWORD`; without them the job
stops with an explicit error instead of falling back to a password in the code.

### 6. Check Lineage

`opslakehouse.lineage_log` holds the table-level edges that Silver/Gold jobs
declare in their YAML (`source.tables` → `target`), written by `ops_lineage_dag`.
**Nothing schedules that DAG** — the table is empty until you run it:

```bash
docker exec banking-airflow-webserver airflow tasks test ops_lineage_dag emit_lineage 2026-09-22
```

Expect 75 rows per run: 51 into Gold, 24 into Silver. `row_count` is NULL — the
DAG does not measure it. Source → Bronze, CDC and dbt edges are not included.
See TD-13 in `docs/05-quality/technical-debt.md`.

```bash
docker exec banking-postgres sh -c 'psql -U "$POSTGRES_USER" -d banking_db -c "SELECT source_table, target_table, transform_type, dag_id, created_at FROM opslakehouse.lineage_log ORDER BY created_at DESC LIMIT 20"'
```

For lineage you can trust today, read the generated
[`docs/03-data/LINEAGE.md`](docs/03-data/LINEAGE.md) — built from the data
contracts and checked by tests — or OpenMetadata.

**Stacks created before 2026-09-24** need the table once. Idempotent:

```bash
sed -n '/^-- Table: lineage_log/,/^COMMENT ON TABLE opslakehouse.lineage_log/p' docker/init_postgres/00_extensions.sql | docker exec -i banking-postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d banking_db'
```

### 7. dbt semantic layer (dbt-core + dbt-trino)

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

### 8. PII masking salt

`ops_pii_masking_daily_dag` hashes `cccd` as `sha2(cccd || salt)` when it builds
`sandbox.dim_customer_masked` and `sandbox.mart_customer_360_masked`. **There is no
default salt.** The DAG reads the Airflow Variable `pii_hash_salt` at task render
time and fails the masking task if it is unset or empty — a committed fallback salt
is a salt every reader of this repo knows, and a deterministic hash under a known
salt is pseudonymisation in name only (CCCD is 12 digits, so it is enumerable).

Set it once per environment, before the first masking run:

```bash
# Any high-entropy string, >= 32 chars
docker compose exec airflow-scheduler \
  airflow variables set pii_hash_salt "$(openssl rand -hex 32)"

# Confirm it exists (this prints the value — treat the output as a secret)
docker compose exec airflow-scheduler airflow variables get pii_hash_salt
```

Or via the UI: **Admin → Variables → +**, key `pii_hash_salt`.

**If it is missing**, `mask_silver_dim_customer` and `mask_gold_mart_customer_360`
fail with a message naming the Variable and the command above. The DAG still
imports and stays visible in the UI — the failure is scoped to the task, not to
DAG parsing, so a missing salt never hides the DAG.

**Rotating the salt invalidates every hash already published.** `cccd_hash` is
deterministic: a new salt produces a different hash for the same person. After
changing the Variable you must

1. rebuild both masked tables — `airflow dags trigger ops_pii_masking_daily_dag`
   (the job uses `CREATE OR REPLACE TABLE`, so one run replaces them in full), and
2. re-key or drop anything downstream that joins on `cccd_hash`. Hashes from
   before and after the rotation never match, and such a join returns zero rows
   silently instead of failing.

There is no re-hashing path for consumers holding old `cccd_hash` values, so treat
rotation as a migration, not a config change.

> Airflow does not mask this Variable in the UI: `pii_hash_salt` matches none of
> Airflow's sensitive-name patterns, so its value is readable in Admin → Variables
> and in the task's *Rendered Template* view. To hide it, add `salt` to
> `[core] sensitive_var_conn_names`.

### 9. Trino access control

Trino enforces file-based access control. Rules live in
`docker/init_trino/rules.json`, which is **generated** from
`governance/rbac.py` — never edit it by hand. Decision record:
[ADR-0015](docs/02-architecture/adr/0015-trino-access-control-generated-from-rbac.md).

**Who connects as whom**

| Client | Trino user | Can do |
|---|---|---|
| dbt | `dbt` | read `gold`; create and replace tables in `serving` |
| Superset, Customer API, Streamlit, ML jobs | `superset` · `customer_api` · `streamlit` · `ml` | read `serving` only |
| Freshness exporter, metrics manifest | `freshness_exporter` · `manifest_collector` | read every layer, customer PII masked |
| Operators, CI, `docker exec … trino` | `admin` · `trino` | everything |
| Analysts | `analytics_report` | read `gold` / `silver` / `sandbox` / `serving`, Silver PII masked |

Any other user name can reach no data at all.

**Query as a specific user** — the CLI defaults to user `trino` (admin):

```bash
docker compose exec trino trino --user analytics_report \
  --execute "SELECT cccd FROM iceberg.silver.dim_customer LIMIT 3"
# → ***********1234   (masked)
```

**Change a permission or add a user**

```bash
# 1. Edit ROLES / USERS in governance/rbac.py
# 2. Regenerate the rules — the drift test fails if you skip this
py -3 scripts/generate_trino_access_control.py
# 3. Commit both files. Trino re-reads rules.json every 60s; no restart needed.
```

**Verify enforcement on the running stack** (also runs in CI):

```bash
py -3 scripts/verify_trino_access_control.py --container banking-trino
```

If every "must be denied" check *succeeds*, Trino is not loading the rules — usually
`access-control.properties` is not mounted at `/etc/trino/access-control.properties`.
Without it Trino silently falls back to allowing everything.

**What this does not protect**

- **No authentication yet.** Trino trusts the user name the client sends, so anyone
  who can reach port 8080/8085 can claim to be `admin`. The rules stop *accidental*
  access and mask PII for well-behaved clients; they are not a defence against a
  deliberate one.
- **Spark bypasses Trino.** Spark jobs read and write Iceberg through the REST
  catalog and MinIO directly. Anyone with MinIO credentials can read the raw files.
- A symptom worth knowing: `Access Denied: Cannot access catalog iceberg` from a
  client means it is connecting under a user name that is not in `rbac.py`.

### 10. Gold schema migrations

`gold_job.py` writes with `writeTo(...).overwritePartitions()` and does **not**
evolve schemas. `docker/init_iceberg/03_ddl_gold.sql` only runs
`CREATE TABLE IF NOT EXISTS`, so a column added to the DDL never reaches a table
that already exists. The job then computes its result, passes its guards, and
fails at the write:

```text
[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] Cannot write to
`lakehouse`.`gold`.`aml_monitoring`, the reason is too many data columns
```

The write is atomic, so the old partition is left intact. CI never sees this —
it builds a fresh lakehouse from the DDL.

Every lakehouse created before the change needs the matching `ALTER` once. Each
entry below is the change that introduced it:

2026-09-23 — ground-truth columns (ROADMAP 2.3). One command per table:

```bash
docker exec banking-spark-worker-1 /opt/spark/bin/spark-sql -S -e "ALTER TABLE lakehouse.gold.aml_monitoring ADD COLUMNS (is_fraud INT, fraud_reason STRING)"
```

```bash
docker exec banking-spark-worker-1 /opt/spark/bin/spark-sql -S -e "ALTER TABLE lakehouse.gold.fraud_risk_txn ADD COLUMNS (is_fraud INT)"
```

Keep them separate: `spark-sql -e` stops at the first failing statement, so if
one table was already migrated, a combined command would never reach the other.

Existing rows read `NULL` for the new columns until the Gold job reruns for
their `cob_dt`. Running an `ALTER` a second time fails with
`FIELDS_ALREADY_EXISTS` and leaves schema and data unchanged — verified.

---

## 🐛 Troubleshooting

### Service Won't Start
```bash
# Check logs
docker compose logs [service_name]

# Check health
docker compose ps

# Restart specific service
docker compose restart [service_name]
```

### Iceberg REST Connection Error
```bash
# Check if iceberg_catalog database exists
docker exec banking-postgres psql -U banking_admin -d banking_db -c "\l"

# Create if missing
docker exec banking-postgres psql -U banking_admin -d banking_db -c "CREATE DATABASE iceberg_catalog;"
docker compose restart iceberg-rest
```

### Airflow DAG Import Errors
```bash
# Check DAG import errors
docker compose exec airflow-scheduler airflow dags list-import-errors

# Add missing connections
docker compose exec airflow-scheduler airflow connections add 'spark_default' \
  --conn-type 'spark' --conn-host 'spark://spark-master' --conn-port '7077'
```

### SparkSubmit Failed
```bash
# Check Spark worker logs
docker compose logs spark-worker-1

# Verify Iceberg jars
docker compose exec spark-worker-1 ls /opt/spark/jars/ | grep iceberg
```

---

## 📊 Monitoring

### Check Pipeline Status
```bash
# Via Airflow CLI
docker compose exec airflow-scheduler airflow dags list-runs -d bronze_core_banking_dag

# Check task states
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
  silver_all_dag "manual__2026-08-04T18:18:11+00:00"
```

### Check Data Volume
```bash
docker compose exec trino trino --catalog lakehouse

SELECT 'bronze' as layer, COUNT(*) FROM lakehouse.bronze.core_customer
UNION ALL
SELECT 'silver', COUNT(*) FROM lakehouse.silver.dim_customer
UNION ALL
SELECT 'gold', COUNT(*) FROM lakehouse.gold.mart_customer_360;
```

---

## 🔄 Backup & Restore

### Backup PostgreSQL
```bash
docker exec banking-postgres pg_dump -U banking_admin banking_db > backup.sql
```

### Restore PostgreSQL
```bash
cat backup.sql | docker exec -i banking-postgres psql -U banking_admin banking_db
```

---

## 📚 Related

- [README.md](README.md) — Quick start
- [ARCHITECTURE.md](ARCHITECTURE.md) — Architecture details
- [DEMO_GUIDE.md](DEMO_GUIDE.md) — Demo walkthrough


---

## Incident Severity Matrix

| Level | Name | Examples | Response Time | Resolution Target |
|-------|------|----------|---------------|-------------------|
| P0 | Critical | Gold data corrupt, pipeline down 4h+, security breach | 15 min | 2 hours |
| P1 | High | Silver/Gold not updating, DQ failures blocking downstream | 30 min | 4 hours |
| P2 | Medium | Single domain delayed, warning alerts, partial failures | 1 hour | 8 hours |
| P3 | Low | Non-blocking warnings, documentation gaps, cosmetic issues | 4 hours | Next business day |

### Escalation Path
1. On-call engineer detects alert
2. P0/P1: Notify team lead within 15 min
3. P0: Escalate to data platform manager within 1 hour
4. Post-incident review required for P0/P1 within 48 hours

---

## RCA Template (Root Cause Analysis)

```text
Incident: [Brief title]
Date: [YYYY-MM-DD]
Severity: P0/P1/P2/P3
Author: [Name]

1. Timeline
   - Detection: [when alert fired]
   - Investigation: [key findings]
   - Mitigation: [what was done]
   - Resolution: [when fixed]

2. Impact
   - Tables affected: [list]
   - Rows/reports affected: [count]
   - Downstream impact: [what broke]

3. Root Cause
   Why 1: [first why]
   Why 2: [second why]
   Why 3: [third why]
   Why 4: [fourth why]
   Why 5: [root cause]

4. Fix
   - Immediate: [what was done now]
   - Permanent: [preventive action]

5. Prevention
   - Add monitoring/check
   - Update runbook
   - Add test
```

---

## Backfill Playbook

### Prerequisites
1. Verify Silver source tables exist for target dates
2. Check Iceberg snapshots are retained
3. Notify downstream consumers of reprocessing

### Steps
1. Trigger Bronze for target dates:
   docker compose exec airflow-scheduler airflow dags trigger bronze_core_banking_dag
2. Wait for Bronze completion (check flag_job_etl)
3. Trigger Silver:
   docker compose exec airflow-scheduler airflow dags trigger silver_all_dag
4. Wait for Silver completion
5. Trigger Gold:
   docker compose exec airflow-scheduler airflow dags trigger gold_all_dag
6. Verify via reconciliation queries
7. Rebuild serving tables via dbt

### Iceberg Snapshot Rollback (Emergency)
If bad data was written, rollback to previous snapshot:
```sql
-- List snapshots
SELECT * FROM lakehouse.gold.mart_customer_360.snapshots
ORDER BY committed_at DESC LIMIT 5;
-- Restore previous version
CALL iceberg_rest.system.rollback_to_snapshot('gold', 'mart_customer_360', snapshot_id);
```
---

## SLA Definitions

| Pipeline | Schedule | SLA | Retry | Alert On |
|----------|----------|-----|-------|----------|
| Bronze Core Banking | 02:00 daily | 2h | 2 | Failure |
| Bronze Card CRM | 02:00 daily | 2h | 2 | Failure |
| Bronze Digital Banking | 02:00 daily | 2h | 2 | Failure |
| Silver All | 04:00 daily | 3h | 2 | Failure + SLA |
| Gold Mart360 | 06:00 daily | 3h | 2 | Failure + SLA |
| Ops Data Quality | 08:00 daily | 4h | 2 | Failure |
| CDC Streaming | Continuous | 1h freshness | 3 | Failure + Freshness |
| dbt Run | 07:00 daily | 2h | 1 | Failure |

### Data Freshness SLAs
| Layer | Max Latency | Check |
|-------|-------------|-------|
| Bronze | 2h from source | freshness_check in dq_rules.yml |
| Silver | 1h after Bronze | Silver DAG completion |
| Gold | 1h after Silver | Gold DAG completion |
| Serving | 30min after Gold | dbt run completion |
