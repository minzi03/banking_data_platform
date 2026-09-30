# Runbook — Banking Data Platform

## 🚀 Quick Reference

### Start All Services
```bash
cd banking_data_platform
cp docker/.env.example docker/.env   # first time only; replace every CHANGE_ME
make up                              # checks docker/.env, generates Trino secrets, compose up
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
cd banking_data_platform
make up            # = env check + scripts/bootstrap_trino_auth.py + docker compose up -d
# Wait 2 minutes for services to become healthy
docker compose -f docker/docker-compose.yml ps  # 25 running, 4 one-shot jobs exited (0)
```

On Linux, set `DOCKER_GID` in `docker/.env` to the group of the host socket
(`stat -c %g /var/run/docker.sock`): Airflow submits every Spark/dbt job with
`docker exec` and needs access to `/var/run/docker.sock`. Docker Desktop: `0`.

A stack created before every Bronze table was partitioned by `cob_dt` must be
migrated once — Bronze jobs refuse to write a snapshot into an unpartitioned table:

```bash
make bronze-partition-migrate   # ALTER … ADD PARTITION FIELD cob_dt + rewrite_data_files
```

`dim_product` / `dim_branch` (SCD1 → SCD2) need no manual step: the first SCD2 run
adds `*_sk`, `effective_from`, `effective_to`, `is_current` and marks existing rows
as current, valid from `1900-01-01`.

### 2. Generate Seed Data
```bash
# Needs POSTGRES_USER / POSTGRES_PASSWORD in the environment — there is no
# password default in the code. PowerShell: $env:POSTGRES_PASSWORD = docker exec banking-postgres printenv POSTGRES_PASSWORD
# From host (recommended). --as-of = the cob_dt you will load: the newest
# transaction lands on that date, so "last 30 days" KPIs in Gold have data.
# Default is today. See data_generator/generators/timeline.py (TD-16).
cd banking_data_platform
python data_generator/generate_all.py --host localhost --port 5432 --as-of 2026-09-22
```

### 3. Run ETL Pipeline

**Via Airflow UI (http://localhost:8080):**
1. Login: admin / admin123
2. Enable DAGs
3. Trigger manually

**Via CLI — always pass `cob_dt`** (ADR-0017). Every DAG reads
`dag_run.conf["cob_dt"]`; without it a manual run falls back to the ICT date of its
own data interval, which can differ between DAGs triggered between their schedules.

```bash
COB=2026-09-29
for d in bronze_core_banking_dag bronze_card_crm_dag bronze_digital_banking_dag \
         silver_all_dag gold_all_dag dbt_serving_publish ops_data_quality_dag; do
  docker exec banking-airflow-scheduler airflow dags unpause $d
  docker exec banking-airflow-scheduler airflow dags trigger $d --conf "{\"cob_dt\": \"$COB\"}"
done
```

Scheduled runs need no `conf`: every daily DAG gets the business day that just ended.

**Without Airflow (bootstrap, one `cob_dt`):** the Silver and Gold bootstraps default
to one `spark-submit` per job, the same path Airflow uses. `--in-process` runs every job
of the layer in a single Spark app, with a child session per job. Measured on the full
local data (2026-09-28): Silver took 53 s instead of about 5 min, and Gold took 90 s
instead of about 5.5 min. Row counts and content checksums were identical. CI uses it.

```bash
docker exec -w /opt/project banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 code_etl/silver/bootstrap/initial_load.py --cob_dt 2026-09-22 --in-process
```

```bash
docker exec -w /opt/project banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 code_etl/gold/bootstrap/initial_load.py --cob_dt 2026-09-22 --in-process
```

`--in-process` also shows each job's own log. The default mode captures a job's output
and prints it only when the job fails, which is how a failed Gold z-order
(`OPTIMIZE_FAILED`) went unseen.

### 4. Query Data

**Via Trino (HTTPS 8453 on the host, 8443 inside the network; catalog `iceberg`).**
Plain HTTP answers 403 (ADR-0016). Inside the Trino container the CLI user `trino`
reads its password from `TRINO_PASSWORD`:

```bash
make trino    # interactive CLI

docker exec -i banking-trino trino --server https://localhost:8443 \
  --truststore-path /etc/trino/secrets/trino.pem --user trino --password \
  --catalog iceberg --execute "SELECT COUNT(*) FROM serving.mart_customer_360_current"
```

```sql
-- History snapshot table (one partition per cob_dt)
SELECT cob_dt, COUNT(*) FROM gold.mart_customer_360 GROUP BY cob_dt ORDER BY cob_dt;

-- Current serving tables (dbt, exactly one cob_dt, 1 row/customer)
SELECT COUNT(*), MIN(cob_dt), MAX(cob_dt) FROM serving.mart_customer_360_current;
SELECT customer_segment, COUNT(*) FROM serving.mart_customer_360_current GROUP BY 1;
SELECT rfm_segment, COUNT(*) FROM serving.rfm_segment_current GROUP BY 1;
SELECT campaign_type, COUNT(*) FROM serving.campaign_target_current GROUP BY 1;
```

The former `gold.*_current` CTAS tables were retired; serving lives in `serving.*`.

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

Expect 74 rows per run: 50 into Gold, 24 into Silver. `row_count` is NULL — the
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

**Stacks created before `opslakehouse.data_lineage` was removed** still have that
table: an older, empty duplicate that no code writes. Drop it. The guard stops
the command if the table has any rows, so nothing is lost silently:

```bash
docker exec -i banking-postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d banking_db' <<'SQL'
DO $$ BEGIN
  IF to_regclass('opslakehouse.data_lineage') IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM opslakehouse.data_lineage) THEN
      RAISE EXCEPTION 'opslakehouse.data_lineage has rows: inspect them before dropping';
    END IF;
  END IF;
END $$;
DROP TABLE IF EXISTS opslakehouse.data_lineage;
SQL
```

`opslakehouse.data_lineage_audit` (column-level, no writer) was defined in
`09_ddl_regulatory.sql`, removed 2026-09-30 with the non-functional regulatory DAG
(TD-20). Existing stacks keep an empty table until it is dropped by hand:
`DROP TABLE IF EXISTS opslakehouse.data_lineage_audit;`.

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

**Authentication (ADR-0016).** Trino accepts queries only over HTTPS with a
password: HTTP answers 403, a wrong password 401, and a logged-in user cannot
impersonate another (`--session-user admin` → "cannot impersonate"). HTTPS is
`trino:8443` inside the docker network and `localhost:8453` on the host. Generate
the secrets once per machine, **before** starting the stack:

```bash
py -3 scripts/bootstrap_trino_auth.py          # everything → docker/secrets/trino/ (gitignored)
py -3 scripts/bootstrap_trino_auth.py --check  # exit 1 if missing or out of sync with governance/rbac.py
```

It prints key *names* only, never values, and keeps existing passwords unless
you pass `--rotate`. Each service gets a file with only its own password
(`env/<user>.env`); `passwords.env` holds them all for host-side tools and is
mounted nowhere. Nothing is written to `docker/.env` — nine services load that
file whole and would see every password.

**Stacks created before authentication** — generate secrets, then recreate Trino and
its clients (api and the freshness exporter copy code at build time, so rebuild them):

```bash
py -3 scripts/bootstrap_trino_auth.py
docker compose up -d --no-deps --force-recreate trino dbt streamlit
docker compose build api freshness-exporter
docker compose up -d --no-deps --force-recreate api freshness-exporter
```

**Query as the admin CLI** — `docker exec … trino` works unchanged: the container's
`~/.trino_config` points the CLI at `https://localhost:8443` as user `trino`, with the
password from its environment.

**Query as a specific user** — pass that user's password in the environment, never
in argv. With `--user X` and no password for X, Trino refuses:

```bash
TRINO_PASSWORD=$(grep '^TRINO_PASSWORD_ANALYTICS_REPORT=' docker/secrets/trino/passwords.env | cut -d= -f2-) \
  docker exec -e TRINO_PASSWORD banking-trino trino --user analytics_report \
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

- **Self-signed certificate, passwords in files.** Authentication is real (ADR-0016)
  but there is no PKI, and passwords live in `docker/secrets/trino/` on the dev
  machine, not in a secret manager. Anyone who can read that directory — or exec
  into a container — has the passwords.
- **Spark bypasses Trino.** Spark jobs read and write Iceberg through the REST
  catalog and MinIO directly. Anyone with MinIO credentials can read the raw files.
- A symptom worth knowing: `Access Denied: Cannot access catalog iceberg` from a
  client means it is connecting under a user name that is not in `rbac.py`.

### 10. Gold schema migrations

> **Since 2026-09-29, Gold adds new columns by itself.** Before every write to
> an existing table, `gold_job.py` runs `code_etl/shared/spark/schema_guard.py`:
> a column the result has and the table lacks is added with
> `ALTER TABLE … ADD COLUMNS`; a column the table has and the result lacks, or a
> change of type family (number ↔ string, date ↔ timestamp), stops the job
> **before** the write with `BreakingSchemaChange` — the old partition stays.
> Numeric types are not widened. The manual `ALTER`s below are only needed on a
> lakehouse that last ran Gold before this change, and Bronze / Silver still need
> theirs.

Before that change, `gold_job.py` wrote with `writeTo(...).overwritePartitions()`
and did **not** evolve schemas. `docker/init_iceberg/03_ddl_gold.sql` only runs
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

The same applies to Bronze (`write_to_iceberg`) and Silver facts
(`fact_txn.py`): both write with `overwritePartitions()` and no schema evolution.

2026-09-29 — `card_txn.entry_mode` / `decline_reason` (SOURCE_DATA_BASELINE §2).
The PostgreSQL source needs nothing by hand: `generate_all.py` applies
`data_generator/migrations/*.sql` (idempotent) before writing. The lakehouse
tables need one `ALTER` each:

```bash
docker exec banking-spark-worker-1 /opt/spark/bin/spark-sql -S -e "ALTER TABLE lakehouse.bronze.core_card_txn ADD COLUMNS (entry_mode STRING, decline_reason STRING)"
```

```bash
docker exec banking-spark-worker-1 /opt/spark/bin/spark-sql -S -e "ALTER TABLE lakehouse.silver.fact_card_txn ADD COLUMNS (entry_mode STRING, decline_reason STRING)"
```

Rows seeded before the change have `entry_mode` / `decline_reason` NULL in the
source too — the migration adds the constraints `NOT VALID`, so old rows are not
rechecked. Re-seed (`--truncate`) to get the columns filled for every row.

### 11. Rotate local secrets

The secret-hygiene PRs (#67, #71, #72, #73) stop new secrets from entering the repo.
They do not change the values the stack already runs on, and those values are in Git
history. As measured on 2026-09-27, every secret in `docker/.env` appeared in 1–12
commits. `scripts/rotate_local_secrets.py` changes them without printing any value.

Plan first. This shows key names, how many commits contain each current value, and
whether the preconditions are met:

```bash
py -3 scripts/rotate_local_secrets.py
```

Precondition: `spark-defaults.conf` and the Trino catalog must read MinIO credentials
from the environment (#71). If they still hold the keys, rotating MinIO breaks Spark.
The script checks this and refuses to run if it is not met.

Apply:

```bash
py -3 scripts/rotate_local_secrets.py --apply
```

- Backs up `docker/.env` to `docker/secrets/rotation/` (gitignored) and prints the path.
- Changes `POSTGRES_PASSWORD` and `CDC_DB_PASSWORD` with `ALTER ROLE`. It sends a
  SCRAM-SHA-256 verifier over psql's stdin, so the server never sees a plaintext
  password. The verifier function matches Postgres 15 byte for byte (tested).
- Sets `etl_user`, `analytics_user` and `readonly_user` to `NOLOGIN` with no password.
- Changes `MINIO_ROOT_PASSWORD` and recreates the running services that read these
  values: minio, iceberg-rest, spark-master, spark-worker-1, trino.
- Verifies that the new passwords log in, the old ones are rejected, and Trino still
  reads Iceberg (metadata through iceberg-rest, data files from MinIO).

Compose gives shell variables priority over `docker/.env`. §2 has you set
`$env:POSTGRES_PASSWORD` for seeding. On the first real run (2026-09-27), that stale
value reached iceberg-rest, which exited with `password authentication failed`. The
script now passes the new values to compose explicitly and warns about stale shell
variables. After rotating, clear them in any shell that still has them, then read the
new values again when you need them (§2):

```powershell
Remove-Item Env:POSTGRES_PASSWORD, Env:POSTGRES_USER -ErrorAction SilentlyContinue
```

If a check fails, restore with the path it printed:

```bash
py -3 scripts/rotate_local_secrets.py --rollback docker/secrets/rotation/<file>.bak
```

Not covered, because the service must be running and use its own tool. The script
lists these at the end:

- Superset: `SECRET_KEY` needs `superset re-encrypt-secrets`; the admin password
  needs `fab reset-password`.
- Airflow: Fernet key rotation and the admin password. The `postgres-*`
  connections stored in the Airflow database keep the old password until
  `airflow-init` runs again. Since 2026-09-27 it upserts them:

  ```bash
  docker compose -f docker/docker-compose.yml up --no-deps airflow-init
  ```
- OpenMetadata's MySQL passwords.
- Debezium connectors registered with the old `CDC_DB_PASSWORD`.

### 12. Replication slots (CDC)

A replication slot keeps WAL until its consumer reads it. With Postgres's default
`max_slot_wal_keep_size = -1`, a slot that nobody reads keeps WAL forever. That
happens whenever the CDC stack is down or a connector's task has failed. Kafka's state
sits in anonymous volumes (declared by the image, not by compose), so a
`docker compose down` loses Debezium's offsets. On
2026-09-27, three stale `debezium_slot_*` slots held 3.6 GB. They belonged to connectors
registered outside the repo, whose tasks had failed since the CDC password was rotated.
See TD-18.

Check how much WAL each slot holds:

```bash
docker exec banking-postgres sh -c 'psql -U "$POSTGRES_USER" -d banking_db -c "SELECT slot_name, active, wal_status, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained FROM pg_replication_slots ORDER BY 1"'
```

New stacks get a 4 GB cap from `05-cdc-setup.sh`. Apply it once on an existing stack.
This is a reload-level setting, so no restart is needed:

```bash
docker exec banking-postgres sh -c 'psql -U "$POSTGRES_USER" -d banking_db -c "ALTER SYSTEM SET max_slot_wal_keep_size = '"'"'4GB'"'"'" -c "SELECT pg_reload_conf()"'
```

The commands above use bash quoting (`'"'"'`). PowerShell mangles it: on 2026-09-27 a
`pg_drop_replication_slot` pasted into PowerShell matched no slot at all. In PowerShell,
send the SQL on stdin instead:

```powershell
"ALTER SYSTEM SET max_slot_wal_keep_size = '4GB';", "SELECT pg_reload_conf();" | docker exec -i banking-postgres sh -c 'psql -U "$POSTGRES_USER" -d banking_db'
```

Drop orphaned slots the same way. List the names from the query above; never drop an
active slot:

```powershell
"SELECT slot_name, pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name IN ('debezium_slot_core','debezium_slot_card','debezium_slot_digital') AND NOT active;" | docker exec -i banking-postgres sh -c 'psql -U "$POSTGRES_USER" -d banking_db'
```

A slot over the cap becomes `wal_status = lost`, and its connector fails with a clear
error. Re-register the connector: Debezium drops nothing by itself, so drop the lost
slot first, and the connector takes a new snapshot. The slot names in use are the
`slot.name` values in `code_etl/cdc/register_connectors.py`; a test keeps the
Airflow DAG in sync with them.

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

### Kafka exits with `NodeExists` right after Zookeeper restarts

```text
ERROR Error while creating ephemeral at /brokers/ids/1, node already exists and owner
'0x…' does not match current session '0x…'
```

Zookeeper keeps its data in a volume. After a restart or recreate, it still holds the
previous broker's ephemeral node until that session times out, and Kafka exits with
code 1. Debezium, which depends on Kafka, is then left created but not started. This
happened twice on 2026-09-27. Wait about 20 seconds, then start the two in order:

```bash
docker start banking-kafka
```

```bash
docker start banking-debezium
```

Connectors, offsets and topics survive this (anonymous volumes). Check that the
connectors are `RUNNING` and all three replication slots are active (RUNBOOK §12).

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

# DAGs use no Spark connection: Spark jobs run inside spark-worker-1 via `docker exec`.
# "docker: permission denied" → set DOCKER_GID in docker/.env (see §1) and recreate Airflow.
# "docker: not found" → rebuild the Airflow image (docker/Dockerfile.airflow copies the docker CLI).
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
make trino   # HTTPS + password, catalog iceberg (ADR-0016)

-- one snapshot per layer (Trino catalog iceberg; replace the date)
SELECT 'bronze' AS layer, COUNT(*) FROM bronze.core_customer WHERE cob_dt = DATE '2026-09-29'
UNION ALL
SELECT 'silver', COUNT(*) FROM silver.dim_customer WHERE is_current = 1
UNION ALL
SELECT 'gold', COUNT(*) FROM gold.mart_customer_360 WHERE cob_dt = DATE '2026-09-29';
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

## CDC Consolidation — replay and reset

Progress per target table is the last merged Iceberg snapshot of its Bronze CDC table
(`meta.cdc_watermark.last_snapshot_id`); each run reads the appends in `(watermark, end]`.

```sql
-- Trino
SELECT * FROM meta.cdc_watermark;
SELECT snapshot_id, made_current_at FROM bronze."core_customer_cdc$history" ORDER BY made_current_at DESC LIMIT 5;
```

- **Replay everything** (e.g. after fixing a conversion): delete the table's watermark row,
  then trigger `cdc_consolidation_pipeline`. The run reads all of Bronze CDC; the MERGE
  never overwrites newer state with an older event, so replay is idempotent.
  `DELETE FROM lakehouse.meta.cdc_watermark WHERE table_name = 'dim_customer_current'` (Spark SQL).
- **Watermark snapshot expired** by `ops_maintenance_weekly_dag`: handled automatically —
  the run logs a WARNING and falls back to a full read.
- **Stop streaming**: trigger `cdc_streaming_stop_all` (SIGTERM to the `cdc_*` drivers only;
  it no longer touches the Spark worker). Restart with `cdc_streaming_pipeline`; queries
  resume from their checkpoints under `s3a://lakehouse/checkpoints/cdc/`.
- **Money columns NULL in Bronze CDC** (stacks whose connectors were registered before
  2026-09-30): Debezium used `decimal.handling.mode=precise`, which sends NUMERIC as base64
  bytes, and the cast to `decimal(18,2)` gave NULL. Re-run `cdc_register_connectors` (it
  PUTs the config, now `string`). Events already in Kafka keep the old encoding, so
  re-emit the rows that feed Silver Current. The UPDATE below changes no value and
  Debezium still emits every row:
  `PQ "UPDATE core_banking.account SET balance = balance"`, then run consolidation.
  Measured 2026-09-30: 30,000 events; `SUM(balance)` in `silver.dim_account_current`
  equals the source to the cent.
- **Resource sizing**: each streaming query runs with `spark.cores.max=1` (six cores in
  total). Without the cap, standalone Spark gave the first query 6 of 8 cores. Four
  queries and consolidation then stayed `WAITING` while Airflow reported success.

---

## Backfill Playbook

### Prerequisites
1. Verify Silver source tables exist for target dates
2. Check Iceberg snapshots are retained
3. Notify downstream consumers of reprocessing

### Steps
Pass the target date to every DAG with `--conf '{"cob_dt": "YYYY-MM-DD"}'` (ADR-0017).

Sensors treat an upstream as done when the **latest** `flag_job_etl` row for
(job, `cob_dt`) is `S` (`etl_flag.upstream_success_sql`). A re-run inserts `R` at its start,
which hides the previous `S`; Gold and dbt also insert `R` for `GOLD_COMPLETE` /
`SERVING_COMPLETE`. Before 2026-09-30 sensors only asked "is there any `S`", so re-running a
date let Silver start while Bronze was still rewriting. Trigger each layer after the one it
waits for has started; triggering all at once only works for a date with no flags yet.

After editing anything in `airflow/plugins/` (e.g. `etl_flag.py`, `cob_dt.py`), restart the
scheduler and webserver: plugins are imported once at start, so DAGs importing a new plugin
function fail to parse and the processor deletes them from `serialized_dag` ("DAG not
found"; seen 2026-09-30): `docker compose -f docker/docker-compose.yml restart airflow-scheduler airflow-webserver`.

1. Trigger Bronze for the target date:
   docker exec banking-airflow-scheduler airflow dags trigger bronze_core_banking_dag --conf '{"cob_dt": "2026-09-20"}'
2. Wait for Bronze completion (check flag_job_etl)
3. Trigger Silver with the same conf.
   **SCD2 dims only move forward:** `scd_type2.py` refuses a `cob_dt` older than the
   newest `effective_from` already stored. Re-running the newest date is safe; older
   dates need the SCD2 tables rolled back first (Iceberg snapshot rollback below).
   Facts and Gold for an older date are safe: they join the dim version valid at that date.
4. Wait for Silver completion
5. Trigger Gold with the same conf.
6. Verify via reconciliation queries
7. Rebuild serving tables via dbt

### Iceberg Snapshot Rollback (Emergency)
If bad data was written, rollback to previous snapshot:
Spark SQL (`docker exec -it banking-spark-worker-1 /opt/spark/bin/spark-sql`; catalog `lakehouse`):
```sql
-- List snapshots
SELECT snapshot_id, committed_at, operation FROM lakehouse.gold.mart_customer_360.snapshots
ORDER BY committed_at DESC LIMIT 5;
-- Restore a previous version
CALL lakehouse.system.rollback_to_snapshot('gold.mart_customer_360', <snapshot_id>);
```
---

## SLA Definitions

| Pipeline | Schedule | SLA | Retry | Alert On |
|----------|----------|-----|-------|----------|
| Bronze Core Banking | 02:00 daily | 2h | 2 | Failure |
| Bronze Card CRM | 02:00 daily | 2h | 2 | Failure |
| Bronze Digital Banking | 02:00 daily | 2h | 2 | Failure |
| Silver All | 04:00 daily | 3h | 2 | Failure + SLA |
| Gold (gold_all_dag) | 06:00 daily | 3h | 2 | Failure + SLA |
| Ops Data Quality | 08:00 daily | 4h | 2 | Failure |
| CDC Streaming | Continuous | 1h freshness | 3 | Failure + Freshness |
| dbt Serving Publish | 07:00 daily | 1h | 1 | Failure |

### Data Freshness SLAs
| Layer | Max Latency | Check |
|-------|-------------|-------|
| Bronze | 2h from source | freshness_check in dq_rules.yml |
| Silver | 1h after Bronze | Silver DAG completion |
| Gold | 1h after Silver | Gold DAG completion |
| Serving | 30min after Gold | dbt run completion |
