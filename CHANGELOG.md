# Changelog

## Unreleased — audit remediation 2026-09-30

A full audit against the original assignment (Customer 360 + cross-sell lakehouse:
`mart_customer_360` with 25+ KPIs, automatic campaign segmentation, history for
customer/account/product/branch). Every fix below is covered by a test that fails on
the previous code. Nothing here has run on the full Docker stack yet (TD-20).

### Data correctness

- **RFM scores were inverted** (inherited from the course template): `NTILE` gave the
  best customer 1, so "Champions" were the worst customers and `campaign_target`
  sent Upsell to them. Scores now follow the KPI dictionary (5 = best), with a
  `customer_id` tie-breaker. `rfm_segment.yml`, `customer_360.yml`. Segment
  cut-offs are unchanged and differ from the KPI dictionary (open question, TD-20).
- Recency, `days_since_last_txn` and churn flags used `CAST(ts AS DATE)` (UTC day);
  they now use the ICT business date like every other Gold window.
- **CDC consolidation nulled columns**: `cast_columns` compared numeric columns with
  `""`, so `date_of_birth`, `register_date`, `open_date`, `close_date` and `balance`
  were NULL in Silver Current. Uses `try_cast` / `timestamp_micros` now.
- CDC progress is the last merged **Iceberg snapshot** of Bronze CDC; each run reads a
  pinned `(watermark, end]` window. The MERGE never overwrites newer state with an older
  event. The old `(max ts, max batch)` watermark skipped late events and could count
  events appended mid-run as processed.
- DLQ: Kafka tombstones are no longer counted as `PARSE_ERROR`. The documented
  primary-key check (`MISSING_PRIMARY_KEY`) is implemented.
- **Bronze**: 18 of 22 tables were unpartitioned, so `overwritePartitions()` replaced
  the whole table every load. Every Bronze table is now partitioned by `cob_dt`;
  writes into an unpartitioned table are refused; `make bronze-partition-migrate`.
- **SCD2**: `dim_product` and `dim_branch` keep history (assignment requirement).
  Untracked columns are updated in place (Type 1). Keys missing from the snapshot are
  closed. Empty snapshots and backward backfills fail. Facts and Gold join the dimension
  version valid at `cob_dt`, not `is_current`.

### Orchestration and reproducibility

- **One `cob_dt` for every DAG** (ADR-0017). `{{ ds }}` is a UTC date: DAGs before
  07:00 ICT got D-2, later ones D-1, so scheduled dbt/DQ/PII runs never found their
  Gold flag. Verified by rendering every DAG with Airflow 2.10.0.
- `init_all.sh`: `pipefail`, explicit namespaces, and runs `06_ddl_silver_cdc_current.sql`,
  which no script ran before.
- `make up` checks `docker/.env` and generates the Trino secrets compose requires.
  `make trino` / `make psql` use HTTPS + password and the container's credentials.
- Airflow image: docker CLI added (DAGs run `docker exec` but the image never had it);
  Java, pyspark and the Spark provider removed (no DAG runs Spark locally any more).
  `group_add: DOCKER_GID` for the socket.
- PII masking and Iceberg maintenance DAGs used `SparkSubmitOperator` from the Airflow
  container, which has no Iceberg jars. They now run in the Spark worker via `docker exec`.
- `cdc_streaming_stop_all` could stop the Spark worker (`stop-slave.sh`), and a `#`
  commented out the rest of its command. Now SIGTERMs only the `cdc_*` drivers.
- Bronze DAGs no longer query the metadata DB at parse time, and the JDBC password
  goes through env, not argv.
- `regulatory_reporting` and `dbt_seed` are unscheduled (non-functional; see TD-20).
  `ops_ml_churn_dag` is manual; its SQL quoting bug and label leakage are fixed.

### API

- `cob_dt` is a validated `date` query parameter bound with `?`, not interpolated into SQL.
- Joined endpoints qualify `cob_dt`, which was ambiguous.

### Found on the running stack (2026-09-30)

These surfaced only once the fixes above ran on Docker. Each has a test that fails on
the previous code.

- **CDC money columns were NULL.** Debezium `decimal.handling.mode=precise` sends
  NUMERIC as base64 bytes. The connectors now use `string`; RUNBOOK covers re-emitting
  on older stacks.
- **Four of six CDC streaming queries never ran.** Each query is now capped at
  `spark.cores.max=1`.
- **Same-transaction CDC events were ordered at random.** INSERT + UPDATE share the
  millisecond and the micro-batch. Bronze CDC now keeps `__kafka_partition` /
  `__kafka_offset`, and dedup breaks ties on the offset (ADR-0010).
- **Bronze partition migration failed its post-check** on the catalog cache
  (`REFRESH TABLE`).
- **Makefile Spark targets could not find `spark-submit`.** They now use
  `/opt/spark/bin/spark-submit`.
- **Batch DAGs could run concurrently for the same `cob_dt`.** They now set
  `max_active_runs=1`.
- **Quarantine never stored a row.** Tables are now created on demand, rows use the
  target schema, and columns follow the target's order. The rule `overdue_account` now
  writes to `overdue_account`.
- **Generator:** CLOSED accounts get balance 0.
- **API recommendations:** no duplicates, no `"None"` product, and the AUM buckets
  match Gold.
- **CI:** the SCD1 smoke step now uses a dimension that is still SCD1, with a static
  guard.
- **Evidence manifest** regenerated and promoted `verified` (`cob_dt` 2026-09-23;
  README 24/24 bindings).

### Documentation

- `DEMO_GUIDE.md` is the single authoritative demo. Older demo and dbt docs are pointer stubs.
- README, ARCHITECTURE, RUNBOOK, dbt README, data-output documentation, ADR-0010 and
  ADR-0017 match the code. The README metrics table stays bound to the last promoted
  manifest, with the known static drift stated under it.

## portfolio-v1.1

Correctness, serving architecture, and verifiable metrics.

### Corrected reporting

- **Corrected transaction-scale reporting** from accumulated physical snapshot
  rows to distinct logical transactions within a verified snapshot.
  `4.6M+` counted `COUNT(*)` across accumulated full-snapshot fact partitions,
  so the same transaction was counted once per `cob_dt`. The verified figure is
  **2,300,000** distinct `(domain, transaction_id)` records in one snapshot.
  The retired claim is preserved in the evidence manifest under
  `superseded_claim` for audit trail.
- Metric counts now carry explicit definitions. Several previously ambiguous
  numbers were corrected once a definition was fixed: Docker services
  (24 = 20 long-running + 4 one-shot, previously reported as 23), source
  workloads (16 executable configs, not 17 YAML files), automated tests
  (source-level `def test_*` functions, tracked separately from pytest node
  count).

### Analytical correctness

- Fixed Cartesian fan-out in `rfm_segment`, `churn_prediction` and
  `customer_card_summary`. Joining two raw facts (or a dimension and a fact) at
  the wrong grain multiplied `SUM()` while `COUNT(DISTINCT)` hid the problem.
  Each source now aggregates to `customer_id` in its own CTE before joining.
- Every Gold query reading a Silver fact now pins `cob_dt` to the snapshot being
  processed. Silver facts are full snapshots per `cob_dt`; filtering only on
  business time double-counted across partitions.
- `churn_prediction.txn_amt_30d` / `txn_amt_90d` now include card transactions
  (previously account-only despite the generic column name), making them
  reconcilable against `customer_transaction_summary`.

### Serving architecture

- Serving layer moved from Spark-created objects to **dbt-managed Iceberg tables
  published through Trino** (`iceberg.serving.*`, 9 tables).
- Retired 8 `gold.*_current` CTAS tables (created once at initialization, never
  refreshed) and the Spark-only `mart_customer_360_current` view (not visible to
  Trino at all).
- Removed 12 `sm_*` semantic models: all were pure passthroughs and all were
  `ephemeral`, so they produced no queryable object — exposures claiming
  consumers depended on them described a path that did not exist.
- Serving models take `cob_dt` from a dbt var rather than `MAX(cob_dt)`, so a
  missing snapshot fails the build instead of silently serving stale data.

### Time semantics

- Storage/engine timezone standardised to **UTC**; banking calendar dates are
  **explicitly derived** in `Asia/Ho_Chi_Minh`.
- Previously Spark ran a local session timezone while Trino ran UTC, so the same
  Iceberg row produced two different calendar dates across engines.
- Spark session timezone is now enforced at runtime — a non-UTC session fails
  fast instead of silently shifting every daily metric.

### Orchestration

- Added `GOLD_COMPLETE` / `SERVING_COMPLETE` completion flags. The serving
  publisher waits for `GOLD_COMPLETE` for the same `cob_dt`, and records
  `SERVING_COMPLETE` only after `dbt build` and serving tests pass.

### Bootstrap reproducibility

Clean-rebuild-from-zero blockers, each found by actually rebuilding from an
empty environment:

- Airflow metadata database was never created by any init script.
- Per-connector Debezium publications (`debezium_pub_core` / `_card` /
  `_digital`) were never created, so all three connectors reported
  `state=RUNNING` while their tasks were `FAILED`.
- The `postgres-etl` Airflow connection used by every DAG was never created.
- The three per-source connections (`postgres-core-banking`, `postgres-card-crm`,
  `postgres-digital-banking`) were never created either. The Bronze DAGs read
  their connection at parse time, so the DAGs failed to *import* — they were
  absent from the UI rather than shown as failing.
- `cdc_consolidation_pipeline` submitted Spark from inside the Airflow
  container, which ships only the pyspark wheel and no Iceberg jars
  (`Cannot find catalog plugin class for catalog 'lakehouse'`). It was the only
  Spark DAG not using `docker exec` into the Spark worker; consolidation had
  never run successfully from Airflow. Fixing it also removed a MinIO secret
  that the DAG re-declared inline.
- Bronze bootstrap exited `0` even when every table failed, and logged a
  hard-coded `0 rows` that was never a measurement.

`tests/governance/test_airflow_dag_contracts.py` now enforces both DAG rules
statically: every `spark-submit` goes through `docker exec`, and every `conn_id`
a DAG references is created by `airflow-init`.

### Test isolation

Several test modules stubbed `sys.modules["pyspark"]` at import time and never
restored it, leaking a global fake `pyspark` into every test that ran
afterwards. Removing that leak revealed two suites that had never provided
their own dependencies and were passing on the leak:

- `tests/governance/test_anomaly_detection.py` — `anomaly_detection.py` imports
  pyspark lazily inside the function under test, so the stub has to be active at
  call time. It now installs its own, via `patch.dict`, unconditionally, so the
  local and CI paths are identical.
- `tests/shared/test_spark_session.py` — every test already patches
  `SparkSession`, so pyspark only needs to be importable. It now stubs pyspark
  only when genuinely absent, and restores `sys.modules` afterwards.

The Gold regression CI job also gained `jinja2`, a real dependency of
`gold_job.py` through `utils/yaml_loader.py`, which the job had never installed.

### CI coverage

The Gold Spark Regression job now runs the business-date semantics suite
alongside the fan-out regression — 32 CI-executed Gold tests (23 fan-out and
snapshot, 9 business-date). The business-date tests guard the UTC engine and
explicit `Asia/Ho_Chi_Minh` derivation, the largest architectural change in this
release, and need only pyspark, which the job already installs. The job's
change-gate was widened to `code_etl/shared/spark/` and `docker/spark/conf/` so
a PR touching only the timezone configuration cannot skip the job that checks it.

34 Trino-backed integration tests remain outside the main CI pipeline because
they require the separate `ci-trino` topology; wiring them into CI is tracked
separately in [docs/technical-debt.md](docs/05-quality/technical-debt.md). They are a known
CI coverage gap and are **not** claimed as passing.

### Evidence

- Added `docs/evidence/metrics-manifest.yaml` — an evidence contract that fixes
  metric definitions, provenance and invariants, plus
  `scripts/generate_metrics_manifest.py` which collects evidence, evaluates
  blocking invariants, and only promotes the canonical manifest when they pass.
- Added `scripts/verify_readme_metrics.py`, run in CI: README is a projection of
  the verified manifest, so a number cannot be hand-edited into drift.

## portfolio-v1.0

Initial feature-frozen portfolio release.
