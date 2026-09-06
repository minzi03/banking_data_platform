# Changelog

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

### Evidence

- Added `docs/evidence/metrics-manifest.yaml` — an evidence contract that fixes
  metric definitions, provenance and invariants, plus
  `scripts/generate_metrics_manifest.py` which collects evidence, evaluates
  blocking invariants, and only promotes the canonical manifest when they pass.
- Added `scripts/verify_readme_metrics.py`, run in CI: README is a projection of
  the verified manifest, so a number cannot be hand-edited into drift.

## portfolio-v1.0

Initial feature-frozen portfolio release.
