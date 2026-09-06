# Technical Debt

Known gaps, recorded so they stay visible rather than being rediscovered.
Each item states what is missing, why it is not fixed yet, and what "done"
looks like — a vague item cannot be closed honestly.

---

## TD-1 — Execute the Trino integration suite in CI

**Status:** open (recorded at `portfolio-v1.1`)

34 existing tests are not run by any workflow:

```text
tests/integration/test_data_quality.py      18
tests/integration/test_etl_validation.py    16
                                            ──
                                            34
```

They query Trino through a container named `ci-trino`, which only
`.github/workflows/benchmark.yml` ever creates — and that workflow runs its own
SQL rather than pytest. So these tests pass only if someone stands the topology
up by hand. **They are not claimed as passing in CI, and CI coverage figures in
the release notes exclude them.**

Not fixed for `v1.1` because it needs a new docker-compose CI topology, which is
a larger change than the release itself and would have made the release a CI
infrastructure PR.

### Acceptance

```text
docker-compose CI Trino stack
→ readiness check (Trino answers SELECT 1, catalogs registered)
→ 34 tests execute
→ failures block the PR
→ teardown runs even when the job fails
```

Not done until failures actually block. A job that runs the suite with
`continue-on-error` restates the problem rather than fixing it.

### What CI does cover today

```text
Gold Spark Regression
├── tests/gold/test_gold_fanout_regression.py     23
└── tests/gold/test_business_date_semantics.py     9
                                                  ──
                                                  32
```

---

## TD-2 — `Performance Benchmark` workflow has been failing on schedule

**Status:** open (recorded at `portfolio-v1.1`)

`.github/workflows/benchmark.yml` runs on a schedule and has failed every week
since at least `2026-08-23`. It is not a required check for any release commit,
so it did not block `v1.1` — but a permanently red scheduled workflow makes the
repository's health signal meaningless, which is how a real regression gets
ignored.

### Acceptance

Either the workflow passes, or it is disabled with the reason recorded here.
Leaving it red is not an outcome.

---

## TD-4 — Makefile bootstrap targets depend on an invalid container working directory

**Status:** open (recorded at `portfolio-v1.1`)

**Not runtime-proven.** A configuration defect with clear evidence, not an
observed failure — `make bronze-bootstrap` has not been run to watch it fail.

`bronze-bootstrap`, `silver-bootstrap` and `gold-bootstrap` invoke
`docker compose exec spark-worker-1 spark-submit ...` without `-w`. Measured:

```text
docker compose exec spark-worker-1 pwd    → /opt/spark/work-dir
ls code_etl from that directory           → does not resolve
working_dir in either compose file        → not set
```

`BRONZE_CONFIGS` and the Silver/Gold job lists hold repo-root-relative paths
(`code_etl/bronze/core_banking/branch.yml`), and `load_config()` opens the path
as given, so the first call should raise `FileNotFoundError`. The benchmark
workflow now passes `-w /opt/project`; the Makefile does not.

The v1.1 clean rebuild did load Bronze — 2,300,000 rows verified through Trino —
so it ran by some path other than these targets. A `make` target that cannot
work is a trap for whoever tries it next.

### Acceptance

```text
[ ] bronze-bootstrap uses /opt/project as working directory
[ ] silver-bootstrap uses /opt/project as working directory
[ ] gold-bootstrap uses /opt/project as working directory
[ ] relative config/module paths resolve inside spark-worker-1
[ ] make targets propagate child exit codes
[ ] one smoke test proves the Makefile path from host works end-to-end
```

The exit-code item is not filler. Losing a child exit code has now happened
four times in this repository: `$?` read after `grep`/`tail`, `|| true` around
the Bronze ETL loop, `git push || true` in the benchmark workflow, and Bronze
bootstrap returning 0 with every table failed. Audit the Makefile for the same
pattern rather than assuming it is absent.

---

## TD-6 — Two workflows build the same lakehouse fixture

**Status:** open (recorded at TD-1)

`ci.yml`'s `trino-integration` job and `benchmark.yml` both run the same
sequence: pull, build, start the `ci-trino` stack, create schemas, seed, then
Bronze → Silver → Gold. Roughly 250 lines exist twice.

Deliberate for now, because the two are expected to **diverge** rather than
converge: the benchmark wants production-like volume, while the 34 integration
tests check structure, uniqueness, referential integrity and count matching and
need nothing like 1.2M transactions. Extracting a composite action today would
couple two consumers that should end up parameterised differently — and GitHub
composite actions cannot carry per-step `timeout-minutes`, which would undo the
separation of image-acquisition from readiness budgets that TD-2 established.

The integration job currently costs ~18 minutes, of which ~13 is seeding and
ETL and ~40 seconds is the tests themselves.

### Acceptance

```text
[ ] one canonical generator/config drives both profiles
      benchmark profile    → production-like volume
      integration profile  → reduced volume
[ ] same schema, same generation rules, same code path — only scale differs
[ ] integration job wall time materially reduced
[ ] no second "mini seed" implementation
```

The failure mode to avoid is a separate small seeder: that is how the schemas
in `trino-init` and `benchmark.yml` drifted apart in the first place.

---

## TD-5 — Shell failure propagation and false-success patterns

**Status:** open (recorded at `portfolio-v1.1`)

A swallowed child exit code has now been found five separate times in this
repository. That is a pattern, not a run of bad luck, and each instance produced
the same outcome: something reported success while doing nothing.

| # | Where | Shape |
| --- | --- | --- |
| 1 | Bronze bootstrap | returned `0` with all 16 tables failed |
| 2 | `benchmark.yml` Bronze ETL loop | `\|\| true` around every ingest |
| 3 | `benchmark.yml` query runner | `$(cmd \| grep …)` takes `grep`'s status |
| 4 | `benchmark.yml` baseline commit | `git push \|\| true` |
| 5 | `ci-trino-init` | `sh -c` ending in `echo`, so five failed `CREATE SCHEMA` calls still exited `0` |
| 6 | `benchmark.yml` benchmark queries | `branch_performance` queried a column that does not exist; the failure was hidden by instance 3, so a query that never ran still recorded a timing in every published benchmark |
| 7 | CI `Lint & Format` job | `ruff check … \|\| true` and `ruff format --check … \|\| true` |

Instance 7 in the format used for triage:

```text
Instance:       CI Lint & Format
Pattern:        ruff ... || true
Effect:         lint/format violations cannot fail the job
Classification: false-green / non-blocking gate
Status:         recorded, not fixed in TD-2
```

It is named as a gate and reports as green while enforcing nothing — the job has
been surfacing `BLE001`, `EXE001` and `DTZ005` findings as annotations for
months. Removing `|| true` would immediately turn a body of pre-existing debt
into a merge blocker, so it is a scope decision rather than a bug fix.

Instances 1–7 were each found by accident or by adding a verification step, never
by a check that looks for the pattern itself.

### Acceptance

```text
[ ] critical shell steps use set -euo pipefail or equivalent
[ ] no critical command is masked by || true
[ ] one-shot containers fail non-zero when any required command fails
[ ] pipeline exit status comes from the producer being verified
[ ] success logs are emitted only after post-condition verification
[ ] CI has a static contract test for known false-success patterns
```

The last item is the one that closes the loop. `tests/governance/` already hosts
static contract tests of this kind (`test_airflow_dag_contracts.py`), so the
pattern check belongs there rather than in review habit.

Related: the fifth instance is why the benchmark workflow verifies schemas after
`trino-init` exits `0`. Exit code is necessary, not sufficient — the post-condition
has to be queried from the platform.

---

## TD-3 — Secrets committed in the repository

**Status:** open (recorded at `portfolio-v1.1`, deliberately deferred to its own PR)

`docker/secrets/*.txt` and a hard-coded password in the Debezium connector
registration script are local development credentials, but they are real
credentials in version control.

### Acceptance

Values move to `.env` / a secret store, the files are removed from the working
tree, history rewriting is decided explicitly (rewrite or accept), and CI gains
a check that fails on new committed secrets.
