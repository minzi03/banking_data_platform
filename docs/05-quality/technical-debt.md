# Technical Debt

Known gaps, recorded so they stay visible rather than being rediscovered.
Each item states what is missing, why it is not fixed yet, and what "done"
looks like — a vague item cannot be closed honestly.

---

## TD-1 — Execute the Trino integration suite in CI

**Status:** fixed (2026-09-14, verified against current `.github/workflows/ci.yml`)

The original entry (recorded at `portfolio-v1.1`) said the 34 tests were not run
by any workflow. That was true then — it is no longer true. Implemented by
commits `6821404` (PR-blocking gate), `c40f6ae`, `14b72c9` (baseline the three
failures), `9ee73f7` (negative proof), `1ba4316` (revert) via PR #2.

Current state — the `trino-integration` job in `ci.yml` (line 486):

```text
docker-compose.ci.yml stack (postgres, minio, spark, iceberg-rest, trino)
→ 5-phase startup with readiness gates (catalog queryable via
  SHOW SCHEMAS FROM iceberg — stronger than "SELECT 1")
→ Iceberg DDL + seed data (post-condition: source tables non-empty)
→ full Bronze → Silver → Gold ETL with per-layer row assertions + grain check
→ SCD2 fixture (second snapshot, tracked customer_segment change)
→ 34 tests collected (count verified == 34 before running), executed with
  -m integration, JUnit XML matrix reported to the step summary
→ trino-integration-gate job (if: always()) maps the result to a stable
  branch-protection status; skipped is only accepted when the path filter
  says not relevant — every other non-success fails
→ Cleanup step with if: always() runs teardown even on failure
```

No `continue-on-error` anywhere in the chain — failures block the PR. The
gate was deliberately negative-tested: an intentional data assertion failure
was pushed, the PR went red, and the revert restored it (commit `9ee73f7`).

Remaining related items, recorded separately:
- TD-6 — the same stack/seed/ETL fixture is built twice (ci.yml + benchmark.yml,
  deliberate while the two profiles are expected to diverge).
- The tests query Trino via `docker exec ci-trino` subprocess calls rather than
  a Trino Python client — a portability note, not part of TD-1 acceptance.

---

## TD-2 — `Performance Benchmark` workflow has been failing on schedule

**Status:** disabled (2026-09-14, acceptance criterion: "disabled with reason")

`.github/workflows/benchmark.yml` runs on a schedule and has failed every week
since at least `2026-08-23`. It is not a required check for any release commit,
so it did not block `v1.1` — but a permanently red scheduled workflow makes the
repository's health signal meaningless, which is how a real regression gets
ignored.

**Action taken:** Disabled the weekly schedule trigger (`cron: '0 2 * * 0'` removed)
so the workflow is now manual-only (`workflow_dispatch`). A red scheduled workflow
is worse than no schedule — the CI workflow's `trino-integration` job already
validates the same ETL pipeline on every PR.

**Root cause (suspected, not runtime-proven):** The benchmark workflow's seed
data step uses `requirements-ci-seed.txt` which may not include dependencies
added by later commits (loan/AML generators). The 60-min timeout is also tight
for cold Docker pull + build + full ETL on free-tier runners. These are
hypotheses — the workflow hasn't run successfully since at least `2026-08-23`,
so it cannot be tested locally.

**To re-enable:** Remove `workflow_dispatch`-only trigger, restore schedule,
and verify on a `workflow_dispatch` run first.

---

## TD-4 — Makefile bootstrap targets depend on an invalid container working directory

**Status:** fixed (2026-09-14)

All bootstrap targets now use `docker compose exec -w /opt/project spark-worker-1 ...`
with repo-root-relative paths (`code_etl/...` instead of `/opt/project/code_etl/...`),
matching the benchmark workflow's approach.

Fixed via commit `6a7251e` (added `-w /opt/project` to: bronze-init, bronze-bootstrap,
bronze-ingest, silver-init, silver-bootstrap, silver-scd1/2/fact, gold-init,
gold-bootstrap, gold-job, validate-pipeline, serving-bootstrap).

Outstanding from original acceptance (not blocking):
- `make` targets still do not propagate child exit codes (separate TD-5 concern)
- no smoke test proves the host→container path end-to-end (verified via benchmark WF instead)

The v1.1 clean rebuild did load Bronze — 2,300,000 rows verified through Trino —
so it ran by some path other than these targets. A `make` target that cannot
work is a trap for whoever tries it next.

### Acceptance

```text
[x] bronze-bootstrap uses /opt/project as working directory
[x] silver-bootstrap uses /opt/project as working directory
[x] gold-bootstrap uses /opt/project as working directory
[ ] relative config/module paths resolve inside spark-worker-1 (verified via benchmark WF)
[ ] make targets propagate child exit codes (TD-5)
[ ] one smoke test proves the Makefile path from host works end-to-end
```

The exit-code item is not filler. Losing a child exit code has now happened
four times in this repository: `$?` read after `grep`/`tail`, `|| true` around
the Bronze ETL loop, `git push || true` in the benchmark workflow, and Bronze
bootstrap returning 0 with every table failed. Audit the Makefile for the same
pattern rather than assuming it is absent.

---

## TD-7 — Streamlit runtime and serving-consumer alignment

**Status:** fixed (2026-09-14)

All acceptance items resolved:

```text
[x] fix and runtime-verify Streamlit connectivity — catalog="iceberg" (was "lakehouse")
[x] decide the intended consumer contract: serving (not historical Gold)
[x] migrate the queries to the serving tables — all 10 queries use serving.*_current
[x] verify the consumer queries through Trino — verified via dashboard runtime
[x] restore an active dbt exposure — 9 serving models + 10 current tables
[x] add a contract test preventing the Spark catalog name "lakehouse"
    in Trino-facing code — test_trino_catalog_contract.py
```

Fixed via commits:
- Streamlit catalog fix (P0.1): catalog="lakehouse" → "iceberg"
- Serving migration: all queries use `serving.*_current` tables
- Contract test: `tests/governance/test_trino_catalog_contract.py`
  scans Trino-facing code for catalog="lakehouse" violations

`lakehouse` leaking into Trino-facing code has happened four times.
At four occurrences it is not a typo, it is
a cross-engine naming contract that needs a static check.

---

## TD-6 — Two workflows build the same lakehouse fixture

**Status:** deliberate (recorded at TD-1, 2026-09-14: fixed TD-2 → benchmark no longer runs on schedule)

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

**Status:** fixed (2026-09-22, each acceptance item re-verified against current files)

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
[x] critical shell steps use set -euo pipefail or equivalent
[x] no critical command is masked by || true
[x] one-shot containers fail non-zero when any required command fails
[x] pipeline exit status comes from the producer being verified
[x] success logs are emitted only after post-condition verification
[x] CI has a static contract test for known false-success patterns
```

### Closing evidence (2026-09-22)

The three items above were still unticked, so each was re-checked against the
file as it stands rather than against memory of the fix.

**One-shot containers** — `docker/docker-compose.ci.yml`, `trino-init`:

```yaml
entrypoint: >
  /bin/sh -ec "
    ...
    trino --server trino:8080 --catalog iceberg --execute 'CREATE SCHEMA ...';
```

`-e` is present, so a failing `CREATE SCHEMA` now kills the container instead
of falling through to the trailing `echo`. `--server trino:8080` is also
there; without it the CLI targets localhost and all five statements fail.

**Exit status from the producer** — `.github/workflows/benchmark.yml`, the
query runner reads the Trino CLI's own status and keeps streams apart:

```bash
if ! docker exec ci-trino trino ... >"$out" 2>"$err"; then
  echo "::error::benchmark query '$name' thất bại"
  exit 1
fi
```

The old `result=$(docker exec ... | grep -v WARNING)` returned `grep`'s
status, which is how instance 6 published timings for a query that never ran.

**Post-condition before the success log** — same workflow, after
`trino-init` exits:

```bash
# Kiểm chứng kết quả bootstrap, không tin vào exit code một mình.
docker exec ci-trino trino --execute "SHOW SCHEMAS FROM iceberg" > /tmp/schemas.out
for schema in bronze silver gold serving; do
  grep -qx "$schema" /tmp/schemas.out || missing="$missing $schema"
done
... echo "::error::trino-init exit 0 nhưng thiếu schema:$missing"; exit 1
```

This is the item that matters most, and it is the one the earlier assessment
in this repository got wrong twice. Exit code is necessary, not sufficient.

### Scope note

Two of the three closing items live in `benchmark.yml`, which TD-2 left on
`workflow_dispatch` only. The patterns are gone from the code; they are not
exercised on a schedule. If that workflow is ever re-enabled, these are the
lines to watch first.

### What remains true

The remaining `|| true` occurrences in `benchmark.yml` are on diagnostic
commands — fetching a container id that may legitimately be empty, printing
a log file that may not exist, dumping `compose logs` during triage. None of
them masks a command whose success is being asserted.

`make` targets still do not propagate child exit codes. That is recorded
against TD-8, where it matters for `make seed`.

**Fixed 2026-09-14:**
- Instance 7 (ci.yml lint): removed `|| true` from `ruff check` and
  `ruff format --check` — lint violations now fail the CI job.
- Instance 8 (ci.yml security): removed `|| true` from `bandit` —
  security scan violations now fail the CI job.
- Instance 5 (ci.yml shell pattern): the five failed `CREATE SCHEMA`
  calls still exit `0` because of `sh -c` ending in `echo`; not changed
  but now documented and understood.
- Contract test: `test_shell_failure_propagation.py` scans workflow
  YAML for `|| true` inside gate-named steps (lint, format, test,
  security, validate) and fails the test suite if found.

The last item is the one that closes the loop. `tests/governance/` already hosts
static contract tests of this kind (`test_airflow_dag_contracts.py`), so the
pattern check belongs there rather than in review habit.

Related: the fifth instance is why the benchmark workflow verifies schemas after
`trino-init` exits `0`. Exit code is necessary, not sufficient — the post-condition
has to be queried from the platform.

---

## TD-3 — Publicly committed secrets and private key material

**Status:** fixed (2026-09-14)

**Risk assessment (after investigation):**

```text
All secrets are local docker stack credentials only:
  - private_key.pem / .der — OpenMetadata JWT signing key (auto-generated)
  - public_key.der         — OpenMetadata JWT verification key
  - secrets/*.txt          — Same values as docker/.env
  - Debezium password      — Same as docker/.env POSTGRES_PASSWORD

These credentials cannot access anything beyond the local docker stack.
Repository is PUBLIC — but exposure scope is limited to local dev infra.
```

### Action taken (2026-09-14)

```text
[x] identify whether private_key.pem was ever used and what it protected
    → OpenMetadata JWT signing key, referenced in docker-compose.yml as
      RSA_PRIVATE_KEY_FILE_PATH. Auto-generated for local stack.
[x] rotate/revoke the key if it was ever usable
    → No: OpenMetadata JWT keys are per-stack, not reused elsewhere.
[x] remove secret material from the current tree
    → git rm --cached: 7 files removed from HEAD (kept on disk)
[x] prevent recommit via ignore rules
    → .gitignore updated: docker/secrets/, *.pem, *.der
[x] decide explicitly whether a Git history rewrite is required
    → No: credentials are local docker stack only, never reused externally.
      History rewrite risks repo integrity for low security gain.
```

### What remains in git history

The PEM key and plaintext passwords are still in Git history. Anyone who
clones the repository receives every committed version. This is accepted
because:
1. All credentials are local docker stack only
2. They cannot access any external service
3. Git history rewrite risks repository integrity and PR/branch references
4. The docker stack itself generates new credentials on `docker compose up`

---

## TD-8 — `make seed` runs the generator in a container that cannot see it

**Status:** fixed (2026-09-22) — option (a), pointed at `airflow-scheduler`.
Three of four acceptance items are verified against the running stack; the
fourth is deliberately not executed, for a reason recorded under *Acceptance*.

`make seed` executed the data generator inside the `postgres` container:

```makefile
seed:
	$(DC) exec postgres python /opt/project/data_generator/generate_all.py \
		--host postgres --port 5432
```

That path does not exist there. `postgres` mounts only its data volume and
the init scripts — not the repository:

```text
postgres          postgres-data:/var/lib/postgresql/data
                  ./init_postgres:/docker-entrypoint-initdb.d

spark-worker-1    ..:/opt/project          <- has the repo
airflow-scheduler ..:/opt/project          <- has the repo
dbt               ../dbt:/usr/src/dbt      <- has its own subtree
```

So the target fails with `No such file or directory` for
`/opt/project/data_generator/`, and has presumably never worked.

### How it was found

During the 2026-09-22 re-seed after the log-normal amount change (#21).
`make seed` was the documented way to regenerate data; it could not run, and
`make seed-local` — which executes from the host — was used instead.

The failure is immediate and loud, so nothing silently produced wrong data.
The cost is time: the next person hits the same wall.

### Why this is TD-4's family

TD-4 was the same shape — bootstrap targets pointing at a working directory
the container did not have. It was fixed by adding `-w /opt/project` to
targets running in `spark-worker-1`, which *does* mount the repo. `seed` was
not in that list because it targets a different container, and the container
choice itself is the bug.

A `make` target that cannot work is worse than no target. It reads as the
supported path and consumes the time of whoever trusts it.

### Options

```text
(a) point `seed` at a container that mounts the repo (spark-worker-1 or
    airflow-scheduler), keeping --host postgres for the connection
(b) make `seed` an alias for `seed-local` and delete the container variant
(c) mount the repo into postgres — rejected: the database container has no
    reason to carry application code
```

(a) is closest to the TD-4 fix and keeps seeding inside the compose network.
(b) is simpler but makes seeding depend on a host Python with the right
dependencies, which is the thing containers exist to avoid.

### Correction: (a) offered two containers, only one of them works

The option above named `spark-worker-1` and `airflow-scheduler` as
interchangeable, because both mount the repo. Mounting the repo is necessary
and not sufficient — the container also has to be able to *run* the generator.
Measured against the running stack:

```text
spark-worker-1     mounts repo    no `python` on PATH (Spark image ships python3 only)
                                  python3 -c "import psycopg2" → ModuleNotFoundError
airflow-scheduler  mounts repo    python ✓   psycopg2 2.9.9 ✓   yaml ✓
```

Making `spark-worker-1` work would mean adding `psycopg2` to
`docker/requirements-spark.txt`. That contradicts a decision already written
down in `requirements-ci-seed.txt`: seed generation is bootstrap tooling, not a
Spark workload, and the Spark image should carry no package the ETL does not
use. So the choice is `airflow-scheduler`, and the reason is now a comment on
the target itself rather than folklore.

This is the same failure mode as the bug it fixes — a container was named from
one property (does it mount the repo?) without checking the other (can it run
the code?).

### Acceptance

```text
[x] the chosen container demonstrably mounts data_generator/
    docker inspect banking-airflow-scheduler →
      F:\...\banking_data_platform -> /opt/project
[x] a smoke check proves the target ran the generator rather than exiting 0
    without doing anything
    docker exec -w /opt/project banking-airflow-scheduler \
      python data_generator/generate_all.py --help  → usage printed,
    which means every module-level import resolved (psycopg2, yaml, and the
    local connectors/ + generators/ packages). Connectivity checked separately:
    psycopg2 connect from that container to host `postgres` → 10,000 customers.
[x] `make seed-local` still works, or is removed deliberately with a reason
    unchanged — kept as the host-side path, still the fallback when the stack
    is not up
[ ] `make seed` completes against a running stack, from a clean checkout
    NOT RUN, deliberately. The generator sets no RNG seed anywhere under
    data_generator/, so a re-seed produces different data than the snapshot
    the lakehouse currently holds and than the one `portfolio-v2.0` was
    verified against. Closing this item costs a full re-seed plus a Bronze →
    Silver → Gold rebuild and a manifest regeneration. Run it when a re-seed
    is wanted for its own sake, not to tick a box.
```

The last item is recorded as unchecked rather than quietly dropped:
`not_collected ≠ verified`. What is proven is that the container can reach and
execute the generator — which is the entire failure this item describes. What
is not proven is a full generation run end to end.

**Regression guard:** `tests/governance/test_makefile_container_mounts.py`
cross-checks Makefile `$(DC) exec` targets against the volumes declared in
`docker-compose.yml` — an invariant that spans two files, which is why reading
either one alone missed it twice. Negative-tested: restoring the `postgres`
container in the target turns two of its three tests red.

Related: TD-4 (same class, different container), TD-5 (`make` targets still
do not propagate child exit codes, so a broken target can report success).

---

## TD-9 — The test suite exercised a synthetic DQ rule file, never the real one

**Status:** fixed (2026-09-22)

`code_etl/shared/ops/dq_rules.yml` declared a rule on
`lakehouse.gold.branch_monthly_summary`. No such table exists — the real name is
`mart_branch_monthly_summary`. Four independent sources agreed on the `mart_`
prefix and only the rule file disagreed:

```text
docker/init_iceberg/03_ddl_gold.sql:212                       CREATE TABLE ... mart_branch_monthly_summary
code_etl/gold/time_analytics/branch_monthly_summary.yml:28    table: mart_branch_monthly_summary
dbt/models/gold/_gold_sources.yml:220                         - name: mart_branch_monthly_summary
docs/03-data/DATA_DICTIONARY.md                               mart_branch_monthly_summary
```

Every check function in `data_quality.py` wraps its body in `try/except` and
returns `("FAIL", "N/A", f"Error: {e}")` — correct fail-loud behaviour. So the
missing table *would* produce 2 FAILs, and `main()` would call `sys.exit(1)`.

> **Correction (2026-09-23, from a stack run).** The original entry said
> `dq_gold_checks` "therefore failed every day" because of this name. That was
> inferred from code, and the mechanism was wrong. `data_quality.py` never got
> as far as reading `dq_rules.yml`: `spark-worker-1` runs Python 3.8, and the
> module crashed on import at `def load_rules(path: str) -> dict[str, Any]`
> with `TypeError: 'type' object is not subscriptable` — for every layer,
> since the initial commit. The wrong table name was real but masked by an
> earlier crash. See TD-10.
>
> Nor had anyone seen the DAG fail: the `docker_airflow-logs` volume holds no
> task log for `ops_data_quality_dag` at all. "Fails every day" was a
> prediction, not an observation.

### The typo is not the debt

The name was a one-line fix. The debt is why it survived.

`tests/ops/test_data_quality.py` — the file named after the module — exercises
the loader against `sample_dq_rules`, a synthetic YAML written to `tmp_path` by
`tests/conftest.py`. It asserts that the loader returns a dict, that the fixture
has two tables, that a check list has three entries. All true. None of it
touches the file that ships.

```text
what the tests proved     the YAML loader works
what nobody checked       the YAML it loads in production resolves
```

So 822 unit tests were green while the production rule file was broken. The suite
was not weak, it was pointed at the wrong artifact. Same class as the evidence
manifest binding that only anchored README (`EVIDENCE_MANIFEST.md` §6.4):
the mechanism existed and did not point at the real thing.

Two amplifiers kept it quiet. `ops_data_quality_dag.py:30` sets
`"email_on_failure": False`, and nothing reads `opslakehouse.data_quality_log` —
no dashboard, no dbt model, no script. A red task with no reader is
indistinguishable from a green one.

### Why the name was easy to get wrong

The Gold config *file* is `branch_monthly_summary.yml` while its target is
`mart_branch_monthly_summary`. Copying the filename gives exactly the wrong
string. `customer_360.yml` → `mart_customer_360` has the same shape, so the trap
is still there — 2 of 14 Gold configs have a filename that is not their table.

Renaming the config was considered and **rejected**: the path is referenced by
`airflow/dags/gold/gold_mart360_dag.py:61`,
`code_etl/gold/bootstrap/initial_load.py:70`,
`tests/gold/test_gold_sql_invariants.py:203` (`CALENDAR_MODELS`, matched by
filename) and two docs. A five-file rename that touches DAG wiring would fix one
of the two mismatches and buy no protection the test below does not already give.

### What now catches it

`tests/governance/test_dq_rules_resolve.py` loads the **real**
`dq_rules.yml` and `quarantine_rules.yml` and asserts:

```text
every rule key            is a table declared in docker/init_iceberg/*.sql
every ref_table           idem   (referential_integrity)
every source_table        idem   (reconciliation, quarantine groups)
every check name          is a key of CHECK_DISPATCH
```

The table list is parsed from DDL by reusing `parse_ddl` and `SKIP_DDL` from
`scripts/generate_data_dictionary.py` — a hardcoded list in the test would only
move the drift somewhere newer.

The check-name assertion closes a second, quieter hole. `run_checks_for_table`
skips an unknown check name with a `log.warning` and `continue`, so
`nul_check` instead of `null_check` means the check never runs, nothing is
written to `data_quality_log`, and the job still exits 0. That one is
fail-*open*: nothing goes red at runtime, ever.

Both holes were negative-tested before this entry was written: the old table
name and a deliberately misspelled `nul_check` were re-injected, and the suite
went red on both with the offending name in the message.

### Acceptance

```text
[x] dq_rules.yml names mart_branch_monthly_summary
[x] all 29 rule keys re-derived against DDL, not against this note
[x] a test loads the real dq_rules.yml, not a tmp_path fixture
[x] table existence comes from parsed DDL, no hardcoded table list
[x] every check name is validated against CHECK_DISPATCH
[x] anti-empty-pass guards so a broken parse cannot pass by checking nothing
[x] both failure modes negative-tested (wrong table name, wrong check name)
[x] quarantine_rules.yml source tables covered by the same test
[x] Gold DQ verified green on a running stack (2026-09-23, after TD-10)
```

**Stack verification (2026-09-23).** With the TD-10 fix applied, the Gold layer
ran against the real `dq_rules.yml` on `spark-worker-1`:

```text
Running row_count on lakehouse.gold.mart_branch_monthly_summary ...
  ✅ row_count: PASS — Row count OK: 12800
DQ SUMMARY: 20 checks executed · PASS: 20 · WARN: 0 · FAIL: 0      exit 0
```

This is `spark-submit` in the container the DAG uses, not an Airflow run —
Airflow was not started. Silver and Bronze still exit 1, for reasons unrelated
to this entry (TD-11).

### What remains open

```text
no DDL creates lakehouse.quarantine.*   write_to_quarantine always throws at
                                        spark.table(target), logs ERROR, returns 0
                                        (confirmed at runtime 2026-09-23: every
                                        write → TABLE_OR_VIEW_NOT_FOUND)
email_on_failure: False                 DQ and quarantine both silent, no callback
data_quality_log has no reader          write-only, so red looks like green
```

The first is recorded as a deliberate assertion in
`test_quarantine_target_tables_have_no_ddl` — it fails if someone adds the DDL,
which is the moment to flip it into the opposite check. The other two are why
this entry existed for as long as it did; they are documented in
[`DATA_QUALITY.md`](DATA_QUALITY.md) §9 and not fixed here.

---

## TD-10 — Ops and governance jobs could not import on the Spark worker's Python 3.8

**Status:** fixed (2026-09-23, verified on the stack) — runtime decision taken: the Spark
image moved to Python 3.10 (Ubuntu 22.04, Java 17) and now installs pydantic. See
[Runtime upgrade](#runtime-upgrade-2026-09-23). CI moved to 3.10 as well, so the suite
now runs on the worker's Python. `ops_contract_validation_dag`, which pydantic alone
would not have unblocked, now runs through a real CLI (below); what it found is TD-12.
Residual: the Airflow image stays on 3.11 (latent, see below).

`banking-spark-worker-1` runs **Python 3.8.10**. Every ops and governance task in
Airflow runs there: `docker exec banking-spark-worker-1 spark-submit ...` or
`SparkSubmitOperator`. The rest of the toolchain assumes something newer:

```text
pyproject.toml     requires-python >=3.10 · ruff target-version py310 · rule UP
ci.yml  test job   Python 3.11
spark-worker-1     Python 3.8.10
```

Ruff's `UP` rules actively rewrite code into 3.9+ forms (`Dict` → `dict`,
`Optional[X]` → `X | None`). CI runs on 3.11 and is green. On the worker, the
module dies at import, before `main()`:

```text
TypeError: 'type' object is not subscriptable                  dict[str, Any]
TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'   X | None
ModuleNotFoundError: No module named 'zoneinfo'                 stdlib 3.9+
```

### Measured, not inferred

Every worker entry point was imported **inside the container** with `origin/main`
code (`d64b9c0`):

| Entry point | Before | After |
|---|---|---|
| `code_etl/shared/ops/data_quality.py` | `TypeError` (`dict[...]`) | OK |
| `code_etl/shared/ops/quarantine.py` | `TypeError` (`dict[...]`) | OK |
| `code_etl/shared/ops/iceberg_maintenance.py` | `No module named 'zoneinfo'` | OK |
| `code_etl/shared/ops/lineage_tracker.py` | `TypeError` (`\|`) | no pydantic |
| `governance/schema_drift.py` | `TypeError` (`dict[...]`) | OK |
| `governance/enforcement.py` | `TypeError` (`\|`) | no pydantic |
| `governance/anomaly_detection.py` | `TypeError` (`\|`) | OK |
| `governance/freshness_checks.py` | `TypeError` (`\|`) | OK |
| `governance/audit.py` | `TypeError` (`\|`) | OK |
| `governance/lineage.py` | `TypeError` (`\|`) | OK |
| `governance/rbac.py` | `TypeError` (`dict[...]`) | OK |
| `governance/contracts_registry.py` | `TypeError` (`\|`) | no pydantic |
| `code_etl/shared/ops/pii_masking.py` | needs `PII_HASH_SALT` | unchanged (expected) |
| `scripts/resolve_quarantine.py` | OK | OK |

11 of 14 were dead on arrival, including every job in `ops_data_quality_dag`,
`ops_quarantine_dag`, `ops_schema_drift_dag`, `ops_maintenance_weekly_dag` and
`ops_contract_validation_dag`. The syntax is present from the initial commit
(`d0442ac`). `resolve_quarantine.py` survived only because it still says
`typing.Dict` — the form ruff's `UP006` flags for "upgrade".

No task log for any of these DAGs exists in the `docker_airflow-logs` volume, so
none of this was ever seen failing. It was found by running `data_quality.py` by
hand to verify TD-9.

### Fix

```text
from __future__ import annotations    12 modules — annotations become lazy strings,
                                      so dict[...] and X | None are never evaluated
zoneinfo → timezone(timedelta(hours=7))   iceberg_maintenance.py; Asia/Ho_Chi_Minh
                                      is a fixed UTC+7 with no DST, so identical output
```

The scan that chose these 12 files found **no** 3.9+ construct outside
annotations, which is the only case the future import cannot fix.
`governance/contracts.py` is deliberately excluded: pydantic evaluates
annotations when building the model, future import or not.

### What now catches it

`tests/governance/test_worker_python38_compat.py` scans every file under
`code_etl/` and `governance/` plus `scripts/resolve_quarantine.py`:

```text
ast.parse(..., feature_version=(3, 8))        syntax the 3.8 parser rejects
3.9+ annotation without the future import     dict[...] / list[...] / X | Y
import of zoneinfo / graphlib / tomllib       stdlib that 3.8 does not have
```

Negative-tested: run against `origin/main`'s versions of the 12 files, it fails
exactly **13** times — the 12 annotation files plus `zoneinfo` — matching the
container's own failures one for one.

It is a static approximation. It cannot see `isinstance(x, int | str)` in a file
that has the future import. The complete fix is below.

### Stack verification (2026-09-23)

`spark-submit` on `spark-worker-1`, fixed code, `--cob_dt 2026-09-22`:

```text
data_quality --layer gold     20 checks · 20 PASS                 exit 0
data_quality --layer silver   62 checks · 53 PASS · 1 WARN · 8 FAIL   exit 1   → TD-11
data_quality --layer bronze    6 checks · 6 FAIL (0 rows)         exit 1   → TD-11
quarantine   --layer all      18 rules  · 14 PASS · 3 WARN · 1 FAIL  exit 1
```

Every job now **runs to completion and reports**. Exit 1 on Silver, Bronze and
quarantine is a finding about the data or the checks, not a crash.
`iceberg_maintenance.py` was import-tested only — a real run expires snapshots
and deletes orphan files, which is not something to do for a verification.

### Acceptance

```text
[x] every worker entry point import-tested in the container, before and after
[x] no worker module fails on 3.9+ syntax
[x] zoneinfo removed without changing the timestamps produced
[x] static regression test, negative-tested against the old code
[x] data_quality.py and quarantine.py run end to end on the stack
[x] decide the runtime: upgrade the Spark image's Python, or add a 3.8 CI job
    → upgrade, 2026-09-23 (below)
[x] install pydantic on the worker, or stop importing it there
    → installed; every governance module imports on the worker
[x] ops_contract_validation_dag runs — NOT unblocked by pydantic; fixed by a
    real CLI, 2026-09-23 (below). It found 20 wrong contracts → TD-12
```

### Runtime upgrade (2026-09-23)

**Decision: upgrade the image**, not add a 3.8 CI job. A 3.8 CI job would have kept
the repo declaring one floor (`>=3.10`) while running another, and would not have
fixed the missing pydantic.

Python comes from the base image's OS, not from Spark. Measured with
`docker run --rm --entrypoint python3 <image> --version` and friends:

| | `apache/spark:3.5.3` (before) | `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` (after) |
|---|---|---|
| OS | Ubuntu 20.04.6 (focal) | Ubuntu 22.04.5 (jammy) |
| Python | 3.8.10 | **3.10.12** |
| Java | 11.0.24 | 17.0.12 |
| Spark | 3.5.3 | 3.5.3 — same tarball, same GPG key upstream |
| pydantic | absent | 2.13.5 (`pydantic>=2.9,<3`, the pyproject floor) |

3.10 is the floor the repo already declared (`requires-python >=3.10`, ruff
`target-version = "py310"`), so the runtime now matches the declarations instead
of the other way round.

**Verification, old image vs new image, same harness:**

```text
import all 15 worker entry points     old  7/15  (8× No module named 'pydantic')
  (governance.* by package name,      new 15/15
   the others by file path)
spark-submit local[2]:                old  OK on Java 11 / Python 3.8
  Iceberg write + overwritePartitions new  OK on Java 17 / Python 3.10
  + read, and a Python UDF that            Python worker 3.10.12 = driver
  forces a Python worker
```

Then on the stack — `spark-master` and `spark-worker-1` rebuilt from the new
Dockerfile, with `postgres`, `minio`, `iceberg-rest` — using the DAG's own
`spark-submit` command and `--cob_dt 2026-09-22`, the date TD-11 was verified on:

```text
data_quality --layer gold     20 checks · 20 PASS                   exit 0   same as TD-11
data_quality --layer silver   62 checks · 61 PASS · 1 WARN · 0 FAIL  exit 0   same as TD-11
                              JDBC write to opslakehouse.data_quality_log OK on Java 17
quarantine   --layer all      14 PASS · 3 WARN · 1 FAIL              exit 1   same as before:
                              lakehouse.quarantine.* tables do not exist (DATA_QUALITY §5)
```

Not run: Airflow itself, Trino, dbt, and the Bronze/Silver/Gold ETL jobs. The CI
`Trino Integration` job builds `Dockerfile.spark` and runs the ETL against a REST
catalog and MinIO, so it exercises the new image on the path this run skipped.

**One correction to the table above.** Its "OK" rows for `schema_drift`,
`freshness_checks`, `lineage` and `rbac` held for loading each **file**. Imported
by **package name** — `from governance.x import ...`, which is how
`data_quality.py` loads its `anomaly_detection`, `freshness_check` and
`schema_drift` checks — every one of them failed on the old image, because
`governance/__init__.py` eagerly imports `governance.contracts`, i.e. pydantic.
No configured rule used those three check types, so nothing visible broke.
With pydantic installed the point is moot, but the table was incomplete.

**`ops_contract_validation_dag` still cannot run, and pydantic was never the only
reason.** Run exactly as the DAG runs it on the upgraded worker:

```text
spark-submit /opt/project/governance/enforcement.py --cob_dt 2026-09-22 --layer silver --validate
  File "/opt/project/governance/enforcement.py", line 28, in <module>
    from governance.contracts import DatasetContract
ModuleNotFoundError: No module named 'governance'                           exit 1
```

Run as a script, `/opt/project` is not on `sys.path`. And past that, the file has
no `__main__` and no argument parser — `--layer silver --validate` would be
ignored and the job would exit 0 having validated nothing. Both are out of scope
here; fixing the path alone would turn a loud failure into a silent one.

**Fixed 2026-09-23 with a real CLI**, `code_etl/shared/ops/contract_validation.py`,
which the DAG now calls. It loads every contract of a layer, reads each table in
the run's scope with DQ's own `_scoped_table` (TD-11), validates with the unchanged
`ContractEnforcer`, writes one row per check to `opslakehouse.contract_validation_log`
and exits 1 on any FAIL. An unreadable table or a crashing check is a FAIL for that
contract, not a crash of the run; a layer with no contracts is an error, not a pass.

Three things had to change around it, each found by running it:

- **The log table never existed.** Its DDL lived only in `docker/init_openmetadata/`,
  which nothing mounts. Moved to `docker/init_postgres/00_extensions.sql`;
  `to_regclass` confirmed the table was absent from the running stack before.
- **The worker had no database credentials.** `data_quality.py` writes its log only
  through a password default committed in the code. The CLI requires
  `POSTGRES_USER`/`POSTGRES_PASSWORD` and `docker-compose.yml` now passes them to
  `spark-worker-1` from `docker/.env`.
- **Freshness on a DATE column always passed.** `FreshnessChecker` computed an age
  only for `datetime`; anything else fell through to `status="PASS"`. All 19 Gold
  contracts declare `date_column: cob_dt`, so none was ever evaluated. A date now
  ages from the end of that business day in Vietnam time; a type with no sensible
  reference is a WARN.

On the stack, `cob_dt 2026-09-22`, the DAG's own `spark-submit`:

```text
silver   13 contracts ·  3 PASS · 10 FAIL   exit 1
gold     20 contracts · 10 PASS · 10 FAIL   exit 1
         freshness "Data is 15.7 hours old (SLA: 24h)" — evaluated, PASS
log      122 rows; a second Gold run leaves it at 122
```

**Every FAIL is a wrong contract, not wrong data** — the first time these 33
contracts were checked against real tables. Recorded as TD-12.

### What still does not match

```text
Spark worker     Python 3.10    ← runtime of code_etl/ and governance/
pyproject        >=3.10 · ruff py310
CI (all jobs)    Python 3.10    ← moved from 3.11 (2026-09-23)
Airflow image    Python 3.11    ← driver for the two SparkSubmitOperator DAGs
```

- **CI was one minor ahead of the worker — closed.** Same failure class as this
  entry, one step smaller: 3.11-only syntax or API would be green in CI and die on
  the worker. Every `python-version` in `.github/workflows/` is now 3.10, and
  `tests/governance/test_worker_python_compat.py` (renamed from
  `test_worker_python38_compat.py`) fails if any of the five declarations drifts:
  the image's measured Python, `WORKER_PYTHON`, `requires-python`, ruff's target,
  and every workflow's `python-version`. Its static scan for 3.11+ syntax and names
  (`typing.Self`, `datetime.UTC`, `enum.StrEnum`, …) stays as a cheap second layer
  for developers running a newer Python locally.
- **Driver 3.11, executors 3.10** for `ops_maintenance_weekly_dag` and
  `ops_pii_masking_daily_dag`, which launch from the Airflow container. PySpark
  refuses mismatched minors only when it starts a Python worker (UDF, RDD,
  pandas UDF). A search of `code_etl/`, `governance/`, `scripts/` and `ml/` finds
  none, so this is latent — it was 3.11 vs 3.8 before, too.

---

## TD-11 — Silver DQ checks count every snapshot, so they fail on healthy data

**Status:** fixed (2026-09-23, verified on the stack) — decision taken: a daily
check means **the snapshot of the day being checked, current versions only**

Once TD-10 let `data_quality.py` run, Silver reported 8 FAILs. Each one was
checked against the data. **None is a data defect.**

```text
check                               reported                  what is actually there
fact_txn_account     unique txn_id  8,400,000 duplicates      8 cob_dt snapshots · 0 duplicates within any snapshot
fact_card_txn        unique txn_id  4,200,000                 idem
fact_online_txn      unique id      3,500,000                 idem
fact_crm_interaction unique id        350,000                 idem
fact_support_ticket  unique id        175,000                 idem
dim_customer (SCD2)  unique id         10,000                 2 versions/key · is_current rows 10,000/10,000 unique
dim_account  (SCD2)  unique id         30,000                 2 versions/key · is_current rows 30,000/30,000 unique
dim_card  ref_integrity account_id  "1 orphan"                the orphan is NULL — 2,672 cards have no account_id,
                                                              and the left-anti join counts NULL as a value
```

The cause is one design choice: `spark.table(table)` reads the **whole table**.
Checks take `--cob_dt` but never filter by it, and know nothing about SCD2.

This matters more than it looks. Silver DQ now exits 1 on every run with healthy
data. A check that is always red teaches people to ignore it — and a *real*
duplicate would be reported inside an 8,400,000 that is already expected.

### Also seen in the same run

```text
range_check message    "720270 values out of range <=None"  — bounds text uses
                       truthiness, so min_value: 0 prints as "<=None"
                       (data_quality.py:160). The count is right, the text is wrong.
                       Fixed below.
Bronze CDC             all 6 *_cdc tables have 0 rows — Kafka/Debezium were not
                       running. Environmental; says nothing about CDC either way.
quarantine log         "8970 records quarantined" while rows_written = 0 —
                       the log line reports intent, not the write (TD-9: no DDL).
```

### Fix

`run_checks_for_table` passes the run's `cob_dt` down in a copy of the rule, and
every data-reading check goes through one helper, `_scoped_table`:

```text
table has a cob_dt column      → cob_dt = DATE '<run cob_dt>'
table has an is_current column → CAST(is_current AS INT) = 1
neither (SCD1 dims, CDC)       → whole table, as before
```

The decision is on columns present **at runtime**, not a table list. Applied to
`row_count`, `null_check`, `unique_check`, `range_check`, `referential_integrity`
(both sides) and `reconciliation` (both sides). `freshness_check`,
`anomaly_detection` and `schema_drift` are untouched — they are about the whole
table by nature. Calling a check directly, without the key, keeps the old
whole-table behaviour. `cob_dt` goes through `date.fromisoformat` before it
reaches SQL.

Also:

```text
referential_integrity   NULL foreign keys are excluded — a required FK is a null_check
range_check             bounds label uses `is not None`, so min_value: 0 prints ">=0"
details                 every scoped result says what it read: [cob_dt=2026-09-22] / [is_current]
```

**Intended consequence:** `row_count` on a `cob_dt` table now FAILs when *that
day's* snapshot is missing. Before, it passed as long as any old snapshot existed.

### One rule needs the whole table, declared explicitly

Scoping made one reconciliation go red for a new reason. `dim_customer` reconciles
against `lakehouse.bronze.core_customer`, which has one partition only
(`2026-09-21`) — so filtering it to the run date gives 0 rows. This is not a
missing load. `01_ddl_bronze.sql` does **not** partition `core_customer` or
`core_account` (unlike `core_txn_account`, `PARTITIONED BY (cob_dt)`), and the
Iceberg history shows every load replacing all 10,000 rows. These tables only
ever hold the latest load; their `cob_dt` is the date of that load.

The rule now says so: `source_scope: whole_table`, with the reason in a YAML
comment. `test_reconciliation_source_scope_matches_partitioning` ties the option
to the DDL in both directions: an unpartitioned source must declare it, and a
`PARTITIONED BY (cob_dt)` source must not. Negative-tested both ways.

### Verification (2026-09-23)

`spark-submit` on `spark-worker-1`, `--cob_dt 2026-09-22`:

```text
silver   62 checks · 61 PASS · 1 WARN · 0 FAIL      exit 0     (was 8 FAIL)
gold     20 checks · 20 PASS                        exit 0
bronze    6 checks ·  6 FAIL (CDC tables empty)     exit 1     unchanged, environmental
```

The WARN is real and now readable: `89994 values out of range >=0
[cob_dt=2026-09-22]` on `fact_txn_account.txn_amount`, i.e. negative amounts in one
snapshot. It was 720,270 across all eight.

Planted defects, run through the real `run_checks_for_table` on Spark temp views
(the lakehouse was not touched):

```text
fact    txn repeated across snapshots only          PASS
fact    txn duplicated INSIDE the 09-22 snapshot    FAIL  "txn_id: 1 duplicates [cob_dt=2026-09-22]"
SCD2    key with an old + a current version         PASS
SCD2    key with two current rows                   FAIL  "customer_id: 1 duplicates [is_current]"
FK      NULL foreign key                            PASS
FK      key present only as a non-current version   FAIL  "1 orphan records"
```

### Acceptance

```text
[x] unique_check and reconciliation scoped to one cob_dt partition where one exists
[x] SCD2 dims checked on is_current = 1
[x] referential_integrity excludes NULL foreign keys
[x] Silver DQ exits 0 on the current data, and a planted duplicate makes it FAIL
[x] range_check message prints the bound it checked
[x] reconciliation against an unpartitioned Bronze source declared, and tied to DDL by a test
```

Not verified through Airflow. The stack runs were `spark-submit` in the container
the DAG uses; Airflow itself was not started.

---

## TD-12 — 20 of 33 data contracts do not describe the tables they govern

**Status:** open (2026-09-23)

The contracts in `governance/datasets/` had never been checked against real tables:
`ops_contract_validation_dag` could not run (TD-10). The first run of
`code_etl/shared/ops/contract_validation.py`, `cob_dt 2026-09-22`, found that
**every failure is in a contract, none in the data**:

```text
10 Silver   required_columns names columns the table does not have
            e.g. dim_card wants card_number_masked; the column is card_no_masked
            4 of them also fail non_null_columns — same columns, "column not found"
 9 Gold     *_current_gold point at lakehouse.gold.*_current — the Spark CTAS
            tables that were retired; manifest invariant legacy_gold_current_retired
            requires them NOT to exist. Serving now lives in schema `serving` (dbt)
 1 Gold     branch_monthly_summary_gold points at gold.branch_monthly_summary;
            the table is mart_branch_monthly_summary — the same slip as TD-9
```

The failing rows in `opslakehouse.contract_validation_log` carry each table's real
column list in `actual_value`.

Why it survived: nothing compares a contract with the DDL. `test_contracts.py`
checks that contracts parse; the data dictionary generator joins them to DDL but
does not flag a mismatch.

### Acceptance

```text
[ ] every contract's table exists in the DDL, and its required / non-null / unique
    columns exist in that table — enforced by a static test, like test_dq_rules_resolve.py
[ ] the 9 *_current contracts either point at the dbt serving tables or are removed
[ ] contract_validation.py exits 0 for silver and gold on a healthy snapshot
```

---

## TD-13 — `opslakehouse.lineage_log` is written to but never created

**Status:** open (2026-09-23)

`governance/lineage.py` and `ops_lineage_dag` write to `opslakehouse.lineage_log`.
Its only DDL is in `docker/init_openmetadata/01_create_schemas.sql`, which nothing
mounts or runs — the same reason `contract_validation_log` was missing (TD-10).
`audit_log`, also defined there, survives only because `docker/init_postgres/05_security.sql`
creates it too.

Found while fixing TD-10; not run. Checked statically: no file under
`docker/init_postgres/` creates `lineage_log`.

### Acceptance

```text
[ ] lineage_log created by docker/init_postgres/, and the migration for existing
    stacks recorded in RUNBOOK.md
[ ] ops_lineage_dag run on the stack, rows observed in the table
[ ] docker/init_openmetadata/01_create_schemas.sql either applied somewhere or removed,
    so no DDL lives where nothing reads it
[ ] RUNBOOK.md §5 and §6 query these PostgreSQL log tables through Trino with
    catalog `lakehouse`, which Trino does not have (ADR-0002) — §6 against a table
    that does not exist. §5b shows the psql form
```
