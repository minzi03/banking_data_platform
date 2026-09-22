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

**Status:** open (recorded 2026-09-22)

`make seed` executes the data generator inside the `postgres` container:

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

### Acceptance

```text
[ ] `make seed` completes against a running stack, from a clean checkout
[ ] the chosen container demonstrably mounts data_generator/
[ ] `make seed-local` still works, or is removed deliberately with a reason
[ ] a smoke check proves the target ran the generator rather than exiting 0
    without doing anything — see TD-5 for why that distinction matters here
```

Related: TD-4 (same class, different container), TD-5 (`make` targets still
do not propagate child exit codes, so a broken target can report success).
