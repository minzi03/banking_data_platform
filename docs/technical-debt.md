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

## TD-3 — Secrets committed in the repository

**Status:** open (recorded at `portfolio-v1.1`, deliberately deferred to its own PR)

`docker/secrets/*.txt` and a hard-coded password in the Debezium connector
registration script are local development credentials, but they are real
credentials in version control.

### Acceptance

Values move to `.env` / a secret store, the files are removed from the working
tree, history rewriting is decided explicitly (rewrite or accept), and CI gains
a check that fails on new committed secrets.
