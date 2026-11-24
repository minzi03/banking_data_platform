# Codebase Cleanup Audit — 2026-11-24

## Scope and result

This was a read-only cleanup audit apart from the separately approved OpenMetadata key-rotation repair and the two subsequently approved cleanup actions recorded below. No unapproved source file, dependency, package marker, or other candidate was changed. This report records the evidence and separates completed approved actions from candidates still awaiting owner decisions.

- Source of truth for inventory: `git ls-files` on branch `chore/codebase-cleanup`, base commit `da6cbc2`.
- Tracked files after the approved key repair and duplicate-config removal: **679** (before the approved duplicate YAML removal there were 680; the two DER key removals are in the key-repair commits).
- Top-level tracked counts: root 16; `.github` 5; `airflow` 33; `api` 4; `code_etl` 140; `data_generator` 18; `dbt` 44; `docker` 59; `docs` 87; `governance` 47; `ml` 4; `openmetadata` 2; `schemas` 7; `scripts` 41; `streamlit` 3; `tests` 170.
- No tracked paths matched `.gitignore` (`git ls-files -i -c --exclude-standard`: empty).
- No missing tracked Markdown image targets found by static path scan.
- No unexpected backup/log/pyc/build-output candidates found among tracked paths.
- `git ls-files` reports **26 zero-byte files** after the approved duplicate YAML and key removals: package `__init__.py` markers and `tests/.gitkeep`; see “Keep” below.

### Baseline on the cleanup branch

- Ruff 0.17.1: check passed; format passed (**243 files formatted** after adding OpenMetadata key tests).
- Full pytest: **2626 collected; 66 failed, 60 errors, 2492 passed, 8 skipped, 2 warnings**. The Spark-dependent failures/errors include an existing test harness problem: `pyspark.find_spark_home` raises `ValueError: pyspark.__spec__ is not set`; separate Gold tests also fail and require their own diagnosis. This is a baseline limitation, not attributed to cleanup.
- Governance-only: **1708 passed, 1 skipped, 1 Pydantic deprecation warning** after the key repair.
- Docker Compose config validation: main, CI, E2E-merchant, and governance profile validated successfully.
- `dbt parse --target docker`: completed successfully using disposable target path; emitted dbt deprecation warnings. The Windows Docker path was interpreted as a POSIX-like path and created an untracked `dbt/C/Users/miynzi/AppData/Local/Temp/dbt-audit-target/` tree. It contains generated `manifest.json`, `osi_document.json`, `partial_parse.msgpack`, `perf_info.json`, and `semantic_manifest.json`. **Not removed in this audit phase.**
- Airflow DAG import baseline: **not run successfully**. Local `import airflow` resolves the repository namespace `airflow/__init__.py` rather than an installed Airflow distribution; the Airflow container was not available during the initial audit baseline. Later, only the OpenMetadata governance profile was started, not Airflow.
- OpenMetadata approved prerequisite: rotated the local RSA key into gitignored `docker/secrets/openmetadata/`; fixed Compose mounts; migration succeeded; server healthy and HTTP 200. The current-tree DER files were removed in the approved key commits; the old key remains in Git history. Focused tests: **40 passed**; after key repair the full governance/ops/DAG/shared suite passed (see current validation below).
- Approved cleanup actions completed: removed the five generated dbt audit-target files and empty parent directories; deleted only the duplicate `code_etl/cdc/consolidation/config/td22_online_transaction_e2e.yml`, retaining the identical copy under `code_etl/cdc/config/` as directed.
- Post-cleanup validation: Ruff check/format pass (**243 files formatted**); governance/ops/DAG/shared **2044 passed, 1 skipped, 1 warning**; Data Dictionary check and README projection were run before these two approved cleanup actions. Focused post-cleanup tests for Compose, links, and manifest collector: **148 passed**. Full regression after cleanup: **2044 passed, 1 skipped, 1 warning**.

## Candidate findings A–H

| Path / candidate | Group | Evidence (commands/results) | Confidence | Risk | Proposed action |
|---|---|---|---|---|---|
| `code_etl/cdc/config/td22_online_transaction_e2e.yml` and `code_etl/cdc/consolidation/config/td22_online_transaction_e2e.yml` | C: duplicate content | SHA-256 of both files was `b833488a353b73edf0c3fc8a8e1a5fd5f4c05afa2686952b45443828979590bb`. No exact filename reference was found; the consolidation directory is counted by the static inventory collector, while the streaming DAG loads configs from `cdc/config`. User explicitly chose to retain `cdc/config`. Post-delete `tests/governance/test_generate_metrics_manifest.py` passed. | High duplicate identity; approved removal | Removed one consolidation config; static current-state config inventory count decreases by one, while retained streaming config remains. Re-run manifest static collection after cleanup. | **Deleted `consolidation/config` copy only**; retained the `cdc/config` copy. |
| Empty `__init__.py` files under `airflow/`, `code_etl/`, `data_generator/`, and `tests/` (26 files) | A/E: empty files / package skeleton | Tracked-file byte scan shows zero length. They establish package namespaces used by imports, test discovery and DAG/plugin structure. Ruff/Pytest/governance imports run through these package paths. | High | Removing may change Python package discovery/import semantics. | **Keep.** |
| `tests/.gitkeep` | A/E: empty placeholder | Tracked zero-byte file; `tests/` contains tests, so the marker is redundant operationally but may document directory-preservation intent. | Medium | Very low, but no need to change during a behavior-preserving audit. | **Keep** unless user explicitly wants it removed. |
| `dbt/C/Users/miynzi/AppData/Local/Temp/dbt-audit-target/*` (5 generated files) | A: generated output / ignored-state anomaly | Created by baseline command `docker compose ... run ... dbt parse --target-path /tmp/dbt-audit-target`; Compose/Docker Desktop path conversion produced a host-looking path under dbt context. Verified the exact five generated dbt artifacts before deletion; no tracked files were inside. | High | Disposable metadata only; removal cannot affect source or runtime configuration. | **Deleted the generated target and empty parent directories only**, as explicitly approved. No ignore-pattern change made. |
| Tracked files matching common backup/build names | A | `git ls-files` search for `_old`, `_copy`, `_backup`, `_v2`, `_final`, `.bak`, `.orig`, `.tmp`, `.log`, `.pyc`, `target/`, `dbt_packages/`, `htmlcov/`, caches returned no candidate. | High | None identified. | **No action.** |
| Tracked ignored files | B | `git ls-files -i -c --exclude-standard` returned no output. Secret-like filename review found `docker/.env.example`, Trino auth config, ADR and credential-reading source; no actual `.env`, PEM, token, DB dump, or private key remained tracked after approved DER removal. | High for path check; content scan is not a secret scanner | `.env.example` intentionally contains development templates; review separately if values become non-placeholder credentials. | **No action from this audit.** |
| `docker/conf/private_key.der`, `docker/conf/public_key.der` | B: tracked key material | Before repair, OpenSSL confirmed a valid RSA private key; derived public-key SHA-256 matched tracked `public_key.der`. Compose referenced these relative paths but rendered services had no volumes. Approved repair commit(s) on audit branch remove current-tree files, generate replacement under gitignored `docker/secrets/openmetadata/`, mount read-only, and tests/README/T.D. updated. OpenMetadata migration completed and server healthy/HTTP 200. | High | Former private key remains in Git history. No history rewrite performed. | **Current-tree remediation done under explicit approval.** History rewrite remains a separate owner decision; treat old key as exposed. |
| Other empty files or TODO/FIXME/`pass` occurrences | A/H | `ruff check --select F401,F841,ERA001` reports unused imports/locals and comment-looking code (see next row); `pass`/TODO scans also hit intentional exception clauses, docstrings and test stubs. No vulture-style call graph proof performed because dynamic Airflow/dbt/config usage makes static absence inconclusive. | Low as deletion candidates | False positives can remove intentional fallback, extension point, exception handling or fixture behavior. | **Do not delete automatically.** See low-confidence section. |
| Ruff F401/F841/ERA001 report | H: static dead-code indicators | Command: `python -m ruff check airflow code_etl dbt governance scripts tests api ml streamlit data_generator --select F401,F841,ERA001 --output-format=concise`. Findings: 13 unused imports, 1 assigned-unused local (`subject`), 16-ish commented-code warnings (the exact count from output is 30 lint diagnostics total). Files include `airflow/dags/cdc/cdc_register_connectors_dag.py`, `api/main.py`, `data_generator/*`, `ml/*`, `streamlit/app.py`, DAGs, and `code_etl/cdc/test_dlq.py`. | High that Ruff reports them; Low that every finding is safe cleanup | Imports may intentionally trigger registrations; commented lines may document operational alternatives; no behavior changes allowed. | **Report only.** Let maintainers decide whether to clean in a separate change. |
| Requirements and runtime imports | F: dependency audit | API requirements are pinned; Spark has several lower-bound ranges; Streamlit has ranges; Airflow installs providers in Dockerfile; dbt versions are pinned in Dockerfile; exporter imports only stdlib. Runtime import checks are not complete for transitive and dynamic imports. | Medium | Removing “unused” dependencies from dynamic plugin/runtime code could break deployed images. | **No dependency edits.** Keep as follow-up package-by-package review. |
| README / architecture historical numbers | G: docs drift | Current README evidence table/projections were synchronized to latest manifest during the evidence projection PR. Older historic comparison text intentionally contains 1,142; `docs/02-architecture/architecture.md` still contains older counts in unrelated tables (e.g. 13 serving, 33 contracts, 29 services) that are not all bound to the manifest. `docs/evidence/` includes historical runs intentionally. | High that historical claims exist; Medium on which are stale versus historically scoped | Blind replacement could erase time-scoped evidence and violate the evidence-history rule. | **No bulk edits.** Candidate for a separate docs-freshness review after this audit report is approved. |
| `regulatory_reporting`, `dbt_seed` references | G: stale-name scan | DAG files were removed in the past. Remaining references appear in CHANGELOG, README historical note, DEMO_GUIDE historical note, and GLOSSARY statement that the DAG was removed. No active DAG/code reference found. | High for historical context; Low for “dead reference” because these are explanatory docs | Removing would erase useful migration/audit history. | **Keep historical references.** |
| Asset screenshot links | G: docs/assets | Static Markdown image target scan found no missing local image references. | High | None found. | **No action.** |

## Keep / do not touch

- `docs/evidence/`, including failed run artifacts and `superseded_claim`: intentionally retained by policy.
- ADRs, `CHANGELOG.md`, migration/DDL files, data contracts, `.github/`, and secrets templates: protected by audit instructions.
- Empty `__init__.py` files: Python/DAG package markers.
- Historical references to removed DAGs and prior counts: retain unless explicitly approved for a history-preserving documentation cleanup.
- OpenMetadata public key, Trino public certificate: public verification material and active runtime inputs; do not treat as private-key cleanup candidates.
- `schemas/online_transaction_event_v2.json`: tracked schema contract; no evidence it is orphaned.

## Needs owner decision

1. Whether either identical TD-22 E2E YAML can be removed or whether both represent distinct future/config inventory contracts.
2. How to avoid the Windows dbt `--target-path` path-conversion artifact on future audit runs. The generated files were removed; no broad ignore rule was added.
3. Whether to perform Git history rewriting for the former OpenMetadata private key. That is intentionally excluded without separate approval.
4. Which low-confidence Ruff dead-code/dependency candidates, if any, should be handled in a separate behavior-preserving PR.
5. Whether architecture-page portfolio counts should be refreshed, preserving explicitly historical baselines.

## Published metric impact

- Removing a `tests/**/test_*.py` function changes `metrics.platform.automated_tests.test_functions` and `collected_pytest_nodes`; rerun `scripts/generate_metrics_manifest.py` from a clean worktree and update only README projections through `scripts/verify_readme_metrics.py`.
- Removing an Airflow DAG file/object changes `airflow_dag_files` / `airflow_dag_objects`.
- Removing a dbt serving SQL model changes `platform.dbt_models` and serving-completeness expectation; do not do so without contract/inventory review.
- Removing CDC/Silver/Gold configs changes static counts and potentially runtime pipeline coverage. A cleanup must not directly hand-edit metric values; regenerate evidence after user-approved changes and, if runtime is affected, run the authorized verification path.

## Stop point

The user explicitly approved removal of the generated dbt audit-target output and deletion of only `code_etl/cdc/consolidation/config/td22_online_transaction_e2e.yml`, retaining its identical `code_etl/cdc/config/td22_online_transaction_e2e.yml` counterpart. Those two removals are complete and focused post-cleanup tests passed. No other cleanup candidate has been changed; all remaining report rows still require separate itemized approval.
