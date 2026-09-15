# Interview Talking Points

Every claim here should be traceable to runtime evidence, a test, the evidence
manifest, source code, or current platform state. Where something is not
demonstrated, this file says so rather than softening the wording.

Scale and model counts live in the README's Verified Portfolio Snapshot and are
drift-checked against the evidence manifest; they are deliberately not repeated
here.

---

## 6 Key Questions

### 1. Why dual-path batch + CDC?

> "Different workloads have different latency requirements. Batch processing
> through JDBC handles daily analytics with full ACID guarantees and idempotent
> reprocessing. CDC through Debezium and Kafka enables near-real-time
> operational reporting without polling the source database. The dual path lets
> us serve both use cases without compromising either."

**Follow-up points**
- Batch: scheduled, full refresh, easier to debug, strong consistency
- CDC: event-driven, incremental, lower latency, higher complexity
- Both write to Iceberg, so downstream consumers don't care about the ingestion path

---

### 2. Why Spark + Iceberg + MinIO?

> "Spark provides distributed compute for both batch and streaming workloads
> with the same API. Iceberg gives us ACID transactions, schema evolution, time
> travel, and hidden partitioning on top of S3-compatible storage. MinIO lets us
> run cloud-native storage semantics locally without cloud dependencies."

**Follow-up points**
- Spark Structured Streaming for CDC ingestion (foreachBatch + Iceberg MERGE)
- Iceberg snapshots enable time travel and rollbacks
- MinIO is S3-compatible, so migration to cloud storage is straightforward
- Trino provides interactive SQL without needing Spark for every query

**Worth knowing:** Spark and Trino name the same warehouse differently — `lakehouse`
in Spark, `iceberg` in Trino. That mismatch has caused four separate defects in
this project, so it is a real cross-engine contract rather than trivia.

---

### 3. Why keep Bronze CDC append-only?

> "Bronze CDC is the audit boundary. Each event is a new row, so there are no
> update conflicts to resolve at Bronze level, and downstream state can be
> rebuilt deterministically from retained events. I selectively consolidate
> high-value entities — customer and account — into Silver current-state tables,
> while the rest of the CDC history stays in Bronze."

**What Bronze CDC actually stores** (verified against the table schema)

```text
Bronze CDC columns
  __cdc_operation        INSERT / UPDATE / DELETE / SNAPSHOT
  __cdc_timestamp
  __cdc_timestamp_ms
  __spark_batch_id
  __ingestion_time

Kafka partition / offset / timestamp
  retained in the DLQ table only — NOT in normal Bronze CDC tables
```

So the honest claim is deterministic reprocessing of retained events using CDC
timestamps and Spark batch identifiers. **Not** replay from an arbitrary Kafka
offset — that metadata is not in Bronze.

This distinction matters in an interview: "replay from any point in time" sounds
stronger and is the kind of claim a good interviewer will ask you to demonstrate.

---

### 4. How do SCD1/SCD2 work in Silver?

> "SCD Type 1 is used for dimensions that don't require historical tracking —
> branch, product, card, employee, device, location. It's a MERGE-based upsert
> that keeps only current state. SCD Type 2 is used for customer and account,
> where attribute changes must be tracked over time. Each tracked change closes
> the previous version and opens a new one with `effective_from`,
> `effective_to`, and `is_current`."

**Follow-up points**
- SCD2 answers "what was true at a point in time"; Silver Current answers "what is true now"
- Surrogate keys (`customer_sk`, `account_sk`) join facts to dimensions
- Idempotent partition reruns using `overwritePartitions`
- The tracked columns are declared in the job YAML, not inferred

**Evidence:** CI builds a second snapshot with a real tracked change and asserts
the transition — two versions, one historical with `effective_to` set, one
current carrying the new source value. The test is not satisfied by a fixture
that merely contains an old row.

---

### 5. How do you handle data quality and governance?

> "Three layers: data contracts define schema expectations, ownership, freshness
> and business rules; data-quality checks validate row counts, nullability,
> uniqueness, ranges, referential integrity and freshness; and PII handling
> covers classification, masking and sandbox-safe datasets. On top of that,
> every metric this project publishes is generated from a versioned evidence
> manifest and drift-checked in CI, so documentation cannot quietly diverge from
> what was measured."

**Follow-up points**
- Quarantine: invalid records routed to separate datasets for investigation
- OpenMetadata: catalog, lineage and glossary
- CI: lint, YAML validation, unit tests, executable Gold regression, and a
  PR-blocking Trino integration gate (34 tests against real pipeline output)
- Gold validation: `require_non_empty` + `require_snapshots` — if a source
  partition is missing, the job fails rather than writing silent zeros
- RFM consistency: canonical 90-day NTILE(5) definition enforced in both
  rfm_segment.yml and mart_customer_360.yml via the same CTE pattern

---

### 6. What would change in real banking production?

> "Distributed deployment — Spark on Kubernetes, HA Airflow, managed Kafka.
> Centralised monitoring and alerting. Proper secrets management (Vault).
> Cross-region DR with tested restores. And a real access-control story
> rather than local development credentials."

**Be honest about current state**
- This is a portfolio platform, not a production system
- Development secrets were committed to Git history (TD-3 — removed from
  current tree, accepted as local-only, no rewrite)
- The dashboard and all 10 serving consumers are now operational (TD-7 — fixed)

Saying this unprompted is usually stronger than being asked.

---

## Six engineering stories

These matter more than the stack list. Each follows the same shape: what I
found, why it was hard to see, what I decided, the trade-off, the evidence, and
what I would do next.

---

### A. A green test is not necessarily a correct test

**Problem.** Three Bronze uniqueness tests compared whole-table `COUNT(*)` to
whole-table `COUNT(DISTINCT key)`. Bronze holds a full snapshot per business
date, so that assertion silently required the table to contain exactly one
snapshot.

**Why it was hard to see.** They passed. The fixture had a single day, so the
wrong invariant and the right one produced the same answer.

**Decision.** Build a real second snapshot, then rewrite the assertion to the
architectural invariant — a key appears at most once *within* each snapshot —
by finding duplicate groups directly, so a failure names the key and snapshot.

**Trade-off.** The fixture costs an extra Bronze and Silver run in CI. I decided
a test suite that only passes on a degenerate fixture is not worth the time it
saves.

**Evidence.** They now pass while two snapshots exist, which the previous
version could not have done. The same second snapshot exercises a real SCD Type
2 transition.

**Next.** Audit the remaining assertions for the same shape — invariants that
happen to hold on the current fixture.

---

### B. Process success is not data success

**Problem.** Across the repository, failures were being converted into success:
`$?` read after a pipe, `|| true` on a critical path, an `sh -c` string ending
in `echo`, and a fallback that turned a parse failure into a confident zero.

**Why it was hard to see.** These shapes look like defensive programming. A
fallback value reads as caution rather than as a fabricated measurement.

**Decision.** Remove them, and add a post-condition after every step that claims
to have done something — query the platform for the result rather than trust the
exit code.

**Trade-off.** More verbose CI steps and more queries per run.

**Evidence.** Removing them exposed four defects that had been running silently:
schemas never created while the container exited zero, a Gold bootstrap that ran
no jobs, a benchmark recording a timing for a query that never executed, and a
seed generator colliding with its own primary key.

**Next.** A static contract test for the pattern. Every instance so far was
found by accident or by adding a verification step, never by a check that looks
for the pattern itself.

---

### C. Readiness should measure the contract, not a proxy

**Problem.** CI waited for a TCP port, then for an HTTP endpoint. The property
that actually matters is that Trino can *use* the Iceberg catalog. A scheduled
workflow had been failing for weeks as a bare timeout naming no service.

**Why it was hard to see.** The failure gave a single exit code and no per-stage
timing, so everyone including me assumed the wrong cause.

**Decision.** One semantic gate — `SHOW SCHEMAS FROM iceberg` through Trino —
plus per-service readiness timings measured from a common start, separated from
image pull and build time.

**Trade-off.** More moving parts in the workflow than a single wait loop.

**Evidence.** The measurements disproved the original hypothesis: image
acquisition and database readiness were both far inside the budget being blamed,
so raising the timeout would have fixed nothing. I also learned that an HTTP
healthcheck can be impossible — the catalog image ships no HTTP client, so my
first attempt deadlocked the stack.

**Next.** Reuse the same readiness contract for any future service rather than
writing a new wait loop each time.

---

### D. Publication correctness is not consumer adoption

**Problem.** The serving layer is built and tested, but an audit found the
dashboard application still queries historical Gold, and connects to Trino with
the Spark catalog name, so its queries do not run at all.

**Why it was hard to see.** The serving models pass their tests, the CI gate is
green, and the documentation described consumers that had never been checked —
including a BI tool that is not in the stack.

**Decision.** State the two claims separately, and empty the dbt exposures
rather than repointing them at a consumer I could not verify. An exposure asserts
that a downstream use is real; it is not a place to record intent.

**Trade-off.** The project looks less "complete" with no declared consumers. It
is more honest, and the gap is now tracked rather than hidden.

**Evidence.** `SHOW CATALOGS` returns `iceberg` and `system`; the application
connects with `lakehouse`. Its queries read `FROM gold.*`, not the serving
tables.

**Next.** TD-7 — fix connectivity, decide whether the consumer contract is
historical Gold or serving, verify it through Trino, and only then restore an
exposure.

---

### E. Two models, one metric name, two different definitions

**Problem.** `rfm_segment.yml` and `mart_customer_360.yml` both published an
`rfm_segment` column — but with different lookback windows (90 vs 30 days) and
different threshold boundaries (New Customers ≥6 vs ≥5, At Risk ≥4 vs ≥3). Same
metric name, two business definitions.

**Why it was hard to see.** Both models passed their tests. Each was individually
correct SQL; nothing compared them to each other. A stakeholder asking "why does
this customer show At Risk in the RFM dashboard but Loyal in Customer 360?" would
have found it, not a test.

**Decision.** One canonical definition: the standalone `rfm_segment` table wins
(90-day window, NTILE(5) scoring, fixed thresholds). Customer 360 now computes
RFM from dedicated 90-day CTEs rather than reusing its own 30-day transaction
aggregates. Documented as design decision #5 in the output documentation.

**Trade-off.** Customer 360 does more work — separate 90-day aggregation CTEs
instead of reusing the 30-day ones. The alternative was redefining RFM to fit
the cheaper computation, which is backwards.

**Evidence.** The two models now share window, scoring method, and every
threshold. The docs state the canonical definition once.

**Next.** The general lesson: when the same metric name appears in two models,
grep for a single source of definition. Metric drift between marts is silent
because each model is tested only against itself.

---

### F. A verifier can falsify its own provenance

**Problem.** The evidence generator recorded `git_dirty: false` whenever
`--allow-dirty` was passed. The flag gated nothing else, so falsifying the
record was its only effect.

**Why it was hard to see.** It lived in the script whose entire purpose is to
make published claims trustworthy, and it had been written to make a local run
convenient.

**Decision.** Provenance always reports the truth. The flag decides whether the
run may promote the canonical manifest — not what the record says.

**Trade-off.** A local exploratory run can no longer promote the manifest
without an explicit override.

**Evidence.** A manifest built from a dirty tree pins a commit that does not
describe what was measured. That is precisely the fact making it
irreproducible, and precisely the fact the flag was erasing.

**Next.** Extend the same rule to any future tooling: a flag may change what a
tool *does*, never what it *reports*.

---

## Two principles

> **A green test is only valuable if the invariant itself is correct.**
>
> **A fallback that looks defensive can be a fabricated measurement.**

Both were learned from defects in this repository, not from reading about them.

---

## Common follow-up questions

### "Tell me about your CDC consolidation implementation"

> "A config-driven engine reads incremental events from append-only Bronze CDC
> tables, deduplicates by business key, and MERGEs into Silver current-state
> tables, handling INSERT, UPDATE and DELETE. Watermarks are persisted in
> Iceberg and survive restarts. I verified the lifecycle end to end: insert
> creates a row, update modifies it, delete removes it, and re-running produces
> no duplicates."

**Be precise about the watermark.** It is keyed on `(timestamp, batch id)` per
table. It is **not** partition-aware, and it does not track Kafka offsets. An
earlier version of this document claimed a composite watermark over
`(table, topic, partition)`; that was wrong and is the kind of detail an
interviewer will probe.

See [P1 Interview Prep](p1-interview-prep.md) for detailed CDC questions.

### "How does Spark handle late-arriving data?"

> "For batch, business-date partitions and idempotent reruns. For CDC, events
> are processed in arrival order within a micro-batch and late events land in a
> later batch; the watermark advances on `(timestamp, batch id)` per table, so
> reprocessing is deterministic rather than offset-based."

### "How do you handle schema evolution?"

> "Iceberg supports it natively — add, rename, and retype columns without
> rewriting data. Data contracts enforce schema expectations at the application
> level, and OpenMetadata catalogs the changes."

### "What's the recovery mechanism?"

> "Spark checkpoints to MinIO; on restart, streaming resumes from the last
> checkpoint. For batch, idempotent partition reruns produce no duplicates. CDC
> consolidation resumes from the persisted per-table watermark."

### "How do you test the pipeline?"

> "Unit tests and static SQL invariants run on every push. An executable Gold
> regression runs real Spark against fixtures that encode the fan-out and
> snapshot bugs. A Trino integration gate stands up the full Docker topology,
> seeds it, runs Bronze, Silver and Gold, builds a second snapshot with a real
> SCD Type 2 change, and asserts against actual pipeline output. That gate
> blocks pull requests, and I proved it by pushing an intentional assertion
> failure, watching the pull request go red, and reverting it."

### "Tell me about a challenging bug you fixed"

Two are worth having ready — one classic debugging story, one that shows how you
think about evidence.

**The classic.** An `InvalidClassException` on Iceberg's `DaysFunction` — a
`serialVersionUID` mismatch between driver and executor. The root cause was a
Docker image inconsistency: the running container had Iceberg 1.4.3 while the
Dockerfile specified 1.6.0, and `spark-defaults.conf` had duplicate
`extraClassPath` entries where only the last took effect. Fixed by aligning
versions, merging the classpath entries, and ensuring the build context was used
rather than a stale prebuilt image.

*Shows:* debugging method, Java serialization, Docker image management, Spark
classpath resolution.

**The one I would actually lead with.** A join fan-out that inflated `SUM()`
while leaving `COUNT(DISTINCT)` correct, so the output looked plausible. It was
found by reconciling two models that should have agreed and did not. The fix was
to aggregate each source to customer grain in its own CTE before joining; the
regression fixture asserts a hand-computed total of `1500.00`.

*Shows:* that I reconcile outputs rather than trusting that a job finished.

---

## Known gaps to state plainly

Being asked about weaknesses is easier when the list is already written down.

```text
TD-5  false-success shell patterns — 3 of 7 instances fixed; a static contract
      test exists (test_shell_failure_propagation.py) but 4 items remain to
      be verified in CI
TD-3  development secrets are in Git history (accepted: local docker stack
      only; removed from current tree, prevented via .gitignore)
TD-6  CI fixture is shared between trino-integration and benchmark — deliberate
      while profiles are expected to diverge; benchmark is now manual-only
```

Full detail in [technical-debt.md](../technical-debt.md).

---

## Resources

- [Architecture](../architecture/architecture.md)
- [Demo Script](../demo/demo.md)
- [CDC Pipeline Details](../cdc-pipeline.md)
- [Technical Debt](../technical-debt.md)
