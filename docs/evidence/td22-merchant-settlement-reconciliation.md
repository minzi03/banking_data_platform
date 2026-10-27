# TD-22 — Merchant Settlement Reconciliation Runtime Evidence

> **Run date:** 2026-10-26
> **Environment:** isolated Docker Compose project (`merchant-e2e-*`)
> **Scope:** deterministic fixture only; no shared `banking-*` resource

## What this proves

The batch transaction aggregate and the settlement aggregate agree at grain
`merchant_id × currency` for the TD-22 cohort, and the verifier fails closed
when they do not.

## Data path

```text
isolated Postgres digital_banking.online_transaction
  → isolated Bronze batch table lakehouse.bronze.online_transaction_batch_e2e
                                              ↓
                          isolated Postgres digital_banking.pos_settlement
                                              ↓
                          scripts/verify_merchant_settlement_reconciliation.py
```

## Cohort

```text
ICT business date: 2026-10-18
merchant_id: 900001, 900002
currency: VND
status filter: SUCCESS (transaction) / SETTLED (settlement)
```

## PASS result

| Merchant | Transaction gross | Settlement gross | Txn count | Settlement count | Outcome |
|---:|---:|---:|---:|---:|---|
| 900001 | 35,000.00 | 35,000.00 | 2 | 2 | PASS |
| 900002 | 5,000.00 | 5,000.00 | 1 | 1 | PASS |

```text
SETTLEMENT_RECONCILIATION_PASS=True
exit code: 0
```

## Fail-closed behaviour

The comparison logic is covered by unit tests that assert every non-matching
case produces a non-PASS outcome:

| Scenario | Outcome |
|---|---|
| Gross amount differs by 0.01 | `GROSS_AMOUNT_MISMATCH` |
| Transaction key absent from settlement | `MISSING_SETTLEMENT` |
| Settlement key absent from transaction | `MISSING_TRANSACTION` |
| Same key appears twice in settlement | `DUPLICATE_SETTLEMENT_KEY` |
| Count differs beyond tolerance | `TXN_COUNT_MISMATCH` |

The verifier returns exit code `1` for any non-PASS row and `2` for an
environment/read failure.

## Scope boundaries

- The settlement fixture was seeded directly into the isolated Postgres because
  the isolated stack has no SFTP service. This proves reconciliation logic and
  wiring, not the production SFTP transport.
- The isolated Silver fact table used for the reference transaction fact is
  `lakehouse.bronze.online_transaction_batch_e2e`; the isolated stack does not
  build `lakehouse.silver.fact_online_transaction`.
- Refund and fee allocation are not reconciled: the settlement feed carries no
  transaction-level refund or fee key.
- The live mutation-based negative control (updating a settlement row) was not
  run because it writes to a data source; the equivalent cases are proven by
  unit tests instead.
