# TD-22 — Merchant EOD Three-way Runtime Evidence

> **Run date:** 2026-10-25
> **Environment:** isolated Docker Compose project (`merchant-e2e-*`)
> **Scope:** deterministic fixture only; no shared `banking-*` resource

## Data path

```text
isolated Postgres
  ├── batch fixture → isolated Bronze batch table
  └── Debezium → isolated Kafka → Spark Structured Streaming
      → isolated Bronze CDC → current-state consolidation
```

## Cohort

```text
ICT business date: 2026-10-18
merchant_id: 900001, 900002
payment_channel: QR, POS
currency: VND
SUCCESS rows: gross count and gross amount
refund_flag: separate refund count
```

## PASS result

| Merchant | Channel | Success count | Gross amount | Refund count |
|---:|---|---:|---:|---:|
| 900001 | QR  | 1 | 10,000.00 | 0 |
| 900001 | POS | 1 | 25,000.00 | 0 |
| 900002 | QR  | 1 | 5,000.00 | 1 |

All three representations matched at `merchant_id × payment_channel × currency`:

```text
source groups:  3
batch groups:   3
current groups: 3
checks:         9 PASS
exit code:      0
```

The isolated CDC path also produced three valid snapshot events with no DLQ rows.

## Negative control

A controlled batch-only amount change (`900001 / POS: 25,000.00 → 99,999.00`)
produced:

```text
tree: FAIL
failed metric: gross_amount
exit code: 1
```

The fixture was restored and a final clean rerun returned `PASS`, 9 checks and
exit code `0`.

## Scope boundaries

- This proves the EOD contract on a deterministic isolated cohort; it does not
  prove the independent synthetic QR producer is a Postgres counterpart.
- SFTP settlement and Oracle are not part of this run.
- Refund amount was not invented; the source contract provides a refund flag,
  so evidence reports refund count separately.
