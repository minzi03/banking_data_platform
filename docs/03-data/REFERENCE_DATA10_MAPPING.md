# Reference Mapping — `new_data1/data10`

> **Status:** reference-only; not imported into the canonical seed path.
> **Source:** `thamkhao/dataset_thamkhao/new_data1/data10/`
> **Reviewed:** 2026-10-19

This note maps the small `data10` Enterprise Banking Risk Analytics dataset to the
current Banking Data Platform. It is a domain/schema reference, not a replacement
for `data_generator/` or PostgreSQL source data.

## 1. What data10 contains

| File | Approximate role | Important fields | Current-platform analogue |
|---|---|---|---|
| `customers.csv` | customer master | `Customer_ID`, income, risk score, KYC status | `core_banking.customer` |
| `accounts.csv` | account master | `Account_ID`, customer, type, balance, status, branch | `core_banking.account` |
| `branches.csv` | branch reference | branch code, city, state, region, manager | `core_banking.branch` |
| `employees.csv` | employee/branch reference | designation, salary, branch | `core_banking.employee` |
| `loans.csv` | loan master | type, amount, rate, term, status, dates | `core_banking.loan` |
| `credit_cards.csv` | card master | type, network, limit, outstanding, status | `card_crm.card` |
| `transactions.csv` | flat transaction sample | account, date/time, type, amount, merchant, city, device, fraud label | `core_banking.txn_account`, `card_crm.card_txn`, `digital_banking.online_transaction` (semantics differ) |
| `bank_data.db` | SQLite copy of the sample | same reference population | not a production source |

The generator creates only a small sample (roughly 100 customers, 1–2 accounts per
customer, 1,000 loans/cards and 10,000 transactions). Its labels are useful for
schema and scenario discussion, but its IDs, geography and distributions are not
compatible with the Vietnamese, larger, ICT-date-aware canonical generator.

## 2. Mapping decisions

### Already covered by the canonical generator

- Customer, branch, employee and account masters.
- Loan and credit-card domains.
- Transaction amount, date/time, channel/device/location and fraud reason concepts.
- KYC/status, branch relationship, card status and loan status scenarios.
- Merchant, MCC, QR/POS, refund and digital-banking fields are covered more richly
  by the current generator than by data10.

### Useful reference signals adopted conceptually, not copied as rows

- Customer `KYC_Status` states (`Complete`, `Pending`, `Expired`) map to the
  platform's controlled KYC vocabulary rather than replacing it.
- Loan lifecycle (`Active`, `Closed`, `Default`) maps to the platform's richer
  `ACTIVE`, `CLOSED`, `OVERDUE`, `WRITTEN_OFF` contract.
- Card lifecycle (`Active`, `Blocked`, `Expired`) is already represented with the
  platform's card types/networks.
- Transaction fraud reasons (`High Amount`, `Multiple Transactions`,
  `Midnight Transaction`, `Different City`) are scenario seeds; they do not become
  ground truth for the platform's fraud model without an explicit label contract.

### Deliberately not imported

- Indian names, cities, currency/locale and branch codes.
- `Annual_Income` and random `Risk_Score` as authoritative risk attributes.
- Flat `transactions.csv` as a second transaction seeder.
- `bank_data.db` or CSV rows into the running PostgreSQL stack.
- `Customer_ID` ranges that do not align with the canonical foreign keys.
- A second generator implementation: ADR-0012 keeps one parameterised generator.

## 3. Data quality observations from the reference

The reference generator is intentionally educational and should not be treated as
an operational contract:

- `generate_loans.py` chooses `Customer_ID` in `1001..1180`, while the sample
  customer generator creates `1001..1100`; this can produce orphan loan references.
- `transactions.csv` is a flat denormalised sample and has no canonical `cob_dt`,
  operation, event-time, ingestion-time or Kafka coordinate metadata.
- `transactions.csv` labels fraud as `Yes/No` with a free-text reason; the current
  platform uses controlled fraud/AML fields and separate Gold risk semantics.
- No Merchant QR/POS settlement contract, CDC envelope, refund amount, currency
  reconciliation key or late-file policy is defined.
- Generator randomness is not a reproducible source-of-truth without an explicit
  seed/as-of contract.

## 4. Canonical source coverage

The current source of truth remains:

```text
PostgreSQL schemas
  core_banking      → data_generator/generators/core_banking.py
  card_crm          → data_generator/generators/card_crm.py
  digital_banking  → data_generator/generators/digital_banking.py
  opslakehouse      → data_generator/generators/ops_metadata.py
```

The canonical path provides the larger validated input base, ICT `cob_dt`, merchant
QR/POS fields, loan payments, standing orders, AML data, source registry, CSV export,
scaling and `--as-of` controls. `data10` should be used to design fixtures and
interview examples, not to replace that path.

## 5. Safe reuse plan

When a test needs a small deterministic fixture, derive it from the current schema
and contracts and use IDs/FKs valid in this repository. Useful cases inspired by
`data10` include:

- KYC pending/expired customer and active/closed account.
- Active/overdue/written-off loan with payment status changes.
- Active/blocked/expired card and card transaction refund/reversal.
- High amount, midnight, velocity and different-location fraud scenarios.
- Branch/employee relationship and regional aggregation.
- Merchant QR/POS success, failed, pending and refund transactions.

Fixtures must remain separate from the full seed dataset and must declare their
business date, grain, keys, currency, PII classification and expected outcome.
