# Governed Kafka Consumer Live Proof

Date: 2026-11-02
Stack: `docker/docker-compose.e2e-merchant.yml`
Network: `merchant_e2e_net`
Broker: `kafka:9092`

## Scope

This proof exercises the governed event path in the isolated Kafka network:

```text
checked-in JSON event
  → contract validation
  → Kafka publish with event_key as record key
  → governed Kafka consumer
  → JSON Schema validation and event_id replay ledger
```

The shared `banking-*` stack was not used or modified.

## Commands and result

The approved fixture was published:

```text
scripts/publish_governed_event.py --apply
Published governed event to postgresql.banking.digital_banking.online_transaction
```

A bounded consumer read one record. This proof used the CLI's then-current no-op callback:

```text
Processed 1 governed events from postgresql.banking.digital_banking.online_transaction
```

The adapter now supports `--output` as an append-only JSONL side-effect sink. The sink is invoked only for accepted, non-replay events; the ledger write follows the sink, and Kafka offsets are committed after processing. This provides a concrete local side effect for a repeatable replay proof, but does not make the file write, SQLite ledger, and Kafka commit atomic.

A second consumer group read the same Kafka record using the same temporary SQLite ledger and emitted no pending event:

```text
Processed 0 governed events from postgresql.banking.digital_banking.online_transaction
```

The empty-topic bounded probe also terminated cleanly:

```text
Processed 0 governed events from postgresql.banking.digital_banking.online_transaction
```

## What this proves

- The host-mounted repository scripts run inside a Python container on `merchant_e2e_net`.
- The governed event passes repository JSON Schema validation before publish.
- Kafka transport delivers the event to the governed consumer adapter.
- `enable_auto_commit=False` keeps offset commits under consumer control.
- A persistent processed-event ledger suppresses a replay across consumer groups.
- Empty polling can be bounded with `--max-empty-polls` for deterministic probes.

## Not proven

- The CLI's `--output` sink is a local JSONL append, not an Iceberg, database, or business-system side effect.
- The live proof recorded above predates the output sink; a Kafka-to-JSONL sink run has not yet been verified.
- Ledger persistence and Kafka offset commit are not one atomic transaction.
- Schema Registry serializer/deserializer wire integration is not exercised; validation uses the repository JSON Schema.
- This is at-least-once replay protection, not an exactly-once claim.
- The fixture was intentionally written to the isolated Kafka topic with explicit operator approval.
