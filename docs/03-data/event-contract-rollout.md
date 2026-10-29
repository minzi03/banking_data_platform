# Kafka Event Contract Rollout

## Current contract

The online transaction topic is governed by [`schemas/topic_registry.yml`](../../schemas/topic_registry.yml) and [`schemas/online_transaction_event_v1.json`](../../schemas/online_transaction_event_v1.json).

The contract defines:

- `event_type`: `ONLINE_TRANSACTION_CHANGED`
- `event_version`: `1`
- business key: `payload.transaction_id`, exposed as `event_key`
- idempotency key: `event_id`
- delivery semantics: at-least-once
- replay boundary: Kafka topic plus retained Bronze topic/partition/offset coordinates
- compatibility mode: `BACKWARD`

`event_id` identifies one logical event for downstream deduplication. It does not provide exactly-once delivery by itself. Consumers must persist their processed-event state or make their side effects idempotent.

## Raw CDC versus domain event

The current JSON Schema is a domain-facing envelope around the online transaction change. It is not a replacement for the raw Debezium envelope.

| Boundary | Purpose | Contract |
|---|---|---|
| Raw Kafka/Debezium CDC | Preserve source change, operation, source metadata, and replay context | CDC topic/config and Bronze metadata |
| Governed event envelope | Stable consumer-facing identity, version, key, and payload semantics | JSON Schema and topic registry |
| Silver current-state | Apply ordered CDC changes to the current transaction state | Consolidation config and merge guard |

The raw CDC path remains the audit and replay source. The governed envelope must not discard source coordinates needed to investigate or replay a change.

## Producer rollout checklist

1. Generate a non-empty `event_id` for every logical event.
2. Set `event_key` to `transaction_id:<positive integer>`.
3. Set the registered `event_type` and `event_version` exactly.
4. Validate the event against the registry before publishing.
5. Keep the existing topic contract during a backward-compatible rollout.
6. Publish a new version only after consumers support it and the registry is updated.

## Consumer rollout checklist

1. Resolve the schema by topic and event version.
2. Validate before applying business logic.
3. Deduplicate by the registered `idempotency_key` within the consumer side-effect boundary.
4. Preserve Kafka topic, partition, and offset in audit records.
5. Route invalid events to DLQ with the original payload and coordinates.
6. Make replay safe: rerunning an event must not duplicate a business side effect.

## Versioning rules

- Adding an optional field is the preferred backward-compatible change.
- Removing a required field, changing its type, or changing key semantics is breaking.
- A breaking change requires a new event version and an explicit producer/consumer migration plan.
- The registry remains v1-only until an actual producer and consumer rollout is ready.

An isolated Schema Registry runtime proof now covers readiness, registration, subject/version read-back, idempotent re-registration, and local-schema drift comparison. Production/shared Registry deployment, producer serializer integration, consumer-side processed-event storage, and exactly-once claims remain future work.
