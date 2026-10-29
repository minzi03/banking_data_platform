# Isolated Schema Registry Contract Verification

## Scope

This note records the code-level contract for running the governed online transaction schema in an
isolated Schema Registry. It is not a runtime PASS: no registry container was running when it was written.

## Components

| Component | Purpose | Path |
|---|---|---|
| Registry service | Isolated Confluent Schema Registry in the opt-in E2E stack | `docker/docker-compose.e2e-merchant.yml` |
| Registration adapter | Build and POST a JSON Schema payload | `scripts/register_event_schema.py` |
| Readiness waiter | Bounded wait on `/subjects` | `scripts/wait_for_schema_registry.py` |
| Read-only verifier | GET subject versions and schema metadata | `scripts/verify_schema_registry.py` |
| Workflow | Wait, dry-run or apply, then optional verify | `scripts/register_isolated_event_schema.py` |

## Isolation

- Container: `merchant-e2e-schema-registry`
- Kafka topic for schema storage: `merchant-e2e-schemas`
- Network: `merchant_e2e_net`
- Replication factor: `1` (single-node E2E only)
- The shared `banking-*` development stack is unchanged.

## Registration contract

- Subject strategy: `<topic>-value` (Confluent TopicNameStrategy)
- Subject: `postgresql.banking.digital_banking.online_transaction-value`
- Payload: `{"schemaType": "JSON", "schema": "<inline JSON Schema v1>"}`
- Compatibility mode declared in the topic registry: `BACKWARD`
- Writes require the explicit `--apply` flag.

## Verification performed

```text
governance/schema-registry stack tests     pass
registration adapter tests                 pass
readiness waiter tests                     pass
read-only verifier tests                   pass
isolated registration workflow tests       pass
ruff check / ruff format --check           pass
dry-run payload generation                 pass
```

## Not proven

- No live Schema Registry registration was executed in this environment.
- Compatibility enforcement (`BACKWARD`) is declared in the registry contract and has not been
  exercised against a running registry.
- No producer or consumer was wired to the registry, so serializer-level compatibility is unproven.
- Exactly-once delivery and consumer-side deduplication stores remain unimplemented.

## Next runtime step

With Docker running, bring up the isolated stack and execute:

```bash
docker compose -f docker/docker-compose.e2e-merchant.yml --project-name merchant-e2e up -d \
  postgres minio iceberg-rest mc zookeeper kafka schema-registry

python scripts/register_isolated_event_schema.py \
  --registry-url http://localhost:8081 --apply --verify --version 1
```

That command is the first runtime proof; until it runs, the claim stays at contract level.
