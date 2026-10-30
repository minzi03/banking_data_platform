# Isolated Schema Registry Runtime Registration

Date: 2026-10-29
Stack: `docker/docker-compose.e2e-merchant.yml`, project `docker`
Registry: `merchant-e2e-schema-registry` (Confluent Schema Registry 7.6.0)

## What was run

The isolated stack containers already existed from the TD-22 work under Compose project `docker`.
Starting the file a second time under project `merchant-e2e` failed on duplicate container names, so
the existing project was reused instead of creating duplicate resources:

```text
docker compose -f docker/docker-compose.e2e-merchant.yml --project-name docker \
  start postgres minio iceberg-rest mc zookeeper kafka
docker compose -f docker/docker-compose.e2e-merchant.yml --project-name docker up -d schema-registry
```

Note: `--project-name docker` is the derived default project of the existing containers, not a
choice to merge with the shared `banking-*` stack. The shared stack was not started or modified.

## Bug found and fixed

`scripts/wait_for_schema_registry.py` imported `scripts.verify_schema_registry` at module level.
Running it as a script puts `scripts/` on `sys.path`, not the repository root, so the import failed:

```text
ModuleNotFoundError: No module named 'scripts'
```

Fixed by inserting the repository root into `sys.path` before the import.

The registry also had no published port, so a host-side verifier could not reach it. A loopback-only
mapping was added:

```yaml
ports:
  - "127.0.0.1:8081:8081"
```

## Registration result

```text
$ python scripts/register_isolated_event_schema.py \
    --registry-url http://localhost:8081 --apply --verify --verify-local --version 1

Schema Registry ready
{"id": 1}
{"subject": "postgresql.banking.digital_banking.online_transaction-value", "version": 1, "schema_id": 1}
exit=0
```

Read-back:

```text
GET /subjects
  ["postgresql.banking.digital_banking.online_transaction-value"]
GET /subjects/.../versions
  [1]
GET /subjects/.../versions/1
  id=1 version=1 schemaType=JSON
GET /config
  {"compatibilityLevel":"BACKWARD"}
Kafka topic in isolated broker
  merchant-e2e-schemas
```

## Compatibility behaviour observed

| Candidate schema | `POST /compatibility/.../versions/latest` |
|---|---|
| v1 plus optional `risk_band` | `{"is_compatible": true}` |
| v1 plus new field made **required** | `{"is_compatible": true}` |

The second result is the notable one: Confluent JSON Schema BACKWARD checks accepted a newly
required payload field. That means registry compatibility approval alone does not prove the contract
in `schemas/online_transaction_event_v1.json`, and the in-repo schema tests remain the authority for
required-field rules. This was not previously assumed and is now measured.

## Not proven

- No producer or consumer uses the registry serializer, so wire-level compatibility is unproven.
- No Avro or Protobuf subjects exist; only JSON Schema is exercised.
- Consumer-side deduplication stores and exactly-once side effects remain unimplemented.
- The `--project-name docker` reuse is an environment detail, not a hardening decision.

## Reproduce

```bash
docker compose -f docker/docker-compose.e2e-merchant.yml --project-name docker \
  up -d postgres minio iceberg-rest mc zookeeper kafka schema-registry

python scripts/register_isolated_event_schema.py \
  --registry-url http://localhost:8081 --apply --verify --verify-local --version 1
```

Re-running is idempotent: the same schema returns the existing schema id instead of a new version.
