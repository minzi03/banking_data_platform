# Container image reference inventory

This inventory records image references and immutability status for the maintained Compose stacks. It is not a runtime availability check. A version tag is more reproducible than `latest`, but only a digest pins exact image content.

## Digest-pinned references

| Image | Compose files | Digest | Notes |
|---|---|---|---|
| `ghcr.io/minzi03/minio:RELEASE.2025-09-07T16-13-09Z` | main, CI, isolated merchant E2E | `sha256:a1a8bd4ac40ad7881a245bab97323e18f971e4d4cba2c2007ec1bedd21cbaba2` | Frozen mirror; cached `linux/amd64` manifest only. Keep TD-15 open. |
| `ghcr.io/minzi03/mc:RELEASE.2025-08-13T08-35-41Z` | main, CI, isolated merchant E2E | `sha256:eb4ea9884b77704230e2423e9004d2fa738dc272876b9cc41a297d29443b8780` | Frozen mirror; cached `linux/amd64` manifest only. Keep TD-15 open. |
| `provectuslabs/kafka-ui` | main | `sha256:8f2ff02d64b0a7a2b71b6b3b3148b85f66d00ec20ad40c30bdcd415d46d31818` | Digest-pinned; tag is not recorded in Compose. |

## Tag-pinned, digest not recorded

The following references use explicit version tags but are not digest-pinned. They can change if a registry retargets a tag. Migrate to digests only after confirming the intended platform manifests and image update process.

| Reference | Compose files |
|---|---|
| `postgres:15-alpine` | main, CI, isolated merchant E2E |
| `atmoz/sftp:alpine` | main |
| `tabulario/iceberg-rest:1.6.0` | main, CI, isolated merchant E2E |
| `confluentinc/cp-zookeeper:7.6.0` | main, isolated merchant E2E |
| `confluentinc/cp-kafka:7.6.0` | main, isolated merchant E2E |
| `confluentinc/cp-schema-registry:7.6.0` | isolated merchant E2E |
| `debezium/connect:2.6` | main, isolated merchant E2E |
| `trinodb/trino:443` | main, CI |
| `mysql:8.0` | main |
| `elasticsearch:8.12.0` | main |
| `openmetadata/server:1.5.6` | main |
| `ghcr.io/mlflow/mlflow:v2.16.0` | main |
| `prom/prometheus:v2.51.0` | main |
| `prom/alertmanager:v0.27.0` | main |
| `grafana/grafana:10.4.0` | main |
| `apache/superset:3.1.3` | custom image base |
| `apache/airflow:2.10.0-python3.11` | custom image base |
| `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` | custom image base |
| `python:3.11-slim` | API, Streamlit, dbt, freshness exporter custom image bases |
| `docker:27.3-cli` | Airflow custom image build stage |

Locally built images (`banking-airflow:2.10.0` and Spark services built from `Dockerfile.spark`) are tied to their Dockerfile and base image, not a registry digest at Compose service level.

## Policy

- Reject floating `latest` and untagged image references.
- Require digest references for the frozen MinIO and `mc` mirrors consistently in main, CI, and E2E Compose files.
- Tag-pinned references remain an explicitly tracked transitional category; do not invent or copy digests without registry/platform verification.
- A future MinIO replacement is a separate compatibility decision requiring a registry source and validation of S3 API behavior with Iceberg REST/Spark, CI pullability, and supported architectures.
