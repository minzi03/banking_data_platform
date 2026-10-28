"""Contract tests for the isolated Schema Registry service."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker" / "docker-compose.e2e-merchant.yml"


def load_compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


def test_schema_registry_isolated_from_shared_stack():
    services = load_compose()["services"]
    registry = services["schema-registry"]

    assert registry["container_name"] == "merchant-e2e-schema-registry"
    assert registry["environment"]["SCHEMA_REGISTRY_KAFKASTORE_TOPIC"] == "merchant-e2e-schemas"
    assert registry["environment"]["SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS"] == "PLAINTEXT://kafka:9092"
    assert registry["networks"] == ["merchant_e2e_net"]


def test_schema_registry_waits_for_isolated_kafka():
    registry = load_compose()["services"]["schema-registry"]

    assert registry["depends_on"]["kafka"]["condition"] == "service_healthy"
    assert registry["healthcheck"]["test"] == ["CMD-SHELL", "curl -sf http://localhost:8081/subjects"]


def test_schema_registry_uses_single_node_replication_factor():
    environment = load_compose()["services"]["schema-registry"]["environment"]

    assert environment["SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR"] == 1
