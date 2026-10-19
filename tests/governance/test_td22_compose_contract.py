"""Static contracts for the isolated TD-22 Merchant EOD stack."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker" / "docker-compose.e2e-merchant.yml"


def test_td22_stack_uses_isolated_names_and_resources():
    config = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = config["services"]
    assert set(services) >= {"postgres", "minio", "zookeeper", "kafka", "debezium"}
    for service in services.values():
        assert service["container_name"].startswith("merchant-e2e-")
        assert "banking-" not in service["container_name"]
    volume_names = set(config["volumes"])
    assert volume_names == {"merchant_e2e_postgres", "merchant_e2e_minio"}
    assert config["networks"]["merchant_e2e_net"]["name"] == "merchant_e2e_net"


def test_td22_compose_does_not_reuse_production_topics_or_checkpoints():
    text = COMPOSE.read_text(encoding="utf-8")
    assert "qr.payment.events" not in text
    assert "qr_payment_v1" not in text
    assert "merchant_e2e" in text
