"""Governance checks for event identity and CDC replay boundaries."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "schemas" / "topic_registry.yml"


def test_registered_event_identity_is_explicit():
    topic = yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))["topics"][0]
    assert topic["key"] == "transaction_id"
    assert topic["idempotency_key"] == "event_id"
    assert topic["delivery_semantics"] == "at_least_once"


def test_cdc_connector_retains_source_coordinates():
    paths = [
        ROOT / "code_etl" / "cdc" / "register_connectors.py",
        ROOT / "airflow" / "dags" / "cdc" / "cdc_register_connectors_dag.py",
    ]
    for path in paths:
        text = path.read_text(encoding="utf-8")
        assert '"transforms.unwrap.add.fields": "op,ts_ms,source.ts_ms"' in text


def test_bronze_online_transaction_retains_kafka_coordinates():
    ddl = (ROOT / "docker" / "init_iceberg" / "04_ddl_bronze_cdc.sql").read_text(encoding="utf-8")
    block = ddl.split("CREATE TABLE IF NOT EXISTS lakehouse.bronze.online_transaction_cdc", 1)[1].split(
        "USING iceberg", 1
    )[0]
    assert "__kafka_topic" in block
    assert "__kafka_partition" in block
    assert "__kafka_offset" in block
