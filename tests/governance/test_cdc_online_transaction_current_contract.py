"""Contract checks for the Merchant EOD CDC current-state projection."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "code_etl" / "cdc" / "consolidation" / "config" / "cdc_consolidation_online_transaction.yml"
DDL = ROOT / "docker" / "init_iceberg" / "06_ddl_silver_cdc_current.sql"
DAG = ROOT / "airflow" / "dags" / "cdc" / "cdc_consolidation_dag.py"


def test_online_transaction_consolidation_config_is_current_state():
    config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    assert config["source_table"] == "lakehouse.bronze.online_transaction_cdc"
    assert config["target_table"] == "lakehouse.silver.fact_online_transaction_current"
    assert config["business_key"] == "transaction_id"
    assert "merchant_id" in config["columns"]
    assert "payment_channel" in config["columns"]
    assert "refund_flag" in config["columns"]


def test_current_state_ddl_contains_merchant_cohort_columns():
    ddl = DDL.read_text(encoding="utf-8")
    block = ddl.split("lakehouse.silver.fact_online_transaction_current", 1)[1]
    for column in ("transaction_id", "amount", "currency", "status", "merchant_id", "payment_channel", "refund_flag"):
        assert column in block


def test_consolidation_dag_runs_online_transaction_after_existing_targets():
    dag = DAG.read_text(encoding="utf-8")
    assert "cdc_consolidation_online_transaction.yml" in dag
    assert '"name": "online_transaction"' in dag
