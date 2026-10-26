"""Static contract for aggregate merchant settlement reconciliation."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "docs" / "04-operations" / "merchant_settlement_reconciliation.yml"
SFTP_CONFIG = ROOT / "code_etl" / "bronze" / "file_sources" / "pos_settlement.yml"
SAMPLE = ROOT / "docker" / "init_sftp" / "pos_settlement_sample.csv"


def load_contract() -> dict:
    return yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))


def test_settlement_contract_matches_existing_sftp_source():
    contract = load_contract()
    config = yaml.safe_load(SFTP_CONFIG.read_text(encoding="utf-8"))
    assert contract["source"]["settlement_table"] == "lakehouse.bronze.pos_settlement"
    assert config["target"]["table"] == "pos_settlement"
    assert config["source"]["file_pattern"] == "pos_settlement_*.csv"
    assert SAMPLE.exists()


def test_aggregate_reconciliation_key_and_metrics_are_explicit():
    contract = load_contract()
    assert contract["key"] == ["merchant_id", "settlement_date", "currency"]
    assert contract["metrics"] == ["gross_amount", "txn_count"]
    assert contract["matching_rules"]["gross_amount_tolerance"] == 0.0
    assert contract["matching_rules"]["txn_count_tolerance"] == 0


def test_mismatch_and_missing_outcomes_fail_closed():
    contract = load_contract()
    outcomes = set(contract["outcomes"])
    assert {
        "MISSING_SETTLEMENT",
        "MISSING_TRANSACTION",
        "GROSS_AMOUNT_MISMATCH",
        "TXN_COUNT_MISMATCH",
        "DUPLICATE_SETTLEMENT_KEY",
    } <= outcomes
    assert contract["matching_rules"]["missing_transaction"] == "fail"
    assert contract["matching_rules"]["missing_settlement"] == "fail"
    assert contract["matching_rules"]["duplicate_settlement_key"] == "fail"


def test_contract_does_not_overclaim_transaction_level_reconciliation():
    limitations = " ".join(load_contract()["limitations"])
    assert "transaction_id" in limitations
    assert "aggregate reconciliation" in limitations
