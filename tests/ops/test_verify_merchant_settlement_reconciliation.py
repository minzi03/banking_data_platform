"""Static checks for the settlement reconciliation verifier."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
VERIFIER = ROOT / "scripts" / "verify_merchant_settlement_reconciliation.py"
CONTRACT = ROOT / "docs" / "04-operations" / "merchant_settlement_reconciliation.yml"


def load_verifier() -> str:
    return VERIFIER.read_text(encoding="utf-8")


def test_verifier_is_aligned_with_isolated_stack():
    text = load_verifier()
    assert "merchant-e2e-postgres" in text
    assert "merchant-e2e-spark-worker" in text
    assert "digital_banking.pos_settlement" in text
    assert "lakehouse.silver.fact_online_transaction" in text


def test_verifier_reports_pass_and_failure_codes():
    text = load_verifier()
    assert "SETTLEMENT_RECONCILIATION_PASS=True" in text or "SETTLEMENT_RECONCILIATION_PASS=" in text
    assert "return 0 if all_pass else 1" in text
    assert "VerificationError" in text


def test_verifier_eod_key_matches_contract():
    contract = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    text = load_verifier()
    for field in contract["key"]:
        assert field in text
    for metric in contract["metrics"]:
        assert metric in text
