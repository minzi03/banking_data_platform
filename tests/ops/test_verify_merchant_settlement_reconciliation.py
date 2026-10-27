"""Static checks for the settlement reconciliation verifier."""

import sys
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


def test_isolated_fixture_uses_td22_aggregate_totals():
    text = (ROOT / "docker" / "init_merchant_e2e" / "03_settlement.sql").read_text(encoding="utf-8")
    assert "35000.00" in text
    assert "5000.00" in text
    assert "txn_count, currency, txn_count" not in text
    assert "2026-10-18" in text


def test_compare_fails_closed_on_every_mismatch():
    from decimal import Decimal

    sys.path.insert(0, str(ROOT / "scripts"))
    import verify_merchant_settlement_reconciliation as verifier

    key = (900001, "VND", "VND")
    ok = {key: {"gross_amount": Decimal("35000.00"), "txn_count": Decimal("2")}}
    amount_bad = {key: {"gross_amount": Decimal("35000.01"), "txn_count": Decimal("2")}}
    count_bad = {key: {"gross_amount": Decimal("35000.00"), "txn_count": Decimal("3")}}

    assert verifier.compare(ok, ok)[0]["outcome"] == "PASS"
    assert verifier.compare(ok, amount_bad)[0]["outcome"] == "GROSS_AMOUNT_MISMATCH"
    assert verifier.compare(ok, count_bad)[0]["outcome"] == "TXN_COUNT_MISMATCH"
    assert verifier.compare(ok, {})[0]["outcome"] == "MISSING_SETTLEMENT"
    assert verifier.compare({}, ok)[0]["outcome"] == "MISSING_TRANSACTION"
