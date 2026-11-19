"""Contract tests for canonical credit data grains and semantics."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "docs" / "04-operations" / "credit_data_grain_contract.yml"


def load_contract():
    return yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))


def test_credit_contract_declares_current_canonical_sources():
    contract = load_contract()
    assert contract["source_system"] == "postgresql"
    assert contract["business_date_field"] == "cob_dt"
    assert contract["entities"]["loan"]["source_table"] == "core_banking.loan"
    assert contract["entities"]["loan_payment"]["source_table"] == "core_banking.loan_payment"


def test_credit_grains_and_keys_are_explicit():
    entities = load_contract()["entities"]
    assert entities["loan"]["grain"] == "one_current_loan_contract_per_loan_id_and_cob_dt"
    assert entities["loan"]["business_key"] == ["loan_id"]
    assert entities["loan_payment"]["grain"] == "one_payment_observation_per_payment_id_and_cob_dt"
    assert entities["loan_payment"]["business_key"] == ["payment_id"]
    assert entities["loan_payment"]["relationship_key"] == ["loan_id"]


def test_payment_statuses_methods_and_measures_match_source_contract():
    payment = load_contract()["entities"]["loan_payment"]
    assert payment["statuses"] == ["PAID", "LATE", "MISSED", "PENDING"]
    assert "scheduled_amount" in payment["measures"]
    assert "amount_paid" in payment["measures"]
    assert payment["payment_methods"] == ["BANK_TRANSFER", "CASH", "CHEQUE", "DEBIT_CARD", "MOBILE_APP"]


def test_contract_does_not_invent_installment_or_regulatory_semantics():
    payment = load_contract()["entities"]["loan_payment"]
    limitations = " ".join(payment["limitations"])
    assert "no separate installment_id" in limitations
    assert "source-owned criteria" in limitations
