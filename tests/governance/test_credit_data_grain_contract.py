"""Contract tests for canonical credit data grains and semantics."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "docs" / "04-operations" / "credit_data_grain_contract.yml"
SOURCE_DDL = ROOT / "docker" / "init_postgres" / "01_ddl_core_banking.sql"
BRONZE_LOAN = ROOT / "code_etl" / "bronze" / "core_banking" / "loan.yml"
BRONZE_PAYMENT = ROOT / "code_etl" / "bronze" / "core_banking" / "loan_payment.yml"
SILVER_LOAN = ROOT / "code_etl" / "silver" / "dims" / "dim_loan.yml"
SILVER_PAYMENT = ROOT / "code_etl" / "silver" / "facts" / "fact_loan_payment.yml"


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


def test_credit_quality_rules_cover_relationship_and_financial_invariants():
    rules = load_contract()["quality_rules"]["loan_payment"]
    assert "payment_id_unique" in rules
    assert "loan_id_not_null" in rules
    assert "amount_fields_non_negative" in rules
    assert "outstanding_after_non_negative" in rules
    assert "payment_date_not_after_cob_dt_for_snapshot" in rules


def test_source_ddl_supports_declared_loan_payment_measures_and_statuses():
    ddl = SOURCE_DDL.read_text(encoding="utf-8")
    for column in (
        "payment_id",
        "loan_id",
        "payment_date",
        "scheduled_amount",
        "amount_paid",
        "principal_component",
        "interest_component",
        "penalty",
        "outstanding_after",
        "days_late",
        "payment_status",
    ):
        assert column in ddl
    for status in ("PAID", "LATE", "MISSED", "PENDING"):
        assert status in ddl


def test_bronze_silver_mappings_preserve_the_credit_grains():
    bronze_loan = BRONZE_LOAN.read_text(encoding="utf-8")
    bronze_payment = BRONZE_PAYMENT.read_text(encoding="utf-8")
    silver_loan = SILVER_LOAN.read_text(encoding="utf-8")
    silver_payment = SILVER_PAYMENT.read_text(encoding="utf-8")
    assert "FROM core_banking.loan" in bronze_loan
    assert "FROM core_banking.loan_payment" in bronze_payment
    assert "business_key:\n  - loan_id" in silver_loan
    assert "business_key:\n  - payment_id" in silver_payment
    assert "p.cob_dt = DATE '{{ cob_dt }}'" in silver_payment


def test_contract_does_not_invent_installment_or_regulatory_semantics():
    payment = load_contract()["entities"]["loan_payment"]
    limitations = " ".join(payment["limitations"])
    assert "no separate installment_id" in limitations
    assert "source-owned criteria" in limitations
