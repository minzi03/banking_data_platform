"""Contracts for the data10 reference mapping."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "03-data" / "REFERENCE_DATA10_MAPPING.md"


def test_data10_is_explicitly_reference_only():
    text = DOC.read_text(encoding="utf-8")
    assert "reference-only" in text
    assert "not a replacement" in text
    assert "bank_data.db" in text


def test_mapping_covers_reference_domains_and_current_generator():
    text = DOC.read_text(encoding="utf-8")
    for name in (
        "customers.csv",
        "accounts.csv",
        "branches.csv",
        "employees.csv",
        "loans.csv",
        "credit_cards.csv",
        "transactions.csv",
    ):
        assert name in text
    assert "data_generator/generators/core_banking.py" in text
    assert "data_generator/generators/card_crm.py" in text
    assert "data_generator/generators/digital_banking.py" in text


def test_reference_quality_warnings_prevent_overclaiming():
    text = DOC.read_text(encoding="utf-8")
    assert "orphan loan references" in text
    assert "no canonical `cob_dt`" in text
    assert "No Merchant QR/POS settlement contract" in text
