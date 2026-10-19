"""Governance contract for the project10 reference mapping."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "03-data" / "REFERENCE_PROJECT10_MAPPING.md"


def test_project10_is_reference_only():
    text = DOC.read_text(encoding="utf-8")
    assert "reference-only" in text
    assert "canonical production path" in text
    assert "are imported" in text
    assert "Do not import" in text


def test_mapping_names_reference_domains_and_current_platform():
    text = DOC.read_text(encoding="utf-8")
    for name in ("clients", "comptes", "transactions", "paiements", "virements", "cartes", "alertes_fraude", "credits"):
        assert f"`{name}`" in text
    for name in (
        "core_banking.customer",
        "core_banking.account",
        "card_crm.card",
        "digital_banking.online_transaction",
    ):
        assert name in text
    assert "data_generator/" in text
    assert "Merchant QR/POS" in text


def test_mapping_prevents_reference_claim_overreach():
    text = DOC.read_text(encoding="utf-8")
    assert "screenshots" in text
    assert "exactly-once" in text
    assert "Great Expectations 51/53" in text
    assert "Dữ liệu tham khảo" not in text
