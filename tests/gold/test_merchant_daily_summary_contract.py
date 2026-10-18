"""Static cohort contract for the Merchant daily Gold summary."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SUMMARY = ROOT / "code_etl" / "gold" / "time_analytics" / "merchant_daily_summary.yml"


def test_merchant_summary_has_explicit_qr_pos_cohort_and_success_revenue():
    text = SUMMARY.read_text(encoding="utf-8")
    assert "t.merchant_id IS NOT NULL" in text
    assert "t.payment_channel IN ('QR', 'POS')" in text
    assert "status = 'SUCCESS'" in text
    assert "refund_flag = 1" in text


def test_merchant_summary_does_not_mix_nonmerchant_channel_rows():
    text = SUMMARY.read_text(encoding="utf-8")
    where = text.split("WHERE t.merchant_id IS NOT NULL", 1)[1].split("GROUP BY", 1)[0]
    assert "payment_channel IN ('QR', 'POS')" in where
