"""Tests for the read-only Merchant EOD three-way verifier."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "verify_merchant_eod_reconciliation.py"
spec = importlib.util.spec_from_file_location("merchant_eod_verify", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_source_query_has_shared_cohort():
    query = module.source_query("2026-09-20")
    assert "merchant_id IS NOT NULL" in query
    assert "payment_channel IN ('QR', 'POS')" in query
    assert "DATE '2026-09-20'" in query
    assert "status = 'SUCCESS'" in query


def test_spark_query_uses_micros_for_current_cdc():
    query = module.spark_query("lakehouse.silver.fact_online_transaction_current", "2026-09-20", epoch_micros=True)
    assert "timestamp_micros" in query
    assert "timestamp_millis" not in query


def test_compare_detects_missing_group_and_amount_mismatch():
    source = {(1, "QR", "VND"): {"success_count": 1, "gross_amount": 10, "refund_count": 0}}
    batch = {}
    current = {(1, "QR", "VND"): {"success_count": 1, "gross_amount": 11, "refund_count": 0}}
    findings = module.compare(source, batch, current)
    assert any(f["status"] == "FAIL" for f in findings)
    assert any(f["metric"] == "success_count" and f["batch"] == "0" for f in findings)
    assert any(f["metric"] == "gross_amount" and f["current"] == "11" for f in findings)


def test_compare_passes_equal_three_way_cohort():
    values = {"success_count": 1, "gross_amount": 10, "refund_count": 0}
    findings = module.compare({(1, "QR", "VND"): values}, {(1, "QR", "VND"): values}, {(1, "QR", "VND"): values})
    assert findings and all(f["status"] == "PASS" for f in findings)
