"""Unit tests for aggregate merchant settlement reconciliation."""

from decimal import Decimal

from code_etl.shared.ops.merchant_settlement_reconciliation import (
    reconcile,
    to_reconciliation_log_rows,
)

BASE_TXN = {
    "merchant_id": 900001,
    "settlement_date": "2026-10-18",
    "currency": "VND",
    "gross_amount": "10000.00",
    "txn_count": 1,
}


def settlement(**overrides):
    row = dict(BASE_TXN)
    row.update(overrides)
    return row


def test_matching_aggregate_passes():
    result = reconcile([BASE_TXN], [settlement()])
    assert result[0]["outcome"] == "PASS"


def test_amount_mismatch_is_fail_closed():
    result = reconcile([BASE_TXN], [settlement(gross_amount="10000.01")])
    assert result[0]["outcome"] == "GROSS_AMOUNT_MISMATCH"


def test_missing_settlement_is_detected():
    result = reconcile([BASE_TXN], [])
    assert result[0]["outcome"] == "MISSING_SETTLEMENT"


def test_missing_transaction_is_detected():
    result = reconcile([], [settlement()])
    assert result[0]["outcome"] == "MISSING_TRANSACTION"


def test_duplicate_settlement_key_is_detected():
    result = reconcile([BASE_TXN], [settlement(), settlement()])
    assert result[0]["outcome"] == "DUPLICATE_SETTLEMENT_KEY"


def test_count_tolerance_is_explicit():
    result = reconcile(
        [BASE_TXN],
        [settlement(txn_count=2)],
        amount_tolerance=Decimal("0.00"),
        count_tolerance=1,
    )
    assert result[0]["outcome"] == "PASS"


def test_result_maps_to_existing_reconciliation_log_schema():
    result = reconcile([BASE_TXN], [settlement(gross_amount="9999.00")])
    rows = to_reconciliation_log_rows(result, "2026-10-18", Decimal("0.00"))
    assert rows[0]["check_status"] == "FAIL"
    assert rows[0]["metric_name"] == "merchant_settlement.gross_amount_mismatch"
    assert rows[0]["cob_dt"] == "2026-10-18"
