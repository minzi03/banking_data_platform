"""Tests for the shared reconciliation result contract."""

import pytest

from code_etl.shared.ops.reconciliation_contract import (
    check_status_for,
    status_for_outcome,
    with_reconciliation_status,
)


@pytest.mark.parametrize(
    ("outcome", "status", "check_status"),
    [
        ("PASS", "MATCHED", "PASS"),
        ("GROSS_AMOUNT_MISMATCH", "MISMATCH", "FAIL"),
        ("TXN_COUNT_MISMATCH", "MISMATCH", "FAIL"),
        ("MISSING_TRANSACTION", "MISSING", "FAIL"),
        ("MISSING_SETTLEMENT", "MISSING", "FAIL"),
        ("DUPLICATE_SETTLEMENT_KEY", "DUPLICATE", "FAIL"),
    ],
)
def test_domain_outcome_maps_to_shared_and_legacy_status(outcome, status, check_status):
    result = with_reconciliation_status({"outcome": outcome})
    assert result["status"] == status
    assert result["check_status"] == check_status


@pytest.mark.parametrize(
    ("status", "check_status"),
    [("PENDING", "WARN"), ("AMBIGUOUS", "WARN"), ("NOT_COMPARABLE", "WARN")],
)
def test_non_terminal_statuses_are_legacy_warnings(status, check_status):
    assert check_status_for(status) == check_status


def test_enrichment_preserves_domain_fields():
    original = {"outcome": "PASS", "merchant_id": 900001, "delta": 0}
    result = with_reconciliation_status(original)
    assert result["merchant_id"] == 900001
    assert result["delta"] == 0
    assert result["outcome"] == "PASS"


@pytest.mark.parametrize(
    "bad_value",
    ["UNKNOWN", "", None],
)
def test_unknown_status_fails_closed(bad_value):
    with pytest.raises(ValueError, match="Unknown"):
        check_status_for(bad_value)


def test_unknown_outcome_fails_closed():
    with pytest.raises(ValueError, match="Unknown"):
        status_for_outcome("UNKNOWN")
