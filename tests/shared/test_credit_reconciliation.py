"""Tests for pure credit expected-vs-paid reconciliation."""

from decimal import Decimal

import pytest

from code_etl.shared.ops.credit_reconciliation import reconcile_payment_observations


def payment(**overrides):
    row = {
        "loan_id": 1,
        "cob_dt": "2026-11-17",
        "payment_id": 10,
        "scheduled_amount": "100.00",
        "amount_paid": "100.00",
        "payment_status": "PAID",
    }
    row.update(overrides)
    return row


def outcome(rows, tolerance=Decimal("0.00")):
    return reconcile_payment_observations(rows, amount_tolerance=tolerance)[0]


def test_exact_payment_matches():
    result = outcome([payment()])
    assert result["outcome"] == "MATCHED"
    assert result["status"] == "MATCHED"
    assert result["check_status"] == "PASS"


def test_tolerance_boundary_is_inclusive():
    assert outcome([payment(amount_paid="100.05")], Decimal("0.05"))["outcome"] == "MATCHED"


def test_amount_outside_tolerance_mismatches():
    result = outcome([payment(amount_paid="99.00")])
    assert result["outcome"] == "MISMATCH"
    assert result["status"] == "MISMATCH"
    assert result["check_status"] == "FAIL"
    assert result["delta"] == Decimal("-1.00")


def test_late_payment_total_with_penalty_is_not_silently_treated_as_scheduled_amount():
    row = payment(payment_status="LATE", scheduled_amount="100.00", amount_paid="102.00", penalty="2.00")
    result = outcome([row])
    assert result["outcome"] == "MISMATCH"
    assert result["delta"] == Decimal("2.00")


def test_overpayment_and_underpayment_both_mismatch_outside_tolerance():
    overpaid = outcome([payment(amount_paid="101.00")])
    underpaid = outcome([payment(amount_paid="99.00")])
    assert (overpaid["outcome"], overpaid["delta"]) == ("MISMATCH", Decimal("1.00"))
    assert (underpaid["outcome"], underpaid["delta"]) == ("MISMATCH", Decimal("-1.00"))


def test_observations_are_aggregated_within_loan_and_cob_date():
    rows = [
        payment(payment_id=10, scheduled_amount="60.00", amount_paid="60.00"),
        payment(payment_id=11, scheduled_amount="40.00", amount_paid="40.00"),
    ]
    result = outcome(rows)
    assert result["outcome"] == "MATCHED"
    assert result["scheduled_amount"] == Decimal("100.00")
    assert result["amount_paid"] == Decimal("100.00")
    assert result["payment_ids"] == ["10", "11"]


def test_missed_observation_is_missing_not_absence():
    result = outcome([payment(payment_status="MISSED", amount_paid="0")])
    assert result["outcome"] == "MISSING"
    assert result["check_status"] == "FAIL"
    assert reconcile_payment_observations([]) == []


def test_pending_observation_is_pending():
    result = outcome([payment(payment_status="PENDING", amount_paid="0")])
    assert result["outcome"] == "PENDING"
    assert result["check_status"] == "WARN"


def test_duplicate_payment_id_is_not_comparable():
    result = outcome([payment(), payment(amount_paid="0")])
    assert result["outcome"] == "NOT_COMPARABLE"
    assert result["check_status"] == "WARN"


@pytest.mark.parametrize(
    "overrides",
    [
        {"loan_id": None},
        {"cob_dt": None},
        {"payment_id": None},
        {"payment_status": "UNKNOWN"},
        {"payment_status": None},
        {"scheduled_amount": None},
        {"amount_paid": "not-money"},
        {"amount_paid": "NaN"},
        {"scheduled_amount": "-1.00"},
    ],
)
def test_invalid_observations_fail_closed(overrides):
    with pytest.raises(ValueError):
        outcome([payment(**overrides)])


def test_negative_tolerance_is_rejected():
    with pytest.raises(ValueError, match="amount_tolerance"):
        reconcile_payment_observations([payment()], amount_tolerance=Decimal("-0.01"))


def test_results_are_deterministically_sorted_by_loan_and_date():
    rows = [payment(loan_id=2, payment_id=20), payment(loan_id=1, payment_id=11, cob_dt="2026-11-18")]
    results = reconcile_payment_observations(rows)
    assert [(row["loan_id"], row["cob_dt"]) for row in results] == [(1, "2026-11-18"), (2, "2026-11-17")]
