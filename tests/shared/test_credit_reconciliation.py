"""Tests for pure credit expected-vs-paid reconciliation."""

from decimal import Decimal

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
    assert outcome([payment()])["outcome"] == "MATCHED"


def test_tolerance_boundary_is_inclusive():
    assert outcome([payment(amount_paid="100.05")], Decimal("0.05"))["outcome"] == "MATCHED"


def test_amount_outside_tolerance_mismatches():
    result = outcome([payment(amount_paid="99.00")])
    assert result["outcome"] == "MISMATCH"
    assert result["delta"] == Decimal("-1.00")


def test_late_payment_compares_paid_amount_including_recorded_penalty():
    row = payment(payment_status="LATE", scheduled_amount="100.00", amount_paid="102.00", penalty="2.00")
    result = outcome([row])
    assert result["outcome"] == "MISMATCH"
    assert result["delta"] == Decimal("2.00")


def test_missed_observation_is_missing_not_absence():
    assert outcome([payment(payment_status="MISSED", amount_paid="0")])["outcome"] == "MISSING"
    assert reconcile_payment_observations([]) == []


def test_pending_observation_is_pending():
    assert outcome([payment(payment_status="PENDING", amount_paid="0")])["outcome"] == "PENDING"


def test_duplicate_payment_id_is_not_comparable():
    assert outcome([payment(), payment(amount_paid="0")])["outcome"] == "NOT_COMPARABLE"


def test_results_are_deterministically_sorted_by_loan_and_date():
    rows = [payment(loan_id=2, payment_id=20), payment(loan_id=1, payment_id=11, cob_dt="2026-11-18")]
    results = reconcile_payment_observations(rows)
    assert [(row["loan_id"], row["cob_dt"]) for row in results] == [(1, "2026-11-18"), (2, "2026-11-17")]
