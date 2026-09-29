"""
Lịch trả nợ có trạng thái trễ hạn nối tiếp (ROADMAP 3.7, bước 1).

Bản trước bốc PAID / LATE / MISSED độc lập từng kỳ. Hệ quả đo được trên chính
generator cũ (negative control của file này): xác suất kỳ sau quá hạn gần như
bằng nhau dù kỳ trước quá hạn hay không — roll rate chỉ là phân phối không điều
kiện; days_late không bao giờ vượt 90 nên nhóm nợ 4–5 không tồn tại; và
loan_status không khớp lịch trả (OVERDUE mà kỳ cuối trả đúng hạn).
"""

from __future__ import annotations

import random
import sys
from collections import defaultdict
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "data_generator"))

from generators.core_banking import (  # noqa: E402
    DEFAULT_ROLL_RATES,
    days_late_for,
    delinquency_path,
    generate_loan_payments,
    generate_loans,
)

SEED_CONFIG = REPO_ROOT / "data_generator" / "config" / "seed_config.yaml"

# Vị trí cột trong tuple của generate_loan_payments / generate_loans
PAY_LOAN_ID, PAY_DATE, PAY_DAYS_LATE, PAY_STATUS = 1, 2, 9, 11
LOAN_ID, LOAN_STATUS = 0, 10


@pytest.fixture(scope="module")
def config() -> dict:
    return yaml.safe_load(SEED_CONFIG.read_text(encoding="utf-8"))["core_banking"]


@pytest.fixture(scope="module")
def generated(config):
    random.seed(21)
    loans = generate_loans(3000, config["loan"], list(range(1, 501)), ["BR001"], ["LOAN001"])
    payments = generate_loan_payments(loans, config["loan_payment"])
    by_loan = defaultdict(list)
    for p in payments:
        by_loan[p[PAY_LOAN_ID]].append(p)
    for rows in by_loan.values():
        rows.sort(key=lambda p: p[PAY_DATE])
    return {loan[LOAN_ID]: loan[LOAN_STATUS] for loan in loans}, by_loan


def test_config_rows_are_distributions(config):
    for rates in (config["loan_payment"]["roll_rates"], DEFAULT_ROLL_RATES):
        assert set(rates) == {0, 1, 2, 3, 4}
        for bucket, row in rates.items():
            assert abs(sum(row.values()) - 1) < 1e-9, bucket
            assert set(row) <= {0, 1, 2, 3, 4}


def test_delinquency_has_memory(generated):
    """Cốt lõi: kỳ trước quá hạn thì kỳ sau dễ quá hạn hơn nhiều. Với generator
    bốc độc lập, hai xác suất này xấp xỉ bằng nhau."""
    _, by_loan = generated
    counts = {True: [0, 0], False: [0, 0]}  # prev_delinquent -> [n, next_delinquent]
    for rows in by_loan.values():
        for prev, nxt in zip(rows, rows[1:], strict=False):
            key = prev[PAY_DAYS_LATE] > 0
            counts[key][0] += 1
            counts[key][1] += nxt[PAY_DAYS_LATE] > 0
    p_after_delinquent = counts[True][1] / counts[True][0]
    p_after_current = counts[False][1] / counts[False][0]
    assert counts[True][0] > 200, "quá ít kỳ quá hạn để đo"
    assert p_after_delinquent > 3 * p_after_current, (p_after_delinquent, p_after_current)


def test_loan_status_matches_latest_installment(generated):
    statuses, by_loan = generated
    checked = defaultdict(int)
    for loan_id, rows in by_loan.items():
        status, last = statuses[loan_id], rows[-1]
        if status in ("ACTIVE", "CLOSED"):
            assert last[PAY_DAYS_LATE] == 0, (loan_id, status, last)
        elif status == "OVERDUE":
            assert last[PAY_DAYS_LATE] > 0, (loan_id, status, last)
        checked[status] += 1
    assert checked["OVERDUE"] > 50 and checked["ACTIVE"] > 50


def test_closed_loans_always_paid_on_time(generated):
    statuses, by_loan = generated
    for loan_id, rows in by_loan.items():
        if statuses[loan_id] == "CLOSED":
            assert all(r[PAY_STATUS] == "PAID" for r in rows), loan_id


def test_status_matches_days_late_bucket(generated):
    _, by_loan = generated
    for rows in by_loan.values():
        for r in rows:
            days, status = r[PAY_DAYS_LATE], r[PAY_STATUS]
            if days == 0:
                assert status == "PAID"
            elif days < 30:
                assert status == "LATE"
            else:
                assert status == "MISSED"


def test_deep_delinquency_exists(generated):
    """Nhóm nợ 3–5 (≥90 ngày) phải xuất hiện — bản cũ chặn trần ở 90."""
    _, by_loan = generated
    deepest = max(r[PAY_DAYS_LATE] for rows in by_loan.values() for r in rows)
    assert deepest >= 180, deepest


def test_path_respects_requested_end():
    random.seed(4)
    for _ in range(200):
        assert delinquency_path(24, DEFAULT_ROLL_RATES, "current")[-1] == 0
        assert delinquency_path(24, DEFAULT_ROLL_RATES, "delinquent")[-1] > 0
    assert delinquency_path(0, DEFAULT_ROLL_RATES, "current") == []
    # Không thể quá hạn ở kỳ đầu nếu ma trận không cho trượt khỏi bucket 0:
    # phải ép kỳ cuối thay vì lặp vô hạn.
    frozen = {**DEFAULT_ROLL_RATES, 0: {0: 1.0}}
    assert delinquency_path(3, frozen, "delinquent", max_tries=5)[-1] > 0


def test_days_late_ranges():
    random.seed(9)
    assert days_late_for(0, 0) == 0
    assert all(1 <= days_late_for(1, 0) <= 29 for _ in range(100))
    assert all(30 <= days_late_for(2, 0) <= 59 for _ in range(100))
    assert all(60 <= days_late_for(3, 0) <= 89 for _ in range(100))
    assert 90 <= days_late_for(4, 1) <= 119
    assert 150 <= days_late_for(4, 3) <= 179
    assert days_late_for(4, 10_000) == 32_767  # trần SMALLINT
