"""
Tài khoản CLOSED phải có số dư 0 và ngày đóng.

Stack 2026-09-30: quarantine `invalid_account/closed_with_balance` (severity FAIL)
bắt 4.622/30.000 tài khoản — generator giữ số dư ngẫu nhiên cho tài khoản đã đóng,
nên ops_quarantine_dag fail mỗi ngày và AUM cộng cả tiền trong tài khoản đã đóng.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "data_generator"))

from generators.core_banking import generate_accounts  # noqa: E402

STATUS = 10  # vị trí cột status trong tuple account
BALANCE = 7
CLOSE_DATE = 9


def _accounts(n=2000):
    random.seed(7)
    return generate_accounts(
        n, {}, customer_ids=list(range(1, 200)), branch_codes=["BR001"], product_codes=["CASA001", "SAV001"]
    )


def test_closed_accounts_exist():
    assert any(row[STATUS] == "CLOSED" for row in _accounts())


def test_closed_accounts_have_zero_balance_and_a_close_date():
    closed = [row for row in _accounts() if row[STATUS] == "CLOSED"]
    assert all(row[BALANCE] == 0 for row in closed)
    assert all(row[CLOSE_DATE] is not None for row in closed)


def test_open_accounts_keep_a_positive_balance():
    assert all(row[BALANCE] > 0 for row in _accounts() if row[STATUS] != "CLOSED")
