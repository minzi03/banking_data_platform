"""
Sinh theo khối (generate_all ở --scale lớn) phải cho cùng dữ liệu hợp lệ như sinh một lần.

Đo 2026-09-30: ~384 byte/dòng giao dịch, nên --scale 10 giữ bốn bảng lớn trong RAM là
~14 GB — vượt RAM máy dev. generate_all giờ sinh và ghi từng khối CHUNK_ROWS dòng; các
test dưới canh ba thứ việc chia khối có thể làm hỏng: id trùng / đứt, số dư không nối
tiếp giữa các khối, và CSV lặp header.
"""

from __future__ import annotations

import csv
import random
import sys
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "data_generator"))

# generate_all → connectors.postgres_writer import psycopg2, mà venv unit CI không cài
# (CI đỏ ở #105). Test này không chạm DB: stub chỉ khi gói thật vắng mặt.
_STUBBED = []
try:
    import psycopg2  # noqa: F401
except ModuleNotFoundError:
    from unittest.mock import MagicMock

    _STUBBED = ["psycopg2", "psycopg2.extras"]
    sys.modules["psycopg2"] = MagicMock()
    sys.modules["psycopg2.extras"] = sys.modules["psycopg2"].extras

import generate_all  # noqa: E402

# Gỡ stub ngay: test khác (test_card_txn_entry_mode) tự patch psycopg2 và cần __spec__ thật.
for _name in _STUBBED:
    sys.modules.pop(_name, None)
import yaml  # noqa: E402
from connectors.csv_writer import CsvWriter  # noqa: E402
from generators.card_crm import generate_card_txn  # noqa: E402
from generators.core_banking import generate_loan_payments, generate_loans, generate_txn_account  # noqa: E402
from generators.digital_banking import generate_online_transactions  # noqa: E402

CFG = yaml.safe_load((REPO_ROOT / "data_generator" / "config" / "seed_config.yaml").read_text(encoding="utf-8"))
TXN_CFG = CFG["core_banking"]["txn_account"]
ONLINE_CFG = CFG["digital_banking"]["online_transaction"]
ACCOUNTS = list(range(1, 51))
CUSTOMER_MAP = {a: a for a in ACCOUNTS}


def test_txn_ids_are_contiguous_across_chunks():
    random.seed(1)
    balances = {a: 1_000_000.0 for a in ACCOUNTS}
    first = generate_txn_account(300, TXN_CFG, ACCOUNTS, CUSTOMER_MAP, balances, start_id=1, running_balances=balances)
    second = generate_txn_account(
        200, TXN_CFG, ACCOUNTS, CUSTOMER_MAP, balances, start_id=301, running_balances=balances
    )
    assert [r[0] for r in first + second] == list(range(1, 501))


def test_balance_continues_across_chunks():
    """balance_after của một tài khoản ở khối 2 phải nối tiếp từ khối 1, không reset."""
    random.seed(2)
    initial = {a: 1_000_000.0 for a in ACCOUNTS}
    state = dict(initial)
    first = generate_txn_account(400, TXN_CFG, ACCOUNTS, CUSTOMER_MAP, initial, start_id=1, running_balances=state)
    last_after_first = {}
    for row in first:
        last_after_first[row[1]] = row[7]
    assert state == {**initial, **last_after_first}, "state phải là số dư cuối của khối 1"


def test_without_shared_state_the_old_behaviour_is_kept():
    random.seed(3)
    initial = {a: 1_000_000.0 for a in ACCOUNTS}
    generate_txn_account(100, TXN_CFG, ACCOUNTS, CUSTOMER_MAP, initial)
    assert initial == {a: 1_000_000.0 for a in ACCOUNTS}, "không truyền running_balances thì không sửa dict gọi vào"


def test_card_txn_ids_and_references_unique_across_chunks():
    cards = [(i, i, "CREDIT", "ACTIVE") for i in range(1, 20)]
    rows = generate_card_txn(100, {}, cards, None, start_id=1) + generate_card_txn(100, {}, cards, None, start_id=101)
    assert [r[0] for r in rows] == list(range(1, 201))
    assert len({r[15] for r in rows}) == 200


def test_online_txn_ids_contiguous_across_chunks():
    rows = generate_online_transactions(50, ONLINE_CFG, [1, 2], [1], [1], start_id=1) + generate_online_transactions(
        50, ONLINE_CFG, [1, 2], [1], [1], start_id=51
    )
    assert [r[0] for r in rows] == list(range(1, 101))


def test_loan_payment_ids_contiguous_when_chunked_by_loans():
    random.seed(4)
    loans = generate_loans(40, {}, list(range(1, 30)), ["BR001"], ["LOAN001"])
    first = generate_loan_payments(loans[:20], {}, start_payment_id=1)
    second = generate_loan_payments(loans[20:], {}, start_payment_id=len(first) + 1)
    ids = [r[0] for r in first + second]
    assert ids == list(range(1, len(ids) + 1))


class _Collector:
    def __init__(self):
        self.calls = []

    def write_rows(self, schema, table, columns, rows, append=False):
        self.calls.append((len(rows), append))


def test_write_chunked_covers_every_row_once(monkeypatch):
    monkeypatch.setattr(generate_all, "CHUNK_ROWS", 3)
    db, out = _Collector(), _Collector()
    seen = []

    def make(start, n):
        seen.append((start, n))
        return [(i,) for i in range(start, start + n)]

    generate_all.write_chunked(db, out, "s", "t", ["id"], 8, make)
    assert seen == [(1, 3), (4, 3), (7, 2)]
    assert [n for n, _ in db.calls] == [3, 3, 2]
    assert [a for _, a in out.calls] == [False, True, True], "chỉ khối đầu ghi đè + header"


def test_csv_append_writes_one_header(tmp_path):
    writer = CsvWriter(str(tmp_path))
    writer.write_rows("s", "t", ["id", "d"], [(1, date(2026, 9, 29))])
    writer.write_rows("s", "t", ["id", "d"], [(2, datetime(2026, 9, 29, 1))], append=True)
    with open(tmp_path / "s" / "t.csv", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["id", "d"]
    assert [r[0] for r in rows[1:]] == ["1", "2"]
