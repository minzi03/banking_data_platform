"""
Tests for mart_customer_360 current-serving semantics.

Goal:
- history table may have multiple rows/customer across cob_dt
- current-serving view logic must return exactly 1 row/customer (latest cob_dt)
"""

from collections import defaultdict
from datetime import date


def build_current_rows(rows):
    """Mimic mart_customer_360_current view logic in pure Python."""
    latest_by_customer = {}
    for row in rows:
        cid = row["customer_id"]
        if cid not in latest_by_customer or row["cob_dt"] > latest_by_customer[cid]["cob_dt"]:
            latest_by_customer[cid] = row
    return list(latest_by_customer.values())


class TestMartCustomer360CurrentLogic:
    def test_history_can_have_multiple_rows_per_customer(self):
        rows = [
            {"customer_id": 1, "cob_dt": date(2026, 8, 4)},
            {"customer_id": 1, "cob_dt": date(2026, 8, 5)},
            {"customer_id": 2, "cob_dt": date(2026, 8, 5)},
        ]
        counts = defaultdict(int)
        for row in rows:
            counts[row["customer_id"]] += 1
        assert counts[1] == 2
        assert counts[2] == 1

    def test_current_logic_returns_one_row_per_customer(self):
        rows = [
            {"customer_id": 1, "customer_sk": "a", "cob_dt": date(2026, 8, 4)},
            {"customer_id": 1, "customer_sk": "b", "cob_dt": date(2026, 8, 5)},
            {"customer_id": 2, "customer_sk": "c", "cob_dt": date(2026, 8, 5)},
        ]
        current_rows = build_current_rows(rows)
        assert len(current_rows) == 2
        assert {r["customer_id"] for r in current_rows} == {1, 2}

    def test_current_logic_keeps_latest_cob_dt(self):
        rows = [
            {"customer_id": 1, "customer_sk": "older", "cob_dt": date(2026, 8, 4)},
            {"customer_id": 1, "customer_sk": "latest", "cob_dt": date(2026, 8, 5)},
        ]
        current_rows = build_current_rows(rows)
        assert len(current_rows) == 1
        assert current_rows[0]["customer_sk"] == "latest"
        assert current_rows[0]["cob_dt"] == date(2026, 8, 5)

    def test_current_logic_handles_already_unique_rows(self):
        rows = [
            {"customer_id": 1, "customer_sk": "a", "cob_dt": date(2026, 8, 5)},
            {"customer_id": 2, "customer_sk": "b", "cob_dt": date(2026, 8, 5)},
        ]
        current_rows = build_current_rows(rows)
        assert len(current_rows) == 2
        assert current_rows == rows
