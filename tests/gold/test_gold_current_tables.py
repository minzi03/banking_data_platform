"""
Tests for Gold current-serving tables pattern.

Each customer-grain Gold history table can contain multiple rows per customer
across multiple `cob_dt`, while the corresponding `_current` object must return
exactly one latest row per customer.
"""

from datetime import date


def build_current_rows(rows, key_cols=("customer_id",)):
    latest = {}
    for row in rows:
        key = tuple(row[col] for col in key_cols)
        if key not in latest or row["cob_dt"] > latest[key]["cob_dt"]:
            latest[key] = row
    return list(latest.values())


class TestGoldCurrentTablesPattern:
    def test_customer_balance_summary_current_logic(self):
        rows = [
            {"customer_id": 1, "cob_dt": date(2026, 8, 4), "aum_total": 100},
            {"customer_id": 1, "cob_dt": date(2026, 8, 5), "aum_total": 120},
            {"customer_id": 2, "cob_dt": date(2026, 8, 5), "aum_total": 90},
        ]
        current = build_current_rows(rows)
        assert len(current) == 2
        assert sorted(r["customer_id"] for r in current) == [1, 2]
        assert next(r for r in current if r["customer_id"] == 1)["aum_total"] == 120

    def test_customer_transaction_summary_current_logic(self):
        rows = [
            {"customer_id": 10, "cob_dt": date(2026, 8, 4), "total_txn_count_30d": 8},
            {"customer_id": 10, "cob_dt": date(2026, 8, 5), "total_txn_count_30d": 12},
        ]
        current = build_current_rows(rows)
        assert len(current) == 1
        assert current[0]["total_txn_count_30d"] == 12

    def test_rfm_segment_current_logic(self):
        rows = [
            {"customer_id": 1, "cob_dt": date(2026, 8, 4), "rfm_segment": "At Risk"},
            {"customer_id": 1, "cob_dt": date(2026, 8, 5), "rfm_segment": "Champions"},
            {"customer_id": 2, "cob_dt": date(2026, 8, 5), "rfm_segment": "Loyal Customers"},
        ]
        current = build_current_rows(rows)
        assert len(current) == 2
        assert next(r for r in current if r["customer_id"] == 1)["rfm_segment"] == "Champions"

    def test_campaign_target_current_logic(self):
        rows = [
            {"customer_id": 100, "cob_dt": date(2026, 8, 4), "campaign_type": "Awareness"},
            {"customer_id": 100, "cob_dt": date(2026, 8, 5), "campaign_type": "Cross_Sell_CC"},
        ]
        current = build_current_rows(rows)
        assert len(current) == 1
        assert current[0]["campaign_type"] == "Cross_Sell_CC"

    def test_branch_monthly_summary_should_not_use_customer_current_pattern(self):
        rows = [
            {"branch_code": "BR001", "txn_year": 2026, "txn_month": 7, "cob_dt": date(2026, 8, 4)},
            {"branch_code": "BR001", "txn_year": 2026, "txn_month": 7, "cob_dt": date(2026, 8, 5)},
        ]
        current = build_current_rows(rows, key_cols=("branch_code", "txn_year", "txn_month"))
        assert len(current) == 1
        assert current[0]["cob_dt"] == date(2026, 8, 5)
