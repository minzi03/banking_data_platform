"""append_day.py — SQL sao chép một ngày giao dịch (dữ liệu cho lượt chạy hằng ngày, ADR-0018)."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "data_generator"))

import append_day  # noqa: E402

COLS = ["txn_id", "card_id", "txn_date", "txn_amount", "reference_number", "created_ts", "last_updated"]
SQL = append_day.build_insert_sql("card_crm.card_txn", COLS, date(2026, 9, 30))


def test_copies_the_same_weekday_one_week_earlier():
    assert "= DATE '2026-09-23'" in SQL


def test_source_day_is_the_ict_business_date():
    assert "(timezone('Asia/Ho_Chi_Minh', timezone('UTC', txn_date)))::date = DATE '2026-09-23'" in SQL


def test_event_timestamps_move_by_the_lag():
    assert "txn_date + INTERVAL '7 days' AS txn_date" in SQL
    assert "created_ts + INTERVAL '7 days' AS created_ts" in SQL


def test_new_ids_continue_after_the_current_max():
    assert (
        "(SELECT COALESCE(MAX(txn_id), 0) FROM card_crm.card_txn) + ROW_NUMBER() OVER (ORDER BY txn_id) AS txn_id"
        in SQL
    )


def test_reference_number_follows_the_new_id():
    assert "'CDN' || LPAD(" in SQL and "AS reference_number" in SQL


def test_other_columns_are_copied_unchanged():
    assert "\n       card_id,\n" in SQL and "\n       txn_amount,\n" in SQL


def test_every_transaction_table_is_covered():
    assert set(append_day.TABLES) == {
        "core_banking.txn_account",
        "card_crm.card_txn",
        "digital_banking.online_transaction",
    }
