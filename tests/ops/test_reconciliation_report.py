"""Tests for the read-only reconciliation report."""

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from scripts.reconciliation_report import build_query, fetch_rows


def test_build_query_filters_by_date_and_status_parameterized():
    query, params = build_query("2026-10-18", "MISMATCH", 10)
    assert "reconciliation_status = %s" in query
    assert "cob_dt = %s" in query
    assert params == ["2026-10-18", "MISMATCH", 10]
    assert "2026-10-18" not in query


def test_build_query_without_filters_is_read_only_and_bounded():
    query, params = build_query(limit=25)
    assert query.startswith("SELECT ")
    assert "DELETE" not in query.upper()
    assert params == [25]


@pytest.mark.parametrize("value", [0, -1])
def test_build_query_rejects_non_positive_limit(value):
    with pytest.raises(ValueError, match="positive"):
        build_query(limit=value)


def test_build_query_rejects_invalid_date_and_status():
    with pytest.raises(ValueError):
        build_query("not-a-date")
    with pytest.raises(ValueError, match="unknown reconciliation status"):
        build_query(status="UNKNOWN")


def test_fetch_rows_returns_json_ready_values():
    cursor = MagicMock()
    cursor.description = [("cob_dt",), ("delta",), ("checked_at",)]
    cursor.fetchall.return_value = [(date(2026, 10, 18), Decimal("1.25"), datetime(2026, 10, 18, 1, 2))]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    rows = fetch_rows(connection, "SELECT 1", [])

    cursor.execute.assert_called_once_with("SELECT 1", [])
    assert rows == [{"cob_dt": "2026-10-18", "delta": "1.25", "checked_at": "2026-10-18 01:02:00"}]
