"""Tests for the read-only pipeline lifecycle report."""

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from scripts.pipeline_run_report import build_query, fetch_rows


def test_build_query_parameterizes_status_and_dag():
    query, params = build_query("FAILED", "ops_eod_reconciliation_dag", 10)
    assert "status = %s" in query
    assert "dag_id = %s" in query
    assert params == ["FAILED", "ops_eod_reconciliation_dag", 10]
    assert "FAILED" not in query


def test_build_query_is_bounded_without_filters():
    query, params = build_query(limit=20)
    assert query.startswith("SELECT ")
    assert "DELETE" not in query.upper()
    assert params == [20]


def test_build_query_rejects_invalid_inputs():
    with pytest.raises(ValueError, match="positive"):
        build_query(limit=0)
    with pytest.raises(ValueError, match="unknown pipeline status"):
        build_query(status="DONE")


def test_fetch_rows_converts_temporal_and_decimal_values():
    cursor = MagicMock()
    cursor.description = [("cob_dt",), ("started_at",), ("execution_time_s",)]
    cursor.fetchall.return_value = [(date(2026, 11, 17), datetime(2026, 11, 17, 1, 2), Decimal("2.5"))]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    rows = fetch_rows(connection, "SELECT 1", [])

    assert rows == [{"cob_dt": "2026-11-17", "started_at": "2026-11-17 01:02:00", "execution_time_s": "2.5"}]
