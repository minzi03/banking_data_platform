"""Tests for pipeline lifecycle audit writes."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from code_etl.shared.ops.pipeline_run_audit import PipelineRun, insert_run


def run(**overrides):
    values = {"dag_id": "daily", "task_id": "silver", "status": "RUNNING", "cob_dt": date(2026, 11, 17)}
    values.update(overrides)
    return PipelineRun(**values)


def test_insert_run_is_parameterized_and_commits():
    connection = MagicMock()
    insert_run(connection, run(rows_processed=12))
    cursor = connection.cursor.return_value.__enter__.return_value
    query, params = cursor.execute.call_args.args
    assert "INSERT INTO opslakehouse.pipeline_run_log" in query
    assert "%s" in query
    assert params[0:5] == ("daily", "silver", "RUNNING", date(2026, 11, 17), 12)
    connection.commit.assert_called_once_with()


@pytest.mark.parametrize("status", ["SUCCESS", "FAILED", "SKIPPED"])
def test_lifecycle_statuses_are_accepted(status):
    insert_run(MagicMock(), run(status=status))


def test_invalid_status_is_rejected_without_db_write():
    connection = MagicMock()
    with pytest.raises(ValueError, match="unknown pipeline status"):
        insert_run(connection, run(status="DONE"))
    connection.cursor.assert_not_called()


def test_negative_row_count_is_rejected():
    with pytest.raises(ValueError, match="rows_processed"):
        insert_run(MagicMock(), run(rows_processed=-1))


def test_error_message_is_parameter_value_not_sql():
    connection = MagicMock()
    message = "failed; DROP TABLE opslakehouse.pipeline_run_log;"
    insert_run(connection, run(status="FAILED", error_message=message))
    params = connection.cursor.return_value.__enter__.return_value.execute.call_args.args[1]
    assert message in params
    assert message not in connection.cursor.return_value.__enter__.return_value.execute.call_args.args[0]
