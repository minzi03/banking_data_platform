"""
Tests for airflow/plugins/etl_flag.py

Covers:
  - _FLAG_SQL_RUNNING / _FLAG_SQL_SUCCESS: SQL template structure
  - make_start_flag_task: returns PostgresOperator with correct params
  - make_end_flag_task: returns PostgresOperator with correct params
  - Default cob_dt value
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Mock airflow modules before import
sys.modules["airflow"] = MagicMock()
sys.modules["airflow.providers"] = MagicMock()
sys.modules["airflow.providers.postgres"] = MagicMock()
sys.modules["airflow.providers.postgres.operators"] = MagicMock()
sys.modules["airflow.providers.postgres.operators.postgres"] = MagicMock()

# Import via importlib
_spec = importlib.util.spec_from_file_location(
    "etl_flag_mod",
    str(PROJECT_ROOT / "airflow" / "plugins" / "etl_flag.py")
)
_efmod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_efmod)

_FLAG_SQL_RUNNING = _efmod._FLAG_SQL_RUNNING
_FLAG_SQL_SUCCESS = _efmod._FLAG_SQL_SUCCESS
make_start_flag_task = _efmod.make_start_flag_task
make_end_flag_task = _efmod.make_end_flag_task
POSTGRES_ETL_CONN_ID = _efmod.POSTGRES_ETL_CONN_ID

# Get the mocked PostgresOperator to inspect call args
_mock_postgres_op = sys.modules["airflow.providers.postgres.operators.postgres"].PostgresOperator


class TestFlagSqlTemplates:
    """Tests for the SQL template strings."""

    def test_running_sql_has_insert(self):
        assert "INSERT INTO" in _FLAG_SQL_RUNNING
        assert "flag_job_etl" in _FLAG_SQL_RUNNING

    def test_running_sql_has_status_r(self):
        assert "'R'" in _FLAG_SQL_RUNNING

    def test_success_sql_has_status_s(self):
        assert "'S'" in _FLAG_SQL_SUCCESS

    def test_success_sql_has_insert(self):
        assert "INSERT INTO" in _FLAG_SQL_SUCCESS
        assert "flag_job_etl" in _FLAG_SQL_SUCCESS

    def test_both_sqls_have_parameters(self):
        assert "%(dag_id)s" in _FLAG_SQL_RUNNING
        assert "%(dag_id)s" in _FLAG_SQL_SUCCESS
        assert "%(cob_dt)s" in _FLAG_SQL_RUNNING
        assert "%(cob_dt)s" in _FLAG_SQL_SUCCESS

    def test_running_sql_has_start_time(self):
        assert "start_time" in _FLAG_SQL_RUNNING

    def test_success_sql_has_end_time(self):
        assert "end_time" in _FLAG_SQL_SUCCESS


class TestMakeStartFlagTask:
    """Tests for make_start_flag_task factory."""

    def _get_call_kwargs(self):
        """Extract kwargs from the most recent PostgresOperator call."""
        _mock_postgres_op.reset_mock()
        dag = MagicMock()
        task = make_start_flag_task("start", "test_dag", "bronze", dag)  # noqa: F841
        # PostgresOperator is called as PostgresOperator(...)
        last_call = _mock_postgres_op.call_args
        return last_call.kwargs if last_call else {}

    def test_returns_operator(self):
        """Should return a callable (mocked PostgresOperator)."""
        dag = MagicMock()
        task = make_start_flag_task("start", "test_dag", "bronze", dag)
        assert task is not None

    def test_default_cob_dt_is_ds(self):
        """Default cob_dt should be '{{ ds }}' Airflow template."""
        kwargs = self._get_call_kwargs()
        assert kwargs.get("parameters", {}).get("cob_dt") == "{{ ds }}"

    def test_custom_cob_dt(self):
        """Should accept custom cob_dt value."""
        _mock_postgres_op.reset_mock()
        dag = MagicMock()
        task = make_start_flag_task("start", "test_dag", "bronze", dag, cob_dt="2025-01-15")  # noqa: F841
        kwargs = _mock_postgres_op.call_args.kwargs
        assert kwargs.get("parameters", {}).get("cob_dt") == "2025-01-15"

    def test_passes_dag_id_and_layer(self):
        """Should pass dag_id and layer in parameters."""
        kwargs = self._get_call_kwargs()
        params = kwargs.get("parameters", {})
        assert params.get("dag_id") == "test_dag"
        assert params.get("layer") == "bronze"

    def test_uses_correct_conn_id(self):
        """Should use the ETL Postgres connection ID."""
        kwargs = self._get_call_kwargs()
        assert kwargs.get("postgres_conn_id") == "postgres-etl"

    def test_uses_correct_sql(self):
        """Should use the running flag SQL."""
        kwargs = self._get_call_kwargs()
        assert kwargs.get("sql") == _FLAG_SQL_RUNNING


class TestMakeEndFlagTask:
    """Tests for make_end_flag_task factory."""

    def _get_call_kwargs(self):
        """Extract kwargs from the most recent PostgresOperator call."""
        _mock_postgres_op.reset_mock()
        dag = MagicMock()
        task = make_end_flag_task("end", "test_dag", "bronze", dag)  # noqa: F841
        last_call = _mock_postgres_op.call_args
        return last_call.kwargs if last_call else {}

    def test_returns_operator(self):
        """Should return a callable (mocked PostgresOperator)."""
        dag = MagicMock()
        task = make_end_flag_task("end", "test_dag", "bronze", dag)
        assert task is not None

    def test_default_cob_dt_is_ds(self):
        """Default cob_dt should be '{{ ds }}'."""
        kwargs = self._get_call_kwargs()
        assert kwargs.get("parameters", {}).get("cob_dt") == "{{ ds }}"

    def test_custom_cob_dt(self):
        """Should accept custom cob_dt value."""
        _mock_postgres_op.reset_mock()
        dag = MagicMock()
        task = make_end_flag_task("end", "test_dag", "bronze", dag, cob_dt="2025-06-01")  # noqa: F841
        kwargs = _mock_postgres_op.call_args.kwargs
        assert kwargs.get("parameters", {}).get("cob_dt") == "2025-06-01"

    def test_uses_correct_sql(self):
        """Should use the success flag SQL."""
        kwargs = self._get_call_kwargs()
        assert kwargs.get("sql") == _FLAG_SQL_SUCCESS
