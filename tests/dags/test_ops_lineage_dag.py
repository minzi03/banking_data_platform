"""
Tests for airflow/dags/ops/ops_lineage_dag.py — emit_lineage ghi gì vào PostgreSQL.

apache-airflow không có trong env test, nên DAG được nạp với sys.modules stub
(cùng cách test_ops_pii_masking_daily_dag.py) và PostgresHook là mock.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DAG_PATH = PROJECT_ROOT / "airflow" / "dags" / "ops" / "ops_lineage_dag.py"

_STUB_MODULES = (
    "airflow",
    "airflow.operators",
    "airflow.operators.python",
    "airflow.providers",
    "airflow.providers.common",
    "airflow.providers.common.sql",
    "airflow.providers.common.sql.sensors",
    "airflow.providers.common.sql.sensors.sql",
    "airflow.providers.postgres",
    "airflow.providers.postgres.hooks",
    "airflow.providers.postgres.hooks.postgres",
    "pendulum",
    "etl_flag",
)


@pytest.fixture
def run_emit(monkeypatch):
    """Chạy emit_lineage với PostgresHook giả; trả về (số cạnh, cursor giả)."""
    stubs = {name: MagicMock() for name in _STUB_MODULES}
    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location("ops_lineage_dag_under_test", str(DAG_PATH))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        monkeypatch.setattr(mod, "PROJECT_ROOT", str(PROJECT_ROOT))

        hook_cls = stubs["airflow.providers.postgres.hooks.postgres"].PostgresHook
        conn = hook_cls.return_value.get_conn.return_value
        cursor = conn.cursor.return_value.__enter__.return_value

        dag = MagicMock()
        dag.dag_id = "ops_lineage_dag"
        written = mod.emit_lineage(dag=dag, run_id="manual__2026-09-22")
        yield written, cursor, hook_cls, conn


def test_uses_the_etl_connection(run_emit):
    _written, _cursor, hook_cls, conn = run_emit
    hook_cls.assert_called_once_with(postgres_conn_id="postgres-etl")
    conn.close.assert_called_once()


def test_replaces_rows_of_the_same_run(run_emit):
    """Chạy lại cùng dag_run không nhân đôi cạnh: DELETE theo run trước khi INSERT."""
    _written, cursor, _hook, _conn = run_emit
    sql, params = cursor.execute.call_args.args
    assert sql.startswith("DELETE FROM opslakehouse.lineage_log")
    assert params == ("ops_lineage_dag", "manual__2026-09-22")


def test_inserts_every_declared_edge(run_emit):
    from governance.lineage import declared_edges

    written, cursor, _hook, _conn = run_emit
    sql, rows = cursor.executemany.call_args.args
    assert "INSERT INTO opslakehouse.lineage_log" in sql
    expected = [
        (s, t, k, "ops_lineage_dag", "manual__2026-09-22") for s, t, k in declared_edges(PROJECT_ROOT / "code_etl")
    ]
    assert rows == expected
    assert written == len(expected) > 0


def test_row_count_is_not_faked(run_emit):
    """Task không đo số dòng — ghi NULL, không ghi 0."""
    _written, cursor, _hook, _conn = run_emit
    sql, _rows = cursor.executemany.call_args.args
    assert "NULL, NULL)" in sql
