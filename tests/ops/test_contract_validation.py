"""
Contract validation CLI — code_etl/shared/ops/contract_validation.py
=====================================================================

`ops_contract_validation_dag` từng gọi `governance/enforcement.py` như script:
một module thư viện không có `__main__`, và chạy như script còn không import
được. DAG chưa từng kiểm contract nào (TD-10). Các test ở đây giữ những tính
chất khiến CLI thay thế nó đáng tin:

- lỗi là FAIL, không crash và không PASS
- 0 contract không phải là một lượt kiểm xanh
- đọc đúng phạm vi của lượt chạy (cùng quy tắc với DQ, TD-11)
- không có mật khẩu mặc định trong code

Chạy trên stack là việc riêng — xem TD-10 và PR.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CLI_PATH = PROJECT_ROOT / "code_etl" / "shared" / "ops" / "contract_validation.py"


def _load_cli():
    """
    Nạp CLI với pyspark giả, rồi dọn sạch mọi module được nạp kèm.

    CLI import `ops.data_quality`, module đó import `spark.spark_session`, và
    cái đó import pyspark — không có trong job unit test của CI. Chỉ stub trong
    lúc nạp là chưa đủ: `spark.spark_session` sẽ nằm lại trong `sys.modules`
    với pyspark giả, và `tests/shared/test_spark_session.py` — thu thập sau thư
    mục này — sẽ nhận nhầm bản đó.
    """
    before = set(sys.modules)
    stubs = {name: MagicMock() for name in ("pyspark", "pyspark.sql", "pyspark.sql.types")}
    saved = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        spec = importlib.util.spec_from_file_location("contract_validation_under_test", CLI_PATH)
        module = importlib.util.module_from_spec(spec)
        # @dataclass tra sys.modules[cls.__module__] lúc định nghĩa lớp.
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    finally:
        for name in set(sys.modules) - before:
            sys.modules.pop(name, None)
        for name, previous in saved.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
    return module


cli = _load_cli()


def _contract(dataset_id: str = "banking.t_silver", table: str = "t"):
    contract = MagicMock()
    contract.dataset_id = dataset_id
    contract.physical_location.full_table_name = f"lakehouse.silver.{table}"
    return contract


def _passing_result(dataset_id: str):
    result = cli.ValidationResult(dataset_id=dataset_id)
    result.add_check(cli.CheckResult("row_count", "PASS", "[1, ∞]", "10", "Row count OK: 10"))
    return result


# ---------------------------------------------------------------------------
# Chọn contract
# ---------------------------------------------------------------------------


class TestSelectContracts:
    def test_registry_errors_stop_the_run(self):
        registry = MagicMock(has_errors=True, errors=[{"file": "x.yaml", "error": "bad"}])
        with pytest.raises(ValueError, match="không nạp được"):
            cli.select_contracts(registry, "silver")

    def test_a_layer_without_contracts_is_an_error_not_a_pass(self):
        registry = MagicMock(has_errors=False)
        registry.get_contracts_by_layer.return_value = {}
        with pytest.raises(ValueError, match="xanh vô nghĩa"):
            cli.select_contracts(registry, "bronze")

    def test_contracts_come_back_in_stable_order(self):
        registry = MagicMock(has_errors=False)
        registry.get_contracts_by_layer.return_value = {"b": "B", "a": "A", "c": "C"}
        assert cli.select_contracts(registry, "gold") == ["A", "B", "C"]

    def test_real_registry_has_contracts_for_silver_and_gold(self):
        """Hai tầng mà DAG kiểm phải có contract thật — chống pass rỗng từ phía dữ liệu."""
        registry = cli.ContractRegistry()
        assert not registry.has_errors, registry.errors
        assert len(cli.select_contracts(registry, "silver")) >= 10
        assert len(cli.select_contracts(registry, "gold")) >= 10


# ---------------------------------------------------------------------------
# Kiểm một contract
# ---------------------------------------------------------------------------


class TestValidateContract:
    def test_reads_the_scoped_snapshot(self, monkeypatch):
        """Phạm vi đi qua đúng hàm của DQ, với cob_dt của lượt chạy."""
        seen = {}

        def fake_scope(spark, table, rule):
            seen["table"], seen["rule"] = table, rule
            return "scoped-df", " [cob_dt=2026-09-22]"

        monkeypatch.setattr(cli, "_scoped_table", fake_scope)
        enforcer = MagicMock()
        enforcer.validate_before_write.return_value = _passing_result("banking.t_silver")

        run = cli.validate_contract(MagicMock(), enforcer, _contract(), "2026-09-22")

        assert seen == {"table": "lakehouse.silver.t", "rule": {cli.SCOPE_KEY: "2026-09-22"}}
        assert enforcer.validate_before_write.call_args.args[1] == "scoped-df"
        assert run.scope == " [cob_dt=2026-09-22]"
        assert run.result.passed

    def test_unreadable_table_is_a_fail_not_a_crash(self, monkeypatch):
        def missing(*_):
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")

        monkeypatch.setattr(cli, "_scoped_table", missing)
        run = cli.validate_contract(MagicMock(), MagicMock(), _contract(), "2026-09-22")

        assert not run.result.passed
        [check] = run.result.checks
        assert (check.check_name, check.status) == ("table_readable", "FAIL")
        assert "TABLE_OR_VIEW_NOT_FOUND" in check.details

    def test_a_check_that_crashes_is_a_fail_not_a_pass(self, monkeypatch):
        monkeypatch.setattr(cli, "_scoped_table", lambda *_: ("df", ""))
        enforcer = MagicMock()
        enforcer.validate_before_write.side_effect = ValueError("boom")

        run = cli.validate_contract(MagicMock(), enforcer, _contract(), "2026-09-22")

        assert not run.result.passed
        assert run.result.checks[0].check_name == "validation_error"


# ---------------------------------------------------------------------------
# Tổng kết và log
# ---------------------------------------------------------------------------


class TestSummaryAndLog:
    def _runs(self):
        ok = cli.ContractRun(_contract("banking.ok_silver"), _passing_result("banking.ok_silver"), " [is_current]")
        bad_result = cli.ValidationResult(dataset_id="banking.bad_silver")
        bad_result.add_check(cli.CheckResult("required_columns", "FAIL", "[a]", "missing", "Missing columns: ['a']"))
        bad = cli.ContractRun(_contract("banking.bad_silver"), bad_result, "")
        return [ok, bad]

    def test_summary_counts_failed_contracts(self):
        assert cli.summarize(self._runs()) == 1

    def test_one_log_row_per_check_with_scope_in_details(self):
        rows = cli.to_log_rows(self._runs(), "2026-09-22", datetime(2026, 9, 23, 9, 0))
        assert [(r["dataset_id"], r["check_name"], r["check_status"]) for r in rows] == [
            ("banking.ok_silver", "row_count", "PASS"),
            ("banking.bad_silver", "required_columns", "FAIL"),
        ]
        assert rows[0]["details"].endswith(" [is_current]")
        assert str(rows[0]["cob_dt"]) == "2026-09-22"

    def test_log_rows_match_the_ddl_columns(self):
        """Tên cột trong code và trong init_postgres phải là một."""
        ddl = (PROJECT_ROOT / "docker" / "init_postgres" / "00_extensions.sql").read_text(encoding="utf-8")
        block = ddl.split("CREATE TABLE IF NOT EXISTS opslakehouse.contract_validation_log", 1)[1].split(");", 1)[0]
        rows = cli.to_log_rows(self._runs(), "2026-09-22", datetime(2026, 9, 23))
        for column in rows[0]:
            assert f"\n    {column} " in block, f"{column} không có trong DDL của contract_validation_log"


class TestCredentials:
    def test_missing_env_fails_loudly(self, monkeypatch):
        monkeypatch.delenv("POSTGRES_USER", raising=False)
        monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
        with pytest.raises(OSError, match="POSTGRES_USER"):
            cli._jdbc_credentials()

    def test_no_default_password_in_the_source(self):
        """Mật khẩu mặc định trong code là thứ SECURITY.md cấm thêm mới."""
        source = CLI_PATH.read_text(encoding="utf-8")
        assert 'os.environ.get("POSTGRES_PASSWORD",' not in source
        assert "BankingAdmin123" not in source


class TestWiring:
    def test_dag_runs_this_cli_for_silver_and_gold(self):
        dag = (PROJECT_ROOT / "airflow" / "dags" / "ops" / "ops_contract_validation_dag.py").read_text(encoding="utf-8")
        assert '"/opt/project/code_etl/shared/ops/contract_validation.py"' in dag
        assert "governance/enforcement.py" not in dag, "enforcement.py là thư viện, không chạy được như script"
        assert "--layer silver" in dag and "--layer gold" in dag

    def test_worker_receives_the_database_credentials(self):
        compose = (PROJECT_ROOT / "docker" / "docker-compose.yml").read_text(encoding="utf-8")
        worker = compose.split("\n  spark-worker-1:", 1)[1].split("\n  # ====", 1)[0]
        assert "POSTGRES_USER: ${POSTGRES_USER}" in worker
        assert "POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}" in worker
