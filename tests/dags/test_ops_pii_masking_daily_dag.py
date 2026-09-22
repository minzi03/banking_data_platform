"""
Tests for airflow/dags/ops/ops_pii_masking_daily_dag.py

Covers:
  - resolve_pii_hash_salt: đọc Airflow Variable, không có salt dự phòng
  - Variable KHÔNG bị đọc lúc parse DAG (parse chạy ~30s/lần)
  - PII_ENV: Jinja macro call, và macro đó có đăng ký trên DAG
  - Cả hai task masking đều nhận cùng env chứa salt

apache-airflow không có trong env test (chỉ có trong container), nên DAG module
được nạp với sys.modules stub — cùng cách tests/plugins/test_etl_flag.py, nhưng
dùng patch.dict để stub không rò rỉ sang test khác.
"""

import importlib.util
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DAG_PATH = PROJECT_ROOT / "airflow" / "dags" / "ops" / "ops_pii_masking_daily_dag.py"

_STUB_MODULES = (
    "airflow",
    "airflow.models",
    "airflow.providers",
    "airflow.providers.apache",
    "airflow.providers.apache.spark",
    "airflow.providers.apache.spark.operators",
    "airflow.providers.apache.spark.operators.spark_submit",
    "airflow.providers.common",
    "airflow.providers.common.sql",
    "airflow.providers.common.sql.sensors",
    "airflow.providers.common.sql.sensors.sql",
    "pendulum",
    "etl_flag",
)


@pytest.fixture
def dag_module():
    """Nạp DAG module một lần cho mỗi test, với airflow/pendulum/etl_flag là stub."""
    stubs = {name: MagicMock() for name in _STUB_MODULES}
    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location("ops_pii_masking_dag_under_test", str(DAG_PATH))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


class TestResolvePiiHashSalt:
    """Salt phải đến từ Airflow Variable, không từ giá trị mặc định nào."""

    def test_returns_value_from_variable(self, dag_module):
        dag_module.Variable.get.return_value = "salt-came-from-the-variable"
        assert dag_module.resolve_pii_hash_salt() == "salt-came-from-the-variable"

    def test_reads_variable_without_default(self, dag_module):
        """Gọi Variable.get đúng một tham số — thêm default_var là mở lại lỗ cũ."""
        dag_module.Variable.get.return_value = "any-salt"
        dag_module.resolve_pii_hash_salt()
        dag_module.Variable.get.assert_called_once_with("pii_hash_salt")

    def test_missing_variable_raises_actionable_error(self, dag_module):
        """Variable chưa đặt: Variable.get raise KeyError → lỗi nói rõ cách đặt."""
        dag_module.Variable.get.side_effect = KeyError("Variable pii_hash_salt does not exist")
        with pytest.raises(ValueError) as excinfo:
            dag_module.resolve_pii_hash_salt()
        message = str(excinfo.value)
        assert "pii_hash_salt" in message
        assert "airflow variables set pii_hash_salt" in message

    @pytest.mark.parametrize("value", ["", "   ", "\n"])
    def test_blank_variable_raises(self, dag_module, value):
        """Variable đặt rỗng cũng là thiếu salt — không im lặng hash với chuỗi trắng."""
        dag_module.Variable.get.return_value = value
        with pytest.raises(ValueError, match="pii_hash_salt"):
            dag_module.resolve_pii_hash_salt()


class TestParseTimeBehaviour:
    """Đọc Variable ở module level sẽ query metadata DB mỗi lượt parse DAG."""

    def test_variable_not_read_at_parse_time(self, dag_module):
        dag_module.Variable.get.assert_not_called()

    def test_source_passes_no_default_var(self):
        """Không truyền default_var ở đâu trong file — đó là cách salt dự phòng lọt vào repo.

        (Docstring của DAG có nhắc chữ `default_var` để giải thích, nên chỉ chặn
        trường hợp nó được truyền như tham số.)
        """
        source = DAG_PATH.read_text(encoding="utf-8")
        assert re.search(r"default_var\s*=", source) is None


class TestPiiEnv:
    """env_vars phải là template resolve lúc render task, không phải salt literal."""

    def test_env_is_a_call_to_a_registered_macro(self, dag_module):
        template = dag_module.PII_ENV["PII_HASH_SALT"]
        match = re.fullmatch(r"\{\{\s*(\w+)\(\)\s*\}\}", template)
        assert match, f"PII_HASH_SALT phải là Jinja macro call, đang là {template!r}"

        macros = dag_module.DAG.call_args.kwargs["user_defined_macros"]
        assert macros[match.group(1)] is dag_module.resolve_pii_hash_salt

    def test_both_masking_tasks_receive_the_env(self, dag_module):
        calls = dag_module.SparkSubmitOperator.call_args_list
        assert len(calls) == 2
        for call in calls:
            assert call.kwargs["env_vars"] is dag_module.PII_ENV
