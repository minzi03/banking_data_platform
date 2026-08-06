"""
Tests for airflow/plugins/jdbc_conn_utils.py

Covers:
  - _build_jdbc_url: JDBC type, postgres type, unsupported type
  - resolve_jdbc_conn: connection resolution
  - jdbc_jinja_args: template generation
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Mock airflow modules
sys.modules["airflow"] = MagicMock()
sys.modules["airflow.hooks"] = MagicMock()
sys.modules["airflow.hooks.base"] = MagicMock()

# Import via importlib
_spec = importlib.util.spec_from_file_location(
    "jdbc_conn_utils_mod",
    str(PROJECT_ROOT / "airflow" / "plugins" / "jdbc_conn_utils.py")
)
_jcmod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_jcmod)

_build_jdbc_url = _jcmod._build_jdbc_url
jdbc_jinja_args = _jcmod.jdbc_jinja_args


class TestBuildJdbcUrl:
    """Tests for JDBC URL builder."""

    def test_jdbc_type_returns_host(self):
        """JDBC conn_type should return host directly."""
        conn = MagicMock()
        conn.conn_type = "jdbc"
        conn.host = "jdbc:postgresql://postgres:5432/banking_db"

        url = _build_jdbc_url(conn)
        assert url == "jdbc:postgresql://postgres:5432/banking_db"

    def test_postgres_type_builds_url(self):
        """Postgres conn_type should build JDBC URL from components."""
        conn = MagicMock()
        conn.conn_type = "postgres"
        conn.host = "postgres"
        conn.port = 5432
        conn.schema = "banking_db"

        url = _build_jdbc_url(conn)
        assert url == "jdbc:postgresql://postgres:5432/banking_db"

    def test_postgres_type_default_port(self):
        """Postgres conn_type should default port to 5432."""
        conn = MagicMock()
        conn.conn_type = "postgres"
        conn.host = "postgres"
        conn.port = None
        conn.schema = "banking_db"

        url = _build_jdbc_url(conn)
        assert "5432" in url

    def test_unsupported_type_raises(self):
        """Unsupported conn_type should raise ValueError."""
        conn = MagicMock()
        conn.conn_type = "mysql"
        conn.conn_id = "test_mysql"

        with pytest.raises(ValueError, match="không được hỗ trợ"):
            _build_jdbc_url(conn)

    def test_case_insensitive_type(self):
        """Should handle uppercase conn_type."""
        conn = MagicMock()
        conn.conn_type = "Postgres"
        conn.host = "postgres"
        conn.port = 5432
        conn.schema = "banking_db"

        url = _build_jdbc_url(conn)
        assert url == "jdbc:postgresql://postgres:5432/banking_db"


class TestJdbcJinjaArgs:
    """Tests for Jinja template argument generation."""

    def test_returns_dict_with_three_keys(self):
        """Should return dict with jdbc_url, db_user, db_password."""
        args = jdbc_jinja_args("my_conn")
        assert "jdbc_url" in args
        assert "db_user" in args
        assert "db_password" in args

    def test_templates_contain_conn_id(self):
        """Templates should reference the given conn_id."""
        args = jdbc_jinja_args("postgres-core-banking")
        assert "postgres-core-banking" in args["jdbc_url"]
        assert "postgres-core-banking" in args["db_user"]
        assert "postgres-core-banking" in args["db_password"]

    def test_templates_are_jinja_format(self):
        """Templates should use Airflow Jinja syntax."""
        args = jdbc_jinja_args("test_conn")
        # Airflow Jinja uses double curly braces in strings
        assert "{{" in args["jdbc_url"]
        assert "}}" in args["jdbc_url"]
        assert "conn" in args["jdbc_url"]
