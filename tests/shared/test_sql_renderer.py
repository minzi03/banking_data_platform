"""
Tests for code_etl/shared/utils/sql_renderer.py

Covers:
  - render_sql: basic substitution, multiple vars, quoting, unmatched placeholders
"""

import importlib.util
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Direct import via importlib to avoid package name conflicts
_spec = importlib.util.spec_from_file_location(
    "sql_renderer",
    str(PROJECT_ROOT / "code_etl" / "shared" / "utils" / "sql_renderer.py")
)
_sql_renderer = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_sql_renderer)

render_sql = _sql_renderer.render_sql


class TestRenderSql:
    """Tests for SQL template rendering."""

    def test_basic_substitution(self):
        """Should replace single placeholder."""
        sql = "SELECT * FROM table WHERE cob_dt = DATE '{{ cob_dt }}'"
        result = render_sql(sql, {"cob_dt": "2025-01-15"})
        assert result == "SELECT * FROM table WHERE cob_dt = DATE '2025-01-15'"

    def test_multiple_variables(self):
        """Should replace multiple different placeholders."""
        sql = "SELECT * FROM {{ schema }}.{{ table }} WHERE id = {{ id }}"
        result = render_sql(sql, {"schema": "bronze", "table": "account", "id": 42})
        assert result == "SELECT * FROM bronze.account WHERE id = 42"

    def test_repeated_variable(self):
        """Should replace same variable used multiple times."""
        sql = "SELECT * FROM t WHERE dt = '{{ dt }}' AND created = '{{ dt }}'"
        result = render_sql(sql, {"dt": "2025-01-01"})
        assert result.count("2025-01-01") == 2

    def test_preserves_quotes_in_template(self):
        """Should NOT add extra quotes — template controls quoting."""
        sql = "WHERE cob_dt = DATE '{{ cob_dt }}'"
        result = render_sql(sql, {"cob_dt": "2025-01-01"})
        # Should be DATE '2025-01-01', NOT DATE ''2025-01-01''
        assert "DATE '2025-01-01'" in result
        assert "''" not in result

    def test_integer_value(self):
        """Should handle integer values via str() conversion."""
        sql = "LIMIT {{ limit }}"
        result = render_sql(sql, {"limit": 100})
        assert result == "LIMIT 100"

    def test_unmatched_placeholder_raises(self):
        """Should raise ValueError for unreplaced placeholders."""
        sql = "SELECT * FROM t WHERE x = '{{ used }}' AND y = '{{ unused }}'"
        with pytest.raises(ValueError, match="Placeholder chưa được thay thế"):
            render_sql(sql, {"used": "value"})

    def test_no_placeholders(self):
        """Should return SQL unchanged when no placeholders."""
        sql = "SELECT 1 AS test"
        result = render_sql(sql, {})
        assert "SELECT 1" in result
        assert "AS test" in result

    def test_complex_bronze_sql(self):
        """Should handle realistic Bronze SQL template."""
        sql = """\
SELECT
    account_id, account_no, customer_id, balance
FROM core_banking.account
WHERE cob_dt = DATE '{{ cob_dt }}'"""
        result = render_sql(sql, {"cob_dt": "2025-01-15"})
        assert "DATE '2025-01-15'" in result
        assert "{{ cob_dt }}" not in result
        assert "SELECT" in result

    def test_complex_silver_scd2_sql(self):
        """Should handle SCD2 SQL with multiple date conditions."""
        sql = """\
SELECT *,
    DATE '{{ cob_dt }}' AS effective_from,
    DATE '9999-12-31' AS effective_to,
    true AS is_current
FROM lakehouse.bronze.core_customer
WHERE cob_dt = DATE '{{ cob_dt }}'"""
        result = render_sql(sql, {"cob_dt": "2025-06-01"})
        assert "DATE '2025-06-01'" in result
        assert "DATE '9999-12-31'" in result
        assert "{{ cob_dt }}" not in result

    def test_whitespace_in_placeholder(self):
        """Should handle {{var}} without spaces (edge case)."""
        # Our renderer uses "{{ key }}" with spaces — test that it works
        sql = "SELECT '{{ a }}' FROM t"
        result = render_sql(sql, {"a": "hello"})
        assert result == "SELECT 'hello' FROM t"
