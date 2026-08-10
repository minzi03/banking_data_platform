"""
Tests for code_etl.shared.ops.data_quality — DQ check dispatch and utilities.
"""

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_dq_rules_yaml(tmp_path):
    """Create a minimal DQ rules YAML for testing."""
    content = """
tables:
  lakehouse.silver.dim_customer:
    checks:
      - name: row_count
        severity: FAIL
        min_rows: 1
      - name: null_check
        severity: FAIL
        columns:
          - customer_id
      - name: unique_check
        severity: FAIL
        columns:
          - customer_id

  lakehouse.gold.mart_customer_360:
    checks:
      - name: row_count
        severity: FAIL
        min_rows: 1
      - name: range_check
        severity: WARN
        column: total_balance
        min_value: 0
"""
    yaml_file = tmp_path / "dq_rules.yml"
    yaml_file.write_text(content, encoding="utf-8")
    return yaml_file


# ---------------------------------------------------------------------------
# Test CHECK_DISPATCH
# ---------------------------------------------------------------------------

class TestCheckDispatch:
    def test_all_check_types_registered(self):
        """All standard check types should be registered."""
        from code_etl.shared.ops.data_quality import CHECK_DISPATCH

        expected_checks = [
            "row_count",
            "null_check",
            "unique_check",
            "range_check",
            "referential_integrity",
            "anomaly_detection",
            "freshness_check",
            "schema_drift",
        ]
        for check in expected_checks:
            assert check in CHECK_DISPATCH, f"Check type '{check}' not registered"

    def test_check_functions_are_callable(self):
        """All check functions should be callable."""
        from code_etl.shared.ops.data_quality import CHECK_DISPATCH

        for check_name, check_func in CHECK_DISPATCH.items():
            assert callable(check_func), f"Check function '{check_name}' is not callable"


# ---------------------------------------------------------------------------
# Test LAYER_PREFIXES
# ---------------------------------------------------------------------------

class TestLayerPrefixes:
    def test_layer_prefixes(self):
        from code_etl.shared.ops.data_quality import LAYER_PREFIXES

        assert LAYER_PREFIXES["silver"] == "lakehouse.silver."
        assert LAYER_PREFIXES["gold"] == "lakehouse.gold."
        assert LAYER_PREFIXES["bronze"] == "lakehouse.bronze."


# ---------------------------------------------------------------------------
# Test print_summary
# ---------------------------------------------------------------------------

class TestPrintSummary:
    def test_print_summary_all_pass(self):
        from code_etl.shared.ops.data_quality import print_summary

        results = [
            {"check_status": "PASS", "table_name": "t1", "check_name": "c1", "details": "ok"},
            {"check_status": "PASS", "table_name": "t2", "check_name": "c2", "details": "ok"},
        ]
        pass_count, warn_count, fail_count = print_summary(results)
        assert pass_count == 2
        assert warn_count == 0
        assert fail_count == 0

    def test_print_summary_mixed(self):
        from code_etl.shared.ops.data_quality import print_summary

        results = [
            {"check_status": "PASS", "table_name": "t1", "check_name": "c1", "details": "ok"},
            {"check_status": "WARN", "table_name": "t2", "check_name": "c2", "details": "warn"},
            {"check_status": "FAIL", "table_name": "t3", "check_name": "c3", "details": "fail"},
        ]
        pass_count, warn_count, fail_count = print_summary(results)
        assert pass_count == 1
        assert warn_count == 1
        assert fail_count == 1

    def test_print_summary_empty(self):
        from code_etl.shared.ops.data_quality import print_summary

        results = []
        pass_count, warn_count, fail_count = print_summary(results)
        assert pass_count == 0
        assert warn_count == 0
        assert fail_count == 0
