"""
Tests for code_etl/shared/ops/data_quality.py

Covers:
  - load_rules: YAML loading
  - check_row_count: pass/fail scenarios
  - check_null: pass/fail scenarios
  - check_unique: pass/fail scenarios
  - check_range: pass/fail scenarios
  - check_referential_integrity: pass/fail scenarios
  - run_checks_for_table: dispatcher routing
  - print_summary: count aggregation
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Import via importlib to avoid package conflicts
_spec = importlib.util.spec_from_file_location(
    "data_quality_mod", str(PROJECT_ROOT / "code_etl" / "shared" / "ops" / "data_quality.py")
)
_dq_mod = importlib.util.module_from_spec(_spec)

# Mock pyspark CHỈ trong lúc exec, rồi khôi phục ngay.
# pytest import toàn bộ test module ở giai đoạn collection TRƯỚC khi chạy test
# nào, nên stub ở module scope mà không restore sẽ để lại MagicMock trong
# sys.modules cho mọi test chạy sau — test nào cần pyspark thật sẽ chết với
# `ValueError: pyspark.__spec__ is not set`.
_STUBBED = {
    "pyspark": MagicMock(),
    "pyspark.sql": MagicMock(),
    "pyspark.sql.types": MagicMock(),
}
_SAVED = {name: sys.modules.get(name) for name in _STUBBED}
sys.modules.update(_STUBBED)
try:
    _spec.loader.exec_module(_dq_mod)
finally:
    for _name, _previous in _SAVED.items():
        if _previous is None:
            sys.modules.pop(_name, None)
        else:
            sys.modules[_name] = _previous

load_rules = _dq_mod.load_rules
check_row_count = _dq_mod.check_row_count
check_null = _dq_mod.check_null
check_unique = _dq_mod.check_unique
check_range = _dq_mod.check_range
check_referential_integrity = _dq_mod.check_referential_integrity
run_checks_for_table = _dq_mod.run_checks_for_table
print_summary = _dq_mod.print_summary
CHECK_DISPATCH = _dq_mod.CHECK_DISPATCH
check_reconciliation = _dq_mod.check_reconciliation


class TestLoadRules:
    """Tests for DQ rules YAML loader."""

    def test_load_rules_returns_dict(self, sample_dq_rules):
        """Should load DQ rules from YAML file."""
        rules = load_rules(sample_dq_rules)
        assert isinstance(rules, dict)
        assert "tables" in rules

    def test_load_rules_has_two_tables(self, sample_dq_rules):
        """Should load both silver and gold table rules."""
        rules = load_rules(sample_dq_rules)
        assert len(rules["tables"]) == 2

    def test_load_rules_table_checks(self, sample_dq_rules):
        """Should load check definitions for each table."""
        rules = load_rules(sample_dq_rules)
        silver_checks = rules["tables"]["lakehouse.silver.dim_customer"]["checks"]
        assert len(silver_checks) == 3  # row_count, null_check, unique_check

    def test_missing_file_raises(self):
        """Should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_rules("/nonexistent/dq_rules.yml")


class TestCheckRowCount:
    """Tests for row count validation."""

    def test_pass_when_above_min(self):
        """Should PASS when row count >= min_rows."""
        spark = MagicMock()
        df = MagicMock()
        df.count.return_value = 100
        spark.table.return_value = df

        status, expected, details = check_row_count(spark, "t", {"min_rows": 1})
        assert status == "PASS"
        assert "100" in details

    def test_fail_when_below_min(self):
        """Should FAIL when row count < min_rows."""
        spark = MagicMock()
        df = MagicMock()
        df.count.return_value = 0
        spark.table.return_value = df

        status, expected, details = check_row_count(spark, "t", {"min_rows": 1})
        assert status == "FAIL"
        assert "0" in details

    def test_fail_when_above_max(self):
        """Should FAIL when row count > max_rows."""
        spark = MagicMock()
        df = MagicMock()
        df.count.return_value = 1000
        spark.table.return_value = df

        status, expected, details = check_row_count(spark, "t", {"min_rows": 1, "max_rows": 500})
        assert status == "FAIL"
        assert "max: 500" in details

    def test_error_handling(self):
        """Should FAIL gracefully on exception."""
        spark = MagicMock()
        spark.table.side_effect = Exception("Table not found")

        status, expected, details = check_row_count(spark, "t", {"min_rows": 1})
        assert status == "FAIL"
        assert "Error" in details


class TestCheckNull:
    """Tests for NULL check validation."""

    def test_pass_when_no_nulls(self):
        """Should PASS when no NULL values in checked columns."""
        spark = MagicMock()
        df = MagicMock()
        df.columns = ["col_a", "col_b", "col_c"]
        df.filter.return_value.count.return_value = 0
        spark.table.return_value = df

        status, expected, details = check_null(spark, "t", {"columns": ["col_a", "col_b"]})
        assert status == "PASS"

    def test_fail_when_nulls_found(self):
        """Should FAIL when NULL values exist."""
        spark = MagicMock()
        df = MagicMock()
        df.columns = ["col_a", "col_b"]
        df.filter.return_value.count.return_value = 5
        spark.table.return_value = df

        status, expected, details = check_null(spark, "t", {"columns": ["col_a"]})
        assert status == "FAIL"
        assert "5 nulls" in details

    def test_column_not_found(self):
        """Should report missing column."""
        spark = MagicMock()
        df = MagicMock()
        df.columns = ["col_a"]
        spark.table.return_value = df

        status, expected, details = check_null(spark, "t", {"columns": ["nonexistent"]})
        assert status == "FAIL"
        assert "not found" in details


class TestCheckUnique:
    """Tests for uniqueness validation."""

    def test_pass_when_unique(self):
        """Should PASS when all values are unique."""
        spark = MagicMock()
        df = MagicMock()
        df.columns = ["id"]
        df.count.return_value = 100
        df.select.return_value.distinct.return_value.count.return_value = 100
        spark.table.return_value = df

        status, expected, details = check_unique(spark, "t", {"columns": ["id"]})
        assert status == "PASS"

    def test_fail_when_duplicates(self):
        """Should FAIL when duplicates exist."""
        spark = MagicMock()
        df = MagicMock()
        df.columns = ["id"]
        df.count.return_value = 100
        df.select.return_value.distinct.return_value.count.return_value = 95
        spark.table.return_value = df

        status, expected, details = check_unique(spark, "t", {"columns": ["id"]})
        assert status == "FAIL"
        assert "5 duplicates" in details


class TestCheckRange:
    """Tests for range validation."""

    def test_pass_when_in_range(self):
        """Should PASS when all values within bounds."""
        spark = MagicMock()
        df = MagicMock()
        df.columns = ["balance"]
        # Mock column access to support comparison operators
        mock_col = MagicMock()
        mock_col.__lt__ = MagicMock(return_value=MagicMock())
        mock_col.__gt__ = MagicMock(return_value=MagicMock())
        mock_col.__or__ = MagicMock(return_value=MagicMock())
        df.__getitem__ = MagicMock(return_value=mock_col)

        filtered_df = MagicMock()
        filtered_df.count.return_value = 0
        df.filter.return_value = filtered_df
        spark.table.return_value = df

        status, expected, details = check_range(spark, "t", {"column": "balance", "min_value": 0, "max_value": 1000000})
        assert status == "PASS"

    def test_fail_when_out_of_range(self):
        """Should FAIL when values outside bounds."""
        spark = MagicMock()
        df = MagicMock()
        df.columns = ["balance"]
        mock_col = MagicMock()
        mock_col.__lt__ = MagicMock(return_value=MagicMock())
        mock_col.__gt__ = MagicMock(return_value=MagicMock())
        mock_col.__or__ = MagicMock(return_value=MagicMock())
        df.__getitem__ = MagicMock(return_value=mock_col)

        filtered_df = MagicMock()
        filtered_df.count.return_value = 10
        df.filter.return_value = filtered_df
        spark.table.return_value = df

        status, expected, details = check_range(spark, "t", {"column": "balance", "min_value": 0, "max_value": 1000000})
        assert status == "FAIL"
        assert "10 values out of range" in details

    def test_min_only(self):
        """Should check min_value only when max_value is None."""
        spark = MagicMock()
        df = MagicMock()
        df.columns = ["balance"]
        df.filter.return_value.count.return_value = 3
        spark.table.return_value = df

        status, expected, details = check_range(spark, "t", {"column": "balance", "min_value": 0})
        assert status == "FAIL"

    def test_column_not_found(self):
        """Should FAIL when column doesn't exist."""
        spark = MagicMock()
        df = MagicMock()
        df.columns = ["balance"]
        spark.table.return_value = df

        status, expected, details = check_range(spark, "t", {"column": "nonexistent", "min_value": 0, "max_value": 100})
        assert status == "FAIL"
        assert "not found" in details


class TestCheckReferentialIntegrity:
    """Tests for referential integrity validation."""

    def test_pass_when_all_fks_exist(self):
        """Should PASS when all FK values exist in reference table."""
        spark = MagicMock()
        source_df = MagicMock()
        source_df.columns = ["branch_code"]
        ref_df = MagicMock()
        ref_df.columns = ["branch_code"]

        source_vals = MagicMock()
        ref_vals = MagicMock()
        # select → filter(FK IS NOT NULL) → distinct: FK NULL không tính là orphan (TD-11)
        source_df.select.return_value.filter.return_value.distinct.return_value = source_vals
        ref_df.select.return_value.distinct.return_value = ref_vals
        source_vals.join.return_value.count.return_value = 0

        spark.table.side_effect = [source_df, ref_df]

        status, expected, details = check_referential_integrity(
            spark,
            "t",
            {
                "column": "branch_code",
                "ref_table": "lakehouse.silver.dim_branch",
                "ref_column": "branch_code",
            },
        )
        assert status == "PASS"

    def test_fail_when_orphans_exist(self):
        """Should FAIL when orphan records found."""
        spark = MagicMock()
        source_df = MagicMock()
        source_df.columns = ["branch_code"]
        ref_df = MagicMock()
        ref_df.columns = ["branch_code"]

        source_vals = MagicMock()
        ref_vals = MagicMock()
        # select → filter(FK IS NOT NULL) → distinct: FK NULL không tính là orphan (TD-11)
        source_df.select.return_value.filter.return_value.distinct.return_value = source_vals
        ref_df.select.return_value.distinct.return_value = ref_vals
        source_vals.join.return_value.count.return_value = 5

        spark.table.side_effect = [source_df, ref_df]

        status, expected, details = check_referential_integrity(
            spark,
            "t",
            {
                "column": "branch_code",
                "ref_table": "lakehouse.silver.dim_branch",
                "ref_column": "branch_code",
            },
        )
        assert status == "FAIL"
        assert "5 orphan" in details
        source_df.__getitem__.assert_called_with("branch_code")
        source_df.__getitem__.return_value.isNotNull.assert_called_once()

    def test_missing_config_fields(self):
        """Should FAIL when required fields are missing."""
        spark = MagicMock()

        status, expected, details = check_referential_integrity(
            spark,
            "t",
            {
                "column": "branch_code"
                # Missing ref_table and ref_column
            },
        )
        assert status == "FAIL"
        assert "Missing" in details


class TestRunChecksForTable:
    """Tests for the check dispatcher."""

    def test_runs_multiple_checks(self):
        """Should run all checks for a table and return results."""
        spark = MagicMock()
        df = MagicMock()
        df.columns = ["customer_id"]
        df.count.return_value = 50
        df.filter.return_value.count.return_value = 0
        spark.table.return_value = df

        checks = [
            {"name": "row_count", "severity": "FAIL", "min_rows": 1},
            {"name": "null_check", "severity": "FAIL", "columns": ["customer_id"]},
        ]

        results = run_checks_for_table(spark, "table", checks, "2025-01-15")
        assert len(results) == 2
        assert all(r["check_status"] == "PASS" for r in results)

    def test_unknown_check_type_skipped(self):
        """Should skip unknown check types with warning."""
        spark = MagicMock()
        checks = [{"name": "unknown_check", "severity": "FAIL"}]

        results = run_checks_for_table(spark, "table", checks, "2025-01-15")
        assert len(results) == 0

    def test_severity_warn_downgrades_fail(self):
        """Should downgrade FAIL to WARN when severity is WARN."""
        spark = MagicMock()
        df = MagicMock()
        df.count.return_value = 0
        spark.table.return_value = df

        checks = [{"name": "row_count", "severity": "WARN", "min_rows": 1}]
        results = run_checks_for_table(spark, "table", checks, "2025-01-15")
        assert results[0]["check_status"] == "WARN"


class TestPrintSummary:
    """Tests for summary printing."""

    def test_all_pass(self):
        """Should return correct counts when all pass."""
        results = [
            {"check_status": "PASS", "table_name": "t1", "check_name": "c1", "details": "ok"},
            {"check_status": "PASS", "table_name": "t2", "check_name": "c2", "details": "ok"},
        ]
        p, w, f = print_summary(results)
        assert p == 2
        assert w == 0
        assert f == 0

    def test_mixed_results(self):
        """Should handle mixed PASS/WARN/FAIL."""
        results = [
            {"check_status": "PASS", "table_name": "t1", "check_name": "c1", "details": "ok"},
            {"check_status": "WARN", "table_name": "t1", "check_name": "c2", "details": "warn"},
            {"check_status": "FAIL", "table_name": "t2", "check_name": "c3", "details": "fail"},
        ]
        p, w, f = print_summary(results)
        assert p == 1
        assert w == 1
        assert f == 1

    def test_empty_results(self):
        """Should handle empty results list."""
        p, w, f = print_summary([])
        assert p == 0
        assert w == 0
        assert f == 0


class TestCheckDispatch:
    """Tests for the check dispatcher mapping."""

    def test_all_check_types_registered(self):
        """Should have dispatch entries for all supported check types."""
        expected = {
            "row_count",
            "null_check",
            "unique_check",
            "range_check",
            "referential_integrity",
            "anomaly_detection",
            "freshness_check",
            "schema_drift",
            # Phase 1 reconciliation checks were registered in CHECK_DISPATCH
            # but never added here, so this test could not catch a regression
            # in them. Reconciled with the manifest's dq_check_types metric.
            "reconciliation",
        }
        assert set(CHECK_DISPATCH.keys()) == expected

    def test_dispatch_returns_callable(self):
        """Each dispatch entry should be a callable."""
        for name, func in CHECK_DISPATCH.items():
            assert callable(func), f"CHECK_DISPATCH['{name}'] is not callable"


# ---------------------------------------------------------------------------
# Scope theo cob_dt / is_current (TD-11)
# ---------------------------------------------------------------------------
SCOPE_KEY = _dq_mod.SCOPE_KEY
_scoped_table = _dq_mod._scoped_table
_bounds_label = _dq_mod._bounds_label


def _df(columns):
    """DataFrame giả: filter() trả về một DataFrame giả khác, giữ nguyên cột."""
    df = MagicMock()
    df.columns = list(columns)
    df.filter.side_effect = lambda *_a, **_k: _df(columns)
    return df


class TestScopedTable:
    """
    Check đọc cả bảng thì đếm mỗi giao dịch một lần cho MỖI snapshot cob_dt, và
    mỗi khách hàng một lần cho MỖI phiên bản SCD2 — Silver đỏ trên dữ liệu lành.
    """

    def test_without_scope_key_reads_whole_table(self):
        """Gọi check trực tiếp (không qua run_checks_for_table) giữ hành vi cũ."""
        spark = MagicMock()
        base = _df(["cob_dt", "is_current", "id"])
        spark.table.return_value = base
        df, label = _scoped_table(spark, "t", {})
        assert df is base
        assert label == ""
        base.filter.assert_not_called()

    def test_snapshot_table_filters_to_one_cob_dt(self):
        spark = MagicMock()
        base = _df(["cob_dt", "txn_id"])
        spark.table.return_value = base
        _, label = _scoped_table(spark, "t", {SCOPE_KEY: "2026-09-22"})
        base.filter.assert_called_once_with("cob_dt = DATE '2026-09-22'")
        assert label == " [cob_dt=2026-09-22]"

    def test_scd2_table_filters_to_current_version(self):
        spark = MagicMock()
        base = _df(["customer_id", "is_current"])
        spark.table.return_value = base
        _, label = _scoped_table(spark, "t", {SCOPE_KEY: "2026-09-22"})
        base.filter.assert_called_once_with("CAST(is_current AS INT) = 1")
        assert label == " [is_current]"

    def test_table_without_scope_columns_is_not_filtered(self):
        """Dim SCD1 và bảng CDC không có cột nào để lọc — đọc cả bảng là đúng."""
        spark = MagicMock()
        base = _df(["branch_code"])
        spark.table.return_value = base
        df, label = _scoped_table(spark, "t", {SCOPE_KEY: "2026-09-22"})
        assert df is base
        assert label == ""
        base.filter.assert_not_called()

    def test_rejects_cob_dt_that_is_not_a_date(self):
        """cob_dt đi vào chuỗi SQL — chuỗi lạ phải bị chặn, không được nối vào."""
        spark = MagicMock()
        spark.table.return_value = _df(["cob_dt"])
        with pytest.raises(ValueError):
            _scoped_table(spark, "t", {SCOPE_KEY: "2026-09-22' OR '1'='1"})

    def test_bad_cob_dt_makes_the_check_fail_not_pass(self):
        spark = MagicMock()
        spark.table.return_value = _df(["cob_dt", "txn_id"])
        status, _, details = check_unique(spark, "t", {"columns": ["txn_id"], SCOPE_KEY: "not-a-date"})
        assert status == "FAIL"
        assert "Error" in details

    def test_unique_check_counts_only_the_scoped_snapshot(self):
        """8 snapshot × 1 giao dịch không được đọc thành 7 bản trùng."""
        spark = MagicMock()
        base = _df(["cob_dt", "txn_id"])
        scoped = _df(["cob_dt", "txn_id"])
        scoped.count.return_value = 100
        scoped.select.return_value.distinct.return_value.count.return_value = 100
        base.filter.side_effect = None
        base.filter.return_value = scoped
        base.count.return_value = 800  # toàn bảng: 8 snapshot
        spark.table.return_value = base

        status, _, details = check_unique(spark, "t", {"columns": ["txn_id"], SCOPE_KEY: "2026-09-22"})
        assert status == "PASS", details
        base.count.assert_not_called()
        assert "cob_dt=2026-09-22" in details

    def test_referential_integrity_scopes_both_sides(self):
        """FK so với phiên bản HIỆN HÀNH của dim, không phải mọi phiên bản."""
        spark = MagicMock()
        source = _df(["account_id"])
        ref = _df(["account_id", "is_current"])
        spark.table.side_effect = [source, ref]
        source.select.return_value.filter.return_value.distinct.return_value.join.return_value.count.return_value = 0

        status, _, _ = check_referential_integrity(
            spark,
            "lakehouse.silver.dim_card",
            {
                "column": "account_id",
                "ref_table": "lakehouse.silver.dim_account",
                "ref_column": "account_id",
                SCOPE_KEY: "2026-09-22",
            },
        )
        assert status == "PASS"
        ref.filter.assert_called_once_with("CAST(is_current AS INT) = 1")

    def test_run_checks_passes_cob_dt_without_mutating_rules(self):
        """Rule từ YAML không được bị ghi thêm khoá — nó dùng lại cho bảng khác."""
        spark = MagicMock()
        check = {"name": "row_count", "min_rows": 1}
        seen = {}

        def fake_row_count(_spark, _table, rule):
            seen.update(rule)
            return "PASS", "1", "ok"

        original = CHECK_DISPATCH["row_count"]
        CHECK_DISPATCH["row_count"] = fake_row_count
        try:
            run_checks_for_table(spark, "t", [check], "2026-09-22")
        finally:
            CHECK_DISPATCH["row_count"] = original

        assert seen[SCOPE_KEY] == "2026-09-22"
        assert SCOPE_KEY not in check


class TestReconciliationSourceScope:
    """Nguồn Bronze không partition chỉ giữ lần nạp mới nhất — không lọc theo cob_dt."""

    def _run(self, source_scope):
        spark = MagicMock()
        source = _df(["cob_dt", "customer_id"])
        target = _df(["customer_id", "is_current"])
        tables = {"lakehouse.bronze.core_customer": source, "lakehouse.silver.dim_customer": target}
        spark.table.side_effect = lambda name: tables[name]
        rule = {
            "source_table": "lakehouse.bronze.core_customer",
            "source_key_column": "customer_id",
            "target_key_column": "customer_id",
            SCOPE_KEY: "2026-09-22",
        }
        if source_scope:
            rule["source_scope"] = source_scope
        check_reconciliation(spark, "lakehouse.silver.dim_customer", rule)
        return source, target

    def test_whole_table_source_is_not_filtered(self):
        source, target = self._run("whole_table")
        source.filter.assert_not_called()
        target.filter.assert_called_once_with("CAST(is_current AS INT) = 1")

    def test_default_source_is_filtered_to_cob_dt(self):
        source, _ = self._run(None)
        source.filter.assert_called_once_with("cob_dt = DATE '2026-09-22'")


class TestBoundsLabel:
    """Cận 0 là cận thật. Bản cũ dùng truthiness nên in `>=0` thành `<=None`."""

    def test_zero_lower_bound(self):
        assert _bounds_label(0, None) == ">=0"

    def test_zero_upper_bound(self):
        assert _bounds_label(None, 0) == "<=0"

    def test_both_bounds_including_zero(self):
        assert _bounds_label(0, 100) == "[0, 100]"
