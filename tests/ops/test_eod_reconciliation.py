"""
Tests for code_etl/shared/ops/eod_reconciliation.py

Covers:
  - RECONCILIATIONS spec shape (source query, bronze/batch tables)
  - reconcile_one: PASS khi ba con số bằng nhau; FAIL khi lệch quá tolerance
  - tolerance: nới ngưỡng cho phép lệch nhỏ
  - parse_args: mặc định tolerance=0

Không cần Spark thật: ba hàm đọc số được patch để trả giá trị biết trước.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Stub CHỈ trong lúc exec rồi khôi phục — cùng khuôn tests/ops/test_iceberg_maintenance.py.
_STUBBED = {
    "pyspark": MagicMock(),
    "pyspark.sql": MagicMock(),
    "pyspark.sql.types": MagicMock(),
    "spark": MagicMock(),
    "spark.spark_session": MagicMock(),
}
_SAVED = {name: sys.modules.get(name) for name in _STUBBED}
sys.modules.update(_STUBBED)

_spec = importlib.util.spec_from_file_location(
    "eod_reconciliation_mod",
    str(PROJECT_ROOT / "code_etl" / "shared" / "ops" / "eod_reconciliation.py"),
)
_imod = importlib.util.module_from_spec(_spec)
try:
    _spec.loader.exec_module(_imod)
finally:
    for _name, _previous in _SAVED.items():
        if _previous is None:
            sys.modules.pop(_name, None)
        else:
            sys.modules[_name] = _previous

RECONCILIATIONS = _imod.RECONCILIATIONS
COUNT_RECONCILIATIONS = _imod.COUNT_RECONCILIATIONS
CONTROL_TOTAL_RECONCILIATIONS = _imod.CONTROL_TOTAL_RECONCILIATIONS
MERCHANT_METRICS = _imod.MERCHANT_METRICS
_metric_map = _imod._metric_map
reconcile_one = _imod.reconcile_one
parse_args = _imod.parse_args
reconcile_merchant_transactions = _imod.reconcile_merchant_transactions
merchant_source_query = _imod._merchant_source_query
merchant_metric_sql = _imod._merchant_metric_sql


def test_merchant_source_query_pins_ict_date_and_cohort():
    query = merchant_source_query("2025-12-31")
    assert "payment_channel IN ('QR', 'POS')" in query
    assert "merchant_id IS NOT NULL" in query
    assert "DATE '2025-12-31'" in query
    assert "status = 'SUCCESS'" in query


def test_merchant_metric_sql_uses_same_cohort():
    query = merchant_metric_sql("lakehouse.silver.fact_online_transaction", "2025-12-31")
    assert "payment_channel IN ('QR', 'POS')" in query
    assert "DATE '2025-12-31'" in query
    assert "refund_flag = 1" in query


def test_metric_map_preserves_merchant_channel_currency_and_defaults():
    rows = [
        {
            "merchant_id": 1,
            "payment_channel": "QR",
            "currency": "VND",
            "success_count": 2,
            "gross_amount": 100.0,
            "refund_count": None,
        }
    ]
    assert _metric_map(rows)[(1, "QR", "VND")] == {"success_count": 2.0, "gross_amount": 100.0, "refund_count": 0.0}
    assert set(MERCHANT_METRICS) == {"success_count", "gross_amount", "refund_count"}


class TestSpec:
    """Tests for the reconciliation spec list."""

    def test_specs_not_empty(self):
        assert len(RECONCILIATIONS) > 0
        assert len(COUNT_RECONCILIATIONS) > 0
        assert len(CONTROL_TOTAL_RECONCILIATIONS) > 0

    def test_combined_list_is_union_of_both_kinds(self):
        assert RECONCILIATIONS == COUNT_RECONCILIATIONS + CONTROL_TOTAL_RECONCILIATIONS

    def test_every_spec_has_common_keys(self):
        for spec in RECONCILIATIONS:
            for key in ("entity", "kind", "source_query", "batch_table"):
                assert key in spec, f"{spec} thiếu khoá {key}"

    def test_count_specs_have_bronze_table(self):
        for spec in COUNT_RECONCILIATIONS:
            assert spec["kind"] == "count"
            assert "bronze_table" in spec, spec

    def test_control_total_specs_have_balance_col(self):
        for spec in CONTROL_TOTAL_RECONCILIATIONS:
            assert spec["kind"] == "control_total"
            assert "batch_balance_col" in spec, spec
            # control total neo vào nguồn ↔ batch; Bronze CDC là event log append-only.
            assert "bronze_table" not in spec, spec

    def test_tables_are_iceberg_formatted(self):
        for spec in RECONCILIATIONS:
            for key in ("bronze_table", "batch_table"):
                if key not in spec:
                    continue
                parts = spec[key].split(".")
                assert len(parts) == 3 and parts[0] == "lakehouse", spec[key]


class TestMerchantReconciliationHelpers:
    def test_source_query_has_shared_cohort(self):
        query = merchant_source_query("2025-12-31")
        assert "merchant_id IS NOT NULL" in query
        assert "payment_channel IN ('QR', 'POS')" in query
        assert "DATE '2025-12-31'" in query
        assert "status = 'SUCCESS'" in query

    def test_metric_sql_has_same_cohort(self):
        query = merchant_metric_sql("lakehouse.silver.fact_online_transaction", "2025-12-31")
        assert "merchant_id IS NOT NULL" in query
        assert "payment_channel IN ('QR', 'POS')" in query
        assert "refund_flag = 1" in query

    def test_metric_map_normalizes_currency_and_nulls(self):
        rows = [
            {
                "merchant_id": 1,
                "payment_channel": "QR",
                "currency": " VND ",
                "success_count": 2,
                "gross_amount": 100.0,
                "refund_count": None,
            }
        ]
        assert _metric_map(rows) == {
            (1, "QR", "VND"): {
                "success_count": 2.0,
                "gross_amount": 100.0,
                "refund_count": 0.0,
            }
        }


class TestReconcileOne:
    """Tests for reconcile_one — the pass/fail threshold logic."""

    @staticmethod
    def _run(source, bronze, batch, tolerance=0.0):
        spark = MagicMock()
        spec = RECONCILIATIONS[0]
        with (
            patch.object(_imod, "_read_source_count", return_value=source),
            patch.object(_imod, "_count_table", side_effect=[bronze, batch]),
        ):
            return reconcile_one(spark, spec, "2025-12-31", tolerance)

    def test_all_equal_passes(self):
        results = self._run(source=100, bronze=100, batch=100)
        assert all(r["check_status"] == "PASS" for r in results), results

    def test_bronze_mismatch_fails(self):
        results = self._run(source=100, bronze=90, batch=100)
        failed = [r for r in results if r["check_status"] == "FAIL"]
        assert any(r["metric_name"] == "streaming_vs_source" for r in failed)

    def test_batch_mismatch_fails(self):
        results = self._run(source=100, bronze=100, batch=80)
        failed = [r for r in results if r["check_status"] == "FAIL"]
        assert any(r["metric_name"] == "batch_vs_source" for r in failed)

    def test_streaming_vs_batch_caught_even_if_source_off(self):
        # Cả hai đường cùng lệch so với nguồn nhưng khớp nhau: vẫn phải FAIL
        # hai check so với nguồn.
        results = self._run(source=100, bronze=120, batch=120)
        failed = {r["metric_name"] for r in results if r["check_status"] == "FAIL"}
        assert failed == {"streaming_vs_source", "batch_vs_source"}
        assert not any(r["check_status"] == "FAIL" and r["metric_name"] == "streaming_vs_batch" for r in results)

    def test_tolerance_allows_small_delta(self):
        results = self._run(source=100, bronze=103, batch=102, tolerance=5)
        assert all(r["check_status"] == "PASS" for r in results), results

    def test_tolerance_boundary_is_inclusive(self):
        results = self._run(source=100, bronze=105, batch=100, tolerance=5)
        assert all(r["check_status"] == "PASS" for r in results), results

    def test_delta_reported(self):
        results = self._run(source=100, bronze=100, batch=100)
        first = results[0]
        assert first["delta"] == 0
        assert first["cob_dt"] == "2025-12-31"


class TestControlTotal:
    """Tests for the balance control-total path (data2 GL pattern)."""

    @staticmethod
    def _run(source_balance, batch_balance, tolerance=0.0):
        spark = MagicMock()
        spec = CONTROL_TOTAL_RECONCILIATIONS[0]
        with (
            patch.object(_imod, "_read_source_count", return_value=source_balance),
            patch.object(_imod, "_sum_table", return_value=batch_balance),
        ):
            return reconcile_one(spark, spec, "2025-12-31", tolerance)

    def test_balances_match_passes(self):
        results = self._run(source_balance=1_000_000.0, batch_balance=1_000_000.0)
        assert len(results) == 1
        assert results[0]["check_status"] == "PASS"
        assert results[0]["metric_name"] == "balance_control_total"

    def test_balance_mismatch_fails_even_with_same_row_count(self):
        # Đây là điểm khác biệt cốt lõi: cùng số dòng, khác tiền → vẫn FAIL.
        results = self._run(source_balance=1_000_000.0, batch_balance=999_000.0)
        assert results[0]["check_status"] == "FAIL"
        assert results[0]["delta"] == 1000.0

    def test_tolerance_absorbs_rounding(self):
        results = self._run(source_balance=1_000_000.0, batch_balance=999_999.5, tolerance=1.0)
        assert results[0]["check_status"] == "PASS"

    def test_entity_and_cob_dt_reported(self):
        results = self._run(source_balance=100.0, batch_balance=100.0)
        assert results[0]["entity"] == "account_balance"
        assert results[0]["cob_dt"] == "2025-12-31"


class TestParseArgs:
    def test_tolerance_defaults_to_zero(self):
        with patch("sys.argv", ["eod_reconciliation.py", "--cob_dt", "2025-12-31"]):
            args = parse_args()
            assert args.tolerance == 0.0
            assert args.cob_dt == "2025-12-31"

    def test_tolerance_parsed(self):
        with patch("sys.argv", ["eod_reconciliation.py", "--cob_dt", "2025-12-31", "--tolerance", "5"]):
            args = parse_args()
            assert args.tolerance == 5.0

    def test_cob_dt_required(self):
        with patch("sys.argv", ["eod_reconciliation.py"]), pytest.raises(SystemExit):
            parse_args()
