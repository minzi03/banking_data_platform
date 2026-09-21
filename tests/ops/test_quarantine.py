"""
Tests for code_etl/shared/ops/quarantine.py — business-rule violation checks.

Uses MagicMock for Spark: the module's logic is which records get selected,
what gets written to the quarantine table, and how the exit code is derived.
None of that needs a cluster.

The tests that matter most are the write_to_quarantine schema-alignment cases.
The function builds Rows whose keys must match the target table exactly — a
mismatch raises inside a try/except that returns 0, so a real failure is
indistinguishable from "nothing to quarantine" unless it is asserted here.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


# pyspark is not installed in the unit-test environment. quarantine.py needs it
# twice: at import time (it pulls in spark_session) and at call time
# (write_to_quarantine imports Row lazily). Both resolve through sys.modules, so
# the stub stays installed for the whole module. It is torn down in
# _restore_pyspark below, because leaving it in place would break any test that
# runs after this module and expects the real package.
class _DictRow(dict):
    """Stand-in for pyspark.sql.Row — keeps `row["col"]` assertions readable."""

    def asDict(self):
        return dict(self)


_STUBBED = {
    "pyspark": MagicMock(),
    "pyspark.sql": MagicMock(),
    "spark": MagicMock(),
    "spark.spark_session": MagicMock(),
}
_SAVED = {name: sys.modules.get(name) for name in _STUBBED}
sys.modules.update(_STUBBED)
sys.modules["pyspark.sql"].Row = _DictRow

_spec = importlib.util.spec_from_file_location(
    "quarantine_mod", str(PROJECT_ROOT / "code_etl" / "shared" / "ops" / "quarantine.py")
)
_quarantine = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_quarantine)

quarantine = _quarantine


def _restore_pyspark():
    for name, previous in _SAVED.items():
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


import atexit  # noqa: E402

atexit.register(_restore_pyspark)


# ---------------------------------------------------------------------------
# load_rules
# ---------------------------------------------------------------------------


class TestLoadRules:
    def test_loads_the_shipped_rules_file(self):
        rules = quarantine.load_rules(quarantine.RULES_FILE)
        assert "quarantine_rules" in rules
        assert rules["quarantine_rules"], "shipped rules file declares no rule sets"

    def test_shipped_rules_have_required_keys(self):
        rules = quarantine.load_rules(quarantine.RULES_FILE)["quarantine_rules"]
        for name, cfg in rules.items():
            assert "source_table" in cfg, name
            assert "target_table" in cfg, name
            assert cfg.get("violations"), f"{name} declares no violations"

    def test_shipped_violations_have_name_and_condition(self):
        rules = quarantine.load_rules(quarantine.RULES_FILE)["quarantine_rules"]
        for name, cfg in rules.items():
            for v in cfg["violations"]:
                assert "name" in v, f"{name}/{v}"
                assert "condition" in v, f"{name}/{v}"

    def test_loads_from_an_explicit_path(self, tmp_path):
        path = tmp_path / "rules.yml"
        path.write_text(
            yaml.safe_dump({"quarantine_rules": {"r": {"source_table": "s", "target_table": "t"}}}),
            encoding="utf-8",
        )
        rules = quarantine.load_rules(str(path))
        assert rules["quarantine_rules"]["r"]["source_table"] == "s"

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            quarantine.load_rules(str(tmp_path / "nope.yml"))


# ---------------------------------------------------------------------------
# check_violation
# ---------------------------------------------------------------------------


class TestCheckViolation:
    def test_returns_dicts_for_matching_rows(self):
        spark = MagicMock()
        row_a, row_b = MagicMock(), MagicMock()
        row_a.asDict.return_value = {"txn_id": 1}
        row_b.asDict.return_value = {"txn_id": 2}
        spark.table.return_value.filter.return_value.collect.return_value = [row_a, row_b]

        result = quarantine.check_violation(spark, "lakehouse.silver.fact_txn_account", "amount < 0")

        assert result == [{"txn_id": 1}, {"txn_id": 2}]
        spark.table.assert_called_once_with("lakehouse.silver.fact_txn_account")

    def test_applies_the_given_condition(self):
        spark = MagicMock()
        spark.table.return_value.filter.return_value.collect.return_value = []
        quarantine.check_violation(spark, "t", "days_late > 30")
        spark.table.return_value.filter.assert_called_once_with("days_late > 30")

    def test_no_matches_returns_empty_list(self):
        spark = MagicMock()
        spark.table.return_value.filter.return_value.collect.return_value = []
        assert quarantine.check_violation(spark, "t", "x") == []

    def test_swallows_errors_and_returns_empty(self):
        """
        A failed check must not abort the run — the caller records zero
        violations for that rule. Pinned so a future change to fail-loud is
        deliberate: that would be a behaviour change, not a bugfix.
        """
        spark = MagicMock()
        spark.table.side_effect = RuntimeError("TABLE_NOT_FOUND")
        assert quarantine.check_violation(spark, "missing.table", "x") == []


# ---------------------------------------------------------------------------
# write_to_quarantine
# ---------------------------------------------------------------------------


class TestWriteToQuarantine:
    def _spark_with_columns(self, columns):
        spark = MagicMock()
        target = MagicMock()
        target.columns = columns
        spark.table.return_value = target
        return spark

    def test_empty_records_does_not_touch_spark(self):
        spark = MagicMock()
        assert quarantine.write_to_quarantine(spark, [], "t", "v", "s") == 0
        spark.table.assert_not_called()

    def test_returns_number_of_source_records(self):
        spark = self._spark_with_columns(["txn_id", "violation_type", "source_table", "detected_at"])
        records = [{"txn_id": 1}, {"txn_id": 2}, {"txn_id": 3}]
        assert quarantine.write_to_quarantine(spark, records, "t", "v", "s") == 3

    def test_appends_to_the_target_table(self):
        spark = self._spark_with_columns(["txn_id", "violation_type", "source_table", "detected_at"])
        quarantine.write_to_quarantine(spark, [{"txn_id": 1}], "lakehouse.ops.quarantine", "v", "s")

        writer = spark.createDataFrame.return_value.write
        writer.format.assert_called_once_with("iceberg")
        writer.format.return_value.mode.assert_called_once_with("append")
        writer.format.return_value.mode.return_value.saveAsTable.assert_called_once_with("lakehouse.ops.quarantine")

    def test_injects_violation_type_and_source_table(self):
        spark = self._spark_with_columns(["txn_id", "violation_type", "source_table"])
        quarantine.write_to_quarantine(spark, [{"txn_id": 7}], "t", "negative_amount", "lakehouse.silver.f")

        row = spark.createDataFrame.call_args[0][0][0]
        assert row["txn_id"] == 7
        assert row["violation_type"] == "negative_amount"
        assert row["source_table"] == "lakehouse.silver.f"

    def test_fills_metadata_columns_the_source_lacks(self):
        spark = self._spark_with_columns(["txn_id", "detected_at", "violation_detail"])
        quarantine.write_to_quarantine(spark, [{"txn_id": 7}], "t", "v", "src.table")

        row = spark.createDataFrame.call_args[0][0][0]
        assert row["detected_at"] is not None
        assert "src.table" in row["violation_detail"]

    def test_unknown_target_columns_become_none(self):
        """Columns in the target but not in the source record are nulled."""
        spark = self._spark_with_columns(["txn_id", "extra_col", "violation_type"])
        quarantine.write_to_quarantine(spark, [{"txn_id": 7}], "t", "v", "s")

        row = spark.createDataFrame.call_args[0][0][0]
        assert row["extra_col"] is None

    def test_drops_source_keys_absent_from_the_target(self):
        """
        The row is built by iterating target columns, so a source key the target
        does not declare must not appear — otherwise createDataFrame raises on
        an unexpected field.
        """
        spark = self._spark_with_columns(["txn_id", "violation_type"])
        quarantine.write_to_quarantine(spark, [{"txn_id": 7, "secret": "keep-out"}], "t", "v", "s")

        row = spark.createDataFrame.call_args[0][0][0]
        assert "secret" not in row.asDict()

    def test_write_failure_returns_zero(self):
        """
        The failure path returns 0 rather than raising. That makes a broken
        write indistinguishable from an empty result at the call site, so it is
        asserted explicitly here as the current contract.
        """
        spark = MagicMock()
        spark.table.return_value.columns = ["txn_id"]
        spark.createDataFrame.side_effect = RuntimeError("schema mismatch")

        assert quarantine.write_to_quarantine(spark, [{"txn_id": 1}], "t", "v", "s") == 0


# ---------------------------------------------------------------------------
# run_quarantine_checks
# ---------------------------------------------------------------------------


class TestRunQuarantineChecks:
    def test_clean_rule_reports_pass(self):
        spark = MagicMock()
        with patch.object(quarantine, "check_violation", return_value=[]):
            results = quarantine.run_quarantine_checks(
                spark,
                "rule_a",
                {
                    "source_table": "s",
                    "target_table": "t",
                    "violations": [{"name": "v1", "condition": "x"}],
                },
                "2026-09-17",
            )

        assert len(results) == 1
        assert results[0]["status"] == "PASS"
        assert results[0]["violations_found"] == 0

    def test_fail_severity_reports_quarantined(self):
        spark = MagicMock()
        with (
            patch.object(quarantine, "check_violation", return_value=[{"id": 1}]),
            patch.object(quarantine, "write_to_quarantine", return_value=1),
        ):
            results = quarantine.run_quarantine_checks(
                spark,
                "rule_a",
                {
                    "source_table": "s",
                    "target_table": "t",
                    "violations": [{"name": "v1", "condition": "x", "severity": "FAIL"}],
                },
                "2026-09-17",
            )

        assert results[0]["status"] == "QUARANTINED"
        assert results[0]["violations_found"] == 1

    def test_warn_severity_reports_warned(self):
        spark = MagicMock()
        with (
            patch.object(quarantine, "check_violation", return_value=[{"id": 1}]),
            patch.object(quarantine, "write_to_quarantine", return_value=1),
        ):
            results = quarantine.run_quarantine_checks(
                spark,
                "rule_a",
                {
                    "source_table": "s",
                    "target_table": "t",
                    "violations": [{"name": "v1", "condition": "x", "severity": "WARN"}],
                },
                "2026-09-17",
            )

        assert results[0]["status"] == "WARNED"

    def test_severity_defaults_to_fail(self):
        spark = MagicMock()
        with (
            patch.object(quarantine, "check_violation", return_value=[{"id": 1}]),
            patch.object(quarantine, "write_to_quarantine", return_value=1),
        ):
            results = quarantine.run_quarantine_checks(
                spark,
                "r",
                {"source_table": "s", "target_table": "t", "violations": [{"name": "v", "condition": "c"}]},
                "d",
            )

        assert results[0]["severity"] == "FAIL"
        assert results[0]["status"] == "QUARANTINED"

    def test_every_violation_produces_a_result(self):
        spark = MagicMock()
        with patch.object(quarantine, "check_violation", return_value=[]):
            results = quarantine.run_quarantine_checks(
                spark,
                "r",
                {
                    "source_table": "s",
                    "target_table": "t",
                    "violations": [
                        {"name": "v1", "condition": "c1"},
                        {"name": "v2", "condition": "c2"},
                        {"name": "v3", "condition": "c3"},
                    ],
                },
                "d",
            )

        assert [r["violation_name"] for r in results] == ["v1", "v2", "v3"]

    def test_rule_with_no_violations_yields_nothing(self):
        spark = MagicMock()
        results = quarantine.run_quarantine_checks(
            spark, "r", {"source_table": "s", "target_table": "t", "violations": []}, "d"
        )
        assert results == []

    def test_result_carries_routing_fields(self):
        spark = MagicMock()
        with patch.object(quarantine, "check_violation", return_value=[]):
            results = quarantine.run_quarantine_checks(
                spark,
                "my_rule",
                {"source_table": "src", "target_table": "tgt", "violations": [{"name": "v", "condition": "c"}]},
                "d",
            )

        assert results[0]["rule_name"] == "my_rule"
        assert results[0]["source_table"] == "src"
        assert results[0]["target_table"] == "tgt"


# ---------------------------------------------------------------------------
# print_summary — also returns the counters that drive the exit code
# ---------------------------------------------------------------------------


class TestPrintSummary:
    def test_counts_by_status(self):
        results = [
            {"status": "PASS", "violations_found": 0, "rule_name": "a", "violation_name": "v"},
            {"status": "WARNED", "violations_found": 2, "rule_name": "a", "violation_name": "v"},
            {"status": "QUARANTINED", "violations_found": 3, "rule_name": "a", "violation_name": "v"},
            {"status": "QUARANTINED", "violations_found": 4, "rule_name": "a", "violation_name": "v"},
        ]
        assert quarantine.print_summary(results) == (1, 1, 2)

    def test_all_pass(self):
        results = [{"status": "PASS", "violations_found": 0, "rule_name": "a", "violation_name": "v"}]
        assert quarantine.print_summary(results) == (1, 0, 0)

    def test_empty_results(self):
        assert quarantine.print_summary([]) == (0, 0, 0)

    def test_warn_does_not_count_as_failure(self):
        """Only QUARANTINED drives the non-zero exit; WARNED must not."""
        results = [{"status": "WARNED", "violations_found": 5, "rule_name": "a", "violation_name": "v"}]
        assert quarantine.print_summary(results) == (0, 1, 0)


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


class TestParseArgs:
    def test_requires_cob_dt(self):
        with patch.object(sys, "argv", ["quarantine.py", "--layer", "silver"]), pytest.raises(SystemExit):
            quarantine.parse_args()

    def test_requires_layer(self):
        with patch.object(sys, "argv", ["quarantine.py", "--cob_dt", "2026-09-17"]), pytest.raises(SystemExit):
            quarantine.parse_args()

    def test_rejects_unknown_layer(self):
        with (
            patch.object(sys, "argv", ["quarantine.py", "--cob_dt", "d", "--layer", "bronze"]),
            pytest.raises(SystemExit),
        ):
            quarantine.parse_args()

    @pytest.mark.parametrize("layer", ["silver", "gold", "all"])
    def test_accepts_supported_layers(self, layer):
        with patch.object(sys, "argv", ["quarantine.py", "--cob_dt", "d", "--layer", layer]):
            assert quarantine.parse_args().layer == layer

    def test_rules_file_defaults_to_shipped_file(self):
        with patch.object(sys, "argv", ["quarantine.py", "--cob_dt", "d", "--layer", "all"]):
            assert quarantine.parse_args().rules_file == quarantine.RULES_FILE


# ---------------------------------------------------------------------------
# Layer filtering — the logic main() applies before running checks
# ---------------------------------------------------------------------------


class TestLayerFiltering:
    """
    main() filters rule sets by source_table prefix. That filtering is inline
    rather than a named function, so it is reproduced here against the real
    rules file to pin the expected outcome.
    """

    def _filter(self, rules, layer):
        if layer == "all":
            return rules
        return {k: v for k, v in rules.items() if v.get("source_table", "").startswith(f"lakehouse.{layer}.")}

    def test_all_layer_keeps_every_rule(self):
        rules = quarantine.load_rules(quarantine.RULES_FILE)["quarantine_rules"]
        assert self._filter(rules, "all") == rules

    def test_layer_filter_selects_only_that_layer(self):
        rules = quarantine.load_rules(quarantine.RULES_FILE)["quarantine_rules"]
        for layer in ("silver", "gold"):
            for cfg in self._filter(rules, layer).values():
                assert cfg["source_table"].startswith(f"lakehouse.{layer}.")

    def test_unknown_layer_selects_nothing(self):
        rules = quarantine.load_rules(quarantine.RULES_FILE)["quarantine_rules"]
        assert self._filter(rules, "bronze") == {}
