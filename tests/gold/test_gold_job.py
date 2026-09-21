"""
Tests for code_etl/gold/base_job/gold_job.py — the shared Gold job runner.

Covers:
  - validate_config: required sections, job type gate
  - assert_source_snapshots: the fail-loud guard against silent corruption
  - assert_non_empty: the guard against overwriting a partition with nothing
  - _qualify: catalog qualification rules
  - run_gold_job: write path, table creation, Z-Order dispatch
  - _run_zorder_if_needed: which tables get optimized

The two `assert_*` guards are the point of this suite. Without them a Gold job
whose upstream partition is missing does not fail — it writes a full row set
with every metric at zero, which looks like valid data. The tests below pin
that behaviour so a refactor cannot quietly remove the guard.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# gold_job.py imports spark session/iceberg helpers, which pull in pyspark.
# Stub only for the duration of exec, then restore — leaving the stubs in
# sys.modules would break every later test module that imports the real
# packages. gold_job.py binds the names it needs at exec time, so it keeps
# working against the mocks afterwards.
_STUBBED = {
    "spark": MagicMock(),
    "spark.spark_session": MagicMock(),
    "spark.iceberg_utils": MagicMock(),
    "utils": MagicMock(),
    "utils.logger": MagicMock(),
    "utils.yaml_loader": MagicMock(),
    "common_utils": MagicMock(),
}
_SAVED = {name: sys.modules.get(name) for name in _STUBBED}
sys.modules.update(_STUBBED)

_spec = importlib.util.spec_from_file_location(
    "gold_job_mod", str(PROJECT_ROOT / "code_etl" / "gold" / "base_job" / "gold_job.py")
)
_gold_job = importlib.util.module_from_spec(_spec)
try:
    _spec.loader.exec_module(_gold_job)
finally:
    for _name, _prev in _SAVED.items():
        if _prev is None:
            sys.modules.pop(_name, None)
        else:
            sys.modules[_name] = _prev

gold_job = _gold_job

VALID_JOB_TYPES = gold_job.VALID_JOB_TYPES
ZORDER_COLUMNS = gold_job.ZORDER_COLUMNS
validate_config = gold_job.validate_config
assert_source_snapshots = gold_job.assert_source_snapshots
assert_non_empty = gold_job.assert_non_empty
run_gold_job = gold_job.run_gold_job


@pytest.fixture
def logger():
    return MagicMock()


def _config(**overrides):
    base = {
        "job": {"type": "mart360"},
        "source": {"tables": ["silver.dim_customer"]},
        "target": {"catalog": "lakehouse", "schema": "gold", "table": "mart_x"},
        "sql": "SELECT 1",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Job type gate — this is what rejected the three risk marts
# ---------------------------------------------------------------------------


class TestJobTypeGate:
    def test_risk_is_a_valid_job_type(self):
        """
        loan_portfolio_risk, fraud_risk_txn and aml_monitoring declare
        `type: risk`. Before it was added here, every run failed in
        validate_config and the three marts could never be materialized.
        """
        assert "risk" in VALID_JOB_TYPES

    @pytest.mark.parametrize("job_type", ["mart360", "segment", "time_analytics", "risk"])
    def test_every_supported_type_passes_validation(self, job_type):
        validate_config(_config(job={"type": job_type}))

    def test_unknown_job_type_is_rejected(self):
        with pytest.raises(ValueError, match="Loại job không hợp lệ"):
            validate_config(_config(job={"type": "not_a_type"}))

    def test_missing_job_type_is_rejected(self):
        with pytest.raises(ValueError, match="Loại job không hợp lệ"):
            validate_config(_config(job={}))

    @pytest.mark.parametrize("section", ["job", "source", "target", "sql"])
    def test_every_required_section_is_required(self, section):
        cfg = _config()
        del cfg[section]
        with pytest.raises(ValueError, match="Thiếu section bắt buộc"):
            validate_config(cfg)


# ---------------------------------------------------------------------------
# assert_source_snapshots — the silent-corruption guard
# ---------------------------------------------------------------------------


class TestAssertSourceSnapshots:
    def test_no_requirement_is_a_noop(self, logger):
        """Configs without require_snapshots do not query anything."""
        spark = MagicMock()
        assert_source_snapshots(spark, _config(), "2026-09-17", logger)
        spark.sql.assert_not_called()

    def test_present_partition_passes(self, logger):
        spark = MagicMock()
        spark.sql.return_value.take.return_value = [1]
        cfg = _config(validation={"require_snapshots": ["silver.fact_txn_account"]})

        assert_source_snapshots(spark, cfg, "2026-09-17", logger)
        spark.sql.assert_called_once()

    def test_missing_partition_raises(self, logger):
        """
        The whole point: a missing upstream partition must stop the job rather
        than let it write a full row set of zeros.
        """
        spark = MagicMock()
        spark.sql.return_value.take.return_value = []
        cfg = _config(validation={"require_snapshots": ["silver.fact_txn_account"]})

        with pytest.raises(RuntimeError, match="Thiếu snapshot nguồn"):
            assert_source_snapshots(spark, cfg, "2026-09-17", logger)

    def test_error_names_the_missing_table(self, logger):
        spark = MagicMock()
        spark.sql.return_value.take.return_value = []
        cfg = _config(validation={"require_snapshots": ["silver.fact_loan_payment"]})

        with pytest.raises(RuntimeError) as exc:
            assert_source_snapshots(spark, cfg, "2026-09-17", logger)
        assert "fact_loan_payment" in str(exc.value)

    def test_every_declared_table_is_checked(self, logger):
        spark = MagicMock()
        spark.sql.return_value.take.return_value = [1]
        cfg = _config(validation={"require_snapshots": ["silver.a", "silver.b", "silver.c"]})

        assert_source_snapshots(spark, cfg, "2026-09-17", logger)
        assert spark.sql.call_count == 3

    def test_first_missing_table_does_not_hide_the_rest(self, logger):
        """All sources are checked before raising, so the error is complete."""
        spark = MagicMock()
        spark.sql.return_value.take.return_value = []
        cfg = _config(validation={"require_snapshots": ["silver.a", "silver.b"]})

        with pytest.raises(RuntimeError) as exc:
            assert_source_snapshots(spark, cfg, "2026-09-17", logger)
        assert "silver.a" in str(exc.value) and "silver.b" in str(exc.value)

    def test_cob_dt_reaches_the_query(self, logger):
        spark = MagicMock()
        spark.sql.return_value.take.return_value = [1]
        cfg = _config(validation={"require_snapshots": ["silver.a"]})

        assert_source_snapshots(spark, cfg, "2026-09-17", logger)
        assert "2026-09-17" in spark.sql.call_args[0][0]


# ---------------------------------------------------------------------------
# assert_non_empty
# ---------------------------------------------------------------------------


class TestAssertNonEmpty:
    def test_not_required_is_a_noop(self, logger):
        result = MagicMock()
        assert_non_empty(result, _config(), "2026-09-17", logger)
        result.take.assert_not_called()

    def test_empty_result_raises(self, logger):
        result = MagicMock()
        result.take.return_value = []
        cfg = _config(validation={"require_non_empty": True})

        with pytest.raises(RuntimeError, match="không sinh dòng nào"):
            assert_non_empty(result, cfg, "2026-09-17", logger)

    def test_non_empty_passes(self, logger):
        result = MagicMock()
        result.take.return_value = [object()]
        assert_non_empty(result, _config(validation={"require_non_empty": True}), "d", logger)


# ---------------------------------------------------------------------------
# _qualify
# ---------------------------------------------------------------------------


class TestQualify:
    def test_two_part_name_gets_the_catalog(self):
        assert gold_job._qualify("silver.fact_txn", "lakehouse") == "lakehouse.silver.fact_txn"

    def test_three_part_name_is_left_alone(self):
        assert gold_job._qualify("lakehouse.gold.t", "lakehouse") == "lakehouse.gold.t"

    def test_single_part_name_is_left_alone(self):
        """Temp views created by tests have no schema to qualify."""
        assert gold_job._qualify("temp_view", "lakehouse") == "temp_view"


# ---------------------------------------------------------------------------
# run_gold_job
# ---------------------------------------------------------------------------


class TestRunGoldJob:
    def test_writes_with_overwrite_partitions(self, logger):
        spark = MagicMock()
        result = MagicMock()
        with (
            patch.object(gold_job, "assert_source_snapshots"),
            patch.object(gold_job, "load_source_df", return_value=result),
            patch.object(gold_job, "assert_non_empty"),
            patch.object(gold_job, "table_exists", return_value=True),
            patch.object(gold_job, "_run_zorder_if_needed"),
        ):
            run_gold_job(spark, _config(), "2026-09-17", logger)

        result.writeTo.return_value.overwritePartitions.assert_called_once()

    def test_target_is_materialized_before_the_write(self, logger):
        """
        Iceberg's V2 writer resolves the target before writing and fails with
        TABLE_OR_VIEW_NOT_FOUND if it does not exist, so the table has to be
        created first.
        """
        spark = MagicMock()
        result = MagicMock()
        with (
            patch.object(gold_job, "assert_source_snapshots"),
            patch.object(gold_job, "load_source_df", return_value=result),
            patch.object(gold_job, "assert_non_empty"),
            patch.object(gold_job, "table_exists", return_value=False),
            patch.object(gold_job, "create_iceberg_table_if_not_exists") as create,
            patch.object(gold_job, "_run_zorder_if_needed"),
        ):
            run_gold_job(spark, _config(), "2026-09-17", logger)

        create.assert_called_once()

    def test_existing_table_is_not_recreated(self, logger):
        spark = MagicMock()
        result = MagicMock()
        with (
            patch.object(gold_job, "assert_source_snapshots"),
            patch.object(gold_job, "load_source_df", return_value=result),
            patch.object(gold_job, "assert_non_empty"),
            patch.object(gold_job, "table_exists", return_value=True),
            patch.object(gold_job, "create_iceberg_table_if_not_exists") as create,
            patch.object(gold_job, "_run_zorder_if_needed"),
        ):
            run_gold_job(spark, _config(), "2026-09-17", logger)

        create.assert_not_called()

    def test_snapshot_guard_runs_before_reading(self, logger):
        """A missing partition must fail before any work is done."""
        spark = MagicMock()
        with (
            patch.object(gold_job, "assert_source_snapshots", side_effect=RuntimeError("guard")),
            patch.object(gold_job, "load_source_df") as load,
        ):
            with pytest.raises(RuntimeError, match="guard"):
                run_gold_job(spark, _config(), "2026-09-17", logger)
            load.assert_not_called()

    def test_non_empty_guard_runs_before_writing(self, logger):
        spark = MagicMock()
        result = MagicMock()
        with (
            patch.object(gold_job, "assert_source_snapshots"),
            patch.object(gold_job, "load_source_df", return_value=result),
            patch.object(gold_job, "assert_non_empty", side_effect=RuntimeError("empty")),
        ):
            with pytest.raises(RuntimeError, match="empty"):
                run_gold_job(spark, _config(), "2026-09-17", logger)
            result.writeTo.assert_not_called()

    def test_zorder_is_dispatched_after_the_write(self, logger):
        spark = MagicMock()
        result = MagicMock()
        with (
            patch.object(gold_job, "assert_source_snapshots"),
            patch.object(gold_job, "load_source_df", return_value=result),
            patch.object(gold_job, "assert_non_empty"),
            patch.object(gold_job, "table_exists", return_value=True),
            patch.object(gold_job, "_run_zorder_if_needed") as zorder,
        ):
            run_gold_job(spark, _config(), "2026-09-17", logger)

        zorder.assert_called_once()


# ---------------------------------------------------------------------------
# _run_zorder_if_needed
# ---------------------------------------------------------------------------


class TestZOrder:
    def test_unlisted_table_is_skipped(self, logger):
        spark = MagicMock()
        gold_job._run_zorder_if_needed(spark, "lakehouse.gold.unknown_table", "mart360", logger)
        spark.sql.assert_not_called()

    def test_listed_table_is_optimized(self, logger):
        spark = MagicMock()
        spark.table.return_value.count.return_value = 100
        gold_job._run_zorder_if_needed(spark, "lakehouse.gold.mart_customer_360", "mart360", logger)
        spark.sql.assert_called_once()
        assert "OPTIMIZE" in spark.sql.call_args[0][0]

    def test_large_table_is_skipped(self, logger):
        """Z-Order on a multi-million row table costs more than it saves."""
        spark = MagicMock()
        spark.table.return_value.count.return_value = 2_000_000
        gold_job._run_zorder_if_needed(spark, "lakehouse.gold.mart_customer_360", "mart360", logger)
        spark.sql.assert_not_called()

    def test_zorder_failure_is_not_fatal(self, logger):
        """
        A failed OPTIMIZE must not fail the job — the data is already written.
        """
        spark = MagicMock()
        spark.table.return_value.count.return_value = 100
        spark.sql.side_effect = RuntimeError("not supported")
        gold_job._run_zorder_if_needed(spark, "lakehouse.gold.mart_customer_360", "mart360", logger)
        logger.warning.assert_called()

    def test_all_risk_marts_have_zorder_columns(self):
        for table in ("loan_portfolio_risk", "fraud_risk_txn", "aml_monitoring"):
            assert table in ZORDER_COLUMNS, f"{table} has no Z-Order configuration"

    def test_zorder_columns_are_non_empty_lists(self):
        for table, cols in ZORDER_COLUMNS.items():
            assert isinstance(cols, list) and cols, f"{table} has an empty Z-Order column list"
