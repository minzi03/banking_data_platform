"""
Tests for code_etl/gold/base_job/gold_job.py — the shared Gold job runner.

Covers:
  - validate_config: required sections, job type gate
  - assert_source_snapshots: the fail-loud guard against silent corruption
  - assert_non_empty: the guard against overwriting a partition with nothing
  - _qualify: catalog qualification rules
  - run_gold_job: write path, table creation, optimize dispatch
  - optimize_written_partition / build_rewrite_sql: Iceberg rewrite, failure policy

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
    "spark.schema_guard": MagicMock(),
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
            patch.object(gold_job, "optimize_written_partition"),
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
            patch.object(gold_job, "optimize_written_partition"),
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
            patch.object(gold_job, "optimize_written_partition"),
        ):
            run_gold_job(spark, _config(), "2026-09-17", logger)

        create.assert_not_called()

    def test_schema_guard_runs_before_the_write_on_existing_tables(self, logger):
        spark = MagicMock()
        result = MagicMock()
        calls = []
        result.writeTo.return_value.overwritePartitions.side_effect = lambda: calls.append("write")
        with (
            patch.object(gold_job, "assert_source_snapshots"),
            patch.object(gold_job, "load_source_df", return_value=result),
            patch.object(gold_job, "assert_non_empty"),
            patch.object(gold_job, "table_exists", return_value=True),
            patch.object(gold_job, "guard_and_evolve", side_effect=lambda *a: calls.append("guard")),
            patch.object(gold_job, "optimize_written_partition"),
        ):
            run_gold_job(spark, _config(), "2026-09-17", logger)

        assert calls == ["guard", "write"]

    def test_breaking_schema_change_stops_the_write(self, logger):
        """Gold chỉ publish khi schema ổn định: guard raise thì không ghi gì."""
        spark = MagicMock()
        result = MagicMock()
        with (
            patch.object(gold_job, "assert_source_snapshots"),
            patch.object(gold_job, "load_source_df", return_value=result),
            patch.object(gold_job, "assert_non_empty"),
            patch.object(gold_job, "table_exists", return_value=True),
            patch.object(gold_job, "guard_and_evolve", side_effect=RuntimeError("BREAKING")),
        ):
            with pytest.raises(RuntimeError, match="BREAKING"):
                run_gold_job(spark, _config(), "2026-09-17", logger)
            result.writeTo.assert_not_called()

    def test_new_table_skips_the_schema_guard(self, logger):
        spark = MagicMock()
        result = MagicMock()
        with (
            patch.object(gold_job, "assert_source_snapshots"),
            patch.object(gold_job, "load_source_df", return_value=result),
            patch.object(gold_job, "assert_non_empty"),
            patch.object(gold_job, "table_exists", return_value=False),
            patch.object(gold_job, "create_iceberg_table_if_not_exists"),
            patch.object(gold_job, "guard_and_evolve") as guard,
            patch.object(gold_job, "optimize_written_partition"),
        ):
            run_gold_job(spark, _config(), "2026-09-17", logger)

        guard.assert_not_called()

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

    def test_written_partition_is_optimized_after_the_write(self, logger):
        spark = MagicMock()
        result = MagicMock()
        calls = []
        result.writeTo.return_value.overwritePartitions.side_effect = lambda: calls.append("write")
        with (
            patch.object(gold_job, "assert_source_snapshots"),
            patch.object(gold_job, "load_source_df", return_value=result),
            patch.object(gold_job, "assert_non_empty"),
            patch.object(gold_job, "table_exists", return_value=True),
            patch.object(gold_job, "get_target_table", return_value="lakehouse.gold.mart_x"),
            patch.object(
                gold_job, "optimize_written_partition", side_effect=lambda *a: calls.append("optimize")
            ) as optimize,
        ):
            run_gold_job(spark, _config(), "2026-09-17", logger)

        assert calls == ["write", "optimize"]
        assert optimize.call_args[0][1:3] == ("lakehouse.gold.mart_x", "2026-09-17")


# ---------------------------------------------------------------------------
# build_rewrite_sql / optimize_written_partition
# ---------------------------------------------------------------------------


class TestRewriteSql:
    """
    Gold từng chạy `OPTIMIZE … ZORDER BY` — cú pháp Delta Lake. Iceberg trả
    PARSE_SYNTAX_ERROR, lỗi bị nuốt thành WARNING, và không bảng Gold nào được
    sắp xếp lại. Các test này khoá cú pháp Iceberg.
    """

    def test_uses_iceberg_procedure_not_delta_syntax(self):
        sql = gold_job.build_rewrite_sql("lakehouse.gold.rfm_segment", ["rfm_segment", "customer_id"], "2026-09-22")
        assert sql.startswith("CALL lakehouse.system.rewrite_data_files(")
        assert "OPTIMIZE" not in sql.upper()
        assert "ZORDER BY" not in sql.upper()

    def test_table_argument_is_catalog_relative(self):
        sql = gold_job.build_rewrite_sql("lakehouse.gold.rfm_segment", ["a", "b"], "2026-09-22")
        assert "table => 'gold.rfm_segment'" in sql

    def test_multi_column_uses_zorder(self):
        sql = gold_job.build_rewrite_sql("lakehouse.gold.rfm_segment", ["rfm_segment", "customer_id"], "2026-09-22")
        assert "strategy => 'sort'" in sql
        assert "sort_order => 'zorder(rfm_segment,customer_id)'" in sql

    def test_single_column_uses_linear_sort(self):
        sql = gold_job.build_rewrite_sql("lakehouse.gold.mart_customer_360", ["customer_id"], "2026-09-22")
        assert "sort_order => 'customer_id ASC NULLS LAST'" in sql
        assert "zorder" not in sql

    def test_rewrite_is_scoped_to_the_written_partition(self):
        sql = gold_job.build_rewrite_sql("lakehouse.gold.rfm_segment", ["a", "b"], "2026-09-22")
        assert "where => \"cob_dt = '2026-09-22'\"" in sql

    def test_small_partitions_are_still_rewritten(self):
        sql = gold_job.build_rewrite_sql("lakehouse.gold.rfm_segment", ["a", "b"], "2026-09-22")
        assert "'rewrite-all', 'true'" in sql


class TestOptimizeWrittenPartition:
    def test_unlisted_table_is_skipped(self, logger):
        spark = MagicMock()
        assert gold_job.optimize_written_partition(
            spark, "lakehouse.gold.unknown_table", "2026-09-22", "mart360", logger
        )
        spark.sql.assert_not_called()

    def test_listed_table_runs_the_procedure(self, logger):
        spark = MagicMock()
        ok = gold_job.optimize_written_partition(
            spark, "lakehouse.gold.mart_customer_360", "2026-09-22", "mart360", logger
        )
        assert ok
        spark.sql.assert_called_once()
        assert "rewrite_data_files" in spark.sql.call_args[0][0]
        logger.warning.assert_not_called()

    def test_no_full_table_count_before_optimizing(self, logger):
        """Rewrite chỉ đụng một partition — không cần (và không được) count cả bảng."""
        spark = MagicMock()
        gold_job.optimize_written_partition(spark, "lakehouse.gold.mart_customer_360", "2026-09-22", "mart360", logger)
        spark.table.assert_not_called()

    def test_failure_is_not_fatal_but_is_marked(self, logger):
        """
        Dữ liệu đã commit trước bước này → lỗi không được làm job fail.
        Nhưng phải để lại dấu grep được: marker, bảng, cob_dt, loại lỗi.
        """
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("[PARSE_SYNTAX_ERROR] Syntax error")
        ok = gold_job.optimize_written_partition(
            spark, "lakehouse.gold.mart_customer_360", "2026-09-22", "mart360", logger
        )
        assert ok is False
        logger.warning.assert_called_once()
        msg = logger.warning.call_args[0][0]
        assert gold_job.OPTIMIZE_FAILED_MARKER in msg
        assert "lakehouse.gold.mart_customer_360" in msg
        assert "2026-09-22" in msg
        assert "RuntimeError" in msg

    def test_all_risk_marts_have_zorder_columns(self):
        for table in ("loan_portfolio_risk", "fraud_risk_txn", "aml_monitoring"):
            assert table in ZORDER_COLUMNS, f"{table} has no Z-Order configuration"

    def test_zorder_columns_are_non_empty_lists(self):
        for table, cols in ZORDER_COLUMNS.items():
            assert isinstance(cols, list) and cols, f"{table} has an empty Z-Order column list"


def test_no_delta_optimize_syntax_left_in_etl_code():
    """
    `OPTIMIZE <bảng> ZORDER BY (...)` là cú pháp Delta Lake — không được quay lại
    code chạy trên Iceberg. Bắt dạng câu SQL (bảng là f-string, ZORDER BY có
    ngoặc), không bắt comment nhắc tên lệnh.
    """
    import re

    pattern = re.compile(r"\bOPTIMIZE\s+\{|\bZORDER\s+BY\s*\(", re.IGNORECASE)
    offenders = [
        str(p.relative_to(PROJECT_ROOT))
        for p in (PROJECT_ROOT / "code_etl").rglob("*.py")
        if pattern.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == []
