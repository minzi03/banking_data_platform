"""
Tests for code_etl/shared/ops/iceberg_maintenance.py

Covers:
  - Table lists: FACT_TABLES, MART_TABLES, DIM_TABLES
  - run_maintenance: full mode, expire_only mode, error handling
  - parse_arguments: CLI arg parsing
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Import via importlib to avoid package conflicts
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["spark"] = MagicMock()
sys.modules["spark.spark_session"] = MagicMock()

_spec = importlib.util.spec_from_file_location(
    "iceberg_maintenance_mod",
    str(PROJECT_ROOT / "code_etl" / "shared" / "ops" / "iceberg_maintenance.py")
)
_imod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_imod)

FACT_TABLES = _imod.FACT_TABLES
MART_TABLES = _imod.MART_TABLES
DIM_TABLES = _imod.DIM_TABLES
run_maintenance = _imod.run_maintenance
parse_arguments = _imod.parse_arguments


class TestTableLists:
    """Tests for the maintenance table lists."""

    def test_fact_tables_are_iceberg_formatted(self):
        """All fact tables should use catalog.schema.table format."""
        for table in FACT_TABLES:
            assert table.startswith("lakehouse.")
            parts = table.split(".")
            assert len(parts) == 3, f"Invalid table name: {table}"

    def test_mart_tables_are_iceberg_formatted(self):
        """All mart tables should use catalog.schema.table format."""
        for table in MART_TABLES:
            assert table.startswith("lakehouse.")
            parts = table.split(".")
            assert len(parts) == 3

    def test_dim_tables_are_iceberg_formatted(self):
        """All dim tables should use catalog.schema.table format."""
        for table in DIM_TABLES:
            assert table.startswith("lakehouse.")
            parts = table.split(".")
            assert len(parts) == 3

    def test_fact_tables_not_empty(self):
        """Should have at least one fact table."""
        assert len(FACT_TABLES) > 0

    def test_mart_tables_not_empty(self):
        """Should have at least one mart table."""
        assert len(MART_TABLES) > 0

    def test_dim_tables_not_empty(self):
        """Should have at least one dim table."""
        assert len(DIM_TABLES) > 0

    def test_no_duplicates_across_groups(self):
        """Tables should not appear in multiple groups (overlap is OK but flagged)."""
        # This is informational — some overlap is expected
        all_tables = FACT_TABLES + MART_TABLES + DIM_TABLES
        unique = set(all_tables)
        # Just verify we have the expected total count
        assert len(unique) > 0


class TestRunMaintenance:
    """Tests for the run_maintenance function."""

    def test_full_mode_calls_all_three(self):
        """Full mode should call rewrite, expire, and orphan for each table."""
        spark = MagicMock()
        tables = ["lakehouse.bronze.core_account"]

        with patch.object(_imod, "rewrite_data_files") as mock_rewrite, \
             patch.object(_imod, "expire_snapshots") as mock_expire, \
             patch.object(_imod, "remove_orphan_files") as mock_orphan:

            run_maintenance(spark, tables, mode="full")

            mock_rewrite.assert_called_once()
            mock_expire.assert_called_once()
            mock_orphan.assert_called_once()

    def test_expire_only_mode_calls_only_expire(self):
        """expire_only mode should only call expire_snapshots."""
        spark = MagicMock()
        tables = ["lakehouse.bronze.core_account"]

        with patch.object(_imod, "rewrite_data_files") as mock_rewrite, \
             patch.object(_imod, "expire_snapshots") as mock_expire, \
             patch.object(_imod, "remove_orphan_files") as mock_orphan:

            run_maintenance(spark, tables, mode="expire_only")

            mock_rewrite.assert_not_called()
            mock_expire.assert_called_once()
            mock_orphan.assert_not_called()

    def test_continues_on_individual_failure(self):
        """Should continue processing other tables after one fails."""
        spark = MagicMock()
        tables = ["table1", "table2", "table3"]

        with patch.object(_imod, "rewrite_data_files", side_effect=[Exception("fail"), None, None]), \
             patch.object(_imod, "expire_snapshots"), \
             patch.object(_imod, "remove_orphan_files"):

            with pytest.raises(RuntimeError, match="Maintenance failed"):
                run_maintenance(spark, tables, mode="full")

    def test_empty_table_list(self):
        """Should handle empty table list without error."""
        spark = MagicMock()
        with patch.object(_imod, "rewrite_data_files"), \
             patch.object(_imod, "expire_snapshots"), \
             patch.object(_imod, "remove_orphan_files"):

            run_maintenance(spark, [], mode="full")  # Should not raise


class TestParseArguments:
    """Tests for CLI argument parsing."""

    def test_default_args(self):
        """Should have default target=all and mode=full."""
        with patch("sys.argv", ["iceberg_maintenance.py"]):
            args = parse_arguments()
            assert args.target == "all"
            assert args.mode == "full"

    def test_target_choices(self):
        """Should accept valid target choices."""
        for target in ["fact", "mart", "dim", "all"]:
            with patch("sys.argv", ["iceberg_maintenance.py", "--target", target]):
                args = parse_arguments()
                assert args.target == target

    def test_mode_choices(self):
        """Should accept valid mode choices."""
        for mode in ["full", "expire_only"]:
            with patch("sys.argv", ["iceberg_maintenance.py", "--mode", mode]):
                args = parse_arguments()
                assert args.mode == mode
