"""
Tests for governance.freshness_checks — SLA-based data freshness monitoring.
"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from governance.freshness_checks import FreshnessChecker, FreshnessResult


class _Row:
    """Helper to simulate a Spark Row with __getitem__ support."""
    def __init__(self, data: dict):
        self._data = data
    def __getitem__(self, key):
        return self._data[key]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def checker():
    return FreshnessChecker()


@pytest.fixture
def mock_spark():
    """Create a mock SparkSession."""
    spark = MagicMock()
    spark._sc._jvm = MagicMock()
    return spark


@pytest.fixture
def mock_df_fresh():
    """Mock DataFrame with fresh data (recent timestamp)."""
    df = MagicMock()
    df.columns = ["customer_id", "_ingested_at"]

    # The code aliases max(col) as "last_updated", so the row key must match
    recent_row = _Row({"last_updated": datetime.now(timezone.utc) - timedelta(hours=1)})
    df.select.return_value.collect.return_value = [recent_row]

    return df


@pytest.fixture
def mock_df_stale():
    """Mock DataFrame with stale data (old timestamp)."""
    df = MagicMock()
    df.columns = ["customer_id", "_ingested_at"]

    old_row = _Row({"last_updated": datetime.now(timezone.utc) - timedelta(hours=48)})
    df.select.return_value.collect.return_value = [old_row]

    return df


@pytest.fixture
def mock_df_empty():
    """Mock empty DataFrame."""
    df = MagicMock()
    df.columns = ["customer_id"]
    empty_row = _Row({"last_updated": None})
    df.select.return_value.collect.return_value = [empty_row]
    return df


# ---------------------------------------------------------------------------
# Test FreshnessResult
# ---------------------------------------------------------------------------

class TestFreshnessResult:
    def test_creation(self):
        result = FreshnessResult(
            check_name="freshness",
            status="PASS",
            details="Data is fresh",
            last_updated="2026-08-05T00:00:00",
            age_hours=1.5,
            sla_hours=24,
        )
        assert result.check_name == "freshness"
        assert result.status == "PASS"
        assert result.age_hours == 1.5
        assert result.sla_hours == 24


# ---------------------------------------------------------------------------
# Test FreshnessChecker — check_freshness
# ---------------------------------------------------------------------------

class TestCheckFreshness:
    @patch("governance.freshness_checks.F")
    def test_fresh_data(self, mock_F, checker, mock_spark, mock_df_fresh):
        """Fresh data within SLA should PASS."""
        mock_spark.table.return_value = mock_df_fresh
        mock_F.max.return_value.alias.return_value = "max_col"

        result = checker.check_freshness(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            sla_hours=24,
            date_column="_ingested_at",
        )
        assert result.status == "PASS"
        assert result.age_hours is not None
        assert result.age_hours < 24

    @patch("governance.freshness_checks.F")
    def test_stale_data(self, mock_F, checker, mock_spark, mock_df_stale):
        """Stale data beyond SLA should FAIL."""
        mock_spark.table.return_value = mock_df_stale
        mock_F.max.return_value.alias.return_value = "max_col"

        result = checker.check_freshness(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            sla_hours=24,
            date_column="_ingested_at",
        )
        assert result.status == "FAIL"
        assert result.age_hours is not None
        assert result.age_hours > 24

    def test_missing_date_column(self, checker, mock_spark):
        """Missing date column should WARN."""
        df = MagicMock()
        df.columns = ["customer_id", "full_name"]  # No _ingested_at
        mock_spark.table.return_value = df

        result = checker.check_freshness(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            sla_hours=24,
            date_column="_ingested_at",
        )
        assert result.status == "WARN"
        assert "not found" in result.details

    def test_table_not_found(self, checker, mock_spark):
        """Table not found should FAIL."""
        mock_spark.table.side_effect = Exception("Table not found")

        result = checker.check_freshness(
            spark=mock_spark,
            table="lakehouse.silver.nonexistent",
            sla_hours=24,
        )
        assert result.status == "FAIL"
        assert "Could not read table" in result.details or "Error" in result.details

    def test_empty_table(self, checker, mock_spark, mock_df_empty):
        """Empty table should FAIL or WARN."""
        mock_spark.table.return_value = mock_df_empty

        result = checker.check_freshness(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            sla_hours=24,
            date_column="_ingested_at",
        )
        # Empty table returns None for max timestamp, which triggers "No data" FAIL
        assert result.status in ["FAIL", "WARN"]


# ---------------------------------------------------------------------------
# Test FreshnessChecker — check_partition_freshness
# ---------------------------------------------------------------------------

class TestCheckPartitionFreshness:
    @patch("governance.freshness_checks.F")
    def test_fresh_partition(self, mock_F, checker, mock_spark):
        """Fresh partition should PASS."""
        df = MagicMock()
        df.columns = ["customer_id", "cob_dt"]

        from datetime import date
        recent_row = _Row({"latest": date.today()})
        df.select.return_value.collect.return_value = [recent_row]
        mock_spark.table.return_value = df
        mock_F.max.return_value.alias.return_value = "latest"

        result = checker.check_partition_freshness(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            partition_column="cob_dt",
            sla_days=1,
        )
        assert result.status == "PASS"

    @patch("governance.freshness_checks.F")
    def test_stale_partition(self, mock_F, checker, mock_spark):
        """Stale partition should FAIL."""
        df = MagicMock()
        df.columns = ["customer_id", "cob_dt"]

        from datetime import date
        old_row = _Row({"latest": date(2020, 1, 1)})
        df.select.return_value.collect.return_value = [old_row]
        mock_spark.table.return_value = df
        mock_F.max.return_value.alias.return_value = "latest"

        result = checker.check_partition_freshness(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            partition_column="cob_dt",
            sla_days=1,
        )
        assert result.status == "FAIL"
