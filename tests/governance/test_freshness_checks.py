"""
Tests for governance.freshness_checks — SLA-based data freshness monitoring.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

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


# ---------------------------------------------------------------------------
# Cột ngày (DATE) — trước đây luôn PASS mà không đánh giá gì
# ---------------------------------------------------------------------------


def _df_with_latest(value):
    df = MagicMock()
    df.columns = ["customer_id", "cob_dt"]
    df.select.return_value.collect.return_value = [_Row({"last_updated": value})]
    return df


def _today_in_vietnam():
    from governance.freshness_checks import BUSINESS_TZ

    return datetime.now(BUSINESS_TZ).date()


class TestFreshnessOnDateColumns:
    """
    19 contract Gold khai `date_column: cob_dt`, kiểu DATE. Nhánh cũ chỉ tính tuổi
    cho `datetime`; mọi kiểu khác rơi vào `status="PASS"` với "Latest record: ...".
    Nên freshness của cả 19 chưa từng được đánh giá, và luôn báo xanh.
    """

    def test_business_day_ends_at_midnight_vietnam_time(self):
        from datetime import date

        from governance.freshness_checks import _reference_instant

        instant, label = _reference_instant(date(2026, 9, 22))
        assert instant == datetime(2026, 9, 22, 17, 0, tzinfo=timezone.utc)
        assert "2026-09-22" in label

    def test_naive_datetime_is_still_read_as_utc(self):
        from governance.freshness_checks import _reference_instant

        instant, _ = _reference_instant(datetime(2026, 9, 22, 8, 0))
        assert instant == datetime(2026, 9, 22, 8, 0, tzinfo=timezone.utc)

    @patch("governance.freshness_checks.F")
    def test_yesterdays_snapshot_is_fresh(self, mock_F, checker, mock_spark):
        """Lượt chạy hằng ngày thấy snapshot hôm qua: ngày đó mới khép < 24h trước."""
        mock_spark.table.return_value = _df_with_latest(_today_in_vietnam() - timedelta(days=1))
        result = checker.check_freshness(mock_spark, "lakehouse.gold.t", sla_hours=24, date_column="cob_dt")
        assert result.status == "PASS"
        assert result.age_hours is not None and 0 <= result.age_hours < 24

    @patch("governance.freshness_checks.F")
    def test_snapshot_three_days_old_is_stale(self, mock_F, checker, mock_spark):
        """Đây là ca mà bản cũ báo PASS."""
        mock_spark.table.return_value = _df_with_latest(_today_in_vietnam() - timedelta(days=3))
        result = checker.check_freshness(mock_spark, "lakehouse.gold.t", sla_hours=24, date_column="cob_dt")
        assert result.status == "FAIL"
        assert result.age_hours is not None and result.age_hours >= 48

    @patch("governance.freshness_checks.F")
    def test_unknown_type_is_not_reported_as_pass(self, mock_F, checker, mock_spark):
        """Không tính được tuổi thì nói là không tính được — WARN, không phải PASS."""
        mock_spark.table.return_value = _df_with_latest("2026-09-22")
        result = checker.check_freshness(mock_spark, "lakehouse.gold.t", sla_hours=24, date_column="cob_dt")
        assert result.status == "WARN"
        assert "str" in result.details
