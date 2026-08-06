"""
Tests for governance.schema_drift — Schema drift detection.
"""

import pytest
from unittest.mock import MagicMock
from governance.schema_drift import SchemaDriftDetector, SchemaDriftResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def detector():
    return SchemaDriftDetector()


@pytest.fixture
def mock_spark():
    """Create a mock SparkSession."""
    spark = MagicMock()
    spark._sc._jvm = MagicMock()
    return spark


@pytest.fixture
def mock_df_matching():
    """Mock DataFrame with matching schema."""
    df = MagicMock()
    df.columns = ["customer_id", "full_name", "phone", "email"]
    df.dtypes = [
        ("customer_id", "bigint"),
        ("full_name", "string"),
        ("phone", "string"),
        ("email", "string"),
    ]
    return df


@pytest.fixture
def mock_df_extra_column():
    """Mock DataFrame with extra column (drift)."""
    df = MagicMock()
    df.columns = ["customer_id", "full_name", "phone", "email", "new_column"]
    df.dtypes = [
        ("customer_id", "bigint"),
        ("full_name", "string"),
        ("phone", "string"),
        ("email", "string"),
        ("new_column", "string"),
    ]
    return df


@pytest.fixture
def mock_df_missing_column():
    """Mock DataFrame with missing column (drift)."""
    df = MagicMock()
    df.columns = ["customer_id", "full_name"]  # Missing phone, email
    df.dtypes = [
        ("customer_id", "bigint"),
        ("full_name", "string"),
    ]
    return df


# ---------------------------------------------------------------------------
# Test SchemaDriftResult
# ---------------------------------------------------------------------------

class TestSchemaDriftResult:
    def test_creation(self):
        result = SchemaDriftResult(
            check_name="schema_drift",
            status="PASS",
            details="Schema matches",
            added_columns=[],
            removed_columns=[],
            type_changes=[],
            current_columns=["a", "b"],
            expected_columns=["a", "b"],
        )
        assert result.status == "PASS"
        assert result.added_columns == []
        assert result.removed_columns == []

    def test_with_drift(self):
        result = SchemaDriftResult(
            check_name="schema_drift",
            status="WARN",
            details="Added columns",
            added_columns=["new_col"],
            removed_columns=[],
            type_changes=[],
            current_columns=["a", "b", "new_col"],
            expected_columns=["a", "b"],
        )
        assert result.status == "WARN"
        assert "new_col" in result.added_columns


# ---------------------------------------------------------------------------
# Test SchemaDriftDetector
# ---------------------------------------------------------------------------

class TestSchemaDriftDetector:
    def test_no_drift(self, detector, mock_spark, mock_df_matching):
        """Matching schema should PASS."""
        mock_spark.table.return_value = mock_df_matching

        result = detector.detect_drift(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            expected_columns=["customer_id", "full_name", "phone", "email"],
        )
        assert result.status == "PASS"
        assert len(result.added_columns) == 0
        assert len(result.removed_columns) == 0

    def test_added_columns(self, detector, mock_spark, mock_df_extra_column):
        """Extra columns should WARN."""
        mock_spark.table.return_value = mock_df_extra_column

        result = detector.detect_drift(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            expected_columns=["customer_id", "full_name", "phone", "email"],
        )
        assert result.status == "WARN"
        assert "new_column" in result.added_columns

    def test_removed_columns(self, detector, mock_spark, mock_df_missing_column):
        """Missing columns should FAIL."""
        mock_spark.table.return_value = mock_df_missing_column

        result = detector.detect_drift(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            expected_columns=["customer_id", "full_name", "phone", "email"],
        )
        assert result.status == "FAIL"
        assert "phone" in result.removed_columns
        assert "email" in result.removed_columns

    def test_ignore_case(self, detector, mock_spark):
        """Case-insensitive comparison should work."""
        df = MagicMock()
        df.columns = ["Customer_ID", "Full_Name"]
        df.dtypes = [("Customer_ID", "bigint"), ("Full_Name", "string")]
        mock_spark.table.return_value = df

        result = detector.detect_drift(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            expected_columns=["customer_id", "full_name"],
            ignore_case=True,
        )
        assert result.status == "PASS"

    def test_table_not_found(self, detector, mock_spark):
        """Table not found should FAIL."""
        mock_spark.table.side_effect = Exception("Table not found")

        result = detector.detect_drift(
            spark=mock_spark,
            table="lakehouse.silver.nonexistent",
            expected_columns=["customer_id"],
        )
        assert result.status == "FAIL"

    def test_get_schema_diff(self, detector, mock_spark, mock_df_extra_column):
        """get_schema_diff should return added/removed/common sets."""
        mock_spark.table.return_value = mock_df_extra_column

        diff = detector.get_schema_diff(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            expected_columns=["customer_id", "full_name", "phone", "email"],
        )
        assert "new_column" in diff["added"]
        assert "customer_id" in diff["common"]

    def test_empty_expected_columns(self, detector, mock_spark, mock_df_matching):
        """Empty expected columns should WARN or show added columns."""
        mock_spark.table.return_value = mock_df_matching

        result = detector.detect_drift(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            expected_columns=[],
        )
        # Empty expected means all current columns are "added"
        assert result.status in ["WARN", "PASS"]
        assert len(result.added_columns) > 0 or len(result.expected_columns) == 0

    def test_type_changes(self, detector, mock_spark):
        """Type changes should FAIL."""
        df = MagicMock()
        df.columns = ["customer_id", "balance"]
        df.dtypes = [
            ("customer_id", "bigint"),
            ("balance", "string"),  # Expected double
        ]
        mock_spark.table.return_value = df

        result = detector.detect_drift(
            spark=mock_spark,
            table="lakehouse.silver.dim_customer",
            expected_columns=["customer_id", "balance"],
            expected_types={"balance": "double"},
        )
        assert result.status == "FAIL"
        assert len(result.type_changes) > 0
