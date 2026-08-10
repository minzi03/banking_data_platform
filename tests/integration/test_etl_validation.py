"""
Integration tests for ETL pipeline validation.

These tests verify the end-to-end ETL flow:
- Bronze: Data ingestion from PostgreSQL
- Silver: SCD Type 1/2 transformations
- Gold: Mart aggregations

Requires Docker stack running (PostgreSQL, MinIO, Spark, Trino).
"""

import subprocess

import pytest

# ---------------------------------------------------------------------------
# Helper: Run Trino query and return result
# ---------------------------------------------------------------------------

def run_trino_query(query: str, catalog: str = "iceberg", schema: str = "bronze") -> list:
    """Execute a Trino query and return results as list of tuples."""
    cmd = [
        "docker", "exec", "ci-trino",
        "trino", f"--catalog={catalog}", f"--schema={schema}",
        f"--execute={query}"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise RuntimeError(f"Trino query failed: {result.stderr}")

    # Parse output (each line is a quoted value)
    lines = [line.strip().strip('"') for line in result.stdout.strip().split("\n") if line.strip()]
    return lines


def get_row_count(table: str, catalog: str = "iceberg", schema: str = "bronze") -> int:
    """Get row count for a table."""
    result = run_trino_query(f"SELECT COUNT(*) FROM {table}", catalog, schema)
    return int(result[0]) if result else 0


# ---------------------------------------------------------------------------
# Bronze Layer Tests
# ---------------------------------------------------------------------------

class TestBronzeLayer:
    """Test Bronze layer data ingestion."""

    @pytest.mark.integration
    def test_bronze_core_customer_exists(self):
        """Bronze core_customer table should exist and have data."""
        count = get_row_count("core_customer", schema="bronze")
        assert count > 0, "core_customer table is empty"

    @pytest.mark.integration
    def test_bronze_core_account_exists(self):
        """Bronze core_account table should exist and have data."""
        count = get_row_count("core_account", schema="bronze")
        assert count > 0, "core_account table is empty"

    @pytest.mark.integration
    def test_bronze_core_branch_exists(self):
        """Bronze core_branch table should exist and have data."""
        count = get_row_count("core_branch", schema="bronze")
        assert count > 0, "core_branch table is empty"

    @pytest.mark.integration
    def test_bronze_core_product_exists(self):
        """Bronze core_product table should exist and have data."""
        count = get_row_count("core_product", schema="bronze")
        assert count > 0, "core_product table is empty"

    @pytest.mark.integration
    def test_bronze_row_counts_reasonable(self):
        """Bronze tables should have reasonable row counts."""
        tables = {
            "core_customer": 1000,  # At least 1000 customers
            "core_account": 1000,   # At least 1000 accounts
            "core_branch": 10,      # At least 10 branches
            "core_product": 5,      # At least 5 products
        }

        for table, min_count in tables.items():
            count = get_row_count(table, schema="bronze")
            assert count >= min_count, f"{table} has {count} rows, expected >= {min_count}"


# ---------------------------------------------------------------------------
# Silver Layer Tests
# ---------------------------------------------------------------------------

class TestSilverLayer:
    """Test Silver layer transformations."""

    @pytest.mark.integration
    def test_silver_dim_branch_exists(self):
        """Silver dim_branch table should exist and have data."""
        count = get_row_count("dim_branch", schema="silver")
        assert count > 0, "dim_branch table is empty"

    @pytest.mark.integration
    def test_silver_dim_product_exists(self):
        """Silver dim_product table should exist and have data."""
        count = get_row_count("dim_product", schema="silver")
        assert count > 0, "dim_product table is empty"

    @pytest.mark.integration
    def test_silver_scd_type2_has_current_records(self):
        """Silver SCD Type 2 tables should have current records."""
        # Check dim_customer has is_current flag
        result = run_trino_query(
            "SELECT COUNT(*) FROM dim_customer WHERE is_current = 1",
            schema="silver"
        )
        current_count = int(result[0]) if result else 0
        assert current_count > 0, "dim_customer has no current records"

    @pytest.mark.integration
    def test_silver_scd_type2_has_history(self):
        """Silver SCD Type 2 tables should have historical records."""
        # Check dim_customer has both current and historical
        result = run_trino_query(
            "SELECT is_current, COUNT(*) FROM dim_customer GROUP BY is_current",
            schema="silver"
        )
        # Should have at least 2 rows (current=0 and current=1)
        assert len(result) >= 2, "dim_customer doesn't have both current and historical records"


# ---------------------------------------------------------------------------
# Gold Layer Tests
# ---------------------------------------------------------------------------

class TestGoldLayer:
    """Test Gold layer mart aggregations."""

    @pytest.mark.integration
    def test_gold_mart_customer_360_exists(self):
        """Gold mart_customer_360 table should exist and have data."""
        count = get_row_count("mart_customer_360", schema="gold")
        assert count > 0, "mart_customer_360 table is empty"

    @pytest.mark.integration
    def test_gold_rfm_segment_exists(self):
        """Gold rfm_segment table should exist and have data."""
        count = get_row_count("rfm_segment", schema="gold")
        assert count > 0, "rfm_segment table is empty"

    @pytest.mark.integration
    def test_gold_churn_prediction_exists(self):
        """Gold churn_prediction table should exist and have data."""
        count = get_row_count("churn_prediction", schema="gold")
        assert count > 0, "churn_prediction table is empty"

    @pytest.mark.integration
    def test_gold_customer_360_has_all_columns(self):
        """Gold mart_customer_360 should have expected columns."""
        result = run_trino_query(
            "SHOW COLUMNS FROM mart_customer_360",
            schema="gold"
        )
        # Should have multiple columns
        assert len(result) > 10, f"mart_customer_360 has only {len(result)} columns"

    @pytest.mark.integration
    def test_gold_rfm_segments_valid(self):
        """Gold rfm_segment should have valid segment values."""
        result = run_trino_query(
            "SELECT DISTINCT rfm_segment FROM rfm_segment ORDER BY rfm_segment",
            schema="gold"
        )
        valid_segments = {"Champions", "Loyal Customers", "Potential Loyalists",
                         "At Risk", "New Customers", "Hibernating"}

        actual_segments = set(result)
        # At least some valid segments should exist
        assert len(actual_segments.intersection(valid_segments)) > 0, \
            f"No valid RFM segments found. Got: {actual_segments}"


# ---------------------------------------------------------------------------
# Cross-Layer Tests
# ---------------------------------------------------------------------------

class TestCrossLayer:
    """Test cross-layer data consistency."""

    @pytest.mark.integration
    def test_bronze_to_silver_customer_count(self):
        """Silver should have fewer or equal customers than Bronze (after dedup)."""
        bronze_count = get_row_count("core_customer", schema="bronze")
        silver_count = get_row_count("dim_customer", schema="silver")  # noqa: F841

        # Silver current records should be <= Bronze (after SCD2)
        result = run_trino_query(
            "SELECT COUNT(*) FROM dim_customer WHERE is_current = 1",
            schema="silver"
        )
        silver_current = int(result[0]) if result else 0

        assert silver_current <= bronze_count, \
            f"Silver current ({silver_current}) > Bronze ({bronze_count})"

    @pytest.mark.integration
    def test_gold_customer_count_matches_silver(self):
        """Gold mart_customer_360 should match Silver current customers."""
        silver_result = run_trino_query(
            "SELECT COUNT(*) FROM dim_customer WHERE is_current = 1",
            schema="silver"
        )
        silver_count = int(silver_result[0]) if silver_result else 0

        gold_count = get_row_count("mart_customer_360", schema="gold")

        # Gold should have similar count to Silver current
        # Allow 10% difference due to filtering
        assert abs(gold_count - silver_count) / max(silver_count, 1) < 0.1, \
            f"Gold ({gold_count}) differs significantly from Silver ({silver_count})"
