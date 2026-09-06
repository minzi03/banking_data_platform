"""
Integration tests for data quality validation.

These tests verify data quality constraints:
- Row count thresholds
- Null checks on critical columns
- Unique constraints
- Referential integrity
- Data freshness

Requires Docker stack running (PostgreSQL, MinIO, Spark, Trino).
"""

import subprocess

import pytest

# ---------------------------------------------------------------------------
# Helper: Run Trino query
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

    lines = [line.strip().strip('"') for line in result.stdout.strip().split("\n") if line.strip()]
    return lines


def get_null_count(table: str, column: str, schema: str = "bronze") -> int:
    """Get count of NULL values in a column."""
    result = run_trino_query(
        f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL",
        schema=schema
    )
    return int(result[0]) if result else 0


def get_distinct_count(table: str, column: str, schema: str = "bronze") -> int:
    """Get count of distinct values in a column."""
    result = run_trino_query(
        f"SELECT COUNT(DISTINCT {column}) FROM {table}",
        schema=schema
    )
    return int(result[0]) if result else 0


def get_row_count(table: str, schema: str = "bronze") -> int:
    """Get row count for a table."""
    result = run_trino_query(f"SELECT COUNT(*) FROM {table}", schema=schema)
    return int(result[0]) if result else 0


def assert_unique_grain(table: str, key: str, schema: str = "bronze") -> None:
    """
    Khẳng định `key` là duy nhất TRONG MỖI cob_dt của một bảng full-snapshot.

    Tìm thẳng nhóm bị trùng thay vì so hai con số đếm: câu lỗi nói được đúng
    key nào và ở snapshot nào, thay vì chỉ "N rows, M distinct". Cũng tránh
    COUNT(DISTINCT (a, b)) — cú pháp composite distinct phụ thuộc dialect.
    """
    duplicates = run_trino_query(
        f"""
        SELECT {key}, cob_dt, COUNT(*) AS n
        FROM {table}
        GROUP BY {key}, cob_dt
        HAVING COUNT(*) > 1
        ORDER BY n DESC
        LIMIT 5
        """,
        schema=schema,
    )
    assert not duplicates, (
        f"{schema}.{table}: ({key}, cob_dt) không duy nhất — "
        f"{len(duplicates)} nhóm trùng (5 đầu): {duplicates}"
    )


# ---------------------------------------------------------------------------
# Row Count Tests
# ---------------------------------------------------------------------------

class TestRowCounts:
    """Test minimum row count thresholds."""

    @pytest.mark.integration
    def test_bronze_core_customer_min_rows(self):
        """core_customer should have at least 1000 rows."""
        count = get_row_count("core_customer", schema="bronze")
        assert count >= 1000, f"core_customer has {count} rows, expected >= 1000"

    @pytest.mark.integration
    def test_bronze_core_account_min_rows(self):
        """core_account should have at least 1000 rows."""
        count = get_row_count("core_account", schema="bronze")
        assert count >= 1000, f"core_account has {count} rows, expected >= 1000"

    @pytest.mark.integration
    def test_bronze_core_branch_min_rows(self):
        """core_branch should have at least 10 rows."""
        count = get_row_count("core_branch", schema="bronze")
        assert count >= 10, f"core_branch has {count} rows, expected >= 10"

    @pytest.mark.integration
    def test_silver_dim_customer_min_rows(self):
        """dim_customer should have at least 500 rows."""
        count = get_row_count("dim_customer", schema="silver")
        assert count >= 500, f"dim_customer has {count} rows, expected >= 500"

    @pytest.mark.integration
    def test_gold_mart_customer_360_min_rows(self):
        """mart_customer_360 should have at least 500 rows."""
        count = get_row_count("mart_customer_360", schema="gold")
        assert count >= 500, f"mart_customer_360 has {count} rows, expected >= 500"


# ---------------------------------------------------------------------------
# Null Check Tests
# ---------------------------------------------------------------------------

class TestNullChecks:
    """Test that critical columns have no NULLs."""

    @pytest.mark.integration
    def test_bronze_customer_id_not_null(self):
        """core_customer.customer_id should have no NULLs."""
        null_count = get_null_count("core_customer", "customer_id", schema="bronze")
        assert null_count == 0, f"core_customer.customer_id has {null_count} NULLs"

    @pytest.mark.integration
    def test_bronze_account_id_not_null(self):
        """core_account.account_id should have no NULLs."""
        null_count = get_null_count("core_account", "account_id", schema="bronze")
        assert null_count == 0, f"core_account.account_id has {null_count} NULLs"

    @pytest.mark.integration
    def test_bronze_branch_code_not_null(self):
        """core_branch.branch_code should have no NULLs."""
        null_count = get_null_count("core_branch", "branch_code", schema="bronze")
        assert null_count == 0, f"core_branch.branch_code has {null_count} NULLs"

    @pytest.mark.integration
    def test_silver_customer_sk_not_null(self):
        """dim_customer.customer_sk should have no NULLs."""
        null_count = get_null_count("dim_customer", "customer_sk", schema="silver")
        assert null_count == 0, f"dim_customer.customer_sk has {null_count} NULLs"

    @pytest.mark.integration
    def test_gold_customer_id_not_null(self):
        """mart_customer_360.customer_id should have no NULLs."""
        null_count = get_null_count("mart_customer_360", "customer_id", schema="gold")
        assert null_count == 0, f"mart_customer_360.customer_id has {null_count} NULLs"


# ---------------------------------------------------------------------------
# Unique Constraint Tests
# ---------------------------------------------------------------------------

class TestUniqueConstraints:
    """Test that key columns have unique values."""

    # Bronze là FULL SNAPSHOT theo cob_dt: cùng một business key xuất hiện lại
    # ở mỗi ngày là ĐÚNG. Ba test này trước đây so COUNT(*) toàn bảng với
    # COUNT(DISTINCT key) toàn bảng, tức mã hoá giả định "Bronze chỉ có một
    # snapshot" — sai với kiến trúc. Chúng pass ở baseline chỉ vì fixture khi
    # đó đúng một cob_dt, và sẽ biến thành false failure ngay khi có snapshot
    # thứ hai. Invariant thật là: một key tối đa một lần TRONG MỖI snapshot.

    @pytest.mark.integration
    def test_bronze_customer_id_unique_within_each_snapshot(self):
        """core_customer.customer_id unique trong từng cob_dt."""
        assert_unique_grain("core_customer", "customer_id")

    @pytest.mark.integration
    def test_bronze_account_id_unique_within_each_snapshot(self):
        """core_account.account_id unique trong từng cob_dt."""
        assert_unique_grain("core_account", "account_id")

    @pytest.mark.integration
    def test_bronze_branch_code_unique_within_each_snapshot(self):
        """core_branch.branch_code unique trong từng cob_dt."""
        assert_unique_grain("core_branch", "branch_code")

    @pytest.mark.integration
    def test_silver_customer_sk_unique(self):
        """dim_customer.customer_sk should be unique."""
        count = get_row_count("dim_customer", schema="silver")
        distinct = get_distinct_count("dim_customer", "customer_sk", schema="silver")
        assert count == distinct, \
            f"dim_customer.customer_sk has duplicates: {count} rows, {distinct} distinct"


# ---------------------------------------------------------------------------
# Referential Integrity Tests
# ---------------------------------------------------------------------------

class TestReferentialIntegrity:
    """Test referential integrity between tables."""

    @pytest.mark.integration
    def test_account_customer_referential_integrity(self):
        """All account customer_ids should exist in core_customer."""
        result = run_trino_query("""
            SELECT COUNT(*)
            FROM iceberg.bronze.core_account a
            LEFT JOIN iceberg.bronze.core_customer c
              ON a.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """, schema="bronze")
        orphan_count = int(result[0]) if result else 0
        assert orphan_count == 0, f"Found {orphan_count} accounts with invalid customer_id"

    @pytest.mark.integration
    def test_silver_dim_branchReferential_integrity(self):
        """Silver dim_branch branch_codes should exist in Bronze."""
        result = run_trino_query("""
            SELECT COUNT(*)
            FROM iceberg.silver.dim_branch s
            LEFT JOIN iceberg.bronze.core_branch b
              ON s.branch_code = b.branch_code
            WHERE b.branch_code IS NULL
        """, schema="silver")
        orphan_count = int(result[0]) if result else 0
        assert orphan_count == 0, f"Found {orphan_count} silver branches not in bronze"


# ---------------------------------------------------------------------------
# Data Freshness Tests
# ---------------------------------------------------------------------------

class TestDataFreshness:
    """Test that data is recent (has current cob_dt)."""

    @pytest.mark.integration
    def test_bronze_has_recent_data(self):
        """Bronze tables should have data with recent cob_dt."""
        result = run_trino_query(
            "SELECT MAX(cob_dt) FROM core_customer",
            schema="bronze"
        )
        max_cob_dt = result[0] if result else None
        assert max_cob_dt is not None, "core_customer has no data"
        # Just check it's not NULL - actual date validation depends on data generation
        assert len(max_cob_dt) > 0, "core_customer.cob_dt is empty"

    @pytest.mark.integration
    def test_silver_has_current_records(self):
        """Silver SCD2 tables should have current records."""
        result = run_trino_query(
            "SELECT COUNT(*) FROM dim_customer WHERE is_current = 1",
            schema="silver"
        )
        current_count = int(result[0]) if result else 0
        assert current_count > 0, "dim_customer has no current records"
