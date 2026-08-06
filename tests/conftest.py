"""
Shared pytest fixtures for Banking Data Platform tests.

Provides tmp YAML configs, mock Spark sessions, and sample data
so individual test files stay focused on assertions.
"""

import os
import sys
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Project root — so we can import code_etl modules
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ETL_ROOT = PROJECT_ROOT / "code_etl"
SHARED_ROOT = ETL_ROOT / "shared"
GOVERNANCE_ROOT = PROJECT_ROOT / "governance"

# Add shared/ to sys.path so `from utils.yaml_loader import ...` works
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

# Add code_etl/ to sys.path for cross-module imports
if str(ETL_ROOT) not in sys.path:
    sys.path.insert(0, str(ETL_ROOT))

# Add project root to sys.path so `import governance` resolves as package
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Also keep direct governance path available for local module access
if str(GOVERNANCE_ROOT) not in sys.path:
    sys.path.insert(0, str(GOVERNANCE_ROOT))


# ---------------------------------------------------------------------------
# Fixtures: Temporary YAML config files
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_bronze_config(tmp_path):
    """Create a minimal Bronze YAML config for testing."""
    config = textwrap.dedent("""\
        source:
          type: postgresql
          schema: core_banking
          fetchsize: 10000

        target:
          catalog: lakehouse
          schema: bronze
          table: core_account

        load:
          strategy: full_snapshot

        sql: |
          SELECT account_id, account_no, customer_id, balance
          FROM core_banking.account
    """)
    config_file = tmp_path / "account.yml"
    config_file.write_text(config, encoding="utf-8")
    return str(config_file)


@pytest.fixture
def sample_silver_scd1_config(tmp_path):
    """Create a minimal Silver SCD Type 1 YAML config for testing."""
    config = textwrap.dedent("""\
        job:
          type: scd_type1
          description: Silver dim_branch

        source:
          catalog: lakehouse
          schema: bronze
          table: core_branch

        target:
          catalog: lakehouse
          schema: silver
          table: dim_branch

        business_key:
          - branch_code

        sql: |
          SELECT branch_code, branch_name, region, city, is_active, cob_dt
          FROM lakehouse.bronze.core_branch
          WHERE cob_dt = DATE '{{ cob_dt }}'
    """)
    config_file = tmp_path / "dim_branch.yml"
    config_file.write_text(config, encoding="utf-8")
    return str(config_file)


@pytest.fixture
def sample_gold_config(tmp_path):
    """Create a minimal Gold mart YAML config for testing."""
    config = textwrap.dedent("""\
        job:
          type: mart360
          description: Customer 360

        target:
          catalog: lakehouse
          schema: gold
          table: mart_customer_360

        sql: |
          SELECT
              c.customer_id,
              c.full_name AS customer_name,
              c.customer_segment,
              c.branch_code AS primary_branch_code
          FROM lakehouse.silver.dim_customer c
          WHERE c.cob_dt = DATE '{{ cob_dt }}'
            AND c.is_current = true
    """)
    config_file = tmp_path / "customer_360.yml"
    config_file.write_text(config, encoding="utf-8")
    return str(config_file)


@pytest.fixture
def sample_cdc_config(tmp_path):
    """Create a minimal CDC YAML config for testing."""
    config = textwrap.dedent("""\
        kafka:
          topic: postgresql.banking.core_banking.account
          starting_offsets: earliest
          checkpoint_location: s3a://lakehouse/checkpoints/cdc/core_account
          trigger_interval: 30 seconds
          max_offsets_per_trigger: 100000

        target:
          catalog: lakehouse
          schema: bronze
          table: core_account_cdc
          columns:
            - name: account_id
              type: long
            - name: account_no
              type: string
            - name: balance
              type: decimal
    """)
    config_file = tmp_path / "cdc_core_account.yml"
    config_file.write_text(config, encoding="utf-8")
    return str(config_file)


@pytest.fixture
def sample_empty_yaml(tmp_path):
    """Create an empty YAML file for error testing."""
    config_file = tmp_path / "empty.yml"
    config_file.write_text("", encoding="utf-8")
    return str(config_file)


@pytest.fixture
def sample_dq_rules(tmp_path):
    """Create a minimal DQ rules YAML for testing."""
    config = textwrap.dedent("""\
        tables:
          lakehouse.silver.dim_customer:
            checks:
              - name: row_count
                severity: FAIL
                min_rows: 1
              - name: null_check
                severity: FAIL
                columns:
                  - customer_id
              - name: unique_check
                severity: FAIL
                columns:
                  - customer_id

          lakehouse.gold.mart_customer_360:
            checks:
              - name: row_count
                severity: FAIL
                min_rows: 1
              - name: range_check
                severity: WARN
                column: total_balance
                min_value: 0
    """)
    config_file = tmp_path / "dq_rules.yml"
    config_file.write_text(config, encoding="utf-8")
    return str(config_file)


# ---------------------------------------------------------------------------
# Fixtures: Mock Spark session
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_spark():
    """Create a mock SparkSession with common behaviors."""
    spark = MagicMock()
    spark.sparkContext = MagicMock()
    spark.sparkContext.getConf = MagicMock(return_value=MagicMock(get=MagicMock(return_value="client")))
    spark.sql = MagicMock()
    spark.table = MagicMock()
    spark.createDataFrame = MagicMock()
    spark.readStream = MagicMock()
    spark._sc = MagicMock()
    spark._sc._jvm = MagicMock()
    return spark


@pytest.fixture
def mock_spark_with_df(mock_spark):
    """Mock SparkSession that returns a mock DataFrame from spark.table()."""
    mock_df = MagicMock()
    mock_df.columns = ["customer_id", "full_name", "phone", "email", "cccd", "city", "district"]
    mock_df.count = MagicMock(return_value=100)
    mock_df.filter = MagicMock(return_value=mock_df)
    mock_df.select = MagicMock(return_value=mock_df)
    mock_df.distinct = MagicMock(return_value=mock_df)
    mock_df.createOrReplaceTempView = MagicMock()
    mock_df.writeTo = MagicMock()
    mock_df.isEmpty = MagicMock(return_value=False)
    mock_df.sparkSession = mock_spark

    mock_spark.table = MagicMock(return_value=mock_df)
    mock_spark.sql = MagicMock(return_value=mock_df)
    mock_spark.createDataFrame = MagicMock(return_value=mock_df)

    return mock_spark, mock_df


# ---------------------------------------------------------------------------
# Fixtures: Sample data
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_customer_data():
    """Sample customer DataFrame columns for PII masking tests."""
    return {
        "customer_id": [1, 2, 3],
        "full_name": ["Nguyễn Văn An", "Trần Thị Bình", "Lê"],
        "phone": ["0912345678", "0987654321", None],
        "email": ["an.nguyen@gmail.com", "binh.tran@yahoo.com", "le@bank.vn"],
        "cccd": ["001234567890", "009876543210", "001111111111"],
        "city": ["HCMC", "Hanoi", "HCMC"],
        "district": ["District 1", "Hoàn Kiếm", "District 3"],
    }
