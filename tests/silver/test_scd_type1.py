"""
Tests for code_etl/silver/base_job/scd_type1.py

Covers:
  - validate_config: required fields, job type validation
  - get_target_table: table name assembly (from common_utils)

Note: validate_config is a pure function — we import it directly.
get_target_table comes from common_utils which has no heavy deps.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ETL_ROOT = PROJECT_ROOT / "code_etl"

# Import validate_config directly from source (it has no external deps)
# Read and exec just the validate_config function
_spec_src = (ETL_ROOT / "silver" / "base_job" / "scd_type1.py").read_text(encoding="utf-8")

# Extract validate_config source code (lines 21-30)
import textwrap, ast

# Parse the full module to extract just validate_config
tree = ast.parse(_spec_src)
validate_config_code = ""
for node in ast.walk(tree):
    if isinstance(node, ast.FunctionDef) and node.name == "validate_config":
        # Get the source lines for this function
        start = node.lineno - 1
        end = node.end_lineno
        lines = _spec_src.splitlines()[start:end]
        validate_config_code = "\n".join(lines)
        break

# Execute validate_config in a clean namespace
_ns = {}
exec(validate_config_code, _ns)
validate_config = _ns["validate_config"]


# Import get_target_table from common_utils via importlib
# common_utils imports utils.sql_renderer which needs mocking
sys.modules["utils"] = MagicMock()
sys.modules["utils.yaml_loader"] = MagicMock()
sys.modules["utils.logger"] = MagicMock()
sys.modules["utils.sql_renderer"] = MagicMock()

_spec_common = importlib.util.spec_from_file_location(
    "common_utils_silver",
    str(ETL_ROOT / "silver" / "base_job" / "common_utils.py")
)
_common_mod = importlib.util.module_from_spec(_spec_common)
_spec_common.loader.exec_module(_common_mod)

get_target_table = _common_mod.get_target_table


class TestValidateConfig:
    """Tests for SCD Type 1 config validation."""

    def test_valid_config_passes(self):
        """Should not raise for valid config."""
        config = {
            "job": {"type": "scd_type1"},
            "source": {"catalog": "lakehouse", "schema": "bronze"},
            "target": {"catalog": "lakehouse", "schema": "silver", "table": "dim_branch"},
            "business_key": ["branch_code"],
            "sql": "SELECT * FROM t",
        }
        validate_config(config)  # Should not raise

    def test_missing_job_section(self):
        """Should raise ValueError when 'job' section is missing."""
        config = {
            "source": {},
            "target": {},
            "business_key": [],
            "sql": "SELECT 1",
        }
        with pytest.raises(ValueError, match="Thiếu section bắt buộc"):
            validate_config(config)

    def test_missing_source_section(self):
        """Should raise ValueError when 'source' section is missing."""
        config = {
            "job": {"type": "scd_type1"},
            "target": {},
            "business_key": [],
            "sql": "SELECT 1",
        }
        with pytest.raises(ValueError, match="Thiếu section bắt buộc"):
            validate_config(config)

    def test_missing_target_section(self):
        """Should raise ValueError when 'target' section is missing."""
        config = {
            "job": {"type": "scd_type1"},
            "source": {},
            "business_key": [],
            "sql": "SELECT 1",
        }
        with pytest.raises(ValueError, match="Thiếu section bắt buộc"):
            validate_config(config)

    def test_missing_business_key(self):
        """Should raise ValueError when 'business_key' is missing."""
        config = {
            "job": {"type": "scd_type1"},
            "source": {},
            "target": {},
            "sql": "SELECT 1",
        }
        with pytest.raises(ValueError, match="Thiếu section bắt buộc"):
            validate_config(config)

    def test_missing_sql(self):
        """Should raise ValueError when 'sql' is missing."""
        config = {
            "job": {"type": "scd_type1"},
            "source": {},
            "target": {},
            "business_key": [],
        }
        with pytest.raises(ValueError, match="Thiếu section bắt buộc"):
            validate_config(config)

    def test_wrong_job_type(self):
        """Should raise ValueError when job.type is not scd_type1."""
        config = {
            "job": {"type": "scd_type2"},
            "source": {},
            "target": {},
            "business_key": [],
            "sql": "SELECT 1",
        }
        with pytest.raises(ValueError, match="Sai loại job"):
            validate_config(config)


class TestGetTargetTable:
    """Tests for target table name assembly."""

    def test_bronze_table(self):
        """Should assemble Bronze table name."""
        config = {"target": {"catalog": "lakehouse", "schema": "bronze", "table": "core_account"}}
        result = get_target_table(config)
        assert result == "lakehouse.bronze.core_account"

    def test_silver_table(self):
        """Should assemble Silver table name."""
        config = {"target": {"catalog": "lakehouse", "schema": "silver", "table": "dim_customer"}}
        result = get_target_table(config)
        assert result == "lakehouse.silver.dim_customer"

    def test_gold_table(self):
        """Should assemble Gold table name."""
        config = {"target": {"catalog": "lakehouse", "schema": "gold", "table": "mart_customer_360"}}
        result = get_target_table(config)
        assert result == "lakehouse.gold.mart_customer_360"

    def test_custom_catalog(self):
        """Should handle custom catalog name."""
        config = {"target": {"catalog": "my_catalog", "schema": "my_schema", "table": "my_table"}}
        result = get_target_table(config)
        assert result == "my_catalog.my_schema.my_table"
