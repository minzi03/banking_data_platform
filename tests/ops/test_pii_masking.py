"""
Tests for code_etl/shared/ops/pii_masking.py

Covers:
  - _mask_name_udf: SQL UDF generation
  - parse_arguments: CLI arg parsing
  - Salt initialization

Note: Actual PII masking requires Spark SQL execution, so we test
the helper functions and argument parsing here.
"""

import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class TestPiiSaltInit:
    """Tests for PII salt initialization."""

    def test_salt_set_from_env(self, monkeypatch):
        """Should read PII_HASH_SALT from environment."""
        monkeypatch.setenv("PII_HASH_SALT", "TestSalt123")
        # Mock pyspark and import
        sys.modules["pyspark"] = MagicMock()
        sys.modules["pyspark.sql"] = MagicMock()
        sys.modules["spark"] = MagicMock()
        sys.modules["spark.spark_session"] = MagicMock()
        spec = importlib.util.spec_from_file_location(
            "pii_masking_test",
            str(PROJECT_ROOT / "code_etl" / "shared" / "ops" / "pii_masking.py")
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.PII_SALT == "TestSalt123"

    def test_salt_missing_raises(self, monkeypatch):
        """Should raise EnvironmentError when PII_HASH_SALT not set."""
        monkeypatch.delenv("PII_HASH_SALT", raising=False)
        sys.modules["pyspark"] = MagicMock()
        sys.modules["pyspark.sql"] = MagicMock()
        sys.modules["spark"] = MagicMock()
        sys.modules["spark.spark_session"] = MagicMock()
        with pytest.raises(EnvironmentError, match="PII_HASH_SALT"):
            spec = importlib.util.spec_from_file_location(
                "pii_masking_test2",
                str(PROJECT_ROOT / "code_etl" / "shared" / "ops" / "pii_masking.py")
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)


class TestMaskNameUdf:
    """Tests for the name masking SQL UDF generator."""

    def _load_module(self, salt="test"):
        """Helper to load pii_masking module with given salt."""
        with patch.dict(os.environ, {"PII_HASH_SALT": salt}):
            sys.modules["pyspark"] = MagicMock()
            sys.modules["pyspark.sql"] = MagicMock()
            sys.modules["spark"] = MagicMock()
            sys.modules["spark.spark_session"] = MagicMock()
            spec = importlib.util.spec_from_file_location(
                f"pii_masking_{salt}",
                str(PROJECT_ROOT / "code_etl" / "shared" / "ops" / "pii_masking.py")
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod

    def test_returns_sql_string(self):
        """Should return a SQL expression string."""
        mod = self._load_module()
        sql = mod._mask_name_udf()
        assert isinstance(sql, str)
        assert "CASE" in sql
        assert "full_name" in sql

    def test_udf_handles_three_part_name(self):
        """Should handle Vietnamese 3-part names."""
        mod = self._load_module()
        sql = mod._mask_name_udf()
        assert "size(split(full_name, ' ')) >= 3" in sql

    def test_udf_handles_two_part_name(self):
        """Should handle 2-part names."""
        mod = self._load_module()
        sql = mod._mask_name_udf()
        assert "size(split(full_name, ' ')) = 2" in sql

    def test_udf_handles_single_name(self):
        """Should handle single-word names."""
        mod = self._load_module()
        sql = mod._mask_name_udf()
        assert "ELSE" in sql


class TestParseArguments:
    """Tests for CLI argument parsing."""

    def _load_module(self, salt="test"):
        """Helper to load pii_masking module."""
        with patch.dict(os.environ, {"PII_HASH_SALT": salt}):
            sys.modules["pyspark"] = MagicMock()
            sys.modules["pyspark.sql"] = MagicMock()
            sys.modules["spark"] = MagicMock()
            sys.modules["spark.spark_session"] = MagicMock()
            spec = importlib.util.spec_from_file_location(
                f"pii_masking_args_{salt}",
                str(PROJECT_ROOT / "code_etl" / "shared" / "ops" / "pii_masking.py")
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod

    def test_required_args(self):
        """Should parse cob_dt argument."""
        mod = self._load_module()
        with patch("sys.argv", ["pii_masking.py", "--cob_dt", "2025-01-15"]):
            args = mod.parse_arguments()
            assert args.cob_dt == "2025-01-15"

    def test_target_choices(self):
        """Should accept valid target choices."""
        mod = self._load_module()
        for target in ["dim_customer", "mart_360", "all"]:
            with patch("sys.argv", ["pii_masking.py", "--cob_dt", "2025-01-15", "--target", target]):
                args = mod.parse_arguments()
                assert args.target == target
