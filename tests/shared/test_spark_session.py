"""
Tests for code_etl/shared/spark/spark_session.py

Covers:
  - get_spark_session: env var override, default behavior, appName setting
  - get_iceberg_table_name: table name concatenation

Uses mock to avoid requiring actual Spark/MinIO.
"""

import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Direct imports via importlib
_spec_spark = importlib.util.spec_from_file_location(
    "spark_session_mod",
    str(PROJECT_ROOT / "code_etl" / "shared" / "spark" / "spark_session.py")
)
_spark_mod = importlib.util.module_from_spec(_spec_spark)
_spec_spark.loader.exec_module(_spark_mod)

_spec_iceberg = importlib.util.spec_from_file_location(
    "iceberg_utils_mod",
    str(PROJECT_ROOT / "code_etl" / "shared" / "spark" / "iceberg_utils.py")
)
_iceberg_mod = importlib.util.module_from_spec(_spec_iceberg)
_spec_iceberg.loader.exec_module(_iceberg_mod)


class TestGetSparkSession:
    """Tests for SparkSession factory."""

    @patch.object(_spark_mod, "SparkSession")
    def test_creates_session_with_app_name(self, mock_spark_cls):
        """Should create SparkSession with the given app name."""
        mock_builder = MagicMock()
        mock_spark_cls.builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        spark = _spark_mod.get_spark_session("test_app")
        mock_spark_cls.builder.appName.assert_called_with("test_app")

    @patch.object(_spark_mod, "SparkSession")
    def test_default_app_name(self, mock_spark_cls):
        """Should use default app name when none provided."""
        mock_builder = MagicMock()
        mock_spark_cls.builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        _spark_mod.get_spark_session()
        mock_spark_cls.builder.appName.assert_called_with("banking-lakehouse-job")

    @patch.object(_spark_mod, "SparkSession")
    def test_env_vars_override_config(self, mock_spark_cls, monkeypatch):
        """When ICEBERG_CATALOG_URI is set, should configure Iceberg catalog."""
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://custom-catalog:8181")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3a://custom/warehouse")
        monkeypatch.setenv("MINIO_ENDPOINT", "http://custom-minio:9000")
        monkeypatch.setenv("MINIO_ACCESS_KEY", "custom_key")
        monkeypatch.setenv("MINIO_SECRET_KEY", "custom_secret")

        mock_builder = MagicMock()
        mock_spark_cls.builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        spark = _spark_mod.get_spark_session("test_env")

        calls = mock_builder.config.call_args_list
        config_keys = [call[0][0] for call in calls]
        assert "spark.sql.catalog.lakehouse.uri" in config_keys
        assert "spark.hadoop.fs.s3a.access.key" in config_keys

        monkeypatch.delenv("ICEBERG_CATALOG_URI")
        monkeypatch.delenv("ICEBERG_WAREHOUSE")
        monkeypatch.delenv("MINIO_ENDPOINT")
        monkeypatch.delenv("MINIO_ACCESS_KEY")
        monkeypatch.delenv("MINIO_SECRET_KEY")

    @patch.object(_spark_mod, "SparkSession")
    def test_no_env_vars_skips_catalog_config(self, mock_spark_cls, monkeypatch):
        """When no env vars set, should NOT configure Iceberg catalog."""
        monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)

        mock_builder = MagicMock()
        mock_spark_cls.builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        spark = _spark_mod.get_spark_session("test_no_env")
        mock_builder.config.assert_not_called()

    @patch.object(_spark_mod, "SparkSession")
    def test_sets_log_level_to_warn(self, mock_spark_cls):
        """Should set Spark log level to WARN."""
        mock_builder = MagicMock()
        mock_spark_cls.builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        spark = _spark_mod.get_spark_session("test_loglevel")
        spark.sparkContext.setLogLevel.assert_called_with("WARN")


class TestIcebergUtils:
    """Tests for code_etl/shared/spark/iceberg_utils.py"""

    def test_get_iceberg_table_name(self):
        """Should concatenate catalog.schema.table."""
        result = _iceberg_mod.get_iceberg_table_name("lakehouse", "bronze", "core_account")
        assert result == "lakehouse.bronze.core_account"

    def test_get_iceberg_table_name_gold(self):
        """Should work for Gold schema."""
        result = _iceberg_mod.get_iceberg_table_name("lakehouse", "gold", "mart_customer_360")
        assert result == "lakehouse.gold.mart_customer_360"
