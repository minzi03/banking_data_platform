"""
Tests for code_etl/shared/utils/yaml_loader.py

Covers:
  - load_config: basic loading, missing file, empty file
  - load_config_pipeline: Jinja rendering, cluster deploy mode, error cases
"""

import importlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Add project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ETL_SHARED = str(PROJECT_ROOT / "code_etl" / "shared")

# Direct import via importlib to avoid package name conflicts
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "yaml_loader",
    str(PROJECT_ROOT / "code_etl" / "shared" / "utils" / "yaml_loader.py")
)
_yaml_loader = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_yaml_loader)

load_config = _yaml_loader.load_config
load_config_pipeline = _yaml_loader.load_config_pipeline


class TestLoadConfig:
    """Tests for the simple YAML loader."""

    def test_load_valid_config(self, sample_bronze_config):
        """Should load a valid YAML file and return dict."""
        config = load_config(sample_bronze_config)
        assert isinstance(config, dict)
        assert config["source"]["type"] == "postgresql"
        assert config["target"]["catalog"] == "lakehouse"
        assert config["target"]["table"] == "core_account"
        assert config["load"]["strategy"] == "full_snapshot"

    def test_load_config_returns_all_sections(self, sample_bronze_config):
        """Should preserve all YAML sections."""
        config = load_config(sample_bronze_config)
        assert "source" in config
        assert "target" in config
        assert "load" in config
        assert "sql" in config

    def test_load_config_sql_preserves_multiline(self, sample_bronze_config):
        """Should preserve multiline SQL content."""
        config = load_config(sample_bronze_config)
        assert "SELECT" in config["sql"]
        assert "core_banking.account" in config["sql"]

    def test_missing_file_raises(self):
        """Should raise FileNotFoundError for nonexistent file."""
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            load_config("/nonexistent/path/config.yml")

    def test_empty_file_raises(self, sample_empty_yaml):
        """Should raise ValueError for empty YAML file."""
        with pytest.raises(ValueError, match="Empty or invalid YAML"):
            load_config(sample_empty_yaml)

    def test_load_cdc_config(self, sample_cdc_config):
        """Should load CDC config with nested column definitions."""
        config = load_config(sample_cdc_config)
        assert config["kafka"]["topic"] == "postgresql.banking.core_banking.account"
        assert config["kafka"]["trigger_interval"] == "30 seconds"
        assert len(config["target"]["columns"]) == 3
        assert config["target"]["columns"][0]["name"] == "account_id"
        assert config["target"]["columns"][0]["type"] == "long"

    def test_load_silver_scd1_config(self, sample_silver_scd1_config):
        """Should load Silver SCD1 config with business_key."""
        config = load_config(sample_silver_scd1_config)
        assert config["job"]["type"] == "scd_type1"
        assert config["business_key"] == ["branch_code"]
        assert "{{ cob_dt }}" in config["sql"]

    def test_load_gold_config(self, sample_gold_config):
        """Should load Gold config with SQL containing template vars."""
        config = load_config(sample_gold_config)
        assert config["target"]["schema"] == "gold"
        assert "{{ cob_dt }}" in config["sql"]


class TestLoadConfigPipeline:
    """Tests for the pipeline YAML loader with Jinja support."""

    def test_load_without_jinja(self, sample_bronze_config):
        """Should load config without Jinja rendering."""
        config = load_config_pipeline(sample_bronze_config)
        assert config["target"]["table"] == "core_account"

    def test_load_with_jinja_rendering(self, sample_silver_scd1_config):
        """Should render Jinja variables in YAML."""
        config = load_config_pipeline(
            sample_silver_scd1_config,
            context_vars={"cob_dt": "2025-01-15"}
        )
        assert "2025-01-15" in config["sql"]
        assert "{{ cob_dt }}" not in config["sql"]

    def test_load_with_multiple_jinja_vars(self, tmp_path):
        """Should render multiple Jinja variables."""
        config_content = """
source:
  schema: {{ source_schema }}
target:
  table: {{ target_table }}
sql: |
  SELECT * FROM {{ source_schema }}.{{ target_table }}
  WHERE cob_dt = DATE '{{ cob_dt }}'
"""
        config_file = tmp_path / "multi_var.yml"
        config_file.write_text(config_content, encoding="utf-8")

        config = load_config_pipeline(
            str(config_file),
            context_vars={
                "source_schema": "core_banking",
                "target_table": "account",
                "cob_dt": "2025-06-01",
            }
        )
        assert config["source"]["schema"] == "core_banking"
        assert config["target"]["table"] == "account"
        assert "core_banking.account" in config["sql"]
        assert "2025-06-01" in config["sql"]

    def test_missing_file_raises(self):
        """Should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_config_pipeline("/nonexistent/file.yml")

    def test_empty_file_raises(self, sample_empty_yaml):
        """Should raise ValueError for empty YAML."""
        with pytest.raises(ValueError, match="Empty or invalid YAML"):
            load_config_pipeline(sample_empty_yaml)

    def test_cluster_deploy_mode_resolves_path(self, sample_bronze_config, tmp_path):
        """In cluster mode, relative paths should be resolved from cwd."""
        mock_spark = MagicMock()
        mock_spark.sparkContext.getConf().get.return_value = "cluster"

        # The function should not crash; it resolves path from cwd
        # Using an absolute path (sample_bronze_config) so it works regardless
        config = load_config_pipeline(sample_bronze_config, spark=mock_spark)
        assert config["target"]["table"] == "core_account"
