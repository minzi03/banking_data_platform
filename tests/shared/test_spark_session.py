"""
Tests for code_etl/shared/spark/spark_session.py

Covers:
  - get_spark_session: env var override, default behavior, appName setting
  - get_iceberg_table_name: table name concatenation

Uses mock to avoid requiring actual Spark/MinIO.
"""

import importlib.util

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

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


def _utc_session() -> MagicMock:
    """
    Mock SparkSession báo session timezone = UTC.

    get_spark_session() gọi assert_utc_session(), guard này đọc
    spark.conf.get("spark.sql.session.timeZone"). MagicMock mặc định trả về một
    MagicMock khác nên guard raise — đúng như thiết kế, nhưng mock phải khai
    báo tường minh là UTC.
    """
    session = MagicMock()
    session.conf.get.return_value = "UTC"
    return session


class TestGetSparkSession:
    """Tests for SparkSession factory."""

    @patch.object(_spark_mod, "SparkSession")
    def test_creates_session_with_app_name(self, mock_spark_cls):
        """Should create SparkSession with the given app name."""
        mock_builder = MagicMock()
        mock_spark_cls.builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = _utc_session()

        spark = _spark_mod.get_spark_session("test_app")  # noqa: F841
        mock_spark_cls.builder.appName.assert_called_with("test_app")

    @patch.object(_spark_mod, "SparkSession")
    def test_default_app_name(self, mock_spark_cls):
        """Should use default app name when none provided."""
        mock_builder = MagicMock()
        mock_spark_cls.builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = _utc_session()

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
        mock_builder.getOrCreate.return_value = _utc_session()

        spark = _spark_mod.get_spark_session("test_env")  # noqa: F841

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
        mock_builder.getOrCreate.return_value = _utc_session()

        spark = _spark_mod.get_spark_session("test_no_env")  # noqa: F841
        mock_builder.config.assert_not_called()

    @patch.object(_spark_mod, "SparkSession")
    def test_sets_log_level_to_warn(self, mock_spark_cls):
        """Should set Spark log level to WARN."""
        mock_builder = MagicMock()
        mock_spark_cls.builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = _utc_session()

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


class TestUtcSessionGuard:
    """
    Guard biến precondition ngầm thành lỗi fail-fast.

    Biểu thức chuẩn lấy ngày nghiệp vụ —
    CAST(from_utc_timestamp(ts, 'Asia/Ho_Chi_Minh') AS DATE) — chỉ ĐÚNG dưới
    session=UTC. Đo được: session=Asia/Ho_Chi_Minh dịch hai lần, session=NY sai
    hẳn. Không có guard thì mọi Gold metric theo ngày phụ thuộc thầm lặng vào
    một cấu hình engine.
    """

    def test_accepts_utc(self):
        session = MagicMock()
        session.conf.get.return_value = "UTC"
        _spark_mod.assert_utc_session(session)  # không raise

    @pytest.mark.parametrize("tz", ["Asia/Ho_Chi_Minh", "America/New_York", "Etc/GMT-7"])
    def test_rejects_non_utc(self, tz):
        session = MagicMock()
        session.conf.get.return_value = tz
        with pytest.raises(RuntimeError, match="phải là 'UTC'"):
            _spark_mod.assert_utc_session(session)

    def test_error_message_explains_why(self):
        session = MagicMock()
        session.conf.get.return_value = "Asia/Ho_Chi_Minh"
        with pytest.raises(RuntimeError) as exc:
            _spark_mod.assert_utc_session(session)
        message = str(exc.value)
        assert "from_utc_timestamp" in message
        assert "29,2%" in message, "lỗi phải nêu hệ quả đã đo được, không chỉ nói 'sai'"

    def test_business_timezone_constant_is_explicit(self):
        assert _spark_mod.BUSINESS_TIMEZONE == "Asia/Ho_Chi_Minh"
