"""
Tests for code_etl/shared/spark/spark_session.py

Covers:
  - get_spark_session: env var override, default behavior, appName setting
  - get_iceberg_table_name: table name concatenation

Uses mock to avoid requiring actual Spark/MinIO.

Không cần pyspark thật. Mọi test ở đây đều `@patch.object(_spark_mod,
"SparkSession")`, nên pyspark chỉ cần *import được* để exec_module chạy xong.
Job unit trong CI cố ý không cài pyspark (300MB cho một job 20 giây), nên nếu
thiếu thì stub — và stub phải được gỡ ngay sau khi load, vì để rác trong
sys.modules đã từng làm hỏng các test chạy sau trong repo này.

Chỉ stub khi pyspark thật sự vắng mặt: ở máy dev có pyspark, test vẫn chạy trên
import thật, nên vẫn bắt được nếu API pyspark đổi.
"""

import contextlib
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


@contextlib.contextmanager
def _pyspark_importable():
    """Đảm bảo `from pyspark.sql import SparkSession` chạy được, rồi dọn sạch."""
    try:
        import pyspark.sql  # noqa: F401

        yield
        return
    except ImportError:
        pass

    saved = {name: sys.modules.get(name) for name in ("pyspark", "pyspark.sql")}
    pyspark_stub = MagicMock(name="pyspark")
    sys.modules["pyspark"] = pyspark_stub
    sys.modules["pyspark.sql"] = pyspark_stub.sql
    try:
        yield
    finally:
        for name, previous in saved.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous


def _load(module_name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(module_name, str(PROJECT_ROOT / relative_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


with _pyspark_importable():
    _spark_mod = _load("spark_session_mod", "code_etl/shared/spark/spark_session.py")
    _iceberg_mod = _load("iceberg_utils_mod", "code_etl/shared/spark/iceberg_utils.py")


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
    def test_catalog_override_without_minio_keys_fails_loud(self, mock_spark_cls, monkeypatch):
        """Thiếu key MinIO thì báo lỗi — trước đây âm thầm dùng key viết cứng trong code."""
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://custom-catalog:8181")
        monkeypatch.delenv("MINIO_ACCESS_KEY", raising=False)
        monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)
        mock_builder = MagicMock()
        mock_spark_cls.builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder

        with pytest.raises(OSError, match="MINIO_ACCESS_KEY"):
            _spark_mod.get_spark_session("test_missing_keys")
        mock_builder.getOrCreate.assert_not_called()

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


def _df_on_table(partition_fields: list[str] | None, columns=("id", "cob_dt")) -> MagicMock:
    """
    DataFrame giả có cột `columns`, ghi vào bảng có metadata table `.partitions`
    mô tả `partition_fields` (None = bảng không partition → không có cột `partition`).
    """
    schema = MagicMock()
    if partition_fields is None:
        schema.fieldNames.return_value = ["record_count", "file_count"]
    else:
        schema.fieldNames.return_value = ["partition", "record_count"]
        schema.__getitem__.return_value.dataType.fieldNames.return_value = partition_fields
    df = MagicMock()
    df.columns = list(columns)
    df.sparkSession.table.return_value.schema = schema
    return df


class TestPartitionGuard:
    """
    overwritePartitions() trên bảng không partition thay TOÀN BỘ bảng. Bronze từng
    có 18/22 bảng như vậy: mỗi lần nạp xoá snapshot cũ, lịch sử product/branch mất.
    """

    def test_partition_fields_read_from_metadata_table(self):
        df = _df_on_table(["cob_dt"])
        assert _iceberg_mod.partition_fields(df.sparkSession, "lakehouse.bronze.core_branch") == ["cob_dt"]
        df.sparkSession.table.assert_called_with("lakehouse.bronze.core_branch.partitions")

    def test_table_is_refreshed_before_reading_partitions(self):
        """Không REFRESH, `.partitions` trả về metadata cache cũ: ngay sau ALTER … ADD
        PARTITION FIELD vẫn báo không partition (migration Bronze fail hậu kiểm, 2026-09-30)."""
        df = _df_on_table(["cob_dt"])
        session = df.sparkSession
        _iceberg_mod.partition_fields(session, "lakehouse.bronze.core_branch")
        calls = [c[0] for c in session.mock_calls if c[0] in ("sql", "table")]
        assert calls[:2] == ["sql", "table"]
        session.sql.assert_called_once_with("REFRESH TABLE lakehouse.bronze.core_branch")

    def test_unpartitioned_table_has_no_partition_fields(self):
        df = _df_on_table(None)
        assert _iceberg_mod.partition_fields(df.sparkSession, "t") == []

    def test_snapshot_into_unpartitioned_table_is_refused(self):
        with pytest.raises(RuntimeError, match="ghi đè TOÀN BỘ bảng"):
            _iceberg_mod.assert_partitioned_by_cob_dt(_df_on_table(None), "lakehouse.bronze.core_branch")

    def test_partitioned_by_something_else_is_refused(self):
        with pytest.raises(RuntimeError, match="bronze-partition-migrate"):
            _iceberg_mod.assert_partitioned_by_cob_dt(_df_on_table(["is_current"]), "t")

    def test_partitioned_by_cob_dt_passes(self):
        _iceberg_mod.assert_partitioned_by_cob_dt(_df_on_table(["cob_dt"]), "t")

    def test_dataframe_without_cob_dt_is_not_checked(self):
        df = _df_on_table(None, columns=("id",))
        _iceberg_mod.assert_partitioned_by_cob_dt(df, "t")
        df.sparkSession.table.assert_not_called()

    def test_every_bronze_ddl_table_is_partitioned_by_cob_dt(self):
        import re

        ddl = (PROJECT_ROOT / "docker" / "init_iceberg" / "01_ddl_bronze.sql").read_text(encoding="utf-8")
        blocks = re.findall(r"CREATE TABLE IF NOT EXISTS (\S+)\s*\(.*?;", ddl, re.S)
        tables = re.findall(r"(CREATE TABLE IF NOT EXISTS \S+\s*\(.*?;)", ddl, re.S)
        assert len(blocks) == len(tables) > 0
        missing = [re.search(r"EXISTS (\S+)", t).group(1) for t in tables if "PARTITIONED BY (cob_dt)" not in t]
        assert not missing, f"Bronze DDL không partition theo cob_dt: {missing}"


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
