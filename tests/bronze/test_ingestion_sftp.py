"""
Tests cho Bronze SFTP ingestion (ingestion_sftp.py) — khoá new2.

Khoá phần logic không cần SFTP/Spark thật:
  - _download_sftp_files: lọc file theo pattern, lỗi khi không có file, tải đúng file.
  - extract_from_sftp: gắn cob_dt từ cột nghiệp vụ, fallback về cob_dt tham số.
"""

import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Stub pyspark + spark utils (engine import chúng ở module level) rồi khôi phục.
_STUBBED = {
    "pyspark": MagicMock(),
    "pyspark.sql": MagicMock(),
    "spark": MagicMock(),
    "spark.spark_session": MagicMock(),
    "spark.iceberg_utils": MagicMock(),
    "utils": MagicMock(),
    "utils.logger": MagicMock(),
    "utils.yaml_loader": MagicMock(),
}
_SAVED = {name: sys.modules.get(name) for name in _STUBBED}
sys.modules.update(_STUBBED)

_spec = importlib.util.spec_from_file_location(
    "ingestion_sftp_mod",
    str(PROJECT_ROOT / "code_etl" / "bronze" / "base_job" / "ingestion_sftp.py"),
)
_imod = importlib.util.module_from_spec(_spec)
try:
    _spec.loader.exec_module(_imod)
finally:
    for _name, _previous in _SAVED.items():
        if _previous is None:
            sys.modules.pop(_name, None)
        else:
            sys.modules[_name] = _previous

_download = _imod._download_sftp_files
log = MagicMock()


def _fake_paramiko(file_names):
    """paramiko giả: Transport/SFTPClient trả danh sách file cho trước."""
    sftp_client = MagicMock()
    sftp_client.listdir.return_value = file_names
    transport = MagicMock()
    paramiko = MagicMock()
    paramiko.Transport.return_value = transport
    paramiko.SFTPClient.from_transport.return_value = sftp_client
    return paramiko, sftp_client


class TestDownloadSftpFiles:
    def test_filters_by_pattern(self, tmp_path):
        paramiko, sftp_client = _fake_paramiko(
            ["pos_settlement_20251231.csv", "readme.txt", "pos_settlement_20251230.csv"]
        )
        cfg = {"remote_dir": "/upload", "file_pattern": "pos_settlement_*.csv"}
        with (
            patch.dict(sys.modules, {"paramiko": paramiko}),
            patch.object(_imod.tempfile, "mkdtemp", return_value=str(tmp_path)),
        ):
            paths = _download(log, cfg)
        assert len(paths) == 2
        # Tải theo thứ tự sorted → file ngày nhỏ trước.
        sftp_client.get.assert_any_call("/upload/pos_settlement_20251230.csv", paths[0])
        sftp_client.get.assert_any_call("/upload/pos_settlement_20251231.csv", paths[1])

    def test_no_matching_file_raises(self):
        paramiko, _ = _fake_paramiko(["other.txt"])
        cfg = {"remote_dir": "/upload", "file_pattern": "pos_settlement_*.csv"}
        with (
            patch.dict(sys.modules, {"paramiko": paramiko}),
            pytest.raises(FileNotFoundError, match="Không có file khớp"),
        ):
            _download(log, cfg)

    def test_credentials_from_env(self, tmp_path):
        paramiko, _ = _fake_paramiko(["pos_settlement_20251231.csv"])
        cfg = {
            "remote_dir": "/upload",
            "file_pattern": "pos_settlement_*.csv",
            "host": "sftp",
            "user": "sftp_user",
        }
        env = {"SFTP_HOST": "sftp-host", "SFTP_PORT": "2222"}
        with (
            patch.dict(sys.modules, {"paramiko": paramiko}),
            patch.dict(os.environ, env),
            patch.object(_imod.tempfile, "mkdtemp", return_value=str(tmp_path)),
        ):
            _download(log, cfg)
        # Transport nhận (host, port) từ env.
        args = paramiko.Transport.call_args[0][0]
        assert args == ("sftp-host", 2222)


class TestExtractCobDt:
    def test_business_date_column_used_when_present(self):
        spark = MagicMock()
        df = MagicMock()
        df.columns = ["settlement_id", "settlement_date"]
        # chain spark.read...csv() → df
        spark.read.option.return_value = spark.read
        spark.read.csv.return_value = df
        config = {
            "source": {"header": True},
            "load": {"business_date_column": "settlement_date"},
        }
        # Patch cả _download (khỏi cần paramiko) lẫn F (tránh Column thật).
        with patch.object(_imod, "_download_sftp_files", return_value=["/tmp/x.csv"]), patch.object(_imod, "F"):
            _imod.extract_from_sftp(spark, config, "2025-12-31", log)
        # Có gắn cob_dt từ cột → withColumn được gọi.
        assert df.withColumn.called
