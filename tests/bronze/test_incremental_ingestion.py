"""
Nạp tăng dần theo ngày nghiệp vụ (ADR-0018) — cửa sổ, cấu hình, và hai tầng khớp nhau.

Benchmark 2026-09-30: Bronze nạp lại toàn bộ bảng giao dịch mỗi cob_dt (full_snapshot),
Silver ghi lại toàn bộ — ở ×10 là 23 M giao dịch mỗi ngày dù ngày đó chỉ phát sinh vài
chục nghìn. Ba bảng giao dịch giờ nạp đúng một ngày nghiệp vụ; test này giữ các mảnh
của thiết kế đó không trôi khỏi nhau. Không cần Spark hay Postgres.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = REPO_ROOT / "code_etl" / "bronze"
SILVER_FACTS = REPO_ROOT / "code_etl" / "silver" / "facts"

_STUBS = ["pyspark", "pyspark.sql", "spark", "spark.iceberg_utils", "spark.spark_session"]
_saved = {name: sys.modules.get(name) for name in _STUBS}
for name in _STUBS:
    sys.modules[name] = MagicMock()
sys.path.insert(0, str(REPO_ROOT / "code_etl" / "shared"))
_spec = importlib.util.spec_from_file_location("ingestion_jdbc_mod", BRONZE_DIR / "base_job" / "ingestion_jdbc.py")
ingestion = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ingestion)
for name, mod in _saved.items():
    if mod is None:
        sys.modules.pop(name, None)
    else:
        sys.modules[name] = mod


def _configs() -> dict[str, dict]:
    out = {}
    for path in sorted(BRONZE_DIR.glob("*/*.yml")):
        config = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(config, dict) and "load" in config:
            out[path.relative_to(REPO_ROOT).as_posix()] = config
    return out


CONFIGS = _configs()
INCREMENTAL = {k: v for k, v in CONFIGS.items() if v["load"]["strategy"] == "incremental"}


class TestWindow:
    def test_daily_window_is_one_business_day(self):
        assert ingestion.load_window("2026-09-29") == {"window_start": "2026-09-29", "window_end": "2026-09-30"}

    def test_backfill_widens_the_start_only(self):
        assert ingestion.load_window("2026-09-29", "1900-01-01") == {
            "window_start": "1900-01-01",
            "window_end": "2026-09-30",
        }

    def test_month_end(self):
        assert ingestion.load_window("2026-12-31")["window_end"] == "2027-01-01"

    def test_backfill_after_cob_is_rejected(self):
        with pytest.raises(ValueError):
            ingestion.load_window("2026-09-29", "2026-09-30")


class TestConfigValidation:
    BASE: dict = {
        "source": {"type": "postgresql", "schema": "s"},
        "target": {"catalog": "lakehouse", "schema": "bronze", "table": "t"},
    }

    def _config(self, load, sql):
        return {**self.BASE, "load": load, "sql": sql}

    def test_incremental_needs_business_date_column(self):
        with pytest.raises(ValueError, match="cob_dt_from_column"):
            ingestion.validate_config(
                self._config({"strategy": "incremental"}, "SELECT 1 WHERE {{ window_start }} {{ window_end }}")
            )

    def test_incremental_needs_both_window_bounds(self):
        with pytest.raises(ValueError, match="window_end"):
            ingestion.validate_config(
                self._config({"strategy": "incremental", "cob_dt_from_column": "cob_dt"}, "WHERE {{ window_start }}")
            )

    def test_full_snapshot_needs_no_window(self):
        ingestion.validate_config(self._config({"strategy": "full_snapshot"}, "SELECT 1"))


class TestShippedConfigs:
    def test_the_three_transaction_tables_are_incremental(self):
        tables = {v["target"]["table"] for v in INCREMENTAL.values()}
        assert tables == {"core_txn_account", "core_card_txn", "core_online_transaction"}

    @pytest.mark.parametrize("path", sorted(INCREMENTAL))
    def test_shipped_incremental_configs_validate(self, path):
        ingestion.validate_config(INCREMENTAL[path])

    @pytest.mark.parametrize("path", sorted(INCREMENTAL))
    def test_business_date_uses_the_same_timestamp_as_the_window(self, path):
        """cob_dt và cửa sổ phải cùng một cột thời gian và cùng múi giờ ICT, không thì một
        giao dịch có thể rơi vào cửa sổ ngày D nhưng mang cob_dt D±1."""
        sql = INCREMENTAL[path]["sql"]
        derived = re.search(r"timezone\('Asia/Ho_Chi_Minh', timezone\('UTC', (\w+)\)\)\)::date AS cob_dt", sql)
        assert derived, f"{path}: cob_dt không tính tường minh theo giờ ICT"
        column = derived.group(1)
        for bound in ("window_start", "window_end"):
            assert re.search(
                rf"{column}\s*[<>]=?\s*timezone\('UTC', timezone\('Asia/Ho_Chi_Minh', TIMESTAMP '\{{\{{ {bound} \}}\}}'\)\)",
                sql,
            ), f"{path}: {bound} không so trên {column} theo giờ ICT"

    def test_silver_reads_every_incremental_bronze_table_as_incremental(self):
        """Bronze incremental mà Silver vẫn pin một ngày (hoặc ngược lại) thì Silver đọc sai tập dữ liệu."""
        bronze = {v["target"]["table"] for v in INCREMENTAL.values()}
        silver_sources = set()
        for path in SILVER_FACTS.glob("*.yml"):
            config = yaml.safe_load(path.read_text(encoding="utf-8"))
            if (config.get("job") or {}).get("incremental"):
                assert "{{ from_dt }}" in config["sql"], f"{path.name}: thiếu khoảng from_dt"
                silver_sources |= set(re.findall(r"lakehouse\.bronze\.(\w+)", config["sql"]))
        assert silver_sources == bronze
