"""
Tests cho code_etl/silver/base_job/scd_type2.py.

Phần ghi (MERGE / UPDATE / DELETE) cần Iceberg và được kiểm bằng transition thật
trong CI (bước "FIXTURE SCD2" của job Trino integration). File này khoá phần
QUYẾT ĐỊNH — key nào mở version mới, key nào bị đóng, key nào chỉ ghi đè Type 1 —
chạy trên DataFrame in-memory.

Lỗi được khoá lại:
  - cột không nằm trong tracked_columns không bao giờ được cập nhật (full_name,
    is_active… đứng yên ở giá trị ngày nạp đầu);
  - key biến mất khỏi full snapshot không bao giờ được đóng;
  - chạy lại một cob_dt cũ khi đã có version mới hơn làm đảo lịch sử.
"""

import os
import sys
from datetime import date
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark.sql.functions", reason="pyspark không có trong CI env")

from pyspark.sql import SparkSession  # noqa: E402

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_scd_type2():
    """
    Nạp module thật với sys.modules sạch cho `spark.*` / `utils.*` / `common_utils`:
    vài test khác thay các tên này bằng MagicMock lúc import, và thứ tự collect
    không cố định. patch.dict khôi phục sys.modules sau khi nạp xong.
    """
    import importlib.util
    from unittest.mock import patch

    shared = str(PROJECT_ROOT / "code_etl" / "shared")
    base_job = str(PROJECT_ROOT / "code_etl" / "silver" / "base_job")
    with patch.dict(sys.modules), patch.object(sys, "path", [shared, base_job, *sys.path]):
        for name in list(sys.modules):
            if name.split(".")[0] in {"spark", "utils", "common_utils"}:
                del sys.modules[name]
        spec = importlib.util.spec_from_file_location("scd_type2_under_test", f"{base_job}/scd_type2.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


scd_type2 = _load_scd_type2()

KEYS = ["customer_id"]
COLS = "customer_id long, segment string, email string, full_name string, cob_dt date"
D = date(2026, 1, 2)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.appName("scd2-classify")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def keys(df) -> list:
    return sorted(r["customer_id"] for r in df.collect())


@pytest.fixture(scope="module")
def frames(spark):
    # target_current: version hiện hành trước khi chạy D
    target = spark.createDataFrame(
        [
            (1, "RETAIL", "a@x", "An", date(2026, 1, 1)),  # không đổi
            (2, "RETAIL", "b@x", "Binh", date(2026, 1, 1)),  # đổi tracked (segment)
            (3, "RETAIL", "c@x", "Chi", date(2026, 1, 1)),  # chỉ đổi Type 1 (full_name)
            (4, "RETAIL", "d@x", "Dung", date(2026, 1, 1)),  # biến mất khỏi nguồn
            (5, "RETAIL", None, "Em", date(2026, 1, 1)),  # NULL → NULL: không đổi
            (6, "RETAIL", None, "Giang", date(2026, 1, 1)),  # NULL → giá trị: đổi tracked
        ],
        COLS,
    )
    source = spark.createDataFrame(
        [
            (1, "RETAIL", "a@x", "An", D),
            (2, "VIP", "b@x", "Binh", D),
            (3, "RETAIL", "c@x", "Chi Nguyen", D),
            (5, "RETAIL", None, "Em", D),
            (6, "RETAIL", "g@x", "Giang", D),
            (7, "RETAIL", "h@x", "Hoa", D),  # key mới
        ],
        COLS,
    )
    return source, target


class TestSplitColumns:
    def test_default_tracks_every_attribute(self):
        tracked, type1 = scd_type2.split_columns(["customer_id", "a", "b", "cob_dt"], KEYS, None)
        assert (tracked, type1) == (["a", "b"], [])

    def test_untracked_columns_become_type1(self):
        tracked, type1 = scd_type2.split_columns(["customer_id", "a", "b", "c", "cob_dt"], KEYS, ["b"])
        assert (tracked, type1) == (["b"], ["a", "c"])

    def test_unknown_tracked_column_is_a_config_error(self):
        with pytest.raises(ValueError, match="không có trong SQL nguồn"):
            scd_type2.split_columns(["customer_id", "a"], KEYS, ["typo"])


class TestClassifyChanges:
    TRACKED = ["segment", "email"]
    TYPE1 = ["full_name"]

    def classify(self, frames, close_missing):
        source, target = frames
        return scd_type2.classify_changes(source, target, KEYS, self.TRACKED, self.TYPE1, close_missing)

    def test_new_versions_are_new_keys_and_tracked_changes(self, frames):
        new_rows, _close, _t1 = self.classify(frames, close_missing=True)
        assert keys(new_rows) == [2, 6, 7]

    def test_new_version_carries_all_source_values(self, frames):
        new_rows, _close, _t1 = self.classify(frames, close_missing=True)
        row = [r for r in new_rows.collect() if r["customer_id"] == 2][0]
        assert (row["segment"], row["full_name"], row["cob_dt"]) == ("VIP", "Binh", D)

    def test_tracked_change_and_missing_key_are_closed(self, frames):
        _new, close_keys, _t1 = self.classify(frames, close_missing=True)
        assert keys(close_keys) == [2, 4, 6]

    def test_missing_key_is_kept_open_when_not_configured(self, frames):
        _new, close_keys, _t1 = self.classify(frames, close_missing=False)
        assert keys(close_keys) == [2, 6]

    def test_type1_only_change_is_overwritten_not_versioned(self, frames):
        new_rows, close_keys, type1_rows = self.classify(frames, close_missing=True)
        assert keys(type1_rows) == [3]
        assert 3 not in keys(new_rows) and 3 not in keys(close_keys)
        assert type1_rows.collect()[0]["full_name"] == "Chi Nguyen"

    def test_null_to_null_is_not_a_change(self, frames):
        new_rows, close_keys, type1_rows = self.classify(frames, close_missing=True)
        for df in (new_rows, close_keys, type1_rows):
            assert 5 not in keys(df)
            assert 1 not in keys(df)

    def test_pure_type2_has_no_type1_updates(self, frames):
        source, target = frames
        _new, close_keys, type1_rows = scd_type2.classify_changes(
            source, target, KEYS, ["segment", "email", "full_name"], [], True
        )
        assert keys(type1_rows) == []
        assert keys(close_keys) == [2, 3, 4, 6]


class TestBackfillGuard:
    def test_older_cob_dt_than_latest_version_is_refused(self, spark):
        spark.createDataFrame([(date(2026, 1, 5),)], "effective_from date").createOrReplaceTempView("dim_guard")
        with pytest.raises(RuntimeError, match="backfill"):
            scd_type2.assert_not_backfilling_past_history(spark, "dim_guard", "effective_from", "2026-01-04")

    def test_same_or_later_cob_dt_is_allowed(self, spark):
        spark.createDataFrame([(date(2026, 1, 5),)], "effective_from date").createOrReplaceTempView("dim_guard")
        scd_type2.assert_not_backfilling_past_history(spark, "dim_guard", "effective_from", "2026-01-05")
        scd_type2.assert_not_backfilling_past_history(spark, "dim_guard", "effective_from", "2026-01-06")


class TestProductBranchAreScd2:
    """Đề bài: lưu lịch sử customer/account/product/branch."""

    @pytest.mark.parametrize("dim", ["dim_customer", "dim_account", "dim_product", "dim_branch"])
    def test_config_is_scd2_and_closes_missing_keys(self, dim):
        import yaml

        config = yaml.safe_load((PROJECT_ROOT / "code_etl" / "silver" / "dims" / f"{dim}.yml").read_text("utf-8"))
        scd_type2.validate_config(config)
        assert config["scd"]["close_missing_keys"] is True
