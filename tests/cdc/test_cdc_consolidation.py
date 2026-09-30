"""
Tests cho CDC consolidation (Bronze CDC → Silver Current) và DLQ split.

Phần ghi Iceberg (MERGE, đọc theo snapshot) cần catalog thật; file này khoá phần
logic chạy được trên DataFrame in-memory:

  - cast_columns: bản cũ so `col != ""` trên cột BIGINT/DECIMAL → mọi
    date_of_birth / open_date / balance thành NULL.
  - deduplicate_latest: event mới nhất theo (ts_ms, batch_id).
  - plan_read: tiến độ theo snapshot Iceberg, không theo ts của event.
  - build_merge_sql: event cũ hơn không được ghi đè trạng thái mới hơn.
  - validate_and_split: tombstone không vào DLQ; thiếu khoá chính thì vào DLQ.
"""

import importlib.util
import json
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

pyspark = pytest.importorskip("pyspark.sql.functions", reason="pyspark không có trong CI env")

from pyspark.sql import SparkSession  # noqa: E402

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CDC_ROOT = PROJECT_ROOT / "code_etl" / "cdc"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


consolidation = _load("cdc_consolidation_mod", CDC_ROOT / "consolidation" / "cdc_consolidation.py")
dlq = _load("cdc_dlq_mod", CDC_ROOT / "base_job" / "cdc_dlq.py")

CUSTOMER_CFG = yaml.safe_load((CDC_ROOT / "consolidation" / "config" / "cdc_consolidation_customer.yml").read_text())
ACCOUNT_CFG = yaml.safe_load((CDC_ROOT / "consolidation" / "config" / "cdc_consolidation_account.yml").read_text())
STREAM_CFG = yaml.safe_load((CDC_ROOT / "config" / "cdc_core_customer.yml").read_text())


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.appName("cdc-consolidation-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# cast_columns
# ---------------------------------------------------------------------------

# 7000 ngày sau 1970-01-01
DAYS_7000 = date(1989, 3, 2)
# 2026-09-30T00:00:00Z tính bằng micro giây
MICROS = 1_790_726_400_000_000
MICROS_TS = datetime(2026, 9, 30, 0, 0, 0)


class TestCastColumns:
    def test_bigint_bronze_columns_are_converted_not_nulled(self, spark):
        """Đúng kiểu DDL Bronze CDC: date_of_birth / register_date / last_updated là BIGINT."""
        df = spark.createDataFrame(
            [(1, 7000, 7000, "1", MICROS)],
            "customer_id long, date_of_birth long, register_date long, is_active string, last_updated long",
        )
        row = consolidation.cast_columns(df, CUSTOMER_CFG).first()
        assert row["date_of_birth"] == DAYS_7000
        assert row["register_date"] == DAYS_7000
        assert row["is_active"] == 1
        assert row["last_updated"] == MICROS_TS

    def test_decimal_balance_is_kept(self, spark):
        df = spark.sql("SELECT CAST(1234.50 AS DECIMAL(18,2)) AS balance, CAST(7000 AS BIGINT) AS open_date")
        row = consolidation.cast_columns(df, ACCOUNT_CFG).first()
        assert row["balance"] == Decimal("1234.50")
        assert row["open_date"] == DAYS_7000

    def test_string_inputs_and_empty_strings(self, spark):
        """Payload JSON map<string,string>: '' và NULL phải thành NULL, số dạng chuỗi vẫn đổi được."""
        df = spark.createDataFrame(
            [("7000", "", "1234.50"), ("", None, ""), (None, "0", None)],
            "open_date string, close_date string, balance string",
        )
        rows = consolidation.cast_columns(df, ACCOUNT_CFG).collect()
        assert (rows[0]["open_date"], rows[0]["close_date"], rows[0]["balance"]) == (
            DAYS_7000,
            None,
            Decimal("1234.50"),
        )
        assert (rows[1]["open_date"], rows[1]["close_date"], rows[1]["balance"]) == (None, None, None)
        assert (rows[2]["open_date"], rows[2]["close_date"], rows[2]["balance"]) == (None, date(1970, 1, 1), None)

    def test_micros_conversion_does_not_depend_on_session_timezone(self, spark):
        df = spark.createDataFrame([(MICROS,)], "last_updated long")
        spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        try:
            epoch = (
                consolidation.cast_columns(df, CUSTOMER_CFG).selectExpr("unix_micros(last_updated) AS m").first()["m"]
            )
        finally:
            spark.conf.set("spark.sql.session.timeZone", "UTC")
        assert epoch == MICROS

    def test_unknown_conversion_is_rejected(self, spark):
        df = spark.createDataFrame([(1,)], "x long")
        with pytest.raises(ValueError, match="không hỗ trợ"):
            consolidation.cast_columns(df, {"conversions": {"x": "epoch_seconds"}})


# ---------------------------------------------------------------------------
# deduplicate_latest
# ---------------------------------------------------------------------------


class TestDeduplicateLatest:
    def test_latest_by_timestamp_then_batch(self, spark):
        df = spark.createDataFrame(
            [
                (1, "a@x", 100, 1, "INSERT"),
                (1, "b@x", 200, 1, "UPDATE"),
                (1, "c@x", 200, 2, "UPDATE"),  # cùng ts, batch sau → thắng
                (2, "z@x", 50, 9, "INSERT"),
                (2, None, 60, 3, "DELETE"),  # ts lớn hơn thắng dù batch nhỏ hơn
            ],
            "customer_id long, email string, __cdc_timestamp_ms long, __spark_batch_id long, __cdc_operation string",
        )
        rows = {r["customer_id"]: r for r in consolidation.deduplicate_latest(df, CUSTOMER_CFG).collect()}
        assert rows[1]["email"] == "c@x"
        assert rows[2]["__cdc_operation"] == "DELETE"
        assert len(rows) == 2

    def test_kafka_offset_breaks_a_tie_in_ms_and_batch(self, spark):
        """INSERT + UPDATE trong một transaction: cùng __ts_ms, cùng micro-batch (đo trên
        stack 2026-09-30, customer 990003). Không có offset, row_number chọn ngẫu nhiên."""
        schema = (
            "customer_id long, email string, __cdc_timestamp_ms long, __spark_batch_id long, "
            "__cdc_operation string, __kafka_offset long"
        )
        for rows_in in (
            [(1, "v1", 853, 3, "INSERT", 10), (1, "v2", 853, 3, "UPDATE", 11)],
            [(1, "v2", 853, 3, "UPDATE", 11), (1, "v1", 853, 3, "INSERT", 10)],
        ):
            df = spark.createDataFrame(rows_in, schema).repartition(4)
            (row,) = consolidation.deduplicate_latest(df, CUSTOMER_CFG).collect()
            assert row["email"] == "v2"

    def test_rows_from_before_offsets_sort_after(self, spark):
        schema = (
            "customer_id long, email string, __cdc_timestamp_ms long, __spark_batch_id long, "
            "__cdc_operation string, __kafka_offset long"
        )
        df = spark.createDataFrame([(1, "old", 5, 1, "UPDATE", None), (1, "new", 5, 1, "UPDATE", 7)], schema)
        (row,) = consolidation.deduplicate_latest(df, CUSTOMER_CFG).collect()
        assert row["email"] == "new"


# ---------------------------------------------------------------------------
# plan_read — tiến độ theo snapshot
# ---------------------------------------------------------------------------


class TestPlanRead:
    def test_empty_source_is_skipped(self):
        assert consolidation.plan_read(None, None, False) == "skip"

    def test_no_new_snapshot_is_skipped(self):
        assert consolidation.plan_read(10, 10, True) == "skip"

    def test_first_run_reads_everything(self):
        assert consolidation.plan_read(None, 10, False) == "full"

    def test_expired_watermark_falls_back_to_full_read(self):
        assert consolidation.plan_read(5, 10, False) == "full"

    def test_normal_run_is_incremental(self):
        assert consolidation.plan_read(5, 10, True) == "incremental"


# ---------------------------------------------------------------------------
# MERGE có guard thứ tự
# ---------------------------------------------------------------------------


class TestMergeSql:
    SQL = consolidation.build_merge_sql(
        "lakehouse.silver.dim_customer_current",
        "customer_id",
        "__cdc_operation",
        "__cdc_timestamp_ms",
        ["email", "__cdc_timestamp_ms"],
        ["customer_id", "email", "__cdc_timestamp_ms"],
    )

    def _clause(self, marker: str) -> str:
        line = [ln for ln in self.SQL.splitlines() if marker in ln]
        assert line, marker
        return line[0]

    def test_delete_only_when_not_older(self):
        clause = self._clause("THEN DELETE")
        assert "s.__cdc_timestamp_ms >= t.__cdc_timestamp_ms" in clause

    def test_update_only_when_not_older(self):
        clause = self._clause("THEN UPDATE SET")
        assert "s.__cdc_timestamp_ms >= t.__cdc_timestamp_ms" in clause

    def test_legacy_rows_without_timestamp_can_be_updated(self):
        assert "t.__cdc_timestamp_ms IS NULL" in self._clause("THEN UPDATE SET")

    def test_delete_of_unknown_key_is_not_inserted(self):
        assert "WHEN NOT MATCHED AND s.__cdc_operation <> 'DELETE' THEN INSERT" in self.SQL

    def test_merge_guard_semantics_on_dataframes(self, spark):
        """
        Mô phỏng đúng điều kiện của MERGE trên DataFrame: event cũ hơn trạng thái
        hiện có bị bỏ qua, event mới hơn hoặc bằng (replay) được áp.
        """
        target = spark.createDataFrame([(1, "new@x", 300), (2, "b@x", 100)], "customer_id long, email string, ts long")
        events = spark.createDataFrame([(1, "old@x", 200), (2, "b2@x", 100)], "customer_id long, email string, ts long")
        applied = (
            events.alias("s")
            .join(target.alias("t"), "customer_id")
            .where("t.ts IS NULL OR s.ts >= t.ts")
            .select("customer_id")
        )
        assert sorted(r["customer_id"] for r in applied.collect()) == [2]


# ---------------------------------------------------------------------------
# DLQ split
# ---------------------------------------------------------------------------

KAFKA_SCHEMA = "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"


def kafka_row(key, payload, offset):
    value = None if payload is None else json.dumps({"payload": payload}).encode()
    return (
        None if key is None else key.encode(),
        value,
        STREAM_CFG["kafka"]["topic"],
        0,
        offset,
        datetime(2026, 9, 30, tzinfo=timezone.utc).replace(tzinfo=None),
    )


@pytest.fixture(scope="module")
def split(spark):
    rows = [
        kafka_row("1", {"customer_id": "1", "email": "a@x", "__op": "c", "__ts_ms": "1000"}, 1),
        kafka_row("1", {"customer_id": "1", "email": None, "__op": "d", "__ts_ms": "2000"}, 2),
        kafka_row("1", None, 3),  # tombstone theo sau delete
        kafka_row("2", {"customer_id": None, "email": "x", "__op": "u", "__ts_ms": "3000"}, 4),
        kafka_row("3", {"customer_id": "3", "email": "c@x", "__op": "x", "__ts_ms": "4000"}, 5),
    ]
    batch = spark.createDataFrame(rows, KAFKA_SCHEMA)
    valid, invalid = dlq.validate_and_split(batch, STREAM_CFG, batch_id=7)
    return valid.collect(), invalid.collect()


class TestDlqSplit:
    def test_valid_events_pass(self, split):
        valid, _ = split
        assert sorted(r["__cdc_operation"] for r in valid) == ["DELETE", "INSERT"]

    def test_tombstone_is_not_a_dlq_event(self, split):
        _, invalid = split
        assert "PARSE_ERROR" not in [r["error_type"] for r in invalid]

    def test_missing_primary_key_goes_to_dlq(self, split):
        _, invalid = split
        assert sorted(r["error_type"] for r in invalid) == ["INVALID_OPERATION", "MISSING_PRIMARY_KEY"]

    def test_dlq_keeps_kafka_coordinates(self, split):
        _, invalid = split
        assert sorted(r["kafka_offset"] for r in invalid) == [4, 5]

    def test_valid_path_carries_only_partition_and_offset(self, split):
        """ADR-0010 (2026-09-30): Bronze CDC giữ __kafka_partition/__kafka_offset làm khoá
        thứ tự; mọi cột Kafka khác vẫn chỉ ở DLQ (DDL không có, lệch là ARITY_MISMATCH)."""
        valid, _ = split
        kafka_cols = sorted(c for c in valid[0].asDict() if "kafka" in c)
        assert kafka_cols == ["__kafka_offset", "__kafka_partition"]
        assert sorted(r["__kafka_offset"] for r in valid) == [1, 2]


class TestKafkaCoordinateColumns:
    """Bảng Bronze CDC tạo trước 2026-09-30 được bổ sung cột khi stream khởi động."""

    class FakeSpark:
        def __init__(self, columns):
            self.columns = columns
            self.statements = []

        def table(self, name):
            return type("T", (), {"columns": self.columns})()

        def sql(self, statement):
            self.statements.append(statement)

    def test_old_table_gets_both_columns(self):
        spark = self.FakeSpark(["customer_id", "__ingestion_time"])
        assert dlq.ensure_kafka_coordinate_columns(spark, "lakehouse.bronze.core_customer_cdc") == [
            "__kafka_partition",
            "__kafka_offset",
        ]
        assert spark.statements == [
            "ALTER TABLE lakehouse.bronze.core_customer_cdc ADD COLUMNS (__kafka_partition INT, __kafka_offset BIGINT)"
        ]

    def test_current_table_is_left_alone(self):
        spark = self.FakeSpark(["customer_id", "__kafka_partition", "__kafka_offset"])
        assert dlq.ensure_kafka_coordinate_columns(spark, "t") == []
        assert spark.statements == []
