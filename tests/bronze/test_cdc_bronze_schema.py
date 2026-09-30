"""
Luồng CDC hợp lệ ghi ĐÚNG các cột của bảng Bronze CDC
======================================================

Đo trên stack 2026-09-27: batch CDC đầu tiên chết ở `writeTo().append()` với
INSERT_COLUMN_ARITY_MISMATCH. `cdc_dlq.validate_and_split` thêm sáu cột toạ độ Kafka
(source_topic, kafka_partition, kafka_offset, kafka_timestamp, raw_payload,
payload_hash) vào luồng hợp lệ, trong khi hai DDL — docker/init_iceberg/
04_ddl_bronze_cdc.sql và code_etl/cdc/create_cdc_tables.py — không có chúng, đúng
như ADR-0010 ghi (chỉ DLQ giữ toạ độ Kafka). Lệch từ commit d84b0e3 (2026-09-08):
mọi batch có dữ liệu đều hỏng từ đó, và CI không chạy luồng CDC nên không thấy.

Hai tầng:
  - tĩnh (unit CI): mỗi config CDC + 7 cột metadata == cột của bảng trong CẢ HAI DDL
  - Spark (integration): validate_and_split thật trên một batch Kafka giả cho ra
    đúng tập cột đó, và event hỏng đi DLQ
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "code_etl" / "cdc" / "config"
DDL_FILES = {
    "04_ddl_bronze_cdc.sql": REPO_ROOT / "docker" / "init_iceberg" / "04_ddl_bronze_cdc.sql",
    "create_cdc_tables.py": REPO_ROOT / "code_etl" / "cdc" / "create_cdc_tables.py",
}
META_COLUMNS = [
    "__cdc_operation",
    "__cdc_timestamp",
    "__cdc_timestamp_ms",
    "__spark_batch_id",
    "__ingestion_time",
    # Toạ độ Kafka: khoá thứ tự cuối trong dedup consolidation (ADR-0010, 2026-09-30)
    "__kafka_partition",
    "__kafka_offset",
]
_TABLE_RE = re.compile(r"CREATE TABLE IF NOT EXISTS lakehouse\.bronze\.(\w+) \((.*?)\)\s*USING iceberg", re.DOTALL)


def _ddl_columns(text: str) -> dict[str, list[str]]:
    tables = {}
    for name, body in _TABLE_RE.findall(text):
        cols = [
            line.strip().split()[0] for line in body.splitlines() if line.strip() and not line.strip().startswith("--")
        ]
        tables[name] = [c.rstrip(",") for c in cols]
    return tables


DDL = {label: _ddl_columns(path.read_text(encoding="utf-8")) for label, path in DDL_FILES.items()}
CONFIGS = {p.name: yaml.safe_load(p.read_text(encoding="utf-8")) for p in sorted(CONFIG_DIR.glob("cdc_*.yml"))}


def _expected(config: dict) -> list[str]:
    return [c["name"] for c in config["target"]["columns"]] + META_COLUMNS


def test_configs_and_ddl_are_found():
    assert len(CONFIGS) == 6
    assert all(len(tables) >= 6 for tables in DDL.values()), {k: sorted(v) for k, v in DDL.items()}


@pytest.mark.parametrize("config_name", sorted(CONFIGS))
def test_config_columns_match_both_ddls(config_name):
    config = CONFIGS[config_name]
    table = config["target"]["table"]
    for label, tables in DDL.items():
        assert table in tables, f"{label} không tạo {table}"
        assert sorted(tables[table]) == sorted(_expected(config)), (
            f"{label}.{table} lệch config {config_name}: "
            f"thiếu {sorted(set(_expected(config)) - set(tables[table]))}, "
            f"thừa {sorted(set(tables[table]) - set(_expected(config)))}"
        )


# ── Spark: chạy validate_and_split thật ──────────────────────────────────────


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    # Python worker phải là CHÍNH interpreter này (Windows: không có thì worker không kết nối lại).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.appName("cdc-bronze-schema")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("wh")))
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def cdc_dlq():
    pytest.importorskip("pyspark")
    spec = importlib.util.spec_from_file_location(
        "_cdc_dlq", REPO_ROOT / "code_etl" / "cdc" / "base_job" / "cdc_dlq.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _kafka_batch(spark, config: dict):
    """Một event snapshot hợp lệ + một event có __op lạ, đúng hình Debezium (unwrap, JSON có schema)."""
    payload = {c["name"]: "1" for c in config["target"]["columns"]}
    good = {"schema": {}, "payload": {**payload, "__op": "r", "__ts_ms": "1790000000000", "__deleted": "false"}}
    bad = {"schema": {}, "payload": {**payload, "__op": "x", "__ts_ms": "1790000000000", "__deleted": "false"}}
    topic = config["kafka"]["topic"]
    rows = [
        (b"k1", json.dumps(good).encode(), topic, 0, 10, datetime(2026, 9, 27)),
        (b"k2", json.dumps(bad).encode(), topic, 0, 11, datetime(2026, 9, 27)),
    ]
    return spark.createDataFrame(
        rows, "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
    )


@pytest.mark.integration
@pytest.mark.parametrize("config_name", sorted(CONFIGS))
def test_valid_rows_carry_exactly_the_table_columns(spark, cdc_dlq, config_name):
    config = CONFIGS[config_name]
    valid_df, dlq_df = cdc_dlq.validate_and_split(_kafka_batch(spark, config), config, batch_id=7)
    assert sorted(valid_df.columns) == sorted(_expected(config)), (
        f"thừa {sorted(set(valid_df.columns) - set(_expected(config)))}, "
        f"thiếu {sorted(set(_expected(config)) - set(valid_df.columns))}"
    )
    assert valid_df.count() == 1
    assert dlq_df.count() == 1
    assert dlq_df.first()["error_type"] == "INVALID_OPERATION"
    assert {"kafka_partition", "kafka_offset"} <= set(dlq_df.columns), "DLQ phải giữ toạ độ Kafka (ADR-0010)"
