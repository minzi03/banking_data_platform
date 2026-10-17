"""
Contract Test — dim_store SCD2 (khoá new2 §2.2).
================================================

Đề bài yêu cầu: *"Lịch sử thay đổi (SCD2) của merchant và cửa hàng"* và
báo cáo doanh thu theo merchant/cửa hàng. Trước P2, repo chỉ có merchant —
không có store source, Bronze hay Silver.

Test chốt toàn bộ chuỗi:
  generator → PostgreSQL store → Bronze core_store → Silver dim_store SCD2.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE = PROJECT_ROOT / "code_etl" / "bronze" / "digital_banking" / "store.yml"
SILVER = PROJECT_ROOT / "code_etl" / "silver" / "dims" / "dim_store.yml"
BRONZE_DDL = PROJECT_ROOT / "docker" / "init_iceberg" / "01_ddl_bronze.sql"
SILVER_DDL = PROJECT_ROOT / "docker" / "init_iceberg" / "02_ddl_silver.sql"


def test_store_bronze_config_exists():
    cfg = yaml.safe_load(BRONZE.read_text(encoding="utf-8"))
    assert cfg["source"]["schema"] == "digital_banking"
    assert cfg["target"]["table"] == "core_store"
    assert cfg["load"]["strategy"] == "full_snapshot"
    for col in ("store_id", "merchant_id", "store_name", "city", "district"):
        assert re.search(rf"\b{col}\b", cfg["sql"])


def test_store_silver_config_is_scd2():
    cfg = yaml.safe_load(SILVER.read_text(encoding="utf-8"))
    assert cfg["job"]["type"] == "scd_type2"
    assert cfg["target"]["table"] == "dim_store"
    assert cfg["business_key"] == ["store_id"]
    assert cfg["scd"]["sk_column"] == "store_sk"
    assert cfg["scd"]["close_missing_keys"] is True
    assert {"city", "district", "store_type", "is_active"} <= set(cfg["tracked_columns"])


def test_bronze_ddl_matches_store_source():
    ddl = BRONZE_DDL.read_text(encoding="utf-8")
    block = ddl.split("lakehouse.bronze.core_store", 1)[1].split("USING iceberg", 1)[0]
    for col in ("store_id", "merchant_id", "store_name", "store_type", "city", "district", "cob_dt"):
        assert col in block, f"core_store DDL thiếu {col}"


def test_silver_ddl_has_scd2_columns():
    ddl = SILVER_DDL.read_text(encoding="utf-8")
    block = ddl.split("lakehouse.silver.dim_store", 1)[1].split("USING iceberg", 1)[0]
    for col in (
        "store_sk", "store_id", "merchant_id", "store_name", "city", "district",
        "effective_from", "effective_to", "is_current", "last_updated",
    ):
        assert col in block, f"dim_store DDL thiếu {col}"
    assert "PARTITIONED BY (is_current)" in ddl[ddl.index("lakehouse.silver.dim_store"):]


def test_bronze_and_silver_bootstraps_schedule_store():
    bronze_bootstrap = (PROJECT_ROOT / "code_etl/bronze/bootstrap/initial_load.py").read_text(encoding="utf-8")
    silver_bootstrap = (PROJECT_ROOT / "code_etl/silver/bootstrap/initial_load.py").read_text(encoding="utf-8")
    assert "digital_banking/store.yml" in bronze_bootstrap
    assert "dims/dim_store.yml" in silver_bootstrap


def test_generator_writes_and_truncates_store():
    generator = (PROJECT_ROOT / "data_generator/generate_all.py").read_text(encoding="utf-8")
    writer = (PROJECT_ROOT / "data_generator/connectors/postgres_writer.py").read_text(encoding="utf-8")
    assert 'writer.write_rows("digital_banking", "store"' in generator
    assert '("digital_banking", "store")' in writer


def test_store_has_quarter_level_geography():
    """district là dữ liệu cần thiết để đáp ứng phần "quận" của đề bài."""
    cfg = yaml.safe_load(BRONZE.read_text(encoding="utf-8"))
    assert re.search(r"\bdistrict\b", cfg["sql"])
