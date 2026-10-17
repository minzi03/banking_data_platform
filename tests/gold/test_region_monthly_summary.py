"""
Contract Test — mart phân tích khu vực (khoá new2 §2.2).
========================================================

Đề bài: *"Phân tích khu vực: doanh thu theo tỉnh/quận, chi nhánh; khu vực tăng
trưởng nhanh."*

Trước mart này platform chỉ có doanh thu theo merchant và theo chi nhánh ngân
hàng, không có cái nhìn theo ĐỊA BÀN của merchant.

Test chốt:
  1. Mart tồn tại, đăng ký trong GOLD_JOB_ORDER và có DDL.
  2. Grain city × tháng, tăng trưởng MoM và xếp hạng có mặt.
  3. Địa bàn lấy từ dim_merchant theo SCD2 (không dùng version cũ).
  4. Serving model + sync khoá gồm chiều thời gian.

Chạy: pytest tests/gold/test_region_monthly_summary.py -v
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MART = PROJECT_ROOT / "code_etl" / "gold" / "time_analytics" / "region_monthly_summary.yml"
GOLD_DDL = PROJECT_ROOT / "docker" / "init_iceberg" / "03_ddl_gold.sql"
BOOTSTRAP = PROJECT_ROOT / "code_etl" / "gold" / "bootstrap" / "initial_load.py"
SERVING_DIR = PROJECT_ROOT / "dbt" / "models" / "serving"
SYNC_CONFIG = PROJECT_ROOT / "code_etl" / "serving" / "config" / "serving_sync.yml"


def _sql() -> str:
    cfg = yaml.safe_load(MART.read_text(encoding="utf-8"))
    return re.sub(r"--[^\n]*", "", cfg["sql"])


def test_mart_exists_and_declares_target():
    assert MART.exists(), "thiếu mart_region_monthly_summary.yml"
    cfg = yaml.safe_load(MART.read_text(encoding="utf-8"))
    assert cfg["target"]["table"] == "mart_region_monthly_summary"
    assert cfg["target"]["schema"] == "gold"


def test_mart_is_scheduled_in_gold_bootstrap():
    """Mart không có trong GOLD_JOB_ORDER thì không bao giờ được tạo."""
    text = BOOTSTRAP.read_text(encoding="utf-8")
    assert "region_monthly_summary.yml" in text, "mart chưa được schedule trong GOLD_JOB_ORDER"


def test_gold_ddl_has_the_table():
    ddl = GOLD_DDL.read_text(encoding="utf-8")
    assert "lakehouse.gold.mart_region_monthly_summary" in ddl
    block = ddl.split("lakehouse.gold.mart_region_monthly_summary", 1)[1].split("USING iceberg", 1)[0]
    for col in ("city", "txn_year", "txn_month", "active_merchants", "revenue", "mom_growth_rate", "rank_in_month"):
        assert col in block, f"DDL thiếu cột {col}"


def test_grain_is_city_and_month():
    """Grain phải gồm city + năm + tháng; chỉ city thì mọi tháng đè lên nhau."""
    sql = _sql().lower()
    assert "group by city, yr, mo" in sql, "phải gom theo city × năm × tháng"


def test_growth_and_ranking_are_present():
    """Đề bài cần cả 'tăng trưởng nhanh' và xếp hạng khu vực."""
    sql = _sql().lower()
    assert "mom_growth_rate" in sql, "thiếu tăng trưởng so với kỳ trước"
    assert "rank()" in sql, "thiếu xếp hạng khu vực"


def test_ranking_has_tie_breaker():
    """Xếp hạng chỉ theo doanh thu cho kết quả không tất định khi hoà."""
    sql = _sql()
    window = re.search(r"RANK\(\)\s*OVER\s*\(([^)]*)\)", sql, re.IGNORECASE)
    assert window, "không tìm thấy RANK() OVER"
    order = window.group(1).split("ORDER BY")[-1]
    assert order.count(",") >= 1, f"RANK thiếu khoá phá hoà: {window.group(1).strip()}"


def test_region_comes_from_scd2_current_version():
    """
    Địa bàn gắn theo version SCD2 đang hiệu lực tại cob_dt. Dùng version cũ sẽ
    gán merchant về thành phố nó đã rời đi — sai nghiệp vụ.
    """
    sql = _sql()
    assert "dim_merchant" in sql
    assert "BETWEEN dm.effective_from AND dm.effective_to" in sql, (
        "phải join SCD2 theo khoảng hiệu lực tại cob_dt"
    )


def test_no_zero_division_in_growth():
    """
    Kỳ trước doanh thu 0 → tăng trưởng phải NULL, không phải chia 0 và không
    giả vờ 'tăng 100%'.
    """
    sql = _sql().lower()
    assert "prev_revenue = 0 then null" in sql, "thiếu guard chia 0 cho MoM growth"


def test_serving_model_and_sync_are_registered():
    model = SERVING_DIR / "region_monthly_summary_current.sql"
    assert model.exists(), "thiếu serving model region_monthly_summary_current"

    cfg = yaml.safe_load(SYNC_CONFIG.read_text(encoding="utf-8"))
    declared = {t["name"]: t["key"] for t in cfg["tables"]}
    assert "region_monthly_summary_current" in declared
    assert declared["region_monthly_summary_current"] == ["city", "txn_year", "txn_month"], (
        "khoá upsert phải gồm chiều thời gian, nếu không các tháng đè lên nhau"
    )
