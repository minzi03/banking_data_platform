"""
Contract Test — ba bảng serving real-time của Phase 2 (khoá new2 §3.2, §5).
============================================================================

Đề bài liệt kê ba use case Phase 2 và gọi chúng là **sản phẩm bàn giao**:

  1. "Thông báo tiền về: mỗi giao dịch QR thành công được ghi vào bảng thông
      báo theo merchant trong vài giây."
  2. "Doanh thu theo cửa hàng theo phút: phục vụ dashboard real-time."
  3. "Giờ cao điểm: số giao dịch và doanh thu theo khung giờ trong ngày."

Trước tính năng này KHÔNG có bảng nào trong ba bảng trên — platform có hạ tầng
streaming nhưng chưa có sản phẩm cuối mà đề bài yêu cầu. Test này chốt:

  1. Ba dbt model tồn tại và dựng từ silver.fact_online_transaction.
  2. Ba bảng có trong serving_sync.yml với khoá upsert gồm cả chiều thời gian.
  3. Bảng giờ cao điểm quy đổi giờ ICT TƯỜNG MINH (bug timezone đã gặp ở Gold).
  4. role serving_builder đọc được Silver nhưng PII vẫn bị che.

Chạy: pytest tests/governance/test_realtime_serving_tables.py -v
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SERVING_DIR = PROJECT_ROOT / "dbt" / "models" / "serving"
SYNC_CONFIG = PROJECT_ROOT / "code_etl" / "serving" / "config" / "serving_sync.yml"

REALTIME_MODELS = {
    "merchant_payment_notification_current": ["merchant_id"],
    "merchant_revenue_per_minute_current": ["merchant_id", "txn_minute"],
    "merchant_peak_hours_current": ["merchant_id", "txn_hour"],
}


def _strip_comments(text: str) -> str:
    return re.sub(r"--[^\n]*", "", text)


def test_all_three_realtime_models_exist():
    """Ba use case Phase 2 phải có đủ ba model — không thiếu cái nào."""
    missing = [name for name in REALTIME_MODELS if not (SERVING_DIR / f"{name}.sql").exists()]
    assert not missing, f"thiếu serving model real-time: {missing}"


def test_realtime_models_read_the_transaction_fact():
    """
    Grain theo giao dịch/phút/giờ chỉ dựng được từ fact chi tiết. Dựng từ Gold
    marts (aggregate ngày) sẽ cho kết quả sai — nên test chốt nguồn.
    """
    for name in REALTIME_MODELS:
        sql = _strip_comments((SERVING_DIR / f"{name}.sql").read_text(encoding="utf-8"))
        assert "fact_online_transaction" in sql, f"{name}: phải đọc silver.fact_online_transaction"


def test_realtime_tables_registered_for_sync():
    """Cả ba bảng phải được sync sang PostgreSQL, đúng khoá upsert."""
    cfg = yaml.safe_load(SYNC_CONFIG.read_text(encoding="utf-8"))
    declared = {t["name"]: t["key"] for t in cfg["tables"]}
    for name, expected_key in REALTIME_MODELS.items():
        assert name in declared, f"{name} chưa có trong serving_sync.yml"
        assert declared[name] == expected_key, (
            f"{name}: khoá upsert {declared[name]} != {expected_key} — thiếu chiều thời gian "
            "sẽ ghi đè nhiều dòng về một"
        )


def test_time_grain_keys_include_a_time_dimension():
    """
    Bảng theo phút/giờ mà khoá chỉ có merchant_id thì mọi khung giờ đè lên nhau.
    """
    cfg = yaml.safe_load(SYNC_CONFIG.read_text(encoding="utf-8"))
    declared = {t["name"]: t["key"] for t in cfg["tables"]}
    for name in ("merchant_revenue_per_minute_current", "merchant_peak_hours_current"):
        assert len(declared[name]) >= 2, f"{name}: khoá upsert phải gồm merchant_id + chiều thời gian"


def test_peak_hours_converts_to_business_timezone():
    """
    Cửa sổ 1 GIỜ phụ thuộc múi giờ. Gom theo UTC sẽ khiến giờ cao điểm
    (tan tầm ~17–19h ICT) hiện ra là 10–12h. Quy đổi phải tường minh.
    """
    sql = _strip_comments((SERVING_DIR / "merchant_peak_hours_current.sql").read_text(encoding="utf-8"))
    assert "Asia/Ho_Chi_Minh" in sql, "peak-hours chưa quy đổi múi giờ nghiệp vụ"


def test_payment_notification_is_one_row_per_merchant():
    """Bảng thông báo phải 1 dòng/merchant (badge mới nhất), không phải log mọi giao dịch."""
    sql = _strip_comments((SERVING_DIR / "merchant_payment_notification_current.sql").read_text(encoding="utf-8"))
    assert "row_number()" in sql.lower(), "phải chọn giao dịch mới nhất mỗi merchant"
    assert REALTIME_MODELS["merchant_payment_notification_current"] == ["merchant_id"]


def test_serving_builder_can_read_silver_but_pii_stays_masked():
    """
    dbt cần Silver fact để dựng 3 bảng real-time. Kiểm tra quyền được mở ĐÚNG
    phạm vi schema và mask PII vẫn được gắn.
    """
    from governance.rbac import ROLES

    perms = ROLES["serving_builder"].permissions
    paths = {p.resource_path for p in perms}
    assert "iceberg.silver.*" in paths, "serving_builder cần đọc Silver fact"

    masked = {p.resource_path for p in perms if p.column_mask}
    assert "iceberg.silver.dim_merchant.merchant_name" in masked, "dbt đọc được Silver nhưng merchant_name phải bị che"
    assert "iceberg.silver.dim_customer.full_name" in masked, "dbt đọc được Silver nhưng full_name khách phải bị che"
