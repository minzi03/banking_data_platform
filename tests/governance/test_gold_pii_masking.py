"""
Contract Test — PII phải được che TRƯỚC khi ra Gold (khoá new2 §4.3).
=====================================================================

Đề bài: *"Bảo mật: PII (tên, SĐT chủ merchant) được mask trước khi ra Gold."*

Bug đã gặp: ba mart merchant (`mart_merchant_360`, `mart_merchant_daily_summary`,
`mart_merchant_ranking`) xuất `merchant_name` NGUYÊN BẢN ra Gold. Với hộ kinh
doanh, tên chủ merchant chính là dữ liệu cá nhân — cùng loại với `full_name`
của khách hàng, vốn đã được che từ lâu thành `full_name_masked`.

Test này chốt hợp đồng đó, để không tái diễn:
  1. Không mart merchant nào SELECT `merchant_name` trần.
  2. Ba mart merchant phải có `merchant_name_masked`.
  3. DDL Gold phải khai `merchant_name_masked` và KHÔNG có `merchant_name`.
  4. Serving thừa hưởng qua `select *` (không tự SELECT tên gốc).

Chạy: pytest tests/governance/test_gold_pii_masking.py -v
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_ROOT / "code_etl" / "gold"
GOLD_DDL = PROJECT_ROOT / "docker" / "init_iceberg" / "03_ddl_gold.sql"
SERVING_DIR = PROJECT_ROOT / "dbt" / "models" / "serving"

MERCHANT_MARTS = {
    "mart360/merchant_360.yml",
    "time_analytics/merchant_daily_summary.yml",
    "time_analytics/merchant_ranking.yml",
}


def _sql(rel: str) -> str:
    cfg = yaml.safe_load((GOLD_DIR / rel).read_text(encoding="utf-8"))
    # Bỏ comment để không bắt oan phần giải thích có chữ merchant_name.
    return re.sub(r"--[^\n]*", "", cfg["sql"])


def test_merchant_marts_exist():
    """Guard: nếu đường dẫn đổi, test dưới sẽ pass rỗng một cách vô nghĩa."""
    for rel in MERCHANT_MARTS:
        assert (GOLD_DIR / rel).exists(), f"thiếu mart {rel}"


# Biểu thức che hợp lệ: tên gốc được PHÉP xuất hiện bên trong SPLIT(...) vì
# kết quả bị cắt còn ký tự đầu. Mọi xuất hiện KHÁC là rò rỉ.
_MASK_EXPR = re.compile(r"SPLIT\(\s*(\w+\.)?merchant_name\s*,", re.IGNORECASE)


def _final_select(sql: str) -> str:
    """
    Đoạn SELECT cuối cùng (cái ghi ra Gold), không tính các CTE trung gian.

    CTE `base` đọc `merchant_name` từ Silver là HỢP LỆ — tên gốc phải tồn tại ở
    Silver để còn che. Chỉ SELECT cuối mới quyết định Gold chứa gì.
    """
    idx = sql.rfind("SELECT")
    return sql[idx:] if idx >= 0 else sql


def test_merchant_marts_do_not_select_raw_name():
    """
    SELECT cuối của mart merchant không được xuất `merchant_name` trần.

    `merchant_name` bên trong `SPLIT(merchant_name, ' ')` là hợp lệ — đó chính
    là biểu thức che. Bỏ các chỗ đó ra rồi mới soi phần còn lại.
    """
    violations = []
    for rel in sorted(MERCHANT_MARTS):
        tail = _final_select(_sql(rel))
        stripped = _MASK_EXPR.sub("SPLIT(<masked>", tail)
        for m in re.finditer(r"\b(\w+\.)?merchant_name\b(?!_masked)", stripped, re.IGNORECASE):
            violations.append(f"{rel}: {m.group(0)}")
    assert not violations, "Gold xuất tên merchant NGUYÊN BẢN — vi phạm §4.3 (PII mask trước Gold):\n  " + "\n  ".join(
        violations
    )


def test_masking_expression_shape_is_expected():
    """Biểu thức che phải giữ ký tự đầu — nếu đổi sang lộ toàn bộ thì test phải bắt."""
    for rel in sorted(MERCHANT_MARTS):
        sql = _sql(rel)
        assert _MASK_EXPR.search(sql), f"{rel}: không thấy biểu thức SPLIT(...) che tên"
        assert "merchant_name_masked" in sql


def test_merchant_marts_expose_masked_name():
    """Cả ba mart merchant phải có `merchant_name_masked`."""
    missing = [rel for rel in sorted(MERCHANT_MARTS) if "merchant_name_masked" not in _sql(rel)]
    assert not missing, f"mart thiếu cột đã che: {missing}"


def test_gold_ddl_declares_masked_column():
    """DDL Gold phải khai merchant_name_masked cho cả 3 bảng, và không có tên gốc."""
    ddl = GOLD_DDL.read_text(encoding="utf-8")
    for table in ("mart_merchant_daily_summary", "mart_merchant_ranking", "mart_merchant_360"):
        block = ddl.split(f"lakehouse.gold.{table}", 1)
        assert len(block) == 2, f"thiếu CREATE TABLE cho {table}"
        body = block[1].split("USING iceberg", 1)[0]
        assert "merchant_name_masked" in body, f"{table}: DDL thiếu merchant_name_masked"
        assert not re.search(r"\bmerchant_name\b(?!_masked)", body), f"{table}: DDL còn khai merchant_name gốc"


def test_serving_merchant_models_inherit_masking():
    """
    Serving model phải dùng `select *` (thừa hưởng cột đã che), KHÔNG tự
    SELECT merchant_name trần — nếu không, masking ở Gold bị vô hiệu.
    """
    for path in sorted(SERVING_DIR.glob("merchant_*_current.sql")):
        text = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8"))
        assert not re.search(r"\b(\w+\.)?merchant_name\b(?!_masked)", text), (
            f"{path.name}: serving SELECT tên merchant gốc — masking Gold bị vô hiệu"
        )


def test_pii_inventory_records_merchant_masking():
    """Bản kiểm kê phải ghi merchant_name_masked — nếu không, tài liệu lệch code."""
    inventory = (PROJECT_ROOT / "docs" / "06-security-compliance" / "PII_INVENTORY.md").read_text(encoding="utf-8")
    assert "merchant_name_masked" in inventory, "PII_INVENTORY chưa ghi nhận merchant_name_masked"
