"""
mcc_code là PRIMARY KEY của digital_banking.mcc_code, nên generator không được
sinh trùng — kể cả một lần trong nhiều lần chạy.

Bug đã gặp trên CI: mỗi mã lấy bằng `random.randint(1000, 9999)` độc lập, không
kiểm trùng. Với ~50 mã bốc từ 9000 giá trị, xác suất đụng khoảng 13% mỗi lần
chạy, nên seed hỏng ngẫu nhiên: hai run xanh rồi run thứ ba chết với
`duplicate key value violates unique constraint "pk_mcc_code"`.

Một test chạy generator ĐÚNG MỘT LẦN sẽ pass khoảng 87% số lần — tức là gần như
vô dụng. Test dưới đây chạy nhiều lần để một implementation dựa vào may mắn
gần như chắc chắn bị bắt.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "data_generator"))

from generators.digital_banking import generate_mcc_codes  # noqa: E402

TARGET_ROW_COUNT = 109


def _codes(config: dict) -> list[str]:
    return [row[0] for row in generate_mcc_codes(config)]


@pytest.mark.parametrize("seed", range(40))
def test_codes_are_unique_across_many_seeds(seed):
    """
    40 seed khác nhau. Nếu generator quay lại bốc ngẫu nhiên có hoàn lại, xác
    suất cả 40 lần đều không trùng là 0.87^40 ≈ 0.4% — coi như chắc chắn đỏ.
    """
    random.seed(seed)
    codes = _codes({"codes": []})
    duplicates = {c for c in codes if codes.count(c) > 1}
    assert not duplicates, f"seed={seed}: mã trùng {sorted(duplicates)}"


def test_generated_codes_do_not_collide_with_configured_codes():
    """Mã sinh thêm không được đụng mã đã khai báo trong seed_config."""
    random.seed(12345)
    configured = [
        {"mcc": "5411", "desc": "Grocery Stores", "group": "RETAIL", "risk": 0},
        {"mcc": "5812", "desc": "Restaurants", "group": "FOOD", "risk": 0},
    ]
    codes = _codes({"codes": configured})
    assert len(codes) == len(set(codes))
    assert codes[: len(configured)] == [c["mcc"] for c in configured]


def test_row_count_is_preserved():
    """Sửa tính duy nhất không được đổi số dòng sinh ra."""
    random.seed(7)
    assert len(_codes({"codes": []})) == TARGET_ROW_COUNT
