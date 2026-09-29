"""
Bảng MCC: chỉ mã thật, không trùng, và mọi nơi dùng mã đều trỏ vào bảng.

Lịch sử hai lỗi mà file này chặn:

1. Trùng khoá. mcc_code là PRIMARY KEY. Generator từng độn thêm mã bằng
   `random.randint(1000, 9999)` không kiểm trùng — seed hỏng ngẫu nhiên khoảng
   13% số lần chạy (`duplicate key … pk_mcc_code`). Bản sửa đầu bốc không hoàn
   lại, hết trùng nhưng vẫn là mã giả.
2. Mã giả. Config khai 28 mã, generator độn 81 mã số ngẫu nhiên cho đủ 109 —
   74% bảng là mã không tồn tại. `card_txn` còn dùng 5422 (không có trong bảng)
   và mã riêng của một hãng bay / hãng thuê xe / chuỗi khách sạn (3000, 3351,
   3501) như mã nhóm ngành.

Giờ số dòng = số mã khai trong seed_config; generator không bốc ngẫu nhiên nên
không cần chạy nhiều seed để bắt trùng — trùng là lỗi config, báo ngay.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "data_generator"))

from generators.card_crm import CARD_CATEGORY_MCC, generate_card_txn  # noqa: E402
from generators.digital_banking import (  # noqa: E402
    MERCHANT_CATEGORY_MCC,
    generate_mcc_codes,
    generate_merchants,
)

SEED_CONFIG = REPO_ROOT / "data_generator" / "config" / "seed_config.yaml"


@pytest.fixture(scope="module")
def config() -> dict:
    return yaml.safe_load(SEED_CONFIG.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def mcc_config(config) -> dict:
    return config["digital_banking"]["mcc_code"]


@pytest.fixture(scope="module")
def configured(mcc_config) -> list[str]:
    return [c["mcc"] for c in mcc_config["codes"]]


def test_configured_codes_are_real_category_codes(configured):
    assert len(configured) >= 80, "danh sách MCC bị rút gọn bất thường"
    assert len(configured) == len(set(configured)), "mã trùng trong seed_config"
    for code in configured:
        assert code.isdigit() and len(code) == 4, code
        # 3000–3999: mã riêng từng hãng bay / hãng thuê xe / chuỗi khách sạn.
        assert not 3000 <= int(code) <= 3999, f"{code} là mã của một doanh nghiệp, không phải nhóm ngành"
        # 7800–7802: xổ số / casino / đua do chính phủ cấp phép (bộ Xóm ghi sai).
        assert not 7800 <= int(code) <= 7802, code


@pytest.mark.parametrize("seed", [0, 1, 7, 12345])
def test_generator_emits_exactly_the_configured_codes(mcc_config, configured, seed):
    """Không độn thêm — với bất kỳ seed nào, số dòng và tập mã = config."""
    random.seed(seed)
    rows = generate_mcc_codes(mcc_config)
    assert [r[0] for r in rows] == configured


def test_generator_rejects_duplicate_codes():
    duplicated = {
        "codes": [
            {"mcc": "5411", "desc": "Grocery Stores and Supermarkets", "group": "RETAIL", "risk": 0},
            {"mcc": "5411", "desc": "Grocery again", "group": "RETAIL", "risk": 0},
        ]
    }
    with pytest.raises(ValueError, match="5411"):
        generate_mcc_codes(duplicated)


@pytest.mark.parametrize("mapping", [CARD_CATEGORY_MCC, MERCHANT_CATEGORY_MCC], ids=["card_txn", "merchant"])
def test_category_maps_point_into_the_table(mapping, configured):
    for category, codes in mapping.items():
        missing = set(codes) - set(configured)
        assert not missing, f"{category}: {sorted(missing)} không có trong bảng MCC"


def test_card_txn_mcc_matches_its_category(config, configured):
    """Mọi category cấu hình đều có mã, nên MCC luôn thuộc đúng category."""
    random.seed(3)
    cards = [(i, i, "DEBIT", "ACTIVE") for i in range(1, 21)]
    rows = generate_card_txn(500, config["card_crm"]["card_txn"], cards, configured)
    for row in rows:
        category, mcc = row[8], row[9]
        assert category in CARD_CATEGORY_MCC, f"category {category} chưa có MCC"
        assert mcc in CARD_CATEGORY_MCC[category], f"{category} nhận MCC {mcc}"


def test_merchant_mcc_is_in_the_table(config, configured):
    random.seed(5)
    rows = generate_merchants(200, config["digital_banking"]["merchant"], configured)
    assert {r[3] for r in rows} <= set(configured)
