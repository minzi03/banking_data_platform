"""
Tests for the QR/POS merchant fields on online_transaction.

Khoá new2 (Merchant Acquiring): chỉ một phần giao dịch đi qua merchant; nhóm đó
có payment_channel QR/POS; refund chỉ xảy ra trên giao dịch thành công qua
merchant; nhóm không qua merchant phải để merchant_id/payment_channel NULL.
"""

import random
from collections import Counter
from pathlib import Path

import pytest
import yaml

from data_generator.generators import digital_banking

CONFIG = yaml.safe_load(
    (Path(__file__).resolve().parents[2] / "data_generator" / "config" / "seed_config.yaml").read_text(encoding="utf-8")
)

# Vị trí cột trong tuple trả về của generate_online_transactions
IDX = {
    "transaction_id": 0,
    "status": 11,
    "merchant_id": 12,
    "payment_channel": 13,
    "refund_flag": 14,
}

CFG = CONFIG["digital_banking"]["online_transaction"]


@pytest.fixture
def rows():
    random.seed(7)
    return digital_banking.generate_online_transactions(
        2000,
        CFG,
        customer_ids=[1, 2, 3],
        device_ids=[1],
        location_ids=[1],
        merchant_ids=list(range(1, 51)),
    )


def test_new_columns_are_present(rows):
    """Tuple phải có đủ 3 cột mới ở đúng vị trí."""
    sample = rows[0]
    assert len(sample) == 18, f"kỳ vọng 18 cột, có {len(sample)}"
    assert sample[IDX["merchant_id"]] is None or isinstance(sample[IDX["merchant_id"]], int)
    assert sample[IDX["payment_channel"]] in (None, "QR", "POS")
    assert sample[IDX["refund_flag"]] in (0, 1)


def test_merchant_payment_rate_is_roughly_honoured(rows):
    """Tỷ lệ giao dịch qua merchant phải gần merchant_payment_rate."""
    with_merchant = sum(1 for r in rows if r[IDX["merchant_id"]] is not None)
    share = with_merchant / len(rows)
    target = CFG["merchant_payment_rate"]
    assert abs(share - target) < 0.06, f"share={share:.3f} lệch xa target={target}"


def test_payment_channel_only_present_with_merchant(rows):
    """Bất biến: có payment_channel ⟺ có merchant_id."""
    for r in rows:
        has_merchant = r[IDX["merchant_id"]] is not None
        has_channel = r[IDX["payment_channel"]] is not None
        assert has_merchant == has_channel, r


def test_channel_values_are_valid(rows):
    for r in rows:
        assert r[IDX["payment_channel"]] in (None, "QR", "POS")


def test_qr_share_is_roughly_honoured(rows):
    channels = Counter(r[IDX["payment_channel"]] for r in rows if r[IDX["payment_channel"]])
    qr_share = channels["QR"] / (channels["QR"] + channels["POS"])
    assert abs(qr_share - CFG["qr_share"]) < 0.08, f"qr_share={qr_share:.3f}"


def test_refund_only_on_successful_merchant_txn(rows):
    """Refund chỉ xảy ra khi: có merchant VÀ status SUCCESS."""
    for r in rows:
        if r[IDX["refund_flag"]] == 1:
            assert r[IDX["merchant_id"]] is not None, "refund không có merchant"
            assert r[IDX["status"]] == "SUCCESS", "refund trên giao dịch không thành công"


def test_no_merchant_ids_given_yields_all_null(rows):
    """Không truyền merchant_ids → mọi giao dịch không qua merchant (giữ hành vi cũ)."""
    random.seed(1)
    out = digital_banking.generate_online_transactions(
        200, CFG, customer_ids=[1], device_ids=[1], location_ids=[1], merchant_ids=None
    )
    assert all(r[IDX["merchant_id"]] is None for r in out)
    assert all(r[IDX["payment_channel"]] is None for r in out)
    assert all(r[IDX["refund_flag"]] == 0 for r in out)
