"""
`fraud_reason` phải khớp ĐÚNG điều kiện generator đã mô phỏng (ROADMAP 2.4)
============================================================================

Nhãn fraud chỉ có giá trị kiểm chứng khi mỗi nhãn nói đúng điều đã xảy ra.
Generator mô phỏng đúng hai điều kiện — đổi sang location rủi ro cao, và đẩy
amount lên vùng cao — nên chỉ có bốn nhãn hợp lệ.

Hai lỗi đã đo được ở bản trước, test này chặn cả hai:

1. Fraud không kích hoạt điều kiện nào (~39%) nhận nhãn mô tả chọn ngẫu nhiên:
   "Unusual location" chỉ 5,8% ở location rủi ro cao — bằng tỷ lệ nền.
2. Khi cả hai điều kiện cùng kích hoạt, chọn ngẫu nhiên MỘT nhãn làm 25,5% dòng
   `UNUSUAL_LOCATION` mang amount đã bị đẩy lên.

Chạy: pytest tests/data_generator/test_fraud_reason.py -v
"""

from __future__ import annotations

import collections
import random
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "data_generator"))

from generators.digital_banking import generate_online_transactions  # noqa: E402

AMOUNT_MAX = 100_000_000
HIGH_AMOUNT_FLOOR = AMOUNT_MAX * 0.6  # nhánh HIGH_AMOUNT: uniform(0.6·max, max)
HIGH_RISK = set(range(1, 11))  # 10 / 200 location → tỷ lệ nền 5%
ALL_LOCATIONS = list(range(1, 201))

# Vị trí cột trong tuple generator trả về.
LOCATION, AMOUNT, IS_FRAUD, REASON = 3, 7, 9, 10

VALID_REASONS = {"HIGH_AMOUNT", "UNUSUAL_LOCATION", "HIGH_AMOUNT+UNUSUAL_LOCATION", "UNSPECIFIED"}


def _generate(fraud_rate: float, count: int, seed: int) -> list[tuple]:
    random.seed(seed)
    config = {
        "fraud_rate": fraud_rate,
        "type_distribution": {"TRANSFER": 1},
        "channel_distribution": {"MOBILE": 1},
        "status_distribution": {"SUCCESS": 1},
        "amount_range": [10_000, AMOUNT_MAX],
    }
    return generate_online_transactions(
        count,
        config,
        customer_ids=list(range(1, 500)),
        device_ids=list(range(1, 50)),
        location_ids=ALL_LOCATIONS,
        high_risk_location_ids=sorted(HIGH_RISK),
    )


@pytest.fixture(scope="module")
def by_reason() -> dict[str, list[tuple]]:
    """Mọi dòng đều fraud, seed cố định — test phải tất định."""
    groups: dict[str, list[tuple]] = collections.defaultdict(list)
    for row in _generate(fraud_rate=1.0, count=20_000, seed=20260923):
        groups[row[REASON]].append(row)
    return groups


def _share(rows: list[tuple], predicate) -> float:
    return sum(1 for r in rows if predicate(r)) / len(rows)


def test_only_the_four_simulated_reasons_exist(by_reason):
    """Không còn nhãn mô tả nào gán nguyên nhân generator không mô phỏng."""
    assert set(by_reason) == VALID_REASONS


def test_every_reason_is_actually_populated(by_reason):
    """Guard chống pass rỗng: một nhánh chết sẽ làm các test dưới xanh vô nghĩa."""
    for reason in VALID_REASONS:
        assert len(by_reason[reason]) >= 500, f"{reason}: chỉ {len(by_reason[reason])} dòng"


def test_high_amount_means_amount_was_raised(by_reason):
    for reason in ("HIGH_AMOUNT", "HIGH_AMOUNT+UNUSUAL_LOCATION"):
        rows = by_reason[reason]
        assert all(r[AMOUNT] >= HIGH_AMOUNT_FLOOR for r in rows), reason


def test_unusual_location_means_location_was_moved(by_reason):
    for reason in ("UNUSUAL_LOCATION", "HIGH_AMOUNT+UNUSUAL_LOCATION"):
        rows = by_reason[reason]
        assert all(r[LOCATION] in HIGH_RISK for r in rows), reason


def test_single_reason_labels_do_not_leak_the_other_condition(by_reason):
    """
    Lỗi 2 của bản trước: 25,5% dòng `UNUSUAL_LOCATION` có amount bị đẩy lên.
    Giờ nhãn đơn chỉ được mang tỷ lệ nền của điều kiện còn lại.
    """
    raised = _share(by_reason["UNUSUAL_LOCATION"], lambda r: r[AMOUNT] >= HIGH_AMOUNT_FLOOR)
    assert raised < 0.03, f"UNUSUAL_LOCATION: {raised:.1%} dòng có amount vùng cao"

    moved = _share(by_reason["HIGH_AMOUNT"], lambda r: r[LOCATION] in HIGH_RISK)
    assert moved < 0.10, f"HIGH_AMOUNT: {moved:.1%} dòng ở location rủi ro cao (nền 5%)"


def test_unspecified_carries_neither_signal(by_reason):
    """Lỗi 1 của bản trước: fraud không qua điều kiện nào mang nhãn nguyên nhân."""
    rows = by_reason["UNSPECIFIED"]
    assert _share(rows, lambda r: r[AMOUNT] >= HIGH_AMOUNT_FLOOR) < 0.03
    assert _share(rows, lambda r: r[LOCATION] in HIGH_RISK) < 0.10


def test_non_fraud_rows_have_no_reason():
    rows = _generate(fraud_rate=0.0, count=2_000, seed=1)
    assert all(r[IS_FRAUD] == 0 and r[REASON] is None for r in rows)
