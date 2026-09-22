"""
Phân phối số tiền giao dịch phải LỆCH PHẢI, không đều
======================================================

Bản đầu dùng `random.uniform(10_000, 500_000_000)`. Phân phối đều đặt median
đúng giữa dải — đo được **250.196.336** trên snapshot 2026-09-20.

Hệ quả cụ thể: `high_value_flag = txn_amount >= 200.000.000` gắn cờ **60%**
giao dịch. 200 triệu VND là ngưỡng hợp lý ngoài đời; rule không sai, dữ liệu
sai. Và "sửa" bằng cách nâng ngưỡng lên p99 (~495tr) sẽ cho một con số vô
nghĩa với ngân hàng thật.

Đối chiếu bộ dữ liệu thật (Xóm Bank, 157.224 giao dịch thẻ trong
`thamkhao/dataset_thamkhao/`): mean 43,72 vs median 31,14 — mean CAO HƠN
median, dấu hiệu kinh điển của lệch phải. `random.uniform` cho mean ≈ median.

Test ở đây giữ hình dạng phân phối, không chỉ giữ việc code chạy được.

Chạy: pytest tests/data_generator/test_amount_distribution.py -v
"""

from __future__ import annotations

import math
import random
import statistics
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "data_generator"))

from generators.amounts import (  # noqa: E402
    Z_P99,
    amount_sampler,
    lognormal_params,
    sample_amount,
)

TXN_PROFILE = {"median": 2_000_000, "p99": 200_000_000, "min": 10_000, "max": 500_000_000}
SAMPLE_SIZE = 40_000


@pytest.fixture(scope="module")
def samples() -> list[float]:
    """Mẫu cố định seed — test phân phối phải tất định, không đỏ ngẫu nhiên."""
    random.seed(20260922)
    sampler = amount_sampler({}, TXN_PROFILE)
    return sorted(sampler() for _ in range(SAMPLE_SIZE))


def _pct(values: list[float], p: float) -> float:
    return values[min(int(len(values) * p), len(values) - 1)]


class TestParameterDerivation:
    def test_median_maps_to_mu(self):
        mu, _ = lognormal_params(median=math.e, p99=math.e**3)
        assert mu == pytest.approx(1.0)

    def test_sigma_uses_the_p99_z_score(self):
        """sigma = (ln(p99) - mu) / z99. Dùng nhầm z của p90 (1.2816) sẽ làm
        đuôi phải rộng gần gấp đôi mà không test nào khác đỏ."""
        _, sigma = lognormal_params(median=1.0, p99=math.e)
        assert sigma == pytest.approx(1.0 / Z_P99)

    def test_z_score_is_the_99th_percentile(self):
        assert pytest.approx(2.3263, abs=1e-4) == Z_P99

    @pytest.mark.parametrize(
        "median,p99",
        [(0, 100), (-1, 100), (100, 100), (100, 50)],
        ids=["median_0", "median_negative", "p99_equals_median", "p99_below_median"],
    )
    def test_invalid_profiles_are_rejected(self, median: float, p99: float):
        with pytest.raises(ValueError):
            lognormal_params(median=median, p99=p99)

    def test_bad_profile_fails_at_build_time(self):
        """
        Profile sai phải nổ khi DỰNG sampler, không phải giữa vòng lặp sinh
        1,2 triệu bản ghi.
        """
        with pytest.raises(ValueError):
            amount_sampler({}, {"median": 100, "p99": 50, "min": 1, "max": 1000})


class TestDistributionShape:
    def test_median_matches_the_profile(self, samples: list[float]):
        assert _pct(samples, 0.50) == pytest.approx(TXN_PROFILE["median"], rel=0.06)

    def test_p99_matches_the_profile(self, samples: list[float]):
        assert _pct(samples, 0.99) == pytest.approx(TXN_PROFILE["p99"], rel=0.12)

    def test_mean_exceeds_median(self, samples: list[float]):
        """
        Dấu hiệu định nghĩa của lệch phải, và là thứ `random.uniform` KHÔNG có.

        Dữ liệu thật: mean 43,72 vs median 31,14.
        """
        mean = statistics.mean(samples)
        median = statistics.median(samples)
        assert mean > median * 2, (
            f"mean {mean:,.0f} không vượt xa median {median:,.0f} — "
            "phân phối không lệch phải, nghi ngờ đã quay về uniform"
        )

    def test_median_is_not_mid_range(self, samples: list[float]):
        """
        Chặn chính xác lỗi cũ: uniform cho median ≈ (min+max)/2 = 250tr.
        """
        mid_range = (TXN_PROFILE["min"] + TXN_PROFILE["max"]) / 2
        assert _pct(samples, 0.50) < mid_range * 0.1, "median nằm gần giữa dải — đó là dấu hiệu của random.uniform"

    def test_high_value_threshold_flags_about_one_percent(self, samples: list[float]):
        """
        Đây là lý do tồn tại của cả thay đổi này.

        Ngưỡng 200tr của `high_value_flag` phải gắn cờ ~1%, không phải 60%.
        """
        rate = sum(1 for v in samples if v >= 200_000_000) / len(samples)
        assert 0.003 <= rate <= 0.025, (
            f"high_value_flag sẽ gắn cờ {rate:.2%} giao dịch. Kỳ vọng ~1%; 60% là trạng thái lỗi cũ."
        )


class TestBounds:
    def test_never_below_minimum(self, samples: list[float]):
        assert samples[0] >= TXN_PROFILE["min"]

    def test_never_above_maximum(self, samples: list[float]):
        assert samples[-1] <= TXN_PROFILE["max"]

    def test_clipping_is_rare(self, samples: list[float]):
        """
        Kẹp làm méo đuôi — chấp nhận được CHỈ KHI hiếm.

        Nếu tỷ lệ kẹp lớn thì median/p99 đặt sai so với dải, và phân phối thực
        tế không còn là log-normal nữa.
        """
        clipped = sum(1 for v in samples if v >= TXN_PROFILE["max"]) / len(samples)
        assert clipped < 0.01, f"{clipped:.2%} mẫu bị kẹp ở max — profile lệch so với dải"

    def test_all_amounts_are_positive(self, samples: list[float]):
        """
        Dấu âm ở tầng này là SAI.

        Refund/reversal được biểu diễn bằng cách đảo dấu ở generator thẻ
        (`card_crm.py`), không phải bằng cách sinh ra số âm ở đây.
        """
        assert samples[0] > 0


class TestSamplerWiring:
    def test_config_profile_overrides_defaults(self):
        random.seed(1)
        sampler = amount_sampler(
            {"amount_profile": {"median": 1_000, "p99": 10_000, "min": 100}},
            TXN_PROFILE,
        )
        values = sorted(sampler() for _ in range(5_000))
        assert _pct(values, 0.50) == pytest.approx(1_000, rel=0.15)

    def test_clip_range_must_contain_the_median(self):
        """
        Phát hiện khi viết chính test ở trên: override `median` xuống 1.000
        nhưng giữ `min` = 10.000 làm MỌI mẫu bị kẹp lên đúng 10.000. Sampler
        vẫn trả số hợp lệ, phân phối thì đã biến thành hằng số, và không gì
        báo. Giờ nó nổ ngay lúc dựng.
        """
        with pytest.raises(ValueError, match="bao median"):
            amount_sampler({"amount_profile": {"median": 1_000, "p99": 10_000}}, TXN_PROFILE)

    def test_missing_profile_falls_back_to_defaults(self):
        """
        Config cũ chỉ có `amount_range` vẫn phải seed được.

        Nổ ở đây nghĩa là mọi môi trường chưa cập nhật config sẽ không seed
        nổi — một thay đổi dữ liệu không nên phá công cụ dựng dữ liệu.
        """
        random.seed(2)
        sampler = amount_sampler({}, TXN_PROFILE)
        assert sampler() > 0

    def test_rounds_to_two_decimals(self):
        random.seed(3)
        value = sample_amount(2_000_000, 200_000_000, minimum=10_000, maximum=500_000_000)
        assert round(value, 2) == value
