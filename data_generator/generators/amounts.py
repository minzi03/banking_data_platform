"""
Sinh số tiền giao dịch theo phân phối log-normal.

VÌ SAO KHÔNG DÙNG random.uniform
=================================

Bản đầu sinh `txn_amount = random.uniform(10_000, 500_000_000)`. Phân phối đều
trên toàn dải nghĩa là **median rơi đúng giữa dải** — đo được 250.196.336 trên
snapshot 2026-09-20.

Hệ quả không nằm ở thẩm mỹ. Nó làm hỏng mọi rule có ngưỡng theo số tiền:

    high_value_flag = txn_amount >= 200.000.000   →  gắn cờ 60% giao dịch

200 triệu VND là ngưỡng hợp lý với ngân hàng thật. Rule không sai — **dữ liệu
sai**. Và nâng ngưỡng lên p99 (~495tr) để "sửa" tỷ lệ sẽ cho một con số vô
nghĩa ngoài đời.

Chi tiêu thật lệch phải mạnh: rất nhiều giao dịch nhỏ, rất ít giao dịch lớn.
Đo trên bộ tham khảo Xóm Bank (`thamkhao/dataset_thamkhao/`, 157.224 giao dịch
thẻ thật): mean 43,72 vs median 31,14 — mean cao hơn median, dấu hiệu kinh
điển của phân phối lệch phải. `random.uniform` cho mean ≈ median, tức sai hẳn
về hình dạng.

Log-normal là mô hình chuẩn cho số tiền giao dịch: log của nó phân phối chuẩn,
nên giá trị luôn dương và đuôi phải dài tự nhiên.

THAM SỐ HOÁ BẰNG MEDIAN VÀ P99
==============================

Khai báo bằng `mu`/`sigma` thì không ai đọc ra được ý nghĩa. Ở đây khai báo
bằng hai điểm nghiệp vụ đọc được ngay:

    median  — giao dịch điển hình
    p99     — ngưỡng "giao dịch lớn"

rồi suy ngược ra tham số:

    mu    = ln(median)
    sigma = (ln(p99) - mu) / 2.3263   (2.3263 = z-score của phân vị 99%)

Nhờ vậy, đặt `p99 = 200_000_000` làm `high_value_flag` gắn cờ đúng ~1% — và
ngưỡng 200tr giữ nguyên ý nghĩa nghiệp vụ của nó.
"""

from __future__ import annotations

import math
import random
from collections.abc import Callable

# z-score của phân vị 99% trong phân phối chuẩn chuẩn tắc.
# scipy.stats.norm.ppf(0.99) = 2.3263478740408408 — hardcode để generator
# không phải phụ thuộc scipy.
Z_P99 = 2.3263478740408408


def lognormal_params(median: float, p99: float) -> tuple[float, float]:
    """
    Suy (mu, sigma) của log-normal từ median và phân vị 99.

    median = exp(mu)                →  mu = ln(median)
    p99    = exp(mu + z99 * sigma)  →  sigma = (ln(p99) - mu) / z99
    """
    if median <= 0 or p99 <= median:
        raise ValueError(f"cần 0 < median < p99, nhận median={median}, p99={p99}")
    mu = math.log(median)
    sigma = (math.log(p99) - mu) / Z_P99
    return mu, sigma


def sample_amount(
    median: float,
    p99: float,
    *,
    minimum: float,
    maximum: float,
    ndigits: int = 2,
) -> float:
    """
    Một số tiền log-normal, kẹp trong [minimum, maximum].

    Kẹp làm méo đuôi phân phối — đó là đánh đổi có chủ ý. `maximum` giữ dữ liệu
    trong dải mà schema và các mô hình downstream đã giả định; nếu không kẹp,
    đuôi log-normal thỉnh thoảng sinh ra số vượt xa mọi giao dịch thật.

    Với median/p99 đặt hợp lý, tỷ lệ bị kẹp rất nhỏ nên hình dạng phân phối
    gần như không đổi.
    """
    mu, sigma = lognormal_params(median, p99)
    value = random.lognormvariate(mu, sigma)
    return round(min(max(value, minimum), maximum), ndigits)


def amount_sampler(config: dict, defaults: dict) -> Callable[[], float]:
    """
    Dựng hàm sinh số tiền từ config, có fallback sang `defaults`.

    Config dùng khoá `amount_profile`:

        amount_profile:
          median: 2000000
          p99:  200000000
          min:     10000
          max:  500000000

    Nếu thiếu `amount_profile`, rơi về `defaults` — nhờ đó generator vẫn chạy
    với config cũ chỉ có `amount_range`, thay vì nổ lúc seed.
    """
    profile = {**defaults, **(config.get("amount_profile") or {})}
    median = float(profile["median"])
    p99 = float(profile["p99"])
    minimum = float(profile["min"])
    maximum = float(profile["max"])

    # Kiểm tham số NGAY lúc dựng, không đợi tới lần gọi đầu. Một profile sai
    # sẽ hỏng ở dòng cấu hình chứ không phải ở giữa vòng lặp sinh 1,2 triệu bản
    # ghi.
    lognormal_params(median, p99)

    # Dải kẹp phải BAO median, nếu không phân phối sụp đổ trong im lặng.
    #
    # Ví dụ thật gặp phải: override `median` xuống 1.000 nhưng quên `min` (vẫn
    # 10.000) — mọi mẫu bị kẹp lên đúng 10.000, sampler vẫn trả số hợp lệ, và
    # không gì báo rằng phân phối đã biến thành hằng số.
    if not minimum < median < maximum:
        raise ValueError(
            f"dải kẹp phải bao median: cần min < median < max, nhận "
            f"min={minimum:,.0f}, median={median:,.0f}, max={maximum:,.0f}. "
            "Đổi median/p99 mà quên đổi min/max là nguyên nhân thường gặp."
        )

    def sample() -> float:
        return sample_amount(median, p99, minimum=minimum, maximum=maximum)

    return sample
