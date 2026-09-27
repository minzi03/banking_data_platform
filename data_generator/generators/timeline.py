"""
Mốc thời gian của dữ liệu sinh ra — neo mọi ngày vào `as_of`.

Các generator viết ngày dưới dạng literal ("2025-06-01", "2026-08-01"…). Những
literal đó được viết quanh một mốc ngầm: giao dịch mới nhất là REFERENCE_AS_OF
(2026-08-01 — cũng là `today` của lịch trả nợ). Trước đây mốc này cố định, còn
`cob_dt` thì là ngày chạy pipeline, nên hai thứ trôi xa nhau: snapshot
2026-09-22 có giao dịch mới nhất là 2026-08-03, cách 50 ngày — mọi KPI "30 ngày
gần nhất" bằng 0, không khách nào ở mức churn Active.

`shift()` dời một literal đi đúng `as_of − REFERENCE_AS_OF` ngày. Mọi quan hệ
tương đối giữ nguyên (thẻ phát hành trước giao dịch, giải ngân trước kỳ trả,
tuổi khách hàng), và giao dịch mới nhất rơi vào `as_of`.

Chỉ bọc `shift()` quanh LITERAL. Ngày đã sinh ra (vd. `open_date`) đã được dời
sẵn — dời lần nữa là dời hai lần.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

REFERENCE_AS_OF = date(2026, 8, 1)

_state = {"offset": timedelta(0)}


def set_as_of(as_of: date) -> None:
    """Đặt mốc cho lượt sinh này. Gọi một lần, trước khi sinh bất kỳ bảng nào."""
    _state["offset"] = as_of - REFERENCE_AS_OF


def current_as_of() -> date:
    return REFERENCE_AS_OF + _state["offset"]


def shift(date_str: str) -> str:
    """Dời một ngày literal `YYYY-MM-DD` theo mốc hiện tại."""
    return (datetime.strptime(date_str, "%Y-%m-%d") + _state["offset"]).strftime("%Y-%m-%d")


def shift_dt(value: datetime) -> datetime:
    """Dời một datetime literal (vd. `today` của lịch trả nợ) theo mốc hiện tại."""
    return value + _state["offset"]
