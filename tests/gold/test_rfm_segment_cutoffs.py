"""
Ngưỡng segment RFM phải đúng KPI dictionary của đề bài (nhóm 6):
Champions >= 13, Loyal >= 10, Potential >= 7, New >= 5, At Risk >= 3,
Hibernating >= 2, còn lại Lost.

Template khoá học dùng New >= 6 / At Risk >= 4 — lệch đặc tả, đổi danh sách khách của
campaign (TD-20, chốt 2026-09-30). Hai model tính segment phải cùng một bảng ngưỡng.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
MODELS = [
    "code_etl/gold/mart360/customer_360.yml",
    "code_etl/gold/segmentation/rfm_segment.yml",
]
DICTIONARY = [
    (13, "Champions"),
    (10, "Loyal Customers"),
    (7, "Potential Loyalists"),
    (5, "New Customers"),
    (3, "At Risk"),
    (2, "Hibernating"),
]


def _cutoffs(path: str) -> list[tuple[int, str]]:
    sql = yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8"))["sql"]
    sql = re.sub(r"--[^\n]*", "", sql)
    # Chỉ khối CASE kết thúc bằng `END AS rfm_segment` (customer_360 còn CASE aum_bucket…).
    block = re.search(r"CASE((?:(?!CASE).)*?)END\s+AS\s+rfm_segment", sql, re.DOTALL).group(1)
    return [(int(n), seg) for n, seg in re.findall(r">=\s*(\d+)\s+THEN\s+'([A-Za-z ]+)'", block)]


@pytest.mark.parametrize("path", MODELS)
def test_segment_cutoffs_follow_the_kpi_dictionary(path):
    assert _cutoffs(path) == DICTIONARY


@pytest.mark.parametrize("path", MODELS)
def test_everything_below_is_lost(path):
    sql = yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8"))["sql"]
    assert re.search(r"ELSE\s+'Lost'\s+END\s+AS\s+rfm_segment", sql)
