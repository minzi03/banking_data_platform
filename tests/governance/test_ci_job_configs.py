"""
Mỗi lệnh `<job>.py --config <yml>` trong CI phải trỏ vào config đúng loại job.

dim_branch / dim_product chuyển SCD1 → SCD2 (2026-09-30), nhưng bước smoke G2 của
Trino Integration vẫn chạy `scd_type1.py --config dims/dim_branch.yml` và chết với
"Sai loại job" — chỉ lộ ra sau 6 phút dựng stack trong CI. Test tĩnh bắt trước.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
CALL = re.compile(r"code_etl/silver/base_job/(scd_type[12])\.py \\s*\n\s*--config (\S+\.yml)")
CALLS = CALL.findall(CI)


def test_calls_are_found():
    assert CALLS, "không tìm thấy lệnh scd_type*.py --config trong ci.yml — regex hỏng?"


@pytest.mark.parametrize(("job", "config"), CALLS)
def test_ci_config_matches_job_type(job, config):
    declared = yaml.safe_load((REPO_ROOT / config).read_text(encoding="utf-8"))["job"]["type"]
    assert declared == job, f"ci.yml chạy {job}.py với {config}, nhưng config là {declared}"
