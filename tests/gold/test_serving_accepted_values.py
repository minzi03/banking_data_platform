"""
`accepted_values` của dbt serving phải chứa MỌI giá trị mà SQL Gold sinh ra
=========================================================================

`accepted_values` chỉ đỏ khi dữ liệu thật chạm vào giá trị thiếu. Hai danh sách
đã sai từ lâu mà không ai biết, vì dữ liệu chưa từng chạm:

- `churn_risk` thiếu 'Active' — nhánh ELSE của churn_prediction. Suốt thời gian
  ngày giao dịch trôi khỏi cob_dt (TD-16) không khách nào Active, nên test xanh.
  Seed lại đúng ngày: 9.655 khách Active, `dbt build` đỏ.
- `aum_bucket` thiếu 'VIP' (AUM ≥ 5 tỷ) nhưng vẫn giữ '0-50M', '1B+'… mà SQL
  không còn sinh. Đỏ khi seed mới có đúng một khách VIP.

Test này so trực tiếp: literal THEN/ELSE của `CASE … END AS <cột>` trong SQL Gold
phải nằm trong `accepted_values` của cột cùng tên ở serving. Không cần dữ liệu.

Chạy: pytest tests/gold/test_serving_accepted_values.py -v
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = REPO_ROOT / "code_etl" / "gold"
SERVING_YML = REPO_ROOT / "dbt" / "models" / "serving" / "_serving_models.yml"

_TOKEN = re.compile(r"\bCASE\b|\bEND\b|\b(?:THEN|ELSE)\s+'([^']*)'", re.IGNORECASE)


def _case_literals(sql: str, column: str) -> set[str]:
    """Literal THEN/ELSE ở tầng ngoài cùng của mọi `CASE … END AS <column>`."""
    found: set[str] = set()
    for end in re.finditer(rf"\bEND\s+AS\s+{re.escape(column)}\b", sql, re.IGNORECASE):
        tokens = list(_TOKEN.finditer(sql, 0, end.start()))
        depth, literals = 1, []
        for tok in reversed(tokens):
            word = tok.group(0).split()[0].upper()
            if word == "END":
                depth += 1
            elif word == "CASE":
                depth -= 1
                if depth == 0:
                    break
            elif depth == 1:
                literals.append(tok.group(1))
        found.update(literals)
    return found


def _gold_literals() -> dict[str, set[str]]:
    sql = [
        re.sub(r"--[^\n]*", "", yaml.safe_load(p.read_text(encoding="utf-8"))["sql"])
        for p in sorted(GOLD_DIR.rglob("*.yml"))
        if isinstance(yaml.safe_load(p.read_text(encoding="utf-8")), dict)
        and "sql" in yaml.safe_load(p.read_text(encoding="utf-8"))
    ]
    columns = {m.group(1) for s in sql for m in re.finditer(r"\bEND\s+AS\s+(\w+)", s, re.IGNORECASE)}
    return {c: set().union(*(_case_literals(s, c) for s in sql)) for c in columns}


def _accepted_string_values() -> list[tuple[str, str, set[str]]]:
    out = []
    for model in yaml.safe_load(SERVING_YML.read_text(encoding="utf-8"))["models"]:
        for col in model.get("columns", []):
            for test in col.get("tests", []) or col.get("data_tests", []) or []:
                if isinstance(test, dict) and "accepted_values" in test:
                    values = test["accepted_values"]["values"]
                    if all(isinstance(v, str) for v in values):
                        out.append((model["name"], col["name"], set(values)))
    return out


GOLD_LITERALS = _gold_literals()
ACCEPTED = _accepted_string_values()


def test_inputs_are_found():
    assert len(ACCEPTED) >= 5, ACCEPTED
    assert GOLD_LITERALS.get("churn_risk"), "không tìm thấy CASE … END AS churn_risk trong Gold"


@pytest.mark.parametrize(
    "model, column, accepted",
    [a for a in ACCEPTED if GOLD_LITERALS.get(a[1])],
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_accepted_values_cover_what_gold_emits(model, column, accepted):
    missing = GOLD_LITERALS[column] - accepted
    assert not missing, f"{model}.{column}: Gold sinh {sorted(missing)} nhưng accepted_values không có"


def test_literal_scanner_ignores_nested_case():
    sql = """
      CASE
        WHEN SUM(CASE WHEN s = 'ACTIVE' THEN x END) >= 5 THEN 'VIP'
        WHEN y > 1 THEN 'AFFLUENT'
        ELSE 'MASS'
      END AS bucket
    """
    assert _case_literals(sql, "bucket") == {"VIP", "AFFLUENT", "MASS"}


def test_scanner_sees_the_old_bug():
    """Danh sách churn_risk cũ phải bị bắt — không pass vì mù."""
    assert "Active" in GOLD_LITERALS["churn_risk"] - {"High", "Medium", "Low"}
