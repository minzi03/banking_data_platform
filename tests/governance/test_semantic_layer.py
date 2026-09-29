"""
Semantic layer (dbt/models/semantic/_semantic_models.yml) trỏ vào cột thật.

`dbt parse` đã kiểm tham chiếu giữa metric ↔ measure, nhưng dbt không chạy trong
CI, và parse không biết cột có tồn tại trong bảng hay không — một measure
`expr: outstanding_balanc` chỉ lộ ra lúc truy vấn. Test này kiểm tĩnh:

- model của mọi semantic model là serving `select *` từ Gold → cột = cột DDL Gold;
- mọi dimension / entity / measure `expr` chỉ dùng cột của bảng đó;
- ratio metric trỏ vào metric có thật, simple metric trỏ vào measure có thật.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from governance.ddl_schema import declared_schemas

REPO_ROOT = Path(__file__).resolve().parents[2]
SEMANTIC_YML = REPO_ROOT / "dbt" / "models" / "semantic" / "_semantic_models.yml"
SERVING_DIR = REPO_ROOT / "dbt" / "models" / "serving"
_SELECT_STAR = re.compile(r"select \*\s+from\s+\{\{\s*source\('gold',\s*'(\w+)'\)\s*\}\}", re.IGNORECASE)
_IDENT = re.compile(r"\b[a-z_][a-z0-9_]*\b")
_SQL_WORDS = {"case", "when", "then", "else", "end", "is", "not", "null", "and", "or"}


def _spec() -> dict:
    return yaml.safe_load(SEMANTIC_YML.read_text(encoding="utf-8"))


def _columns_of(model_ref: str) -> set[str]:
    name = re.fullmatch(r"ref\('(\w+)'\)", model_ref).group(1)
    sql = (SERVING_DIR / f"{name}.sql").read_text(encoding="utf-8")
    source = _SELECT_STAR.search(sql)
    assert source, f"{name}: semantic model phải đặt trên serving `select *` từ Gold để biết cột"
    return set(declared_schemas()[f"lakehouse.gold.{source.group(1)}"])


def _identifiers(expr: str) -> set[str]:
    without_strings = re.sub(r"'[^']*'", "", expr.lower())
    return {w for w in _IDENT.findall(without_strings) if w not in _SQL_WORDS and not w.isdigit()}


def test_inputs_are_found():
    spec = _spec()
    assert len(spec["semantic_models"]) >= 2
    assert {"npl_ratio", "late_payment_rate"} <= {m["name"] for m in spec["metrics"]}


def test_every_expression_uses_real_columns():
    for sm in _spec()["semantic_models"]:
        columns = _columns_of(sm["model"])
        items = sm.get("dimensions", []) + sm.get("entities", []) + sm.get("measures", [])
        for item in items:
            used = _identifiers(str(item.get("expr", item["name"])))
            missing = used - columns
            assert not missing, f"{sm['name']}.{item['name']}: cột không có {sorted(missing)}"


def test_metrics_reference_existing_measures_and_metrics():
    spec = _spec()
    measures = {m["name"] for sm in spec["semantic_models"] for m in sm.get("measures", [])}
    metrics = {m["name"] for m in spec["metrics"]}
    for metric in spec["metrics"]:
        params = metric["type_params"]
        if metric["type"] == "simple":
            assert params["measure"] in measures, metric["name"]
        elif metric["type"] == "ratio":
            assert {params["numerator"], params["denominator"]} <= metrics, metric["name"]
