"""
Contract Test — mọi danh sách nguồn Bronze phải khớp workload thật
==================================================================
Workload Bronze thật là `code_etl/bronze/<schema>/*.yml` (DAG glob thẳng thư mục
này). Ngoài nó có ba danh sách viết tay, và cả ba từng lệch âm thầm:

1. `BRONZE_CONFIGS` trong bootstrap — từng thiếu `loan_payment.yml` dù YAML đã có,
   làm đứt chuỗi xuống Gold (xem comment trong initial_load.py).
2. `templates/source_registry.yml` — không code nào đọc; từng thiếu loan_payment
   và ghi txn_account/card_txn/online_transaction là `incremental`.
3. `opslakehouse.source_table_registry` (data_generator/generators/ops_metadata.py)
   — từng trỏ tới bảng không tồn tại (`card_card`, `digi_device`,
   `fact_online_txn`…) và khai `standing_order`, `merchant` là đã có Bronze khi
   chưa có workload nào nạp.

Test đọc file tĩnh, không cần Spark hay Postgres.

Chạy: pytest tests/governance/test_bronze_source_registry.py -v
"""

from __future__ import annotations

import ast
import importlib.util
import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = REPO_ROOT / "code_etl" / "bronze"
SILVER_DIR = REPO_ROOT / "code_etl" / "silver"
BOOTSTRAP = BRONZE_DIR / "bootstrap" / "initial_load.py"
REGISTRY_YAML = BRONZE_DIR / "templates" / "source_registry.yml"
OPS_METADATA = REPO_ROOT / "data_generator" / "generators" / "ops_metadata.py"

_FROM = re.compile(r"\bFROM\s+(\w+)\.(\w+)", re.IGNORECASE)


def _workloads() -> dict[str, dict]:
    """rel_path → {schema, table, target, strategy} cho mọi workload Bronze."""
    out = {}
    for path in sorted(BRONZE_DIR.glob("*/*.yml")):
        if path.parent.name == "templates":
            continue
        config = yaml.safe_load(path.read_text(encoding="utf-8"))
        match = _FROM.search(config["sql"])
        assert match, f"{path.name}: không tìm thấy FROM <schema>.<table> trong sql"
        out[path.relative_to(REPO_ROOT).as_posix()] = {
            "schema": match.group(1),
            "table": match.group(2),
            "target": config["target"]["table"],
            "strategy": config["load"]["strategy"],
        }
    return out


def _silver_targets() -> set[str]:
    return {
        yaml.safe_load(p.read_text(encoding="utf-8"))["target"]["table"]
        for p in SILVER_DIR.glob("*/*.yml")
        if p.parent.name in {"dims", "facts"}
    }


def _bootstrap_configs() -> list[str]:
    """Đọc BRONZE_CONFIGS bằng AST — import module sẽ kéo pyspark vào."""
    tree = ast.parse(BOOTSTRAP.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "BRONZE_CONFIGS" for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError("không tìm thấy BRONZE_CONFIGS trong bootstrap")


def _ops_registry_rows() -> list[tuple]:
    spec = importlib.util.spec_from_file_location("_ops_metadata_under_test", OPS_METADATA)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.generate_source_registry()


def test_inputs_are_found():
    """Guard chống pass rỗng: glob hay regex hỏng không được làm test xanh."""
    assert len(_workloads()) >= 19
    assert len(_silver_targets()) >= 16
    assert len(_bootstrap_configs()) >= 19
    assert len(_ops_registry_rows()) >= 19


def test_bootstrap_loads_exactly_the_workloads():
    configs = _bootstrap_configs()
    assert len(configs) == len(set(configs)), "BRONZE_CONFIGS có phần tử lặp"
    assert set(configs) == set(_workloads())


def test_registry_yaml_matches_workloads():
    registry = yaml.safe_load(REGISTRY_YAML.read_text(encoding="utf-8"))["tables"]
    declared = {(e["source_schema"], e["source_table"], e["target_table"], e["load_strategy"]) for e in registry}
    assert len(declared) == len(registry), "source_registry.yml có dòng lặp"
    actual = {(w["schema"], w["table"], w["target"], w["strategy"]) for w in _workloads().values()}
    assert declared == actual


def test_ops_registry_points_at_real_tables():
    rows = _ops_registry_rows()
    workloads = {(w["schema"], w["table"]): w["target"] for w in _workloads().values()}
    silver = _silver_targets()

    registered = {(r[0], r[1]) for r in rows}
    assert len(registered) == len(rows), "source_table_registry có nguồn lặp"
    # Registry nói "bảng này vào Bronze" thì phải có workload nạp nó, và ngược lại.
    assert registered == set(workloads)

    for schema, table, _type, _conn, bronze, silver_table, *_ in rows:
        assert bronze == f"lakehouse.bronze.{workloads[(schema, table)]}", f"{schema}.{table}: {bronze}"
        if silver_table is not None:
            assert silver_table.startswith("lakehouse.silver.")
            assert silver_table.removeprefix("lakehouse.silver.") in silver, f"{schema}.{table}: {silver_table}"
