"""
Contract Test — data contract phải mô tả đúng bảng nó quản lý (TD-12)
====================================================================

Contract trong `governance/datasets/` chưa từng được đối chiếu với bảng thật cho
tới khi `contract_validation.py` chạy lần đầu (2026-09-23). Kết quả: 20/33
contract sai, **không dòng dữ liệu nào sai**:

- 10 Silver đòi cột bảng không có (`card_number_masked` — cột thật là `card_no_masked`)
- 9 `*_current` trỏ vào bảng CTAS Spark đã gỡ, thay vì bảng dbt ở `serving`
- 1 trỏ vào `branch_monthly_summary`, thiếu tiền tố `mart_` — lặp lại TD-9
- 10 khai `dag_id: gold_mart360_dag`, một DAG không tồn tại (ID thật: `gold_all_dag`)

Không test nào thấy vì `test_contracts.py` chỉ kiểm contract parse được. Test này
kiểm tĩnh — không cần stack — cùng những gì một lượt chạy thật sẽ vấp phải:

    bảng tồn tại            DDL trong docker/init_*/, hoặc model dbt serving
    cột tồn tại             required / non_null / unique / unique_column_sets / date_column
    layer == namespace      DAG chọn contract theo layer
    dag_id tồn tại          khai trong airflow/dags/

Bảng serving không có DDL: dbt tạo chúng lúc chạy. Model dạng
`select * from source('gold', X)` có cột của `serving.X_current` chính là cột
của `gold.X`; model tổng hợp phải enforce contract dbt (cột + data_type), và test
lấy cột từ đó. Không thuộc hai dạng này thì test đỏ, thay vì đoán.

Chạy: pytest tests/governance/test_contracts_match_tables.py -v
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = sorted((REPO_ROOT / "governance" / "datasets").glob("*.yaml"))
SERVING_MODELS = sorted((REPO_ROOT / "dbt" / "models" / "serving").glob("*.sql"))

sys.path.insert(0, str(REPO_ROOT / "scripts"))
from generate_data_dictionary import collect_tables  # noqa: E402

SELECT_STAR_FROM_GOLD = re.compile(r"\bselect \*\s+from\s+\{\{\s*source\('gold',\s*'(\w+)'\)\s*\}\}", re.IGNORECASE)


SERVING_YML = REPO_ROOT / "dbt" / "models" / "serving" / "_serving_models.yml"


def _enforced_contract_columns() -> dict[str, set[str]]:
    """Model serving có `contract: {enforced: true}`: dbt build kiểm schema thật
    với danh sách cột + data_type khai trong yml, nên danh sách đó đáng tin."""
    out = {}
    for model in yaml.safe_load(SERVING_YML.read_text(encoding="utf-8"))["models"]:
        contract = (model.get("config") or {}).get("contract") or {}
        if contract.get("enforced"):
            columns = model.get("columns") or []
            assert all(c.get("data_type") for c in columns), f"{model['name']}: contract cần data_type cho mọi cột"
            out[model["name"]] = {c["name"] for c in columns}
    return out


def _known_tables() -> dict[str, set[str]]:
    tables = {t.fqn: {c.name for c in t.columns} for t in collect_tables()}
    contracted = _enforced_contract_columns()
    for model in SERVING_MODELS:
        match = SELECT_STAR_FROM_GOLD.search(model.read_text(encoding="utf-8"))
        if match is None:
            # Model tổng hợp (không phải select * từ Gold): cột lấy từ contract dbt đã
            # enforce. Không có contract thì không suy được cột — đỏ, không đoán.
            assert model.stem in contracted, (
                f"{model.name} không phải `select * from source('gold', ...)` và không có "
                "`contract: {enforced: true}` trong _serving_models.yml — không suy được cột."
            )
            tables[f"lakehouse.serving.{model.stem}"] = contracted[model.stem]
            continue
        source = f"lakehouse.gold.{match.group(1)}"
        assert source in tables, f"{model.name} đọc {source}, bảng không có trong DDL"
        tables[f"lakehouse.serving.{model.stem}"] = tables[source]
    return tables


def _known_dag_ids() -> set[str]:
    ids = set()
    for dag in (REPO_ROOT / "airflow" / "dags").rglob("*.py"):
        ids.update(re.findall(r'^DAG_ID\s*=\s*"([\w.-]+)"', dag.read_text(encoding="utf-8"), flags=re.MULTILINE))
    return ids


TABLES = _known_tables()
DAG_IDS = _known_dag_ids()


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _fqn(contract: dict) -> str:
    loc = contract["physical_location"]
    return f"{loc['catalog']}.{loc['namespace']}.{loc['table']}"


def _referenced_columns(contract: dict) -> dict[str, list[str]]:
    rules = contract.get("quality_rules") or {}
    return {
        "required_columns": rules.get("required_columns") or [],
        "non_null_columns": rules.get("non_null_columns") or [],
        "unique_check": rules.get("unique_check") or [],
        "unique_column_sets": [c for s in rules.get("unique_column_sets") or [] for c in s],
        "date_column": [rules["date_column"]] if rules.get("date_column") else [],
    }


def _id(path: Path) -> str:
    return path.stem.removeprefix("banking.")


def test_inputs_are_found():
    """Guard chống pass rỗng: parser DDL hay glob hỏng không được làm test xanh vô nghĩa."""
    assert len(CONTRACTS) >= 30, f"chỉ thấy {len(CONTRACTS)} contract"
    assert len(TABLES) >= 80, f"chỉ thấy {len(TABLES)} bảng"
    assert len(SERVING_MODELS) >= 10, f"chỉ thấy {len(SERVING_MODELS)} model serving"
    assert {"silver_all_dag", "gold_all_dag", "dbt_serving_publish"} <= DAG_IDS, DAG_IDS


@pytest.mark.parametrize("path", CONTRACTS, ids=_id)
def test_table_exists(path: Path):
    contract = _load(path)
    table = _fqn(contract)
    assert table in TABLES, (
        f"{path.name} trỏ vào {table}, bảng không có trong DDL hay model dbt serving. "
        "Kiểm tiền tố (mart_) và schema (gold vs serving)."
    )


@pytest.mark.parametrize("path", CONTRACTS, ids=_id)
def test_referenced_columns_exist(path: Path):
    contract = _load(path)
    columns = TABLES.get(_fqn(contract))
    if columns is None:
        pytest.skip("bảng không tồn tại — test_table_exists đã đỏ")
    missing = {rule: [c for c in cols if c not in columns] for rule, cols in _referenced_columns(contract).items()}
    missing = {rule: cols for rule, cols in missing.items() if cols}
    assert not missing, f"{path.name} nhắc tới cột bảng {_fqn(contract)} không có: {missing}"


@pytest.mark.parametrize("path", CONTRACTS, ids=_id)
def test_layer_matches_namespace(path: Path):
    """contract_validation.py chọn contract theo `layer`; lệch namespace là kiểm nhầm tầng."""
    contract = _load(path)
    assert contract["layer"] == contract["physical_location"]["namespace"], (
        f"{path.name}: layer={contract['layer']} nhưng bảng nằm ở {contract['physical_location']['namespace']}"
    )


@pytest.mark.parametrize("path", CONTRACTS, ids=_id)
def test_dag_id_exists(path: Path):
    contract = _load(path)
    assert contract["dag_id"] in DAG_IDS, (
        f"{path.name}: dag_id={contract['dag_id']!r} không phải DAG nào. Có: {sorted(DAG_IDS)}"
    )
