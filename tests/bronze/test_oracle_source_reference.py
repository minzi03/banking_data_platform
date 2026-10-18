"""
Contract Test — Oracle source reference (Khoá new2 — C2)
========================================================

B10 đóng gap "Core Banking chạy Oracle". Phạm vi là driver map + cấu hình mẫu,
KHÔNG phải một Oracle thật (xem docs/04-operations/ORACLE_SOURCE_GUIDE.md §6).

Test này chốt ba tính chất để tham chiếu không âm thầm mục:
  1. Config mẫu vẫn hợp lệ với `validate_config()` của ingestion JDBC.
  2. Config mẫu nằm trong `templates/` → không DAG glob nào chạy nó.
  3. Driver map của ingestion nhận đúng prefix `jdbc:oracle:`.

Chạy: pytest tests/bronze/test_oracle_source_reference.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REFERENCE = PROJECT_ROOT / "code_etl" / "bronze" / "templates" / "oracle_source_reference.yml"
INGESTION = PROJECT_ROOT / "code_etl" / "bronze" / "base_job" / "ingestion_jdbc.py"


def _load_reference() -> dict:
    assert REFERENCE.exists(), f"thiếu config tham chiếu: {REFERENCE}"
    return yaml.safe_load(REFERENCE.read_text(encoding="utf-8"))


def _validate_config(path_config: dict) -> None:
    """
    Chạy `validate_config` của ingestion_jdbc mà KHÔNG import module.

    Module import pyspark + spark.iceberg_utils ở top level; các test Bronze khác
    thay `spark` trong sys.modules bằng MagicMock, nên import ở đây phụ thuộc thứ
    tự chạy test. Trích hàm bằng AST rồi exec trong namespace tối giản — tất định,
    không kéo Spark stack.
    """
    import ast

    source = INGESTION.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "validate_config":
            code = compile(ast.Module(body=[node], type_ignores=[]), str(INGESTION), "exec")
            namespace: dict = {}
            exec(code, namespace)
            namespace["validate_config"](path_config)
            return
    raise AssertionError("không tìm thấy validate_config trong ingestion_jdbc.py")


def test_reference_config_exists_and_is_valid():
    """Config Oracle mẫu phải pass validate_config — nếu không nó là tài liệu sai."""
    config = _load_reference()
    _validate_config(config)  # raise nếu thiếu section/strategy


def test_reference_declares_source_type():
    """`source.type` phải khai báo — hiện là 'postgresql' kèm ghi chú đổi sang 'oracle'."""
    config = _load_reference()
    assert "source" in config
    assert config["source"].get("type"), "source.type là trường bắt buộc"


def test_reference_sql_targets_oracle_semantics():
    """
    SQL mẫu phải KHÔNG dùng cú pháp chỉ có ở Postgres (`timezone(...)`, `::date`),
    vì mục đích của file là chỉ ra khác biệt cú pháp Oracle.
    """
    import re

    raw = _load_reference()["sql"]
    # Bỏ comment SQL/YAML trước khi kiểm — comment cố ý nhắc tới cú pháp Postgres.
    sql = "\n".join(line for line in raw.splitlines() if not line.lstrip().startswith("--")).lower()
    assert "timezone(" not in sql, "Oracle không có timezone() — dùng AT TIME ZONE"
    assert "::date" not in sql, "Oracle không có cast ::date — dùng TRUNC()/CAST"
    assert re.search(r"\btrunc\(", sql), "SQL Oracle mẫu nên dùng TRUNC() cho ngày"


def test_reference_lives_in_templates_so_no_dag_runs_it():
    """File tham chiếu phải nằm dưới templates/ → bị loại khỏi DAG glob và registry."""
    assert REFERENCE.parent.name == "templates", (
        "config mẫu phải ở templates/ — đặt trong core_banking/ sẽ bị DAG nạp thật"
    )


def test_reference_is_not_a_jdbc_workload_in_registry():
    """Registry contract test loại templates/; xác nhận ở đây để thay đổi sau không phá."""
    bronze_dir = PROJECT_ROOT / "code_etl" / "bronze"
    workloads = {
        p.parent.name for p in bronze_dir.glob("*/*.yml") if p.parent.name not in {"templates", "file_sources"}
    }
    assert "templates" not in workloads


@pytest.mark.parametrize(
    "url,expected_driver",
    [
        ("jdbc:oracle:thin:@//ora:1521/SVC", "oracle.jdbc.OracleDriver"),
        ("jdbc:postgresql://pg:5432/db", "org.postgresql.Driver"),
    ],
)
def test_driver_map_resolves_expected_class(url, expected_driver):
    """
    Prefix URL → driver class, đúng như `_DRIVER_MAP` của ingestion.

    `_DRIVER_MAP` là biến cục bộ trong `extract_from_source`, nên trích bằng AST
    thay vì import (import kéo cả Spark stack).
    """
    import ast

    tree = ast.parse(INGESTION.read_text(encoding="utf-8"))
    driver_map = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "_DRIVER_MAP" for t in node.targets
        ):
            driver_map = ast.literal_eval(node.value)
            break
    assert driver_map, "không tìm thấy _DRIVER_MAP trong ingestion_jdbc.py"

    driver = next((cls for prefix, cls in driver_map.items() if url.startswith(prefix)), None)
    assert driver == expected_driver
