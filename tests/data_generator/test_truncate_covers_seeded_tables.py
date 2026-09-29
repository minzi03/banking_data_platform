"""
`generate_all.py --truncate` phải xoá MỌI bảng mà generator ghi vào.

Bảng ghi mà không truncate thì lần seed lại thứ hai chết ở khoá chính. Đã xảy ra
2026-09-29: bốn bảng AML không có trong PostgresWriter.truncate_all(), seed lại
dừng ở `duplicate key value violates unique constraint "aml_rule_pkey"` sau khi
đã ghi xong mọi bảng khác.

Đọc cả hai file dưới dạng văn bản: postgres_writer.py import psycopg2, còn job
unit test của CI không cài psycopg2.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATE_ALL = REPO_ROOT / "data_generator" / "generate_all.py"
WRITER = REPO_ROOT / "data_generator" / "connectors" / "postgres_writer.py"


def _written() -> set[tuple[str, str]]:
    text = GENERATE_ALL.read_text(encoding="utf-8")
    return set(re.findall(r'(?<![\w.])writer\.write_rows\(\s*"(\w+)",\s*"(\w+)"', text))


def _truncated() -> set[tuple[str, str]]:
    tree = ast.parse(WRITER.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "truncate_all":
            for stmt in ast.walk(node):
                if isinstance(stmt, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "tables" for t in stmt.targets
                ):
                    return set(ast.literal_eval(stmt.value))
    raise AssertionError("không tìm thấy danh sách tables trong truncate_all")


def test_inputs_are_found():
    assert len(_written()) >= 20
    assert len(_truncated()) >= 20


def test_every_seeded_table_is_truncated():
    missing = _written() - _truncated()
    assert not missing, f"generator ghi nhưng --truncate không xoá: {sorted(missing)}"
