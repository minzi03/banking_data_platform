"""
Schema khai báo của lakehouse, đọc từ DDL Iceberg (docker/init_iceberg/*.sql).

DDL là nguồn sự thật cho cột + kiểu (contract chỉ khai tên cột, và chỉ là tập
con). Module này chạy trên Spark worker (Python 3.10) nên chỉ dùng thư viện
chuẩn. scripts/generate_data_dictionary.py có parser riêng cho tài liệu;
tests/governance/test_ddl_schema.py giữ hai parser ra cùng kết quả.
"""

from __future__ import annotations

import re
from pathlib import Path

DEFAULT_DDL_DIR = Path(__file__).resolve().parents[1] / "docker" / "init_iceberg"

_CREATE = re.compile(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([\w.]+)\s*\((.*?)\n[ \t]*\)", re.IGNORECASE | re.DOTALL)
_SKIP = re.compile(r"^(PRIMARY|FOREIGN|CONSTRAINT|UNIQUE|CHECK)\b", re.IGNORECASE)
_ALIASES = {"INTEGER": "INT", "LONG": "BIGINT", "SHORT": "SMALLINT", "BYTE": "TINYINT"}


def normalize_type(dtype: str) -> str:
    """
    'decimal(18, 2)' → 'DECIMAL(18,2)', 'integer' → 'INT' — để DDL và Spark dtypes
    so được. VARCHAR(n) / CHAR(n) → STRING: Iceberg không có kiểu độ dài cố định,
    bảng tạo bằng VARCHAR(10) được Spark đọc ra là `string`.
    """
    t = re.sub(r"\s+", "", dtype).upper()
    if re.fullmatch(r"(VAR)?CHAR(\(\d+\))?", t):
        return "STRING"
    return _ALIASES.get(t, t)


def _column_defs(body: str) -> list[str]:
    """Tách thân CREATE TABLE theo dấu phẩy ở mức ngoặc 0 (DECIMAL(18,2) giữ nguyên)."""
    parts, depth, current = [], 0, []
    for raw_line in body.splitlines():
        for ch in raw_line.split("--", 1)[0]:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if ch == "," and depth == 0:
                parts.append("".join(current))
                current = []
            else:
                current.append(ch)
        current.append(" ")
    parts.append("".join(current))
    return [p.strip() for p in parts if p.strip()]


def _type_of(rest: str) -> str:
    """Phần kiểu của một định nghĩa cột: 'DECIMAL (18,2) NOT NULL' → 'DECIMAL(18,2)'."""
    if "(" in rest.split()[0] or (len(rest.split()) > 1 and rest.split()[1].startswith("(")):
        return normalize_type(rest.split(")")[0] + ")")
    return normalize_type(rest.split()[0])


def parse_ddl_text(text: str) -> dict[str, dict[str, str]]:
    tables: dict[str, dict[str, str]] = {}
    for match in _CREATE.finditer(text):
        columns: dict[str, str] = {}
        for definition in _column_defs(match.group(2)):
            if _SKIP.match(definition):
                continue
            tokens = definition.split(None, 1)
            if len(tokens) == 2:
                columns[tokens[0].strip('"').lower()] = _type_of(tokens[1])
        if columns:
            tables[match.group(1).lower()] = columns
    return tables


def declared_schemas(ddl_dir: Path = DEFAULT_DDL_DIR) -> dict[str, dict[str, str]]:
    """{'lakehouse.silver.dim_customer': {'customer_id': 'BIGINT', ...}, ...} từ mọi file *.sql."""
    tables: dict[str, dict[str, str]] = {}
    for path in sorted(ddl_dir.glob("*.sql")):
        tables.update(parse_ddl_text(path.read_text(encoding="utf-8", errors="replace")))
    return tables
