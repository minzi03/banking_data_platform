"""
Contract Test — mọi bảng opslakehouse.* mà code dùng phải được tạo khi dựng stack
================================================================================

Hai lần liên tiếp một bảng log được code ghi vào mà không hề tồn tại:

- `contract_validation_log` (TD-10) và `lineage_log` (TD-13) chỉ được khai trong
  `docker/init_openmetadata/01_create_schemas.sql` — thư mục không được mount vào
  đâu, nên file không bao giờ chạy. `to_regclass` trên stack xác nhận cả hai vắng.

PostgreSQL chỉ chạy `docker/init_postgres/` lúc khởi tạo. Test này đòi mọi tên
`opslakehouse.<bảng>` xuất hiện trong code runtime phải có `CREATE TABLE` ở đó.

Nó cũng bắt tham chiếu chết: `quarantine.py` từng khai hằng
`QUARANTINE_LOG_TABLE = "opslakehouse.quarantine_log"` mà không dòng nào dùng —
log thật nằm ở Iceberg (`lakehouse.quarantine.quarantine_log`). Đã xoá.

Chạy: pytest tests/governance/test_ops_tables_exist.py -v
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOTS = ("code_etl", "governance", "airflow", "scripts", "streamlit", "api", "ml")
TABLE_REF = re.compile(r"\bopslakehouse\.([a-z_][a-z0-9_]*)\b")


def _referenced() -> dict[str, list[str]]:
    refs: dict[str, list[str]] = {}
    for root in RUNTIME_ROOTS:
        for path in (REPO_ROOT / root).rglob("*.py"):
            for table in TABLE_REF.findall(path.read_text(encoding="utf-8")):
                refs.setdefault(table, []).append(path.relative_to(REPO_ROOT).as_posix())
    return refs


def _created() -> set[str]:
    created = set()
    for sql in (REPO_ROOT / "docker" / "init_postgres").glob("*.sql"):
        created.update(
            re.findall(
                r"CREATE TABLE IF NOT EXISTS opslakehouse\.([a-z_][a-z0-9_]*)",
                sql.read_text(encoding="utf-8"),
                flags=re.IGNORECASE,
            )
        )
    return created


REFERENCED = _referenced()
CREATED = _created()


def test_inputs_are_found():
    """Guard chống pass rỗng."""
    assert len(REFERENCED) >= 5, REFERENCED
    assert len(CREATED) >= 5, CREATED
    assert "flag_job_etl" in REFERENCED and "flag_job_etl" in CREATED


@pytest.mark.parametrize("table", sorted(REFERENCED))
def test_referenced_table_is_created_at_stack_init(table: str):
    assert table in CREATED, (
        f"opslakehouse.{table} được dùng trong {sorted(set(REFERENCED[table]))} "
        "nhưng không có CREATE TABLE nào trong docker/init_postgres/. "
        "PostgreSQL chỉ chạy thư mục đó lúc khởi tạo — bảng sẽ không tồn tại."
    )


# Bảng lineage được phép tạo, kèm lý do. Từng có ba bảng cho cùng một việc và
# chỉ một bảng có writer — người đọc truy vấn nhầm bảng rỗng (TD-13).
LINEAGE_TABLES = {
    "lineage_log": "cấp bảng — ops_lineage_dag ghi",
    "data_lineage_audit": "cấp cột cho audit regulatory — CHƯA có writer, chờ REGULATORY_MAPPING.md",
}


def test_lineage_tables_are_the_declared_ones():
    created = {t for t in CREATED if "lineage" in t}
    assert created == set(LINEAGE_TABLES), (
        f"Bảng lineage được tạo: {sorted(created)}. Thêm bảng lineage mới thì khai nó vào "
        "LINEAGE_TABLES kèm lý do — và ai ghi vào nó."
    )


def test_no_ddl_outside_the_directory_postgres_runs():
    """Thư mục DDL không ai chạy là nơi bảng biến mất mà không ai hay (TD-13)."""
    assert not (REPO_ROOT / "docker" / "init_openmetadata").exists(), (
        "docker/init_openmetadata/ quay lại. Không service nào mount nó — DDL ở đó không bao giờ chạy."
    )
