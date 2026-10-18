"""
Contract Test — Schema DDL phải khớp với SQL SELECT trong YAML (Bronze + Silver)
================================================================================

Bắt đúng lớp bug đã xảy ra ngày 2026-10-16 khi thêm nghiệp vụ Merchant QR/POS:
  - Cột mới (merchant_id, payment_channel, refund_flag) được thêm vào YAML
    Bronze và Silver, nhưng DDL Iceberg chưa cập nhật.
  - Job Spark chạy thật chết với INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS.
  - dim_merchant có YAML nhưng hoàn toàn vắng mặt trong DDL Silver.

Static test này parse DDL từ `docker/init_iceberg/01_ddl_bronze.sql` và `02_ddl_silver.sql`,
đọc SELECT trong YAML của Bronze và Silver, rồi so khớp:
  1. Mọi YAML target table phải có CREATE TABLE tương ứng trong DDL.
  2. Mọi cột trong YAML SELECT phải tồn tại trong DDL của bảng đó.
  3. Với facts: danh sách cột YAML SELECT phải bằng đúng danh sách cột DDL.

Chạy: pytest tests/governance/test_ddl_yaml_column_alignment.py -v
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from generate_data_dictionary import parse_ddl  # noqa: E402

BRONZE_DDL = REPO_ROOT / "docker" / "init_iceberg" / "01_ddl_bronze.sql"
SILVER_DDL = REPO_ROOT / "docker" / "init_iceberg" / "02_ddl_silver.sql"
BRONZE_DIR = REPO_ROOT / "code_etl" / "bronze"
SILVER_DIR = REPO_ROOT / "code_etl" / "silver"

# Cột framework sinh ra, không nằm trong YAML SELECT nguồn:
#   - SCD2: sk, effective_from/to, is_current
#   - Bronze: cob_dt (loader tự thêm nếu strategy=full_snapshot)
FRAMEWORK_GENERATED_COLS = {
    "effective_from",
    "effective_to",
    "is_current",
}


def _ddl_tables() -> dict[str, set[str]]:
    """schema.table → {col_names} từ DDL Bronze + Silver."""
    out: dict[str, set[str]] = {}
    for path in (BRONZE_DDL, SILVER_DDL):
        for table in parse_ddl(path):
            parts = table.fqn.split(".")
            # lakehouse.schema.table → schema.table
            if len(parts) >= 2:
                key = f"{parts[-2]}.{parts[-1]}".lower()
                out[key] = {c.name.lower() for c in table.columns}
    return out


def _yaml_workloads() -> list[dict]:
    """Danh sách {path, target_schema, target_table, select_cols, job_type}."""
    workloads = []

    # Bronze JDBC workloads (bỏ templates và file_sources/SFTP không có SELECT)
    for path in sorted(BRONZE_DIR.glob("*/*.yml")):
        if path.parent.name in {"templates", "file_sources"}:
            continue
        cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(cfg, dict) or "target" not in cfg or "sql" not in cfg:
            continue
        cols = _extract_select_columns(cfg["sql"])
        workloads.append(
            {
                "path": path,
                "target": f"bronze.{cfg['target']['table'].lower()}",
                "cols": cols,
                "layer": "bronze",
                "type": cfg.get("load", {}).get("strategy", "full_snapshot"),
            }
        )

    # Silver dims + facts
    for path in sorted(SILVER_DIR.glob("*/*.yml")):
        if path.parent.name not in {"dims", "facts"}:
            continue
        cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(cfg, dict) or "target" not in cfg or "sql" not in cfg:
            continue
        cols = _extract_select_columns(cfg["sql"])
        workloads.append(
            {
                "path": path,
                "target": f"silver.{cfg['target']['table'].lower()}",
                "cols": cols,
                "layer": "silver",
                "type": (cfg.get("job") or {}).get("type", "fact_txn"),
            }
        )

    return workloads


def _extract_select_columns(sql: str) -> list[str]:
    """Trích tên cột xuất từ SELECT đơn giản trong YAML (không hỗ trợ subquery)."""
    # Lấy đoạn giữa SELECT và FROM ngoài cùng
    # Bỏ comment trước
    cleaned = re.sub(r"--[^\n]*", "", sql)
    # Lấy SELECT đầu tiên tới FROM đầu tiên
    m = re.search(r"\bSELECT\b(.*?)\bFROM\b", cleaned, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    select_body = m.group(1)

    cols = []
    # Tách theo dấu phẩy ở top level (không trong ngoặc)
    depth = 0
    cur = []
    for ch in select_body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            cols.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur and "".join(cur).strip():
        cols.append("".join(cur).strip())

    col_names = []
    for raw in cols:
        ln = " ".join(raw.split()).strip()
        if not ln:
            continue
        # Bắt alias "AS col_name"
        as_match = re.search(r"\bAS\s+([a-zA-Z0-9_]+)\s*$", ln, re.IGNORECASE)
        if as_match:
            col_names.append(as_match.group(1).lower())
            continue
        # Cột đơn giản: [alias.]col_name
        parts = ln.split()
        simple = parts[0].split(".")[-1].strip("`\"' ")
        if re.match(r"^[a-zA-Z0-9_]+$", simple):
            col_names.append(simple.lower())

    return col_names


def test_guard_inputs_are_discovered():
    """Guard chống pass rỗng: DDL và YAML phải tìm được đủ số lượng baseline."""
    ddl = _ddl_tables()
    workloads = _yaml_workloads()
    assert len(ddl) >= 30, f"chỉ đọc được {len(ddl)} bảng từ DDL"
    assert len(workloads) >= 35, f"chỉ đọc được {len(workloads)} YAML workloads"


def test_every_yaml_target_table_exists_in_ddl():
    """Mọi YAML target table phải có CREATE TABLE trong DDL tương ứng."""
    ddl = _ddl_tables()
    missing = []
    for w in _yaml_workloads():
        if w["target"] not in ddl:
            missing.append(f"{w['path'].name} → target `{w['target']}` không có trong DDL")
    assert not missing, "Bảng đích YAML thiếu trong DDL:\n  " + "\n  ".join(missing)


def test_every_yaml_select_column_exists_in_ddl():
    """
    Mọi cột mà YAML SELECT phải có mặt trong DDL bảng đích.

    Chống regression ARITY_MISMATCH: dev thêm cột vào YAML SELECT nhưng
    quên cập nhật CREATE TABLE.
    """
    ddl = _ddl_tables()
    violations = []
    for w in _yaml_workloads():
        target_cols = ddl.get(w["target"], set())
        for col in w["cols"]:
            if col not in target_cols:
                violations.append(
                    f"{w['path'].name} ({w['target']}): cột `{col}` có trong YAML SELECT nhưng THIẾU trong DDL"
                )
    assert not violations, "Cột YAML không tồn tại trong DDL:\n  " + "\n  ".join(violations)


def test_facts_have_exact_column_match():
    """
    Với Silver facts: mọi cột trong DDL (trừ cob_dt nếu YAML không SELECT)
    phải khớp với YAML SELECT — fact table không có cơ chế auto-generate cột
    như SCD2 nên YAML SELECT quyết định 100% schema ghi xuống.
    """
    ddl = _ddl_tables()
    mismatches = []
    for w in _yaml_workloads():
        if w["layer"] != "silver" or w["type"] != "fact_txn":
            continue
        ddl_cols = ddl.get(w["target"], set())
        yaml_cols = set(w["cols"])
        # Cột có trong DDL nhưng thiếu trong YAML (trừ khi có lý do)
        missing_in_yaml = ddl_cols - yaml_cols
        # Cột có trong YAML nhưng thiếu trong DDL (đã bắt ở test trên, nhưng check lại)
        extra_in_yaml = yaml_cols - ddl_cols
        if missing_in_yaml or extra_in_yaml:
            mismatches.append(
                f"{w['path'].name} ({w['target']}):\n"
                f"    thiếu trong YAML: {sorted(missing_in_yaml)}\n"
                f"    thừa trong YAML:  {sorted(extra_in_yaml)}"
            )
    assert not mismatches, "Silver fact DDL và YAML SELECT lệch cột:\n  " + "\n  ".join(mismatches)
