"""
Không có secret viết cứng trong code runtime
============================================

Mẫu từng rải khắp repo: `os.environ.get("POSTGRES_PASSWORD", "<mật khẩu dev>")`.
Thiếu biến thì code ÂM THẦM dùng một mật khẩu đã commit — ai đọc repo cũng biết
nó. Superset còn viết cứng `admin123` cho admin và có SECRET_KEY mặc định (ký
session), trong khi `docker/.env` đã có sẵn giá trị riêng mà không ai đọc.

Test quét code runtime, chặn cả hai dạng: literal secret đã biết, và mọi
`environ.get`/`getenv` cho biến PASSWORD / SECRET / KEY có giá trị mặc định.

Nợ còn lại được KHAI ra, không giấu: `KNOWN_DEBT` liệt kê chỗ chưa sửa kèm lý do,
và test đỏ nếu một chỗ đã sửa mà vẫn nằm trong danh sách — danh sách chỉ được co lại.

Ngoài phạm vi (ghi ở technical-debt): file cấu hình engine (spark-defaults.conf,
iceberg.properties), terraform/, giá trị chỉ dùng cho CI (compose CI, workflow).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOTS = ("code_etl", "governance", "data_generator", "airflow", "api", "ml", "streamlit", "scripts", "docker")
SUFFIXES = {".py", ".sh"}
EXTRA_FILES = ("Makefile",)

# So theo RANH GIỚI TỪ: `admin123` nằm bên trong `Minioadmin123` (nợ nhóm B, KNOWN_DEBT).
# Quét cả comment và docstring — mật khẩu trong ví dụ dùng lệnh vẫn là mật khẩu lộ.
KNOWN_LITERALS = ("BankingAdmin123", "admin123", "banking_platform_secret_key", "CDCPassword123")
_LITERAL_RE = re.compile(r"\b(" + "|".join(KNOWN_LITERALS) + r")\b")

# environ.get("X_PASSWORD", "non-empty") / getenv("X_SECRET", "non-empty") / …
ENV_DEFAULT = re.compile(
    r"""(?:environ\.get|getenv)\(\s*["']([A-Z0-9_]*(?:PASSWORD|SECRET|_KEY)[A-Z0-9_]*)["']\s*,\s*["']([^"']+)["']"""
)

# Chỗ còn nợ, kèm lý do. Chỉ được co lại.
KNOWN_DEBT = {
    ("code_etl/shared/spark/spark_session.py", "MINIO_SECRET_KEY"): "nhóm B: gắn với spark-defaults.conf",
    ("code_etl/shared/spark/spark_session.py", "MINIO_ACCESS_KEY"): "nhóm B: gắn với spark-defaults.conf",
    ("code_etl/cdc/base_job/cdc_streaming.py", "MINIO_ROOT_PASSWORD"): "nhóm B: Spark worker chưa nhận biến MinIO",
    ("code_etl/cdc/create_cdc_tables.py", "MINIO_ROOT_PASSWORD"): "nhóm B: Spark worker chưa nhận biến MinIO",
    ("code_etl/cdc/test_dlq.py", "MINIO_ROOT_PASSWORD"): "nhóm B: Spark worker chưa nhận biến MinIO",
}


def _runtime_files() -> list[Path]:
    files = [REPO_ROOT / f for f in EXTRA_FILES]
    for root in RUNTIME_ROOTS:
        files += [p for p in (REPO_ROOT / root).rglob("*") if p.suffix in SUFFIXES and p.is_file()]
    return sorted(f for f in files if "__pycache__" not in f.parts)


def _findings() -> tuple[list[str], set[tuple[str, str]]]:
    literals, defaults = [], set()
    for path in _runtime_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        text = path.read_text(encoding="utf-8", errors="ignore")
        for n, line in enumerate(text.splitlines(), 1):
            for lit in _LITERAL_RE.findall(line):
                literals.append(f"{rel}:{n}: chứa `{lit}`")
        for var, _value in ENV_DEFAULT.findall(text):
            defaults.add((rel, var))
    return literals, defaults


LITERALS, DEFAULTS = _findings()


def test_scan_sees_the_runtime_code():
    rels = {p.relative_to(REPO_ROOT).as_posix() for p in _runtime_files()}
    assert {"governance/audit.py", "docker/superset/init.sh", "Makefile", "data_generator/generate_all.py"} <= rels


def test_no_known_secret_literal_in_runtime_code():
    assert not LITERALS, "Secret viết cứng trong code runtime:\n" + "\n".join(LITERALS)


def test_no_secret_env_var_has_a_default():
    new = sorted(DEFAULTS - set(KNOWN_DEBT))
    assert not new, "Biến secret có giá trị mặc định — thiếu biến sẽ âm thầm dùng giá trị đã commit:\n" + "\n".join(
        f"  {rel}: {var}" for rel, var in new
    )


@pytest.mark.parametrize(("rel", "var"), sorted(KNOWN_DEBT), ids=lambda v: v if isinstance(v, str) else "")
def test_known_debt_is_still_real(rel, var):
    """Đã sửa thì gỡ khỏi KNOWN_DEBT — danh sách nợ chỉ được co lại."""
    assert (rel, var) in DEFAULTS, f"{rel}: {var} không còn giá trị mặc định — gỡ khỏi KNOWN_DEBT"


def test_scanner_sees_the_old_pattern():
    sample = 'x = os.environ.get("POSTGRES_PASSWORD", "hunter2")'
    assert ENV_DEFAULT.findall(sample) == [("POSTGRES_PASSWORD", "hunter2")]
    assert not ENV_DEFAULT.findall('x = os.environ.get("POSTGRES_HOST", "postgres")')
