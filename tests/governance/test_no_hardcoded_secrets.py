"""
Không có secret viết cứng trong code runtime
============================================

Mẫu từng rải khắp repo: `os.environ.get("POSTGRES_PASSWORD", "<mật khẩu dev>")`.
Thiếu biến thì code ÂM THẦM dùng một mật khẩu đã commit — ai đọc repo cũng biết
nó. Superset còn viết cứng `admin123` cho admin và có SECRET_KEY mặc định (ký
session), trong khi `docker/.env` đã có sẵn giá trị riêng mà không ai đọc.

Test quét code runtime, chặn ba dạng: literal secret đã biết, mọi
`environ.get`/`getenv` cho biến PASSWORD / SECRET / KEY có giá trị mặc định, và
key/secret viết thẳng trong file cấu hình engine.

Cấu hình engine từng là nợ (nhóm B, TD-3): spark-defaults.conf và catalog Trino
chứa key MinIO. Nay Spark và Trino đọc AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
từ env, và danh sách nợ `KNOWN_DEBT` về rỗng nên được gỡ.

Ngoài phạm vi (ghi ở technical-debt): terraform/, giá trị chỉ dùng cho CI
(compose CI, workflow), file secret gitignore (docker/.env, docker/secrets/).
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOTS = ("code_etl", "governance", "data_generator", "airflow", "api", "ml", "streamlit", "scripts", "docker")
SUFFIXES = {".py", ".sh", ".conf", ".properties"}
CONFIG_SUFFIXES = {".conf", ".properties"}
EXTRA_FILES = ("Makefile",)
# Gitignore, không bao giờ commit — nơi secret THẬT được phép nằm.
IGNORED_PARTS = ("__pycache__", "secrets")

# So theo RANH GIỚI TỪ: `admin123` nằm bên trong `Minioadmin123` — hai literal khác nhau.
# Quét cả comment và docstring — mật khẩu trong ví dụ dùng lệnh vẫn là mật khẩu lộ.
KNOWN_LITERALS = ("BankingAdmin123", "admin123", "banking_platform_secret_key", "CDCPassword123", "Minioadmin123")
_LITERAL_RE = re.compile(r"\b(" + "|".join(KNOWN_LITERALS) + r")\b")

# environ.get("X_PASSWORD", "non-empty") / getenv("X_SECRET", "non-empty") / …
ENV_DEFAULT = re.compile(
    r"""(?:environ\.get|getenv)\(\s*["']([A-Z0-9_]*(?:PASSWORD|SECRET|_KEY)[A-Z0-9_]*)["']\s*,\s*["']([^"']+)["']"""
)

# Key/secret trong file cấu hình engine: `<key> <value>` (spark-defaults.conf) hoặc
# `<key>=<value>` (.properties). Giá trị hợp lệ duy nhất là tham chiếu env `${ENV:…}`.
CONFIG_SECRET = re.compile(
    r"^[ \t]*([\w.-]*(?:access[-.]key(?:-id)?|secret[-.\w]*key|password))[ \t]*[= \t][ \t]*(\S+)",
    re.IGNORECASE | re.MULTILINE,
)


def _is_env_reference_or_flag(value: str) -> bool:
    # `password=true` trong trino-cli.properties là CỜ: CLI đọc mật khẩu từ $TRINO_PASSWORD.
    return value.startswith("${ENV:") or value.lower() in {"true", "false"}


def _runtime_files() -> list[Path]:
    files = [REPO_ROOT / f for f in EXTRA_FILES]
    for root in RUNTIME_ROOTS:
        files += [p for p in (REPO_ROOT / root).rglob("*") if p.suffix in SUFFIXES and p.is_file()]
    return sorted(f for f in files if not any(part in f.parts for part in IGNORED_PARTS))


def _findings() -> tuple[list[str], set[tuple[str, str]], list[str]]:
    literals, defaults, config_values = [], set(), []
    for path in _runtime_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        text = path.read_text(encoding="utf-8", errors="ignore")
        for n, line in enumerate(text.splitlines(), 1):
            for lit in _LITERAL_RE.findall(line):
                literals.append(f"{rel}:{n}: chứa `{lit}`")
        for var, _value in ENV_DEFAULT.findall(text):
            defaults.add((rel, var))
        if path.suffix in CONFIG_SUFFIXES:
            for key, value in CONFIG_SECRET.findall(text):
                if not _is_env_reference_or_flag(value):
                    config_values.append(f"{rel}: {key}")
    return literals, defaults, config_values


LITERALS, DEFAULTS, CONFIG_VALUES = _findings()


def test_scan_sees_the_runtime_code():
    rels = {p.relative_to(REPO_ROOT).as_posix() for p in _runtime_files()}
    assert {
        "governance/audit.py",
        "docker/superset/init.sh",
        "Makefile",
        "data_generator/generate_all.py",
        "docker/spark/conf/spark-defaults.conf",
        "docker/init_trino/catalog/iceberg.properties",
    } <= rels


def test_no_known_secret_literal_in_runtime_code():
    assert not LITERALS, "Secret viết cứng trong code runtime:\n" + "\n".join(LITERALS)


def test_no_secret_env_var_has_a_default():
    assert not DEFAULTS, (
        "Biến secret có giá trị mặc định — thiếu biến sẽ âm thầm dùng giá trị đã commit:\n"
        + "\n".join(f"  {rel}: {var}" for rel, var in sorted(DEFAULTS))
    )


def test_engine_config_reads_credentials_from_env():
    """spark-defaults.conf / catalog Trino được commit: key phải là `${ENV:…}` hoặc không có."""
    assert not CONFIG_VALUES, "Credential viết thẳng trong file cấu hình engine:\n" + "\n".join(CONFIG_VALUES)


def test_scanner_sees_the_old_pattern():
    sample = 'x = os.environ.get("POSTGRES_PASSWORD", "hunter2")'
    assert ENV_DEFAULT.findall(sample) == [("POSTGRES_PASSWORD", "hunter2")]
    assert not ENV_DEFAULT.findall('x = os.environ.get("POSTGRES_HOST", "postgres")')


def test_config_scanner_sees_the_old_pattern():
    old = (
        "spark.hadoop.fs.s3a.secret.key   hunter2\n"
        "hive.s3.aws-secret-key=hunter2\n"
        "spark.sql.catalog.x.s3.access-key-id minio\n"
    )
    assert [key for key, _ in CONFIG_SECRET.findall(old)] == [
        "spark.hadoop.fs.s3a.secret.key",
        "hive.s3.aws-secret-key",
        "spark.sql.catalog.x.s3.access-key-id",
    ]
    assert CONFIG_SECRET.findall("hive.s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}")[0][1].startswith("${ENV:")
    assert not CONFIG_SECRET.findall("spark.hadoop.fs.s3a.aws.credentials.provider   org.x.Provider")
    assert _is_env_reference_or_flag("true")
    assert not _is_env_reference_or_flag("hunter2")
