"""
Đổi (rotate) secret của stack local đã lộ trong lịch sử git. Không in secret nào ra màn hình.

    py -3 scripts/rotate_local_secrets.py                    # CHỈ lập kế hoạch: tên khóa, số commit chứa giá trị hiện tại
    py -3 scripts/rotate_local_secrets.py --apply            # đổi thật, sao lưu .env trước, kiểm chứng sau
    py -3 scripts/rotate_local_secrets.py --rollback <file>  # quay về bản sao lưu mà --apply đã in ra

Vì sao cần: gỡ giá trị mặc định (#67, #71, #72, #73) chỉ chặn secret MỚI vào repo. Giá trị
đang chạy trên stack vẫn là giá trị đã nằm trong lịch sử git — đo 2026-09-27: cả mười secret
trong docker/.env đều có trong 1–12 commit.

Phạm vi --apply — secret của service ĐANG CHẠY, đổi được ngay và kiểm chứng được:

  POSTGRES_PASSWORD    ALTER ROLE <POSTGRES_USER>; tạo lại iceberg-rest (JDBC catalog), spark-worker-1 …
  CDC_DB_PASSWORD      ALTER ROLE cdc_user (LOGIN) — Debezium đọc lại khi đăng ký connector
  MINIO_ROOT_PASSWORD  MinIO nhận root credential mới khi tạo lại; Spark/Trino đọc AWS_* từ .env (#71)
  etl_user / analytics_user / readonly_user   NOLOGIN, xoá mật khẩu (#73: không ai đăng nhập bằng chúng)

Mật khẩu Postgres gửi dưới dạng SCRAM-SHA-256 verifier tính sẵn ở đây, qua stdin của psql:
server không bao giờ thấy mật khẩu dạng rõ, và giá trị không lên dòng lệnh nào.

Ngoài phạm vi (in ra ở cuối, làm khi service đó chạy): Superset, Airflow, OpenMetadata —
đổi SECRET_KEY / Fernet key cần mã hoá lại dữ liệu đã lưu bằng công cụ của chính service đó.

Điều kiện tiên quyết (kiểm trước khi đổi gì): spark-defaults.conf và catalog Trino không còn
chứa key MinIO (#71). Nếu còn, đổi mật khẩu MinIO sẽ làm Spark hỏng ngay.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import hashlib
import hmac
import json
import re
import secrets
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Console Windows mặc định cp1258: thông báo tiếng Việt ném UnicodeEncodeError —
# giữa chừng một lần đổi mật khẩu thì đó là trạng thái dở dang. Cùng cách với
# generate_metrics_manifest.py.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        with contextlib.suppress(AttributeError, OSError):
            _stream.reconfigure(encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCKER_DIR = REPO_ROOT / "docker"
ENV_FILE = DOCKER_DIR / ".env"
BACKUP_DIR = DOCKER_DIR / "secrets" / "rotation"
COMPOSE = ["docker", "compose", "-f", str(DOCKER_DIR / "docker-compose.yml")]
PG_CONTAINER = "banking-postgres"
TRINO_CONTAINER = "banking-trino"

# Secret đổi bởi --apply, và các service phải được tạo lại để nhận giá trị mới.
ROTATED = {
    "POSTGRES_PASSWORD": (
        "iceberg-rest",
        "spark-worker-1",
        "mlflow",
        "superset",
        "airflow-webserver",
        "airflow-scheduler",
    ),
    "CDC_DB_PASSWORD": (),
    "MINIO_ROOT_PASSWORD": (
        "minio",
        "iceberg-rest",
        "spark-master",
        "spark-worker-1",
        "trino",
        "mlflow",
        "airflow-webserver",
        "airflow-scheduler",
    ),
}
GROUP_ROLES = ("etl_user", "analytics_user", "readonly_user")

PENDING = {
    "SUPERSET_ADMIN_PASSWORD": "superset fab reset-password --username admin (khi superset chạy)",
    "SUPERSET_SECRET_KEY": "superset re-encrypt-secrets với PREVIOUS_SECRET_KEY, rồi đổi",
    "AIRFLOW_ADMIN_PASSWORD": "airflow users reset-password (khi airflow chạy)",
    "AIRFLOW_FERNET_KEY": "airflow rotate-fernet-key với AIRFLOW__CORE__FERNET_KEY=new,old",
    "AIRFLOW_SECRET_KEY": "đổi trong .env rồi tạo lại webserver (chỉ ký session)",
    "OM_DB_PASSWORD": "ALTER USER trong om-mysql, rồi tạo lại openmetadata",
    "OM_MYSQL_ROOT_PASSWORD": "ALTER USER root trong om-mysql",
}
FOLLOW_UPS = (
    "Connection `postgres` trong Airflow được airflow-init tạo với POSTGRES_PASSWORD cũ — cập nhật khi Airflow chạy.",
    "Connector Debezium đã đăng ký giữ CDC_DB_PASSWORD cũ — đăng ký lại (code_etl/cdc/register_connectors.py).",
)


# ── Thuần: đọc / ghi .env, SCRAM, điều kiện tiên quyết ─────────────────────────


def parse_env(text: str) -> dict[str, str]:
    values = {}
    for line in text.splitlines():
        m = re.match(r"^([A-Z0-9_]+)=(.*)$", line.strip())
        if m:
            values[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return values


def replace_env_values(text: str, updates: dict[str, str]) -> str:
    """Thay đúng dòng KEY=… của các khóa cần đổi; giữ nguyên comment, thứ tự, dòng khác."""
    missing = set(updates)
    out = []
    for line in text.splitlines(keepends=True):
        m = re.match(r"^([A-Z0-9_]+)=", line)
        if m and m.group(1) in updates:
            newline = "\r\n" if line.endswith("\r\n") else ("\n" if line.endswith("\n") else "")
            out.append(f"{m.group(1)}={updates[m.group(1)]}{newline}")
            missing.discard(m.group(1))
        else:
            out.append(line)
    if missing:
        raise KeyError(f"không có trong .env: {sorted(missing)}")
    return "".join(out)


def scram_sha256_verifier(password: str, *, salt: bytes | None = None, iterations: int = 4096) -> str:
    """Verifier SCRAM-SHA-256 đúng định dạng Postgres lưu trong pg_authid (RFC 5802 / 7677)."""
    salt = salt if salt is not None else secrets.token_bytes(16)
    salted = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    b64 = lambda raw: base64.b64encode(raw).decode("ascii")  # noqa: E731
    return f"SCRAM-SHA-256${iterations}:{b64(salt)}${b64(stored_key)}:{b64(server_key)}"


def new_secret() -> str:
    # URL-safe: an toàn trong JDBC URL, biến compose, MinIO (>= 8 ký tự).
    return secrets.token_urlsafe(24)


def precondition_problems(repo: Path = REPO_ROOT) -> list[str]:
    problems = []
    conf = (repo / "docker" / "spark" / "conf" / "spark-defaults.conf").read_text(encoding="utf-8")
    if re.search(r"^\s*spark\.\S*(access[-.]key|secret[-.a-z]*key)\S*\s+\S+", conf, re.M | re.I):
        problems.append("spark-defaults.conf còn chứa key MinIO — merge #71 trước")
    props = (repo / "docker" / "init_trino" / "catalog" / "iceberg.properties").read_text(encoding="utf-8")
    for key in ("hive.s3.aws-access-key", "hive.s3.aws-secret-key"):
        m = re.search(rf"^{re.escape(key)}=(.*)$", props, re.M)
        if m and not m.group(1).startswith("${ENV:"):
            problems.append(f"iceberg.properties: {key} không đọc từ env — merge #71 trước")
    return problems


def sql_for(env: dict[str, str], *, lock_group_roles: bool) -> str:
    """SQL đổi mật khẩu — chỉ chứa verifier, không chứa mật khẩu dạng rõ."""
    user = env["POSTGRES_USER"]
    lines = [
        "\\set ON_ERROR_STOP on",
        f"ALTER ROLE \"{user}\" PASSWORD '{scram_sha256_verifier(env['POSTGRES_PASSWORD'])}';",
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cdc_user') THEN "
        "CREATE ROLE cdc_user WITH REPLICATION NOLOGIN; END IF; END $$;",
        f"ALTER ROLE cdc_user WITH LOGIN PASSWORD '{scram_sha256_verifier(env['CDC_DB_PASSWORD'])}';",
    ]
    if lock_group_roles:
        for role in GROUP_ROLES:
            lines.append(
                f"DO $$ BEGIN IF EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN "
                f"ALTER ROLE {role} NOLOGIN PASSWORD NULL; END IF; END $$;"
            )
    return "\n".join(lines) + "\n"


# ── Tác động lên stack ────────────────────────────────────────────────────────


def _run(cmd: list[str], *, stdin: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    out = subprocess.run(cmd, input=stdin, capture_output=True, text=True, encoding="utf-8", check=False)
    if check and out.returncode != 0:
        # stderr của docker/psql không chứa giá trị secret: SQL chỉ có verifier, qua stdin.
        raise RuntimeError(f"{' '.join(cmd[:4])} … exit {out.returncode}: {out.stderr.strip()[-400:]}")
    return out


def published_commits(value: str) -> int:
    if len(value) < 4:
        return 0
    out = _run(["git", "log", "--all", "--format=%h", "-S", value], check=False)
    return len(out.stdout.split())


def running_services() -> set[str]:
    out = _run([*COMPOSE, "ps", "--status", "running", "--services"])
    return set(out.stdout.split())


def apply_postgres(sql: str) -> None:
    # Socket local trong container: không cần mật khẩu, nên đổi được cả mật khẩu của chính user này.
    _run(
        ["docker", "exec", "-i", PG_CONTAINER, "sh", "-c", 'psql -q -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
        stdin=sql,
    )


def recreate(services: list[str]) -> None:
    if services:
        _run([*COMPOSE, "up", "-d", "--no-deps", "--force-recreate", *services])


def wait_healthy(services: list[str], timeout_s: int = 240) -> list[str]:
    deadline = time.time() + timeout_s
    pending = list(services)
    while pending and time.time() < deadline:
        out = _run([*COMPOSE, "ps", "--format", "json", *pending], check=False)
        rows = [json.loads(line) for line in out.stdout.splitlines() if line.strip().startswith("{")]
        ok = {r["Service"] for r in rows if r.get("State") == "running" and r.get("Health", "") in ("", "healthy")}
        pending = [s for s in pending if s not in ok]
        if pending:
            time.sleep(5)
    return pending


def can_login(user: str, password: str) -> bool:
    import psycopg2

    try:
        psycopg2.connect(host="localhost", port=5432, dbname="banking_db", user=user, password=password).close()
        return True
    except psycopg2.OperationalError:
        return False


def verify(new: dict[str, str], old: dict[str, str]) -> list[str]:
    failures = []
    user = new["POSTGRES_USER"]
    checks = [
        (can_login(user, new["POSTGRES_PASSWORD"]), f"{user} đăng nhập bằng mật khẩu mới"),
        (not can_login(user, old["POSTGRES_PASSWORD"]), f"{user} bị từ chối với mật khẩu cũ"),
        (can_login("cdc_user", new["CDC_DB_PASSWORD"]), "cdc_user đăng nhập bằng mật khẩu mới"),
        (not can_login("cdc_user", old["CDC_DB_PASSWORD"]), "cdc_user bị từ chối với mật khẩu cũ"),
    ]
    failures += [label for ok, label in checks if not ok]
    # Trino đọc file dữ liệu qua MinIO (credential mới) và metadata qua iceberg-rest (JDBC mới).
    q = _run(
        ["docker", "exec", TRINO_CONTAINER, "trino", "--execute", "SELECT count(*) FROM iceberg.gold.fraud_risk_txn"],
        check=False,
    )
    if q.returncode != 0 or not re.search(r'"?\d+"?\s*$', q.stdout.strip()):
        failures.append(f"Trino không đọc được Iceberg sau khi đổi: {q.stderr.strip()[-300:]}")
    return failures


# ── Luồng chính ──────────────────────────────────────────────────────────────


def print_plan(env: dict[str, str]) -> None:
    print("Secret trong docker/.env và số commit đang chứa GIÁ TRỊ HIỆN TẠI (không in giá trị):\n")
    for key in sorted(k for k in env if re.search(r"PASSWORD|SECRET|_KEY|TOKEN", k)):
        action = "đổi bằng --apply" if key in ROTATED else f"chờ: {PENDING.get(key, 'chưa phân loại')}"
        print(f"  {key:26} commits={published_commits(env[key]):>3}   {action}")
    print(f"\n  Role nhóm {', '.join(GROUP_ROLES)}: --apply đặt NOLOGIN, xoá mật khẩu.")
    problems = precondition_problems()
    print("\nĐiều kiện tiên quyết:", "đạt" if not problems else "")
    for p in problems:
        print(f"  ✗ {p}")


def rotate_to(target: dict[str, str], current: dict[str, str], *, lock_group_roles: bool) -> int:
    running = running_services()
    services = sorted({s for key in ROTATED for s in ROTATED[key]} & running, key=lambda s: s != "minio")
    print(f"Service sẽ tạo lại (đang chạy): {', '.join(services) or '—'}")

    apply_postgres(sql_for(target, lock_group_roles=lock_group_roles))
    print("  ✓ Postgres: mật khẩu đổi bằng SCRAM verifier" + (", role nhóm NOLOGIN" if lock_group_roles else ""))

    ENV_FILE.write_text(
        replace_env_values(ENV_FILE.read_text(encoding="utf-8"), {k: target[k] for k in ROTATED}), encoding="utf-8"
    )
    print("  ✓ docker/.env đã cập nhật")

    recreate(services)
    stuck = wait_healthy(services)
    if stuck:
        print(f"  ✗ chưa healthy: {stuck}")
        return 1
    print("  ✓ service đã tạo lại và healthy")

    failures = verify(target, current)
    for f in failures:
        print(f"  ✗ {f}")
    if not failures:
        print("  ✓ kiểm chứng: mật khẩu mới đăng nhập được, mật khẩu cũ bị từ chối, Trino đọc được Iceberg")
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="đổi thật (sao lưu .env trước)")
    mode.add_argument("--rollback", type=Path, help="khôi phục từ bản sao lưu do --apply tạo")
    args = parser.parse_args(argv)

    env = parse_env(ENV_FILE.read_text(encoding="utf-8"))
    if not (args.apply or args.rollback):
        print_plan(env)
        return 0

    problems = precondition_problems()
    if problems:
        print("Dừng — điều kiện tiên quyết chưa đạt:\n  " + "\n  ".join(problems))
        return 2

    if args.rollback:
        backup = parse_env(args.rollback.read_text(encoding="utf-8"))
        print(f"Khôi phục từ {args.rollback.name}")
        return rotate_to(backup, env, lock_group_roles=False)

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = BACKUP_DIR / f"env-{stamp}.bak"
    ignored = _run(["git", "check-ignore", "-q", str(backup_path.relative_to(REPO_ROOT))], check=False)
    if ignored.returncode != 0:
        print(f"Dừng — {backup_path} không bị gitignore")
        return 2
    backup_path.write_bytes(ENV_FILE.read_bytes())
    print(f"Sao lưu: {backup_path.relative_to(REPO_ROOT)}  (quay lại: --rollback {backup_path.relative_to(REPO_ROOT)})")

    target = dict(env) | {key: new_secret() for key in ROTATED}
    code = rotate_to(target, env, lock_group_roles=True)

    print("\nCòn chờ (service chưa chạy, cần công cụ của chính nó):")
    for key, how in PENDING.items():
        print(f"  {key:26} {how}")
    for note in FOLLOW_UPS:
        print(f"  • {note}")
    return code


if __name__ == "__main__":
    sys.exit(main())
