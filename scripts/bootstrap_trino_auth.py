"""
Sinh secret cho xác thực Trino (ADR-0016). Không in secret nào ra màn hình.

    py -3 scripts/bootstrap_trino_auth.py            # sinh cái còn thiếu, giữ cái đã có
    py -3 scripts/bootstrap_trino_auth.py --rotate   # sinh lại mọi mật khẩu và keystore
    py -3 scripts/bootstrap_trino_auth.py --check    # exit 1 nếu thiếu hoặc lệch rbac.py

Ghi vào hai nơi, cả hai đã gitignore:

  docker/secrets/trino/keystore.p12     keystore PKCS12 tự ký (CN=trino, SAN trino/localhost)
  docker/secrets/trino/trino.pem        cert công khai — client dùng để verify HTTPS
  docker/secrets/trino/password.db      user:iterations:salt:hash, PBKDF2-HMAC-SHA1
  docker/.env                           TRINO_PASSWORD_<USER>, TRINO_KEYSTORE_PASSWORD,
                                        TRINO_SHARED_SECRET

Danh sách user lấy từ governance/rbac.py — cùng nguồn với rules.json (ADR-0015):
không ai có mật khẩu mà không có role.

Định dạng hash ĐO trên Trino 443, không suy từ tài liệu: PBKDF2 SHA-1 được nhận,
SHA-256 cùng định dạng bị từ chối. Keystore tạo bằng keytool trong chính image
Trino đã pin — không thêm image hay gói Python nào.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import secrets
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from governance.rbac import USERS  # noqa: E402

SECRETS_DIR = REPO_ROOT / "docker" / "secrets" / "trino"
ENV_FILE = REPO_ROOT / "docker" / ".env"
TRINO_IMAGE = "trinodb/trino:443"
KEYTOOL = "/usr/lib/jvm/jdk-21.0/bin/keytool"

# OWASP khuyến nghị cho PBKDF2-HMAC-SHA1. Đo trên Trino 443: mỗi lần gọi CLI
# ~1,1–1,4s ở cả 210k lẫn 1,3M vòng — chi phí là JVM của CLI, không phải hash.
PBKDF2_ITERATIONS = 1_300_000
PBKDF2_KEY_BYTES = 16
SALT_BYTES = 16

KEYSTORE_PASSWORD_VAR = "TRINO_KEYSTORE_PASSWORD"
SHARED_SECRET_VAR = "TRINO_SHARED_SECRET"


def password_var(username: str) -> str:
    return f"TRINO_PASSWORD_{username.upper()}"


def hash_password(password: str, *, iterations: int = PBKDF2_ITERATIONS, salt: bytes | None = None) -> str:
    """`iterations:salt_hex:hash_hex` — phần sau dấu `user:` của một dòng password.db."""
    salt = salt if salt is not None else os.urandom(SALT_BYTES)
    digest = hashlib.pbkdf2_hmac("sha1", password.encode(), salt, iterations, dklen=PBKDF2_KEY_BYTES)
    return f"{iterations}:{salt.hex()}:{digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    iterations, salt_hex, hash_hex = stored.split(":")
    expected = hash_password(password, iterations=int(iterations), salt=bytes.fromhex(salt_hex))
    return hmac.compare_digest(expected.split(":")[2], hash_hex)


def read_env(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.lstrip().startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def write_env(path: Path, updates: dict[str, str]) -> None:
    """Cập nhật/thêm khoá, giữ nguyên mọi dòng khác (comment, thứ tự, khoá không liên quan)."""
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    pending = dict(updates)
    out = []
    for line in lines:
        key = line.partition("=")[0].strip()
        if "=" in line and not line.lstrip().startswith("#") and key in pending:
            out.append(f"{key}={pending.pop(key)}")
        else:
            out.append(line)
    if pending:
        out += ["", "# Trino authentication — sinh bởi scripts/bootstrap_trino_auth.py (ADR-0016)"]
        out += [f"{k}={v}" for k, v in pending.items()]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")


def plan_env(existing: dict[str, str], *, rotate: bool) -> dict[str, str]:
    """Giá trị cần ghi vào .env: sinh mới cái thiếu (hoặc tất cả khi rotate), giữ cái đã có."""
    wanted = [password_var(u) for u in USERS] + [KEYSTORE_PASSWORD_VAR, SHARED_SECRET_VAR]
    updates = {}
    for key in wanted:
        if rotate or not existing.get(key):
            updates[key] = secrets.token_urlsafe(32 if key == SHARED_SECRET_VAR else 24)
    return updates


def render_password_db(env: dict[str, str]) -> str:
    return "".join(f"{u}:{hash_password(env[password_var(u)])}\n" for u in sorted(USERS))


def password_db_matches(path: Path, env: dict[str, str]) -> list[str]:
    """Lỗi nếu password.db thiếu user của rbac.py, thừa user, hoặc hash không khớp .env."""
    if not path.exists():
        return [f"{path.relative_to(REPO_ROOT)} không tồn tại"]
    stored = dict(line.split(":", 1) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    problems = [f"thiếu user {u}" for u in sorted(set(USERS) - set(stored))]
    problems += [f"user thừa {u} (không có trong rbac.py)" for u in sorted(set(stored) - set(USERS))]
    for user in sorted(set(USERS) & set(stored)):
        password = env.get(password_var(user))
        if not password or not verify_password(password, stored[user]):
            problems.append(f"hash của {user} không khớp {password_var(user)} trong docker/.env")
    return problems


def generate_keystore(keystore_password: str) -> None:
    """Keystore PKCS12 + cert PEM, bằng keytool của image Trino đã pin."""
    for name in ("keystore.p12", "trino.pem"):
        (SECRETS_DIR / name).unlink(missing_ok=True)
    mount = f"{SECRETS_DIR.resolve()}:/out"
    base = ["docker", "run", "--rm", "-v", mount, "-e", "KS_PASS", "--entrypoint", KEYTOOL, TRINO_IMAGE]
    env = {**os.environ, "KS_PASS": keystore_password, "MSYS_NO_PATHCONV": "1"}
    subprocess.run(
        [*base, "-genkeypair", "-alias", "trino", "-keyalg", "RSA", "-keysize", "2048", "-validity", "825",
         "-dname", "CN=trino", "-ext", "SAN=dns:trino,dns:localhost,ip:127.0.0.1",
         "-keystore", "/out/keystore.p12", "-storetype", "PKCS12", "-storepass:env", "KS_PASS"],
        check=True, env=env, capture_output=True,
    )  # fmt: skip
    subprocess.run(
        [*base, "-exportcert", "-rfc", "-alias", "trino", "-keystore", "/out/keystore.p12",
         "-storepass:env", "KS_PASS", "-file", "/out/trino.pem"],
        check=True, env=env, capture_output=True,
    )  # fmt: skip


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--rotate", action="store_true", help="sinh lại mọi mật khẩu và keystore")
    parser.add_argument("--check", action="store_true", help="chỉ kiểm, exit 1 nếu thiếu hoặc lệch")
    args = parser.parse_args(argv)

    existing = read_env(ENV_FILE)
    if args.check:
        problems = [f"thiếu {k} trong docker/.env" for k in plan_env(existing, rotate=False)]
        problems += password_db_matches(SECRETS_DIR / "password.db", existing)
        problems += [f"thiếu {n}" for n in ("keystore.p12", "trino.pem") if not (SECRETS_DIR / n).exists()]
        for p in problems:
            print(f"  ✗ {p}", file=sys.stderr)
        print("Trino auth: OK" if not problems else f"Trino auth: {len(problems)} vấn đề")
        return 1 if problems else 0

    updates = plan_env(existing, rotate=args.rotate)
    env = {**existing, **updates}
    SECRETS_DIR.mkdir(parents=True, exist_ok=True)
    if updates:
        write_env(ENV_FILE, updates)
    if args.rotate or KEYSTORE_PASSWORD_VAR in updates or not (SECRETS_DIR / "keystore.p12").exists():
        generate_keystore(env[KEYSTORE_PASSWORD_VAR])
    if args.rotate or updates or password_db_matches(SECRETS_DIR / "password.db", env):
        (SECRETS_DIR / "password.db").write_text(render_password_db(env), encoding="utf-8", newline="\n")

    # Chỉ in TÊN khoá, không in giá trị.
    print(f"Trino auth: {len(USERS)} user từ rbac.py · {len(updates)} giá trị mới trong docker/.env")
    for key in sorted(updates):
        print(f"  + {key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
