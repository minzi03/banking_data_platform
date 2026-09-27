"""
Sinh secret cho xác thực Trino (ADR-0016). Không in secret nào ra màn hình.

    py -3 scripts/bootstrap_trino_auth.py            # sinh cái còn thiếu, giữ cái đã có
    py -3 scripts/bootstrap_trino_auth.py --rotate   # sinh lại mọi mật khẩu và keystore
    py -3 scripts/bootstrap_trino_auth.py --check    # exit 1 nếu thiếu hoặc lệch rbac.py

Mọi thứ nằm trong docker/secrets/trino/ (đã gitignore):

  passwords.env            nguồn sự thật: TRINO_PASSWORD_<USER> cho mọi user của rbac.py,
                           TRINO_KEYSTORE_PASSWORD, TRINO_SHARED_SECRET. Chỉ công cụ chạy trên
                           HOST đọc file này (generator manifest, verifier) — không mount vào đâu.
  env/<user>.env           đúng MỘT dòng TRINO_PASSWORD=… — service nạp qua `env_file`, nên mỗi
                           client chỉ thấy mật khẩu của chính nó.
  env/trino-server.env     keystore password + shared secret + mật khẩu user `trino` cho CLI
                           trong container Trino.
  keystore.p12, trino.pem  keystore PKCS12 tự ký (CN=trino, SAN trino/localhost) và cert công khai
  password.db              user:iterations:salt:hash, PBKDF2-HMAC-SHA1

KHÔNG ghi vào docker/.env: chín service nạp nguyên file đó qua `env_file` (airflow,
postgres, minio, openmetadata…) và sẽ nhận mọi mật khẩu Trino, kể cả của admin — đo
bằng `docker compose config` khi thiết kế ADR-0016.

Danh sách user lấy từ governance/rbac.py — cùng nguồn với rules.json (ADR-0015):
không ai có mật khẩu mà không có role.

Định dạng hash ĐO trên Trino 443: PBKDF2 SHA-1 được nhận, SHA-256 cùng định dạng bị
từ chối. Keystore tạo bằng keytool trong chính image Trino đã pin.
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


def _load_users() -> dict:
    """USERS từ governance/rbac.py, nạp THẲNG file — không qua governance/__init__.py.

    Package governance import cả contracts (pydantic); job CI dựng stack chỉ cài
    pytest + pyyaml, và bootstrap phải chạy ở đó TRƯỚC khi dựng Trino.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("_rbac_for_bootstrap", REPO_ROOT / "governance" / "rbac.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # @dataclass cần module có trong sys.modules
    spec.loader.exec_module(module)
    return module.USERS


USERS = _load_users()

SECRETS_DIR = REPO_ROOT / "docker" / "secrets" / "trino"
TRINO_IMAGE = "trinodb/trino:443"
KEYTOOL = "/usr/lib/jvm/jdk-21.0/bin/keytool"
SERVER_USER = "trino"  # user của CLI trong container Trino (trino-cli.properties)

# OWASP khuyến nghị cho PBKDF2-HMAC-SHA1. Đo trên Trino 443: mỗi lần gọi CLI
# ~1,1–1,4s ở cả 210k lẫn 1,3M vòng — chi phí là JVM của CLI, không phải hash.
PBKDF2_ITERATIONS = 1_300_000
PBKDF2_KEY_BYTES = 16
SALT_BYTES = 16

KEYSTORE_PASSWORD_VAR = "TRINO_KEYSTORE_PASSWORD"
SHARED_SECRET_VAR = "TRINO_SHARED_SECRET"


def passwords_file() -> Path:
    return SECRETS_DIR / "passwords.env"


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


def _write(path: Path, values: dict[str, str], header: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(f"{k}={v}\n" for k, v in values.items())
    path.write_text(
        f"# {header}\n# Sinh bởi scripts/bootstrap_trino_auth.py (ADR-0016) — không sửa tay.\n{body}",
        encoding="utf-8",
        newline="\n",
    )


def plan(existing: dict[str, str], *, rotate: bool) -> dict[str, str]:
    """Giá trị mới cần sinh: cái thiếu (hoặc tất cả khi rotate). Cái đã có giữ nguyên."""
    wanted = [password_var(u) for u in sorted(USERS)] + [KEYSTORE_PASSWORD_VAR, SHARED_SECRET_VAR]
    return {
        k: secrets.token_urlsafe(32 if k == SHARED_SECRET_VAR else 24) for k in wanted if rotate or not existing.get(k)
    }


def service_env_files(env: dict[str, str]) -> dict[Path, dict[str, str]]:
    """Mỗi user một file chỉ chứa mật khẩu của nó; server một file riêng."""
    files = {SECRETS_DIR / "env" / f"{u}.env": {"TRINO_PASSWORD": env[password_var(u)]} for u in sorted(USERS)}
    files[SECRETS_DIR / "env" / "trino-server.env"] = {
        KEYSTORE_PASSWORD_VAR: env[KEYSTORE_PASSWORD_VAR],
        SHARED_SECRET_VAR: env[SHARED_SECRET_VAR],
        "TRINO_PASSWORD": env[password_var(SERVER_USER)],
    }
    return files


def render_password_db(env: dict[str, str]) -> str:
    return "".join(f"{u}:{hash_password(env[password_var(u)])}\n" for u in sorted(USERS))


def problems(env: dict[str, str]) -> list[str]:
    """Mọi chỗ lệch: thiếu giá trị, thiếu file, password.db / env files không khớp passwords.env."""
    found = [f"thiếu {k} trong passwords.env" for k in plan(env, rotate=False)]
    if found:
        return found
    db = SECRETS_DIR / "password.db"
    if not db.exists():
        found.append("thiếu password.db")
    else:
        stored = dict(line.split(":", 1) for line in db.read_text(encoding="utf-8").splitlines() if line.strip())
        found += [f"password.db thiếu user {u}" for u in sorted(set(USERS) - set(stored))]
        found += [f"password.db có user thừa {u} (không có trong rbac.py)" for u in sorted(set(stored) - set(USERS))]
        found += [
            f"hash của {u} không khớp passwords.env"
            for u in sorted(set(USERS) & set(stored))
            if not verify_password(env[password_var(u)], stored[u])
        ]
    for path, values in service_env_files(env).items():
        if read_env(path) != values:
            found.append(f"{path.relative_to(SECRETS_DIR).as_posix()} thiếu hoặc lệch passwords.env")
    found += [f"thiếu {n}" for n in ("keystore.p12", "trino.pem") if not (SECRETS_DIR / n).exists()]
    return found


def keytool_command(args: list[str]) -> list[str]:
    """`docker run` chạy keytool của image Trino, ghi vào SECRETS_DIR.

    Trên POSIX chạy dưới uid:gid của máy chủ: image mặc định chạy user `trino`
    (uid 1000), còn thư mục mount do runner CI tạo thuộc uid khác — keytool không
    ghi được (CI đỏ 2026-09-27). Docker Desktop trên Windows không có vấn đề này.
    """
    cmd = ["docker", "run", "--rm", "-v", f"{SECRETS_DIR.resolve()}:/out", "-e", "KS_PASS"]
    if hasattr(os, "getuid"):
        cmd += ["--user", f"{os.getuid()}:{os.getgid()}"]
    return [*cmd, "--entrypoint", KEYTOOL, TRINO_IMAGE, *args]


def _keytool(args: list[str], keystore_password: str) -> None:
    env = {**os.environ, "KS_PASS": keystore_password, "MSYS_NO_PATHCONV": "1"}
    result = subprocess.run(keytool_command(args), env=env, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        # keytool in lỗi ra STDOUT ("keytool error: …"), không phải stderr — đo 2026-09-27.
        # Không chứa mật khẩu: mật khẩu đi qua biến môi trường KS_PASS.
        output = (result.stdout + result.stderr).strip()[-600:]
        raise RuntimeError(f"keytool {args[0]} thất bại (exit {result.returncode}): {output}")


def generate_keystore(keystore_password: str) -> None:
    """Keystore PKCS12 + cert PEM, bằng keytool của image Trino đã pin."""
    for name in ("keystore.p12", "trino.pem"):
        (SECRETS_DIR / name).unlink(missing_ok=True)
    _keytool(
        ["-genkeypair", "-alias", "trino", "-keyalg", "RSA", "-keysize", "2048", "-validity", "825",
         "-dname", "CN=trino", "-ext", "SAN=dns:trino,dns:localhost,ip:127.0.0.1",
         "-keystore", "/out/keystore.p12", "-storetype", "PKCS12", "-storepass:env", "KS_PASS"],
        keystore_password,
    )  # fmt: skip
    _keytool(
        ["-exportcert", "-rfc", "-alias", "trino", "-keystore", "/out/keystore.p12",
         "-storepass:env", "KS_PASS", "-file", "/out/trino.pem"],
        keystore_password,
    )  # fmt: skip


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--rotate", action="store_true", help="sinh lại mọi mật khẩu và keystore")
    parser.add_argument("--check", action="store_true", help="chỉ kiểm, exit 1 nếu thiếu hoặc lệch")
    args = parser.parse_args(argv)

    existing = read_env(passwords_file())
    if args.check:
        found = problems(existing)
        for p in found:
            print(f"  ✗ {p}", file=sys.stderr)
        print("Trino auth: OK" if not found else f"Trino auth: {len(found)} vấn đề")
        return 1 if found else 0

    updates = plan(existing, rotate=args.rotate)
    env = {**existing, **updates}
    SECRETS_DIR.mkdir(parents=True, exist_ok=True)
    if updates:
        ordered = {
            k: env[k] for k in [password_var(u) for u in sorted(USERS)] + [KEYSTORE_PASSWORD_VAR, SHARED_SECRET_VAR]
        }
        _write(passwords_file(), ordered, "Nguồn sự thật cho mật khẩu Trino — chỉ công cụ trên host đọc")
    for path, values in service_env_files(env).items():
        if read_env(path) != values:
            _write(path, values, f"Chỉ mật khẩu cho {path.stem}")
    # Thiếu MỘT trong hai (keystore hoặc cert) là sinh lại cả cặp: cert phải khớp keystore.
    keypair_missing = not all((SECRETS_DIR / n).exists() for n in ("keystore.p12", "trino.pem"))
    if args.rotate or KEYSTORE_PASSWORD_VAR in updates or keypair_missing:
        generate_keystore(env[KEYSTORE_PASSWORD_VAR])
    db = SECRETS_DIR / "password.db"
    if args.rotate or updates or not db.exists() or any("password.db" in p or "hash" in p for p in problems(env)):
        db.write_text(render_password_db(env), encoding="utf-8", newline="\n")

    # Chỉ in TÊN khoá, không in giá trị.
    print(
        f"Trino auth: {len(USERS)} user từ rbac.py · {len(updates)} giá trị mới trong {passwords_file().relative_to(REPO_ROOT).as_posix()}"
    )
    for key in sorted(updates):
        print(f"  + {key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
