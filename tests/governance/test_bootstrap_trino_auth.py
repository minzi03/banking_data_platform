"""
scripts/bootstrap_trino_auth.py — secret cho xác thực Trino (ADR-0016).

Không cần Docker: keystore (keytool trong image Trino) được thay bằng stub.
Định dạng hash đã được ĐO trên Trino 443 khi quyết định ADR-0016: PBKDF2
HMAC-**SHA1**, `iterations:salt_hex:hash_hex`. SHA-256 cùng định dạng bị từ
chối — test khoá đúng thuật toán đó.

Phạm vi lộ: mỗi service chỉ nhận mật khẩu của chính nó (env/<user>.env). Bản
đầu ghi vào docker/.env, mà chín service nạp nguyên file đó — mỗi service thấy
cả 14 mật khẩu, kể cả admin. Test khoá cả hai điều.
"""

from __future__ import annotations

import hashlib
import importlib.util
import subprocess
from pathlib import Path

import pytest

from governance.rbac import USERS

REPO_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "bootstrap_trino_auth", REPO_ROOT / "scripts" / "bootstrap_trino_auth.py"
)
boot = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(boot)

FAST = 1000  # vòng lặp tối thiểu Trino nhận — đủ cho test, không phải cho thật


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Chạy script trên thư mục tạm, keystore là stub, hash nhanh."""
    secrets_dir = tmp_path / "secrets" / "trino"
    monkeypatch.setattr(boot, "SECRETS_DIR", secrets_dir)
    monkeypatch.setattr(boot, "REPO_ROOT", tmp_path)

    def fake_keystore(_password: str) -> None:
        secrets_dir.mkdir(parents=True, exist_ok=True)
        (secrets_dir / "keystore.p12").write_bytes(b"stub")
        (secrets_dir / "trino.pem").write_text("stub", encoding="utf-8")

    monkeypatch.setattr(boot, "generate_keystore", fake_keystore)
    real_hash = boot.hash_password
    monkeypatch.setattr(
        boot,
        "hash_password",
        lambda pw, *, iterations=FAST, salt=None: real_hash(pw, iterations=iterations, salt=salt),
    )
    return secrets_dir


def test_hash_is_pbkdf2_sha1_in_the_format_trino_accepts():
    salt = bytes(range(16))
    stored = boot.hash_password("s3cret", iterations=FAST, salt=salt)
    iterations, salt_hex, hash_hex = stored.split(":")
    assert (int(iterations), salt_hex) == (FAST, salt.hex())
    assert hash_hex == hashlib.pbkdf2_hmac("sha1", b"s3cret", salt, FAST, dklen=16).hex()


def test_default_iterations_are_the_owasp_level():
    assert boot.PBKDF2_ITERATIONS >= 1_300_000


def test_verify_accepts_the_right_password_only():
    stored = boot.hash_password("right", iterations=FAST)
    assert boot.verify_password("right", stored)
    assert not boot.verify_password("wrong", stored)


def test_each_service_file_holds_only_its_own_password(sandbox, capsys):
    assert boot.main([]) == 0
    source = boot.read_env(boot.passwords_file())

    for user in USERS:
        values = boot.read_env(sandbox / "env" / f"{user}.env")
        assert values == {"TRINO_PASSWORD": source[boot.password_var(user)]}, f"{user}.env phải chứa đúng MỘT mật khẩu"
    server = boot.read_env(sandbox / "env" / "trino-server.env")
    assert set(server) == {"TRINO_KEYSTORE_PASSWORD", "TRINO_SHARED_SECRET", "TRINO_PASSWORD"}
    assert server["TRINO_PASSWORD"] == source[boot.password_var("trino")]

    printed = capsys.readouterr()
    for value in source.values():
        assert value not in printed.out + printed.err, "script in giá trị secret ra màn hình"

    db_users = {line.split(":", 1)[0] for line in (sandbox / "password.db").read_text().splitlines()}
    assert db_users == set(USERS)
    assert boot.problems(source) == []


def test_docker_env_is_never_written(sandbox):
    """docker/.env được chín service nạp nguyên file — không được chứa secret Trino."""
    docker_env = sandbox.parents[1] / ".env"
    docker_env.write_text("POSTGRES_USER=banking_admin\n", encoding="utf-8")
    boot.main([])
    assert docker_env.read_text(encoding="utf-8") == "POSTGRES_USER=banking_admin\n"


def test_rerun_keeps_existing_passwords(sandbox):
    boot.main([])
    first = boot.read_env(boot.passwords_file())
    boot.main([])
    assert boot.read_env(boot.passwords_file()) == first


def test_rotate_changes_every_password(sandbox):
    boot.main([])
    first = boot.read_env(boot.passwords_file())
    boot.main(["--rotate"])
    second = boot.read_env(boot.passwords_file())
    for user in USERS:
        key = boot.password_var(user)
        assert first[key] != second[key]


def test_check_fails_when_a_service_file_drifts(sandbox):
    boot.main([])
    assert boot.main(["--check"]) == 0
    user = sorted(USERS)[0]
    (sandbox / "env" / f"{user}.env").write_text("TRINO_PASSWORD=changed-behind-its-back\n", encoding="utf-8")
    assert boot.main(["--check"]) == 1


def test_check_fails_when_a_hash_no_longer_matches(sandbox):
    boot.main([])
    user = sorted(USERS)[0]
    db = sandbox / "password.db"
    lines = [
        f"{user}:{boot.hash_password('other')}" if line.startswith(f"{user}:") else line
        for line in db.read_text().splitlines()
    ]
    db.write_text("\n".join(lines) + "\n")
    assert boot.main(["--check"]) == 1


def test_secrets_land_only_in_gitignored_paths():
    for rel in (
        "docker/secrets/trino/passwords.env",
        "docker/secrets/trino/env/dbt.env",
        "docker/secrets/trino/password.db",
        "docker/secrets/trino/keystore.p12",
    ):
        out = subprocess.run(["git", "check-ignore", rel], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
        assert out.returncode == 0, f"{rel} KHÔNG bị gitignore — secret có thể bị commit"


def test_keytool_runs_as_the_host_user_on_posix(monkeypatch):
    """Trên runner Linux, container chạy user `trino` (uid 1000) không ghi được thư mục
    mount thuộc uid của runner — CI đỏ 2026-09-27. keytool phải chạy dưới uid:gid của host."""
    monkeypatch.setattr(boot.os, "getuid", lambda: 1001, raising=False)
    monkeypatch.setattr(boot.os, "getgid", lambda: 127, raising=False)
    cmd = boot.keytool_command(["-help"])
    assert cmd[cmd.index("--user") + 1] == "1001:127"
    assert cmd.index("--user") < cmd.index(boot.TRINO_IMAGE), "--user phải đứng trước image"


def test_keytool_failure_reports_stderr(tmp_path, monkeypatch):
    # SECRETS_DIR tạm: generate_keystore XOÁ keystore cũ trước khi gọi keytool. Bản
    # đầu của test này chạy trên thư mục thật và đã xoá keystore của máy dev.
    monkeypatch.setattr(boot, "SECRETS_DIR", tmp_path)

    class Failed:
        # keytool in lỗi ra stdout, stderr rỗng — đúng như đo được.
        returncode = 1
        stdout = "keytool error: java.io.FileNotFoundException: /out/keystore.p12 (Permission denied)"
        stderr = ""

    monkeypatch.setattr(boot.subprocess, "run", lambda *a, **k: Failed())
    with pytest.raises(RuntimeError, match="Permission denied"):
        boot.generate_keystore("pw")


def test_missing_cert_regenerates_the_keypair(sandbox):
    """Keystore còn mà cert mất (xảy ra thật khi một lần chạy dở) → phải sinh lại, không bỏ qua."""
    boot.main([])
    (sandbox / "trino.pem").unlink()
    assert boot.main(["--check"]) == 1
    boot.main([])
    assert (sandbox / "trino.pem").exists()
    assert boot.main(["--check"]) == 0
