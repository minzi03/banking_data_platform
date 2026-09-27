"""
scripts/bootstrap_trino_auth.py — secret cho xác thực Trino (ADR-0016).

Không cần Docker: keystore (keytool trong image Trino) được thay bằng stub.
Định dạng hash đã được ĐO trên Trino 443 khi quyết định ADR-0016: PBKDF2
HMAC-**SHA1**, `iterations:salt_hex:hash_hex`. SHA-256 cùng định dạng bị từ
chối — test khoá đúng thuật toán đó.
"""

from __future__ import annotations

import hashlib
import importlib.util
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
    env_file = tmp_path / ".env"
    env_file.write_text("# giữ nguyên\nPOSTGRES_USER=banking_admin\n", encoding="utf-8")
    monkeypatch.setattr(boot, "SECRETS_DIR", secrets_dir)
    monkeypatch.setattr(boot, "ENV_FILE", env_file)
    monkeypatch.setattr(boot, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(boot, "PBKDF2_ITERATIONS", FAST)

    def fake_keystore(_password: str) -> None:
        secrets_dir.mkdir(parents=True, exist_ok=True)
        (secrets_dir / "keystore.p12").write_bytes(b"stub")
        (secrets_dir / "trino.pem").write_text("stub", encoding="utf-8")

    monkeypatch.setattr(boot, "generate_keystore", fake_keystore)
    real_hash = boot.hash_password
    monkeypatch.setattr(
        boot, "hash_password", lambda pw, *, iterations=FAST, salt=None: real_hash(pw, iterations=iterations, salt=salt)
    )
    return secrets_dir, env_file


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


def test_one_password_per_rbac_user_and_nothing_printed(sandbox, capsys):
    secrets_dir, env_file = sandbox
    assert boot.main([]) == 0
    env = boot.read_env(env_file)

    for user in USERS:
        assert env.get(boot.password_var(user)), f"thiếu mật khẩu cho {user}"
    assert env["POSTGRES_USER"] == "banking_admin", "khoá có sẵn bị mất"
    assert "# giữ nguyên" in env_file.read_text(encoding="utf-8")

    printed = capsys.readouterr()
    secret_values = [v for k, v in env.items() if k.startswith("TRINO_")]
    for value in secret_values:
        assert value not in printed.out + printed.err, "script in giá trị secret ra màn hình"

    db_users = {line.split(":", 1)[0] for line in (secrets_dir / "password.db").read_text().splitlines()}
    assert db_users == set(USERS)
    assert boot.password_db_matches(secrets_dir / "password.db", env) == []


def test_rerun_keeps_existing_passwords(sandbox):
    _secrets, env_file = sandbox
    boot.main([])
    first = boot.read_env(env_file)
    boot.main([])
    assert boot.read_env(env_file) == first


def test_rotate_changes_every_password(sandbox):
    _secrets, env_file = sandbox
    boot.main([])
    first = boot.read_env(env_file)
    boot.main(["--rotate"])
    second = boot.read_env(env_file)
    for user in USERS:
        key = boot.password_var(user)
        assert first[key] != second[key]


def test_check_fails_when_a_hash_no_longer_matches(sandbox):
    secrets_dir, env_file = sandbox
    boot.main([])
    assert boot.main(["--check"]) == 0
    user = sorted(USERS)[0]
    boot.write_env(env_file, {boot.password_var(user): "changed-behind-its-back"})
    assert boot.main(["--check"]) == 1
    assert any(user in p for p in boot.password_db_matches(secrets_dir / "password.db", boot.read_env(env_file)))


def test_secrets_land_only_in_gitignored_paths():
    import subprocess

    for rel in ("docker/.env", "docker/secrets/trino/password.db", "docker/secrets/trino/keystore.p12"):
        out = subprocess.run(["git", "check-ignore", rel], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
        assert out.returncode == 0, f"{rel} KHÔNG bị gitignore — secret có thể bị commit"
