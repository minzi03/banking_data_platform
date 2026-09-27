"""
scripts/rotate_local_secrets.py — phần thuần, không chạm stack.

Phần chạm stack (ALTER ROLE, tạo lại service, kiểm chứng đăng nhập) do NGƯỜI DÙNG chạy
bằng --apply. Ở đây khoá những thứ mà sai thì hỏng âm thầm: verifier SCRAM (sai là khoá
luôn user quản trị Postgres), việc ghi .env (sai là mất dòng khác), SQL không mang mật
khẩu dạng rõ, bản sao lưu nằm trong thư mục gitignore, và chế độ lập kế hoạch không in
giá trị nào.
"""

from __future__ import annotations

import base64
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("_rotate", REPO_ROOT / "scripts" / "rotate_local_secrets.py")
rot = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = rot
_spec.loader.exec_module(rot)

# Postgres 15.19 tự băm mật khẩu thử `pencil` (CREATE ROLE … PASSWORD 'pencil'), 2026-09-27.
PG_VERIFIER = (
    "SCRAM-SHA-256$4096:WG1Wq0VFDZTRKA/OagtW0g==$TY+PFHQrP49+dAG2rc/ycCvXAyHonCDAq0t2F/zR7xY="
    ":avyBcsPp6X3wOWO62CML1DByEkDi++p6sPFXDQAkESg="
)


def test_scram_verifier_matches_postgres_byte_for_byte():
    salt = base64.b64decode(PG_VERIFIER.split("$")[1].split(":")[1])
    assert rot.scram_sha256_verifier("pencil", salt=salt, iterations=4096) == PG_VERIFIER


def test_scram_verifier_uses_a_fresh_salt():
    assert rot.scram_sha256_verifier("x") != rot.scram_sha256_verifier("x")


def test_replace_env_values_touches_only_the_rotated_lines():
    text = "# comment\r\nPOSTGRES_USER=admin\r\nPOSTGRES_PASSWORD=old\r\nOTHER=keep\r\nMINIO_ROOT_PASSWORD=old2"
    out = rot.replace_env_values(text, {"POSTGRES_PASSWORD": "new", "MINIO_ROOT_PASSWORD": "new2"})
    assert out == "# comment\r\nPOSTGRES_USER=admin\r\nPOSTGRES_PASSWORD=new\r\nOTHER=keep\r\nMINIO_ROOT_PASSWORD=new2"


def test_replace_env_values_refuses_a_missing_key():
    with pytest.raises(KeyError):
        rot.replace_env_values("A=1\n", {"POSTGRES_PASSWORD": "new"})


def test_sql_carries_verifiers_not_passwords():
    env = {
        "POSTGRES_USER": "banking_admin",
        "POSTGRES_PASSWORD": "plain-pg-value",
        "CDC_DB_PASSWORD": "plain-cdc-value",
    }
    sql = rot.sql_for(env, lock_group_roles=True)
    assert "plain-pg-value" not in sql and "plain-cdc-value" not in sql
    assert sql.count("SCRAM-SHA-256$4096:") == 2
    assert all(f"ALTER ROLE {role} NOLOGIN PASSWORD NULL" in sql for role in rot.GROUP_ROLES)
    assert "NOLOGIN PASSWORD NULL" not in rot.sql_for(env, lock_group_roles=False)


def test_new_secrets_are_long_url_safe_and_distinct():
    values = {rot.new_secret() for _ in range(20)}
    assert len(values) == 20
    assert all(
        len(v) >= 32 and set(v) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
        for v in values
    )


def test_every_rotated_key_exists_in_the_env_example():
    example = rot.parse_env((REPO_ROOT / "docker" / ".env.example").read_text(encoding="utf-8"))
    assert set(rot.ROTATED) <= set(example)


def test_backups_land_in_a_gitignored_directory():
    rel = (rot.BACKUP_DIR / "env-20260927T000000Z.bak").relative_to(REPO_ROOT).as_posix()
    out = subprocess.run(["git", "check-ignore", rel], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert out.returncode == 0, f"{rel} KHÔNG bị gitignore — bản sao lưu chứa mọi secret"


def _fake_repo(tmp_path: Path, conf: str, props: str) -> Path:
    (tmp_path / "docker" / "spark" / "conf").mkdir(parents=True)
    (tmp_path / "docker" / "init_trino" / "catalog").mkdir(parents=True)
    (tmp_path / "docker" / "spark" / "conf" / "spark-defaults.conf").write_text(conf, encoding="utf-8")
    (tmp_path / "docker" / "init_trino" / "catalog" / "iceberg.properties").write_text(props, encoding="utf-8")
    return tmp_path


def test_preconditions_block_while_engine_config_holds_keys(tmp_path):
    repo = _fake_repo(
        tmp_path,
        "spark.hadoop.fs.s3a.secret.key   value\n",
        "hive.s3.aws-access-key=minio\nhive.s3.aws-secret-key=value\n",
    )
    assert len(rot.precondition_problems(repo)) == 3


def test_preconditions_pass_once_credentials_come_from_env(tmp_path):
    repo = _fake_repo(
        tmp_path,
        "spark.hadoop.fs.s3a.aws.credentials.provider   org.x.Provider\n",
        "hive.s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}\nhive.s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}\n",
    )
    assert rot.precondition_problems(repo) == []


def test_plan_mode_prints_no_secret_value(tmp_path, monkeypatch, capsys):
    env_file = tmp_path / ".env"
    env_file.write_text("POSTGRES_PASSWORD=value-one-9f2c\nMINIO_ROOT_PASSWORD=value-two-7a1b\n", encoding="utf-8")
    monkeypatch.setattr(rot, "ENV_FILE", env_file)
    monkeypatch.setattr(rot, "published_commits", lambda value: 3)
    monkeypatch.setattr(rot, "precondition_problems", lambda: [])
    assert rot.main([]) == 0
    out = capsys.readouterr().out
    assert "POSTGRES_PASSWORD" in out and "commits=  3" in out
    assert "value-one-9f2c" not in out and "value-two-7a1b" not in out


def test_compose_gets_the_new_values_even_with_stale_shell_variables(monkeypatch):
    """Lần --apply đầu tiên: $env:POSTGRES_PASSWORD cũ trong shell đè .env, iceberg-rest nhận mật khẩu cũ."""
    monkeypatch.setenv("POSTGRES_PASSWORD", "stale-shell-value")
    monkeypatch.delenv("MINIO_ROOT_PASSWORD", raising=False)
    target = {
        "POSTGRES_USER": "banking_admin",
        "POSTGRES_PASSWORD": "n1",
        "CDC_DB_PASSWORD": "n2",
        "MINIO_ROOT_PASSWORD": "n3",
    }
    env = rot.compose_env(target)
    assert env["POSTGRES_PASSWORD"] == "n1"
    assert env["MINIO_ROOT_PASSWORD"] == "n3"
    assert rot.shell_overrides(target) == ["POSTGRES_PASSWORD"]


def test_recreate_passes_the_explicit_environment_to_compose(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "stale-shell-value")
    seen = {}
    monkeypatch.setattr(rot, "_run", lambda cmd, **kw: seen.update(cmd=cmd, **kw))
    target = {"POSTGRES_USER": "u", "POSTGRES_PASSWORD": "n1", "CDC_DB_PASSWORD": "n2", "MINIO_ROOT_PASSWORD": "n3"}
    rot.recreate(["iceberg-rest"], target)
    assert "--force-recreate" in seen["cmd"]
    assert seen["env"]["POSTGRES_PASSWORD"] == "n1"
