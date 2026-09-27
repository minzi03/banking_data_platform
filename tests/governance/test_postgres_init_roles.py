r"""
Role Postgres khởi tạo không mang mật khẩu viết cứng
====================================================

`docker/init_postgres/` từng tạo bốn role LOGIN với mật khẩu viết thẳng trong
file SQL: cdc_user (05-cdc-setup.sql) và etl_user / analytics_user /
readonly_user (05_security.sql). Test quét secret runtime không đọc `.sql`, nên
không thấy. Không service nào đăng nhập bằng ba role sau — chúng là nhóm quyền,
nay NOLOGIN. cdc_user cần đăng nhập (Debezium): mật khẩu từ CDC_DB_PASSWORD qua
`\getenv` của psql; thiếu biến thì role NOLOGIN, không có mặc định.

Kiểm trên Postgres 15 thật (container tạm, 2026-09-27): có biến → cdc_user đăng
nhập được qua scram, sai mật khẩu bị từ chối; không biến → cdc_user NOLOGIN; ba
role còn lại rolcanlogin = false trong cả hai trường hợp.
"""

from __future__ import annotations

import re
from pathlib import Path

INIT_DIR = Path(__file__).resolve().parents[2] / "docker" / "init_postgres"
GROUP_ROLES = ("etl_user", "analytics_user", "readonly_user")


def _init_files() -> dict[str, str]:
    return {p.name: p.read_text(encoding="utf-8") for p in sorted(INIT_DIR.iterdir()) if p.is_file()}


FILES = _init_files()


def test_init_scripts_are_found():
    assert {"05-cdc-setup.sh", "05_security.sql"} <= set(FILES)


def test_no_password_literal_in_init_scripts():
    hits = [
        f"{name}: {m.group(0)[:40]}…"
        for name, text in FILES.items()
        for m in re.finditer(r"PASSWORD\s+'[^']*'", text, re.IGNORECASE)
    ]
    assert not hits, "Mật khẩu viết cứng trong init Postgres:\n" + "\n".join(hits)


def test_group_roles_cannot_log_in():
    text = FILES["05_security.sql"]
    for role in GROUP_ROLES:
        created = re.findall(rf"CREATE ROLE {role}\b[^;]*;", text)
        assert created, f"{role} không còn được tạo"
        assert all("NOLOGIN" in c for c in created), f"{role} phải là NOLOGIN: {created}"


def test_cdc_password_comes_from_the_environment():
    text = FILES["05-cdc-setup.sh"]
    assert r"\getenv cdc_pw CDC_DB_PASSWORD" in text
    assert "ALTER ROLE cdc_user WITH LOGIN PASSWORD :'cdc_pw';" in text
    assert re.search(r"CREATE ROLE cdc_user WITH REPLICATION NOLOGIN;", text), "không có biến thì phải NOLOGIN"
    # Heredoc có quote: shell không nở $$ của khối DO, psql tự thay :'cdc_pw'.
    assert "<<'SQL'" in text
