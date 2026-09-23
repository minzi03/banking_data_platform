"""
Contract test — RBAC_MATRIX.md phải khớp với governance/rbac.py
================================================================

`docs/06-security-compliance/RBAC_MATRIX.md` được sinh bởi
`scripts/generate_rbac_matrix.py`. Ma trận quyền viết tay hỏng im lặng: thêm
một role, đổi một schema, không ai sửa tài liệu — và người duyệt quyền đọc một
chính sách không còn được thực thi.

Chạy: pytest tests/governance/test_rbac_matrix_current.py -v
Sinh lại: py -3 scripts/generate_rbac_matrix.py
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from governance.rbac import ROLES, USERS, AccessLevel, RBACManager

REPO_ROOT = Path(__file__).resolve().parents[2]

_spec = importlib.util.spec_from_file_location(
    "generate_rbac_matrix", REPO_ROOT / "scripts" / "generate_rbac_matrix.py"
)
_generator = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_generator)


def test_committed_matrix_matches_rbac():
    committed = _generator.MATRIX_PATH.read_text(encoding="utf-8")
    assert committed == _generator.render(), (
        "RBAC_MATRIX.md lệch với governance/rbac.py — chạy: py -3 scripts/generate_rbac_matrix.py"
    )


def test_check_mode_detects_drift(tmp_path, monkeypatch):
    stale = tmp_path / "RBAC_MATRIX.md"
    stale.write_text("cũ\n", encoding="utf-8")
    monkeypatch.setattr(_generator, "MATRIX_PATH", stale)
    monkeypatch.setattr(_generator, "REPO_ROOT", tmp_path)
    assert _generator.main(["--check"]) == 1


def test_every_role_and_user_is_listed():
    text = _generator.render()
    for name in ROLES:
        assert f"`{name}`" in text, f"role {name} không có trong ma trận"
    for name in USERS:
        assert f"`{name}`" in text, f"user {name} không có trong ma trận"


@pytest.mark.parametrize("username", sorted(USERS))
@pytest.mark.parametrize("schema", _generator.SCHEMAS)
def test_matrix_cell_agrees_with_rbac_manager(username, schema):
    """Ô ma trận (tính qua Profile của generator Trino) nói cùng điều với RBACManager."""
    level = _generator.Profile(username)._level_for(schema)
    rbac = RBACManager()
    assert rbac.has_access(username, schema, "any_table", "read") == (level != AccessLevel.NONE)
    assert rbac.has_access(username, schema, "any_table", "write") == (level in (AccessLevel.WRITE, AccessLevel.ADMIN))


@pytest.mark.parametrize("schema, table", [("silver", "dim_customer"), ("bronze", "core_customer")])
def test_pii_view_agrees_with_rbac_manager(schema, table):
    """§3.1 'bản gốc' ⇔ user đọc được bảng mà RBACManager không trả mask nào."""
    rbac = RBACManager()
    for roles, users, profile in _generator._groups():
        cell = _generator._pii_cell(profile, schema, table)
        for username in users:
            readable = rbac.has_access(username, schema, table, "read")
            masked = bool(rbac.get_masked_columns(username, schema, table))
            expected = "không đọc được" if not readable else ("che" if masked else "**bản gốc**")
            assert cell.startswith(expected), f"{roles}/{username} trên {schema}.{table}: {cell!r}"
