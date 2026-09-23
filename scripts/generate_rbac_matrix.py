"""
Sinh docs/06-security-compliance/RBAC_MATRIX.md từ governance/rbac.py.

    py -3 scripts/generate_rbac_matrix.py           # ghi RBAC_MATRIX.md
    py -3 scripts/generate_rbac_matrix.py --check   # exit 1 nếu lệch

RBAC_MATRIX.md là file SINH RA. Sửa quyền ở governance/rbac.py, chạy lại
script này và scripts/generate_trino_access_control.py;
tests/governance/test_rbac_matrix_current.py chặn drift.

Ma trận được tính bằng CHÍNH `Profile` mà generator của Trino dùng để sinh
rules.json — nên nó mô tả thứ Trino thực thi, không phải một diễn giải thứ hai
của rbac.py có thể lệch khỏi nó.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from governance.rbac import LEVEL_RANK, ROLES, USERS, AccessLevel  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "generate_trino_access_control", REPO_ROOT / "scripts" / "generate_trino_access_control.py"
)
_trino = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_trino)
Profile = _trino.Profile

MATRIX_PATH = REPO_ROOT / "docs" / "06-security-compliance" / "RBAC_MATRIX.md"

# Schema dữ liệu của catalog `iceberg`, theo thứ tự luồng dữ liệu. Cột
# "schema khác" hỏi quyền trên một schema không có trong danh sách — nó cho
# thấy mặc định là từ chối với mọi role không phải admin.
SCHEMAS = ("bronze", "silver", "silver_cdc", "gold", "sandbox", "serving")
OTHER_SCHEMA = "_unlisted_schema_"

LEVEL_CELL = {
    AccessLevel.NONE: "—",
    AccessLevel.READ: "R",
    AccessLevel.WRITE: "W",
    AccessLevel.ADMIN: "A",
}


def _groups() -> list[tuple[tuple[str, ...], list[str], Profile]]:
    """Một nhóm cho mỗi tổ hợp role — đúng đơn vị mà rules.json sinh khối luật."""
    by_roles: dict[tuple[str, ...], list[str]] = {}
    for username, user in USERS.items():
        by_roles.setdefault(tuple(user.roles), []).append(username)
    groups = [(roles, sorted(users), Profile(users[0])) for roles, users in by_roles.items()]
    return sorted(groups, key=lambda g: (not g[2].is_admin, list(ROLES).index(g[0][0])))


def _role_label(roles: tuple[str, ...]) -> str:
    return " + ".join(f"`{r}`" for r in roles)


def _catalog_access(profile: Profile) -> str:
    if profile.is_admin:
        return "all (mọi catalog)"
    level = profile.catalogs.get(_trino.DATA_CATALOG, AccessLevel.NONE)
    if level == AccessLevel.NONE:
        return "—"
    return "all" if LEVEL_RANK[level] >= LEVEL_RANK[AccessLevel.WRITE] else "read-only"


def _schema_cell(profile: Profile, schema: str) -> str:
    cell = LEVEL_CELL[profile._level_for(schema)]
    if any(s == schema for s, _t in profile.masks):
        cell += " ⁽ᵐ⁾"
    return cell


def _owned(profile: Profile) -> str:
    if not profile.owned_schemas:
        return "—"
    return ", ".join("mọi schema" if s == "*" else f"`{s}`" for s in sorted(profile.owned_schemas))


def _masked_tables() -> list[tuple[str, str]]:
    tables: set[tuple[str, str]] = set()
    for _roles, _users, profile in _groups():
        tables.update(profile.masks)
    return sorted(tables, key=lambda st: (SCHEMAS.index(st[0]) if st[0] in SCHEMAS else len(SCHEMAS), st[1]))


def _pii_cell(profile: Profile, schema: str, table: str) -> str:
    if profile._level_for(schema) == AccessLevel.NONE:
        return "không đọc được"
    cols = profile.masks.get((schema, table))
    if cols:
        return f"che {len(cols)} cột"
    return "**bản gốc**"


def render() -> str:
    groups = _groups()
    out: list[str] = []
    w = out.append

    w("<!-- FILE SINH RA — đừng sửa tay. Nguồn: governance/rbac.py.")
    w("     Sinh lại: py -3 scripts/generate_rbac_matrix.py -->")
    w("")
    w("# RBAC Matrix — ai đọc, ghi được gì qua Trino")
    w("")
    w("> **Nguồn sự thật**: [`governance/rbac.py`](../../governance/rbac.py). Tài liệu này và")
    w("> [`docker/init_trino/rules.json`](../../docker/init_trino/rules.json) đều **sinh ra** từ đó")
    w("> ([ADR-0015](../02-architecture/adr/0015-trino-access-control-generated-from-rbac.md)).")
    w("> CI đỏ nếu một trong hai lệch khỏi `rbac.py`.")
    w(">")
    w("> **Phạm vi**: chỉ truy vấn **đi qua Trino**. Spark, MinIO và Postgres không bị ma trận này")
    w("> ràng buộc — xem §5 trước khi dựa vào nó.")
    w("")
    w(f"{len(ROLES)} role · {len(USERS)} user · {len(groups)} khối luật trong `rules.json`.")
    w("")

    w("## 1. User → role")
    w("")
    w("Trino khớp luật theo **tên user**, không theo group. Mỗi user thuộc đúng một tổ hợp role,")
    w("nên thuộc đúng một khối luật.")
    w("")
    w("| Role | Mô tả | User |")
    w("|---|---|---|")
    for roles, users, _profile in groups:
        desc = " / ".join(ROLES[r].description for r in roles)
        parents = [p for r in roles for p in ROLES[r].parent_roles]
        if parents:
            desc += " — kế thừa " + ", ".join(f"`{p}`" for p in parents)
        w(f"| {_role_label(roles)} | {desc} | {', '.join(f'`{u}`' for u in users)} |")
    w("")

    w("## 2. Role × schema")
    w("")
    w("Quyền trên **bảng** trong từng schema của catalog `iceberg`, sau khi gộp role cha.")
    w("")
    header = ["Role", "Catalog `iceberg`", *[f"`{s}`" for s in SCHEMAS], "schema khác", "Sở hữu schema"]
    w("| " + " | ".join(header) + " |")
    w("|" + "---|" * len(header))
    for roles, _users, profile in groups:
        cells = [
            _role_label(roles),
            _catalog_access(profile),
            *[_schema_cell(profile, s) for s in SCHEMAS],
            _schema_cell(profile, OTHER_SCHEMA),
            _owned(profile),
        ]
        w("| " + " | ".join(cells) + " |")
    w("")
    w("| Ký hiệu | Privilege Trino |")
    w("|---|---|")
    w(f"| `R` | {', '.join(_trino.READ_PRIVILEGES)} |")
    w(f"| `W` | {', '.join(_trino.WRITE_PRIVILEGES)} |")
    w(f"| `A` | {', '.join(_trino.ADMIN_PRIVILEGES)} |")
    w("| `—` | không có luật nào khớp — Trino từ chối |")
    w("| `⁽ᵐ⁾` | một số bảng trong schema bị che cột — §3 |")
    w("")
    w("`OWNERSHIP` trên bảng là thứ cho phép `CREATE`/`DROP TABLE`. *Sở hữu schema* chỉ quyết")
    w("định `CREATE`/`DROP SCHEMA`. Quyền được khai **theo cả schema**: mô hình hiện tại không có")
    w("grant theo từng bảng, và generator từ chối nếu ai đó khai.")
    w("")
    w("Mọi role không phải admin còn đọc được `system.metadata` và `system.jdbc` — client (dbt,")
    w("Superset, JDBC) cần chúng để liệt kê bảng, và Trino tự lọc chúng theo quyền của user hỏi.")
    w("")

    w("## 3. Cột bị che")
    w("")
    masked = _masked_tables()
    w("Che **lúc đọc**: Trino thay giá trị cột bằng biểu thức dưới đây. Dữ liệu lưu trên MinIO")
    w("vẫn là bản gốc.")
    w("")
    w("### 3.1 Ai thấy PII gốc")
    w("")
    header = ["Role", *[f"`{s}.{t}`" for s, t in masked]]
    w("| " + " | ".join(header) + " |")
    w("|" + "---|" * len(header))
    for roles, _users, profile in groups:
        w("| " + " | ".join([_role_label(roles), *[_pii_cell(profile, s, t) for s, t in masked]]) + " |")
    w("")
    w("### 3.2 Biểu thức che")
    w("")
    w("| Bảng | Cột | Biểu thức Trino | Áp cho role |")
    w("|---|---|---|---|")
    for schema, table in masked:
        by_col: dict[tuple[str, str], list[str]] = {}
        for roles, _users, profile in groups:
            for col, expr in profile.masks.get((schema, table), {}).items():
                by_col.setdefault((col, expr.replace("%s", col)), []).append(_role_label(roles))
        for (col, expr), role_labels in sorted(by_col.items()):
            w(f"| `{schema}.{table}` | `{col}` | `{expr}` | {', '.join(role_labels)} |")
    w("")
    w("Gold và `serving` không cần mask đọc: chúng chỉ mang PII đã che **lúc ghi** (`full_name_masked`).")
    w("Phạm vi che là nhóm *Cao nhất* và *Cao* của PII_INVENTORY §3 trên bảng khách hàng; nhóm")
    w("*Trung bình* (account_no, device_id, lat/long) và *Thấp* chưa che.")
    w("Danh mục đầy đủ bảng/cột chứa PII: [`PII_INVENTORY.md`](PII_INVENTORY.md).")
    w("")

    w("## 4. Truy vấn và thông tin hệ thống")
    w("")
    w("| Hành động | Ai |")
    w("|---|---|")
    admins = ", ".join(f"`{u}`" for roles, users, p in groups if p.is_admin for u in users)
    w("| Chạy truy vấn | mọi user |")
    w(f"| Xem / kill truy vấn của **user khác** | {admins} |")
    w(f"| Đọc / ghi `system_information` | {admins} |")
    w("")

    w("## 5. Ma trận này KHÔNG bảo đảm gì")
    w("")
    w("1. **Chưa có xác thực.** Trino tin tên user mà client tự khai. Ai kết nối được tới cổng")
    w("   Trino và khai `admin` thì nhận quyền admin. Ma trận phân quyền đúng cho client **trung")
    w("   thực** — nó chặn nhầm lẫn và lộ PII vô ý, không chặn người cố ý.")
    w("2. **Spark không đi qua Trino.** Job Spark đọc/ghi Iceberg trực tiếp qua REST catalog và")
    w("   MinIO — không luật nào ở đây áp cho nó.")
    w("3. **MinIO và Postgres ngoài phạm vi.** Ai có credential MinIO đọc được file Parquet gốc,")
    w("   gồm PII ở Bronze/Silver. `opslakehouse` trên Postgres có quyền riêng của Postgres.")
    w("4. **Kiểm chứng.** `tests/governance/test_trino_access_control.py` mô phỏng ngữ nghĩa")
    w("   file-based access control của Trino 443 (luật khớp đầu tiên) trên `rules.json` sinh ra;")
    w("   theo docstring của test, evaluator đó đã được đối chiếu với chính Trino 443. Test chạy trong")
    w("   unit CI, không cần stack.")
    w("")
    w("Chi tiết và lộ trình: [ADR-0015](../02-architecture/adr/0015-trino-access-control-generated-from-rbac.md),")
    w("[`PII_INVENTORY.md`](PII_INVENTORY.md) §7, [`RUNBOOK.md`](../../RUNBOOK.md) §9.")
    w("")
    w("## 6. Đổi quyền")
    w("")
    w("```bash")
    w("# 1. sửa ROLES / USERS trong governance/rbac.py")
    w("py -3 scripts/generate_trino_access_control.py   # sinh docker/init_trino/rules.json")
    w("py -3 scripts/generate_rbac_matrix.py            # sinh tài liệu này")
    w("py -3 -m pytest tests/governance/test_trino_access_control.py tests/governance/test_rbac_matrix_current.py")
    w("# 2. restart Trino để nạp rules.json mới")
    w("```")
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--check", action="store_true", help="exit 1 nếu RBAC_MATRIX.md đã commit lệch với bản sinh")
    args = parser.parse_args(argv)

    expected = render()
    rel = MATRIX_PATH.relative_to(REPO_ROOT).as_posix()
    if args.check:
        current = MATRIX_PATH.read_text(encoding="utf-8") if MATRIX_PATH.exists() else ""
        if current != expected:
            print(f"{rel} lệch với governance/rbac.py — chạy: py -3 scripts/generate_rbac_matrix.py", file=sys.stderr)
            return 1
        print(f"{rel} khớp với governance/rbac.py")
        return 0

    MATRIX_PATH.write_text(expected, encoding="utf-8", newline="\n")
    print(f"đã ghi {rel} ({len(ROLES)} role, {len(USERS)} user)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
