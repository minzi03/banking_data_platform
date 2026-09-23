"""
Sinh luật access control của Trino từ governance/rbac.py.

    py -3 scripts/generate_trino_access_control.py           # ghi rules.json
    py -3 scripts/generate_trino_access_control.py --check   # exit 1 nếu lệch

docker/init_trino/rules.json là file SINH RA. Sửa quyền ở governance/rbac.py
rồi chạy lại script này; tests/governance/test_trino_access_control.py chặn
drift giữa hai bên.

Ngữ nghĩa của Trino mà file này phải tôn trọng (tài liệu Trino 443
*File-based access control*, và mã nguồn FileBasedSystemAccessControl 443):
  - Trong mỗi mục (catalogs, schemas, tables…), luật đọc từ trên xuống và
    LUẬT KHỚP ĐẦU TIÊN được áp. Không luật nào khớp → từ chối.
  - Một mục vắng mặt hoàn toàn → cho phép tất cả. Vì vậy mọi mục cần chặn
    đều phải có mặt.
  - CREATE / DROP / RENAME TABLE cần privilege OWNERSHIP trên BẢNG (mục
    tables). Owner của schema (mục schemas) chỉ quyết định CREATE / DROP SCHEMA.
    Cả hai, và mọi privilege ngoài SELECT, đều cần catalog ở mức `all`.
  - Trino tự nối thêm một luật ẩn vào cuối mục catalogs: mọi user → catalog
    `system` mức ALL. Luật `system → read-only` sinh ở đây đứng trước nên thắng
    cho user đã khai; user lạ vào được `system` nhưng không có luật bảng nào.

Hệ quả cho thiết kế:
  - Mỗi tổ hợp role sinh một khối luật với regex user riêng, và mỗi user chỉ
    thuộc một khối — nên thứ tự giữa các khối không đổi kết quả.
  - Trong một khối, luật bảng có mask đứng TRƯỚC luật cấp schema. Đảo lại thì
    luật schema khớp trước và mask không bao giờ được áp.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from governance.rbac import LEVEL_RANK as _RANK  # noqa: E402
from governance.rbac import ROLES, USERS, AccessLevel, RBACManager, ResourceType  # noqa: E402

RULES_PATH = REPO_ROOT / "docker" / "init_trino" / "rules.json"

DATA_CATALOG = "iceberg"


READ_PRIVILEGES = ["SELECT"]
WRITE_PRIVILEGES = ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]
ADMIN_PRIVILEGES = [*WRITE_PRIVILEGES, "GRANT_SELECT"]

# Client đọc metadata qua catalog `system`, nên mọi user đã khai đều cần đọc
# được hai schema này:
#   - dbt-trino (1.9.0, bản đang pin) join system.metadata.materialized_views
#     trong MỌI lần list relation — thiếu quyền là mọi `dbt run` hỏng
#   - SQLAlchemy dialect (Superset) đọc system.metadata.table_comments
#   - JDBC client đọc system.jdbc
# Trino tự lọc các bảng này theo quyền của user đang hỏi, nên cấp đọc không
# làm lộ bảng nào mà user không đọc được sẵn.
ENGINE_METADATA_SCHEMAS = "metadata|jdbc"


def _max(a: AccessLevel, b: AccessLevel) -> AccessLevel:
    return a if _RANK[a] >= _RANK[b] else b


def _schema_regex(name: str) -> str:
    return ".*" if name == "*" else re.escape(name)


def _privileges(level: AccessLevel) -> list[str]:
    if level == AccessLevel.ADMIN:
        return ADMIN_PRIVILEGES
    if level == AccessLevel.WRITE:
        return WRITE_PRIVILEGES
    return READ_PRIVILEGES


class Profile:
    """Những gì Trino cần biết về quyền của một tổ hợp role."""

    def __init__(self, username: str):
        self.catalogs: dict[str, AccessLevel] = {}
        self.owned_schemas: set[str] = set()
        self.table_levels: dict[str, AccessLevel] = {}
        self.masks: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)

        for perm in RBACManager().effective_permissions(username):
            parts = perm.resource_path.split(".")
            level = perm.access_level
            if perm.resource_type == ResourceType.CATALOG:
                self.catalogs[parts[0]] = _max(self.catalogs.get(parts[0], AccessLevel.NONE), level)
            elif perm.resource_type == ResourceType.SCHEMA:
                _require_data_catalog(perm.resource_path, parts)
                if _RANK[level] >= _RANK[AccessLevel.WRITE]:
                    self.owned_schemas.add(parts[1])
            elif perm.resource_type == ResourceType.TABLE:
                _require_data_catalog(perm.resource_path, parts)
                if len(parts) != 3 or parts[2] != "*":
                    raise ValueError(
                        f"{perm.resource_path}: chỉ hỗ trợ quyền bảng theo cả schema "
                        f"(`iceberg.<schema>.*`) — mô hình hiện tại không có grant theo từng bảng"
                    )
                schema = parts[1]
                self.table_levels[schema] = _max(self.table_levels.get(schema, AccessLevel.NONE), level)
            elif perm.resource_type == ResourceType.COLUMN and perm.column_mask:
                _require_data_catalog(perm.resource_path, parts)
                if len(parts) != 4:
                    raise ValueError(f"{perm.resource_path}: mask phải trỏ tới iceberg.<schema>.<table>.<column>")
                self.masks[(parts[1], parts[2])][parts[3]] = perm.column_mask

        wildcard = self.table_levels.get("*")
        if wildcard is not None:
            # Luật cụ thể đứng trước luật `.*`; nâng nó lên mức wildcard để
            # thứ tự không vô tình hạ quyền.
            for schema in self.table_levels:
                self.table_levels[schema] = _max(self.table_levels[schema], wildcard)

        for schema, _table in self.masks:
            if self._level_for(schema) == AccessLevel.NONE:
                raise ValueError(f"mask trên {schema}.{_table} nhưng role không có quyền đọc schema {schema}")

    @property
    def is_admin(self) -> bool:
        return self.catalogs.get("*") == AccessLevel.ADMIN

    def _level_for(self, schema: str) -> AccessLevel:
        return self.table_levels.get(schema, self.table_levels.get("*", AccessLevel.NONE))

    def key(self) -> str:
        return json.dumps(
            {
                "catalogs": sorted((c, lv.value) for c, lv in self.catalogs.items()),
                "owned": sorted(self.owned_schemas),
                "tables": sorted((s, lv.value) for s, lv in self.table_levels.items()),
                "masks": sorted((f"{s}.{t}", sorted(cols.items())) for (s, t), cols in self.masks.items()),
            },
            ensure_ascii=False,
        )


def _require_data_catalog(path: str, parts: list[str]) -> None:
    if parts[0] != DATA_CATALOG:
        raise ValueError(f"{path}: quyền schema/bảng/cột chỉ khai trên catalog `{DATA_CATALOG}`")


def _user_regex(usernames: list[str]) -> str:
    return "|".join(re.escape(u) for u in sorted(usernames))


def build_rules() -> dict:
    blocks: dict[str, tuple[Profile, list[str]]] = {}
    for username in USERS:
        profile = Profile(username)
        blocks.setdefault(profile.key(), (profile, []))[1].append(username)

    # Admin trước, còn lại theo tên user đầu tiên — thứ tự chỉ để dễ đọc,
    # vì regex user của các khối không giao nhau.
    ordered = sorted(blocks.values(), key=lambda b: (not b[0].is_admin, sorted(b[1])[0]))
    admin_users = sorted(u for profile, users in ordered if profile.is_admin for u in users)
    if not admin_users:
        raise ValueError("không user nào có role admin — Trino sẽ không còn ai quản trị được")

    catalogs, schemas, tables = [], [], []
    for profile, users in ordered:
        who = _user_regex(users)

        for catalog, level in sorted(profile.catalogs.items()):
            catalogs.append(
                {
                    "user": who,
                    "catalog": _schema_regex(catalog),
                    "allow": "all" if _RANK[level] >= _RANK[AccessLevel.WRITE] else "read-only",
                }
            )
        if not profile.is_admin:
            catalogs.append({"user": who, "catalog": "system", "allow": "read-only"})

        for schema in sorted(profile.owned_schemas, key=lambda s: (s == "*", s)):
            schemas.append({"user": who, "catalog": DATA_CATALOG, "schema": _schema_regex(schema), "owner": True})

        for (schema, table), cols in sorted(profile.masks.items()):
            tables.append(
                {
                    "user": who,
                    "catalog": DATA_CATALOG,
                    "schema": re.escape(schema),
                    "table": re.escape(table),
                    "privileges": _privileges(profile._level_for(schema)),
                    "columns": [
                        {"name": col, "mask": expr.replace("%s", col)} for col, expr in sorted(cols.items())
                    ],
                }
            )
        for schema, level in sorted(profile.table_levels.items(), key=lambda kv: (kv[0] == "*", kv[0])):
            tables.append(
                {
                    "user": who,
                    "catalog": DATA_CATALOG,
                    "schema": _schema_regex(schema),
                    "privileges": _privileges(level),
                }
            )
        tables.append(
            {
                "user": who,
                "catalog": "system",
                "schema": ".*" if profile.is_admin else ENGINE_METADATA_SCHEMAS,
                "privileges": READ_PRIVILEGES,
            }
        )

    admins = _user_regex(admin_users)
    return {
        "catalogs": catalogs,
        "schemas": schemas,
        "tables": tables,
        # Vắng mục này thì mọi user xem và kill được truy vấn của MỌI user khác.
        "queries": [
            {"user": admins, "allow": ["execute", "view", "kill"]},
            {"allow": ["execute"]},
        ],
        "system_information": [
            {"user": admins, "allow": ["read", "write"]},
        ],
    }


def render() -> str:
    return json.dumps(build_rules(), indent=2, ensure_ascii=False) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--check", action="store_true", help="exit 1 nếu rules.json đã commit lệch với bản sinh")
    args = parser.parse_args(argv)

    expected = render()
    if args.check:
        current = RULES_PATH.read_text(encoding="utf-8") if RULES_PATH.exists() else ""
        if current != expected:
            print(
                f"{RULES_PATH.relative_to(REPO_ROOT)} lệch với governance/rbac.py — "
                "chạy: py -3 scripts/generate_trino_access_control.py",
                file=sys.stderr,
            )
            return 1
        print("rules.json khớp với governance/rbac.py")
        return 0

    RULES_PATH.write_text(expected, encoding="utf-8", newline="\n")
    print(f"đã ghi {RULES_PATH.relative_to(REPO_ROOT)} ({len(USERS)} user, {len(ROLES)} role)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
