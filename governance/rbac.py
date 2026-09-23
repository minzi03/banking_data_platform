"""
RBAC Configuration — Banking Data Platform

Nguồn sự thật duy nhất cho quyền truy cập lakehouse **qua Trino**.
`scripts/generate_trino_access_control.py` sinh `docker/init_trino/rules.json`
từ ROLES/USERS ở đây; test chặn drift giữa hai bên. Sửa quyền ở file này rồi
sinh lại — đừng sửa tay rules.json.

Chưa có xác thực: Trino tin tên user mà client tự khai (header X-Trino-User).
Luật ở đây chặn truy cập *nhầm* và che PII cho các client làm đúng, nhưng
không chặn người cố ý khai tên `admin`. Xem RUNBOOK.md § "Trino access control".

Không phủ: Spark ghi thẳng vào Iceberg REST + MinIO, không đi qua Trino, nên
không chịu luật này. PostgreSQL nguồn có GRANT riêng (05_security.sql).

Usage:
    from governance.rbac import RBACManager

    rbac = RBACManager()
    rbac.print_roles_summary()

    # Check if user has access
    if rbac.has_access("analytics_user", "gold", "mart_customer_360", "read"):
        print("Access granted")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class AccessLevel(Enum):
    """Access levels for resources."""

    NONE = "none"
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


LEVEL_RANK = {AccessLevel.NONE: 0, AccessLevel.READ: 1, AccessLevel.WRITE: 2, AccessLevel.ADMIN: 3}


class ResourceType(Enum):
    """Types of resources that can be protected."""

    CATALOG = "catalog"
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"
    FILE = "file"


@dataclass
class Permission:
    """A single permission entry."""

    resource_type: ResourceType
    resource_path: str  # e.g., "iceberg.gold.mart_customer_360"
    access_level: AccessLevel
    column_mask: str = ""  # Optional column masking expression


@dataclass
class Role:
    """A role with associated permissions."""

    name: str
    description: str
    permissions: list[Permission] = field(default_factory=list)
    parent_roles: list[str] = field(default_factory=list)  # Role inheritance


@dataclass
class User:
    """A user with assigned roles."""

    username: str
    roles: list[str] = field(default_factory=list)
    groups: list[str] = field(default_factory=list)


# =============================================================================
# PII masks
# =============================================================================
# Biểu thức SQL Trino, `%s` là tên cột. Mask phải trả về ĐÚNG kiểu của cột —
# Trino từ chối mask lệch kiểu lúc query, nên literal thuần phải CAST.
#
# Phạm vi: nhóm "Cao nhất" + "Cao" của PII_INVENTORY §3, trên các bảng khách
# hàng. Nhóm "Trung bình" (account_no, device_id, lat/long) và "Thấp" (tên
# nhân viên, địa chỉ chi nhánh) chưa che — ghi nhận, không phải bỏ sót.

_VARCHAR_PII_MASKS = {
    "cccd": "concat('***********', substr(%s, -4))",
    "full_name": "concat(substr(%s, 1, 1), '**')",
    "phone": "concat(substr(%s, 1, 3), '****', substr(%s, 8))",
    "email": "concat(substr(%s, 1, 1), '*****', '@', split_part(%s, '@', 2))",
    "address": "CAST('[REDACTED]' AS VARCHAR)",
}

# date_of_birth có hai kiểu vật lý: DATE ở bảng batch, BIGINT (số ngày từ epoch,
# định dạng Debezium) ở bronze.core_customer_cdc.
_DOB_DATE_MASK = "date_trunc('year', %s)"
_DOB_EPOCH_DAYS_MASK = "CAST(NULL AS BIGINT)"


def _pii_masks(table_path: str, dob_mask: str = _DOB_DATE_MASK) -> list[Permission]:
    """Mask PII khách hàng cho một bảng `iceberg.<schema>.<table>`."""
    masks = {**_VARCHAR_PII_MASKS, "date_of_birth": dob_mask}
    return [
        Permission(ResourceType.COLUMN, f"{table_path}.{col}", AccessLevel.READ, column_mask=expr)
        for col, expr in masks.items()
    ]


# Bảng mang PII khách hàng nguyên bản, theo tầng. Gold/serving không có trong
# đây vì chúng chỉ mang full_name_masked (PII_INVENTORY §2).
_SILVER_CUSTOMER_MASKS = _pii_masks("iceberg.silver.dim_customer") + _pii_masks("iceberg.silver.dim_customer_current")
_BRONZE_CUSTOMER_MASKS = _pii_masks("iceberg.bronze.core_customer") + _pii_masks(
    "iceberg.bronze.core_customer_cdc", dob_mask=_DOB_EPOCH_DAYS_MASK
)


def _read(*schemas: str) -> list[Permission]:
    """Quyền đọc mọi bảng trong các schema của catalog iceberg."""
    perms = []
    for schema in schemas:
        perms.append(Permission(ResourceType.SCHEMA, f"iceberg.{schema}", AccessLevel.READ))
        perms.append(Permission(ResourceType.TABLE, f"iceberg.{schema}.*", AccessLevel.READ))
    return perms


# =============================================================================
# Predefined Roles
# =============================================================================
# Ngữ nghĩa khi dịch sang Trino (scripts/generate_trino_access_control.py):
#   TABLE  READ          → SELECT
#   TABLE  WRITE / ADMIN → SELECT, INSERT, DELETE, UPDATE, OWNERSHIP
#                          (OWNERSHIP = được CREATE / DROP / RENAME bảng)
#   SCHEMA WRITE / ADMIN → owner của schema (CREATE / DROP SCHEMA)
#   CATALOG              → "all" nếu role có quyền ghi, ngược lại "read-only"
#   COLUMN có column_mask→ mask cho cột đó

ROLES = {
    "admin": Role(
        name="admin",
        description="Full access to all resources",
        permissions=[
            Permission(ResourceType.CATALOG, "*", AccessLevel.ADMIN),
            Permission(ResourceType.SCHEMA, "iceberg.*", AccessLevel.ADMIN),
            Permission(ResourceType.TABLE, "iceberg.*.*", AccessLevel.ADMIN),
        ],
    ),
    "etl_user": Role(
        name="etl_user",
        description="Read/Write access for ETL pipelines",
        permissions=[
            Permission(ResourceType.CATALOG, "iceberg", AccessLevel.WRITE),
            Permission(ResourceType.SCHEMA, "iceberg.bronze", AccessLevel.WRITE),
            Permission(ResourceType.SCHEMA, "iceberg.silver", AccessLevel.WRITE),
            Permission(ResourceType.SCHEMA, "iceberg.silver_cdc", AccessLevel.WRITE),
            Permission(ResourceType.SCHEMA, "iceberg.gold", AccessLevel.WRITE),
            Permission(ResourceType.SCHEMA, "iceberg.sandbox", AccessLevel.WRITE),
            Permission(ResourceType.TABLE, "iceberg.*.*", AccessLevel.WRITE),
        ],
    ),
    "analytics": Role(
        name="analytics",
        description="Read-only access to gold/silver/serving, PII in Silver masked",
        permissions=[
            Permission(ResourceType.CATALOG, "iceberg", AccessLevel.READ),
            *_read("gold", "silver", "sandbox", "serving"),
            *_SILVER_CUSTOMER_MASKS,
        ],
    ),
    "readonly": Role(
        name="readonly",
        description="Read-only access to gold and serving",
        permissions=[
            Permission(ResourceType.CATALOG, "iceberg", AccessLevel.READ),
            *_read("gold", "serving"),
        ],
    ),
    "data_steward": Role(
        name="data_steward",
        description="Manage data governance, contracts, and quality",
        parent_roles=["analytics"],
        permissions=[
            Permission(ResourceType.CATALOG, "iceberg", AccessLevel.WRITE),
            Permission(ResourceType.SCHEMA, "iceberg.sandbox", AccessLevel.WRITE),
            Permission(ResourceType.TABLE, "iceberg.sandbox.*", AccessLevel.WRITE),
        ],
    ),
    # ── Service roles: một role cho mỗi loại client thật của Trino ──────────
    "serving_builder": Role(
        name="serving_builder",
        description="dbt: đọc Gold, tạo/thay bảng trong serving",
        permissions=[
            Permission(ResourceType.CATALOG, "iceberg", AccessLevel.WRITE),
            *_read("gold"),
            Permission(ResourceType.SCHEMA, "iceberg.serving", AccessLevel.WRITE),
            Permission(ResourceType.TABLE, "iceberg.serving.*", AccessLevel.WRITE),
        ],
    ),
    "serving_consumer": Role(
        name="serving_consumer",
        description="Superset, API, Streamlit, ML: chỉ đọc serving",
        permissions=[
            Permission(ResourceType.CATALOG, "iceberg", AccessLevel.READ),
            *_read("serving"),
        ],
    ),
    "observer": Role(
        name="observer",
        description="Freshness exporter, metrics manifest: đọc mọi tầng, PII bị che",
        permissions=[
            Permission(ResourceType.CATALOG, "iceberg", AccessLevel.READ),
            *_read("bronze", "silver", "silver_cdc", "gold", "sandbox", "serving"),
            *_BRONZE_CUSTOMER_MASKS,
            *_SILVER_CUSTOMER_MASKS,
        ],
    ),
}

# =============================================================================
# Predefined Users
# =============================================================================
# Mỗi user thuộc đúng MỘT tổ hợp role — Trino áp luật khớp đầu tiên, nên một
# user nằm trong hai khối luật sẽ chỉ nhận khối đứng trước.

USERS = {
    "airflow_etl": User(
        username="airflow_etl",
        roles=["etl_user"],
        groups=["etl_users"],
    ),
    "trino_admin": User(
        username="trino_admin",
        roles=["admin"],
        groups=["admins"],
    ),
    "analytics_report": User(
        username="analytics_report",
        roles=["analytics"],
        groups=["analysts"],
    ),
    "readonly_viewer": User(
        username="readonly_viewer",
        roles=["readonly"],
        groups=["readonly_users"],
    ),
    "data_steward_user": User(
        username="data_steward_user",
        roles=["data_steward"],
        groups=["data_stewards", "analysts"],
    ),
    # ── Tài khoản vận hành ──────────────────────────────────────────────────
    # `admin`: tên mà RUNBOOK, Superset login và thói quen cũ đều dùng.
    # `trino`: user mặc định của Trino CLI khi chạy `docker exec <trino> trino`
    # (CI, benchmark, RUNBOOK). Ai exec được vào container Trino thì đã kiểm
    # soát server — sửa được rules.json — nên cấp admin cho tên này không mở
    # thêm gì. Qua mạng thì mọi tên đều giả được như nhau cho tới khi có xác thực.
    "admin": User(username="admin", roles=["admin"], groups=["admins"]),
    "trino": User(username="trino", roles=["admin"], groups=["admins"]),
    # ── Service users: mỗi client một tên, để luật và audit phân biệt được ──
    "dbt": User(username="dbt", roles=["serving_builder"]),
    "superset": User(username="superset", roles=["serving_consumer"]),
    "customer_api": User(username="customer_api", roles=["serving_consumer"]),
    "streamlit": User(username="streamlit", roles=["serving_consumer"]),
    "ml": User(username="ml", roles=["serving_consumer"]),
    "freshness_exporter": User(username="freshness_exporter", roles=["observer"]),
    "manifest_collector": User(username="manifest_collector", roles=["observer"]),
}


# =============================================================================
# RBAC Manager
# =============================================================================


class RBACManager:
    """
    Manages role-based access control for the Banking Data Platform.

    Provides methods to:
    - Check access permissions
    - Get masked column expressions
    - List roles and users
    - Validate access policies
    """

    def __init__(self):
        self.roles = ROLES
        self.users = USERS

    def has_access(
        self,
        username: str,
        schema: str,
        table: str,
        access_type: str = "read",
    ) -> bool:
        """
        Check if a user has access to a table.

        Args:
            username: Username to check
            schema: Schema name (e.g., "gold")
            table: Table name (e.g., "mart_customer_360")
            access_type: "read" or "write"

        Returns:
            True if access is granted
        """
        required_level = AccessLevel.READ if access_type == "read" else AccessLevel.WRITE

        # Mức cao hơn bao hàm mức thấp hơn: quyền ghi có quyền đọc, như ở Trino
        # (privilege ghi luôn gồm SELECT). Trước đây chỉ khớp đúng mức hoặc
        # ADMIN, nên etl_user "không đọc được" chính bảng nó ghi.
        return any(
            perm.resource_type == ResourceType.TABLE
            and self._matches_resource(perm.resource_path, schema, table)
            and LEVEL_RANK[perm.access_level] >= LEVEL_RANK[required_level]
            for perm in self.effective_permissions(username)
        )

    def get_masked_columns(
        self,
        username: str,
        schema: str,
        table: str,
    ) -> dict[str, str]:
        """
        Get column masking expressions for a user.

        Args:
            username: Username to check
            schema: Schema name
            table: Table name

        Returns:
            Dict of column_name -> mask_expression
        """
        user = self.users.get(username)
        if not user:
            return {}

        masks = {}

        for perm in self.effective_permissions(username):
            if (
                perm.resource_type == ResourceType.COLUMN
                and perm.column_mask
                and self._matches_column(perm.resource_path, schema, table)
            ):
                col_name = perm.resource_path.split(".")[-1]
                masks[col_name] = perm.column_mask

        return masks

    def effective_permissions(self, username: str) -> list[Permission]:
        """
        Mọi permission của user: từ các role trực tiếp và role cha của chúng.

        Mask cũng kế thừa. Trước đây get_masked_columns không đi theo
        parent_roles, trong khi has_access có — nên data_steward kế thừa quyền
        ĐỌC Silver của analytics mà KHÔNG kế thừa mask, tức là đọc được cccd
        gốc. Khi luật được thực thi ở Trino, lệch đó thành lỗ hổng thật.
        """
        user = self.users.get(username)
        if not user:
            return []

        perms: list[Permission] = []
        seen: set[str] = set()
        pending = list(user.roles)
        while pending:
            role_name = pending.pop(0)
            if role_name in seen:
                continue
            seen.add(role_name)
            role = self.roles.get(role_name)
            if not role:
                continue
            perms.extend(role.permissions)
            pending.extend(role.parent_roles)
        return perms

    def _matches_resource(self, pattern: str, schema: str, table: str) -> bool:
        """Check if a resource pattern matches schema.table."""
        parts = pattern.split(".")
        if len(parts) >= 2:
            schema_pattern = parts[-2]
            table_pattern = parts[-1]
            schema_ok = schema_pattern == "*" or schema_pattern == schema
            table_ok = table_pattern == "*" or table_pattern == table
            if schema_ok and table_ok:
                return True
        return False

    def _matches_column(self, resource_path: str, schema: str, table: str) -> bool:
        """Check if a column resource path matches schema.table."""
        parts = resource_path.split(".")
        if len(parts) >= 3:
            return parts[-3] == schema and parts[-2] == table
        return False

    def get_user_info(self, username: str) -> dict:
        """Get detailed user information."""
        user = self.users.get(username)
        if not user:
            return {"error": f"User {username} not found"}

        roles_info = []
        for role_name in user.roles:
            role = self.roles.get(role_name)
            if role:
                roles_info.append(
                    {
                        "name": role.name,
                        "description": role.description,
                        "permission_count": len(role.permissions),
                    }
                )

        return {
            "username": user.username,
            "roles": roles_info,
            "groups": user.groups,
        }

    def print_roles_summary(self) -> None:
        """Print a summary of all roles and their permissions."""
        print("\n" + "=" * 70)
        print("RBAC ROLES SUMMARY")
        print("=" * 70)

        for role in self.roles.values():
            print(f"\n{'─' * 70}")
            print(f"Role: {role.name}")
            print(f"Description: {role.description}")
            print(f"Parent Roles: {', '.join(role.parent_roles) or 'None'}")
            print(f"Permissions ({len(role.permissions)}):")

            for perm in role.permissions:
                mask_info = f" [MASK: {perm.column_mask[:30]}...]" if perm.column_mask else ""
                print(f"  • {perm.resource_type.value}: {perm.resource_path} → {perm.access_level.value}{mask_info}")

        print("\n" + "=" * 70)
        print("USERS SUMMARY")
        print("=" * 70)

        for username, user in self.users.items():
            print(f"\n  {username}: roles={user.roles}, groups={user.groups}")

        print("\n" + "=" * 70)


# =============================================================================
# Convenience Functions
# =============================================================================


def check_access(username: str, schema: str, table: str, access_type: str = "read") -> bool:
    """Check if a user has access to a table."""
    rbac = RBACManager()
    return rbac.has_access(username, schema, table, access_type)


def get_masked_columns(username: str, schema: str, table: str) -> dict[str, str]:
    """Get column masking expressions for a user."""
    rbac = RBACManager()
    return rbac.get_masked_columns(username, schema, table)


def print_access_matrix() -> None:
    """Print a matrix of users vs. schemas with access levels."""
    rbac = RBACManager()

    print("\n" + "=" * 70)
    print("ACCESS MATRIX")
    print("=" * 70)

    schemas = ["bronze", "silver", "gold", "sandbox", "serving"]
    users = list(USERS.keys())

    # Print header
    print(f"\n{'User':<20}", end="")
    for schema in schemas:
        print(f"{schema:<15}", end="")
    print()

    print("-" * 80)

    # Print access for each user
    for username in users:
        print(f"{username:<20}", end="")
        for schema in schemas:
            read_access = rbac.has_access(username, schema, "*", "read")
            write_access = rbac.has_access(username, schema, "*", "write")

            if write_access:
                access_str = "R/W"
            elif read_access:
                access_str = "R"
            else:
                access_str = "---"

            print(f"{access_str:<15}", end="")
        print()

    print("\n" + "=" * 70)


if __name__ == "__main__":
    rbac = RBACManager()
    rbac.print_roles_summary()
    print_access_matrix()
