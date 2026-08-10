"""
RBAC Configuration — Banking Data Platform

Defines role-based access control for:
- Trino (query engine)
- PostgreSQL (source database)
- MinIO (object storage)
- Iceberg (table format)

Usage:
    from governance.rbac import RBACManager

    rbac = RBACManager()
    rbac.print_roles_summary()

    # Check if user has access
    if rbac.has_access("analytics_user", "gold", "mart_customer_360", "read"):
        print("Access granted")
"""

from dataclasses import dataclass, field
from enum import Enum


class AccessLevel(Enum):
    """Access levels for resources."""
    NONE = "none"
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


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
# Predefined Roles
# =============================================================================

ROLES = {
    "admin": Role(
        name="admin",
        description="Full access to all resources",
        permissions=[
            Permission(ResourceType.CATALOG, "iceberg", AccessLevel.ADMIN),
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
            Permission(ResourceType.SCHEMA, "iceberg.gold", AccessLevel.WRITE),
            Permission(ResourceType.SCHEMA, "iceberg.sandbox", AccessLevel.WRITE),
            Permission(ResourceType.TABLE, "iceberg.*.*", AccessLevel.WRITE),
        ],
    ),
    "analytics": Role(
        name="analytics",
        description="Read-only access to gold/silver layers",
        permissions=[
            Permission(ResourceType.CATALOG, "iceberg", AccessLevel.READ),
            Permission(ResourceType.SCHEMA, "iceberg.gold", AccessLevel.READ),
            Permission(ResourceType.SCHEMA, "iceberg.silver", AccessLevel.READ),
            Permission(ResourceType.SCHEMA, "iceberg.sandbox", AccessLevel.READ),
            Permission(ResourceType.TABLE, "iceberg.gold.*", AccessLevel.READ),
            Permission(ResourceType.TABLE, "iceberg.silver.*", AccessLevel.READ),
            # PII masking for analytics users
            Permission(
                ResourceType.COLUMN, "iceberg.gold.mart_customer_360.full_name",
                AccessLevel.READ, column_mask="concat(substr(%s, 1, 1), '**')"
            ),
            Permission(
                ResourceType.COLUMN, "iceberg.gold.mart_customer_360.phone",
                AccessLevel.READ, column_mask="concat(substr(%s, 1, 3), '****', substr(%s, 8))"
            ),
            Permission(
                ResourceType.COLUMN, "iceberg.gold.mart_customer_360.email",
                AccessLevel.READ, column_mask="concat(substr(%s, 1, 1), '*****', '@', split_part(%s, '@', 2))"
            ),
            Permission(
                ResourceType.COLUMN, "iceberg.gold.mart_customer_360.cccd",
                AccessLevel.READ, column_mask="concat('***********', substr(%s, -4))"
            ),
        ],
    ),
    "readonly": Role(
        name="readonly",
        description="Read-only access to gold layer only",
        permissions=[
            Permission(ResourceType.CATALOG, "iceberg", AccessLevel.READ),
            Permission(ResourceType.SCHEMA, "iceberg.gold", AccessLevel.READ),
            Permission(ResourceType.TABLE, "iceberg.gold.*", AccessLevel.READ),
        ],
    ),
    "data_steward": Role(
        name="data_steward",
        description="Manage data governance, contracts, and quality",
        parent_roles=["analytics"],
        permissions=[
            Permission(ResourceType.SCHEMA, "iceberg.sandbox", AccessLevel.WRITE),
            Permission(ResourceType.TABLE, "iceberg.sandbox.*", AccessLevel.WRITE),
        ],
    ),
}

# =============================================================================
# Predefined Users
# =============================================================================

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
        user = self.users.get(username)
        if not user:
            return False

        required_level = AccessLevel.READ if access_type == "read" else AccessLevel.WRITE

        for role_name in user.roles:
            role = self.roles.get(role_name)
            if not role:
                continue

            # Check direct permissions
            for perm in role.permissions:
                if self._matches_resource(perm.resource_path, schema, table):
                    if perm.access_level in (required_level, AccessLevel.ADMIN):
                        return True

            # Check parent roles
            for parent_name in role.parent_roles:
                parent = self.roles.get(parent_name)
                if parent:
                    for perm in parent.permissions:
                        if self._matches_resource(perm.resource_path, schema, table):
                            if perm.access_level in (required_level, AccessLevel.ADMIN):
                                return True

        return False

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

        for role_name in user.roles:
            role = self.roles.get(role_name)
            if not role:
                continue

            for perm in role.permissions:
                if (perm.resource_type == ResourceType.COLUMN and
                    perm.column_mask and
                    self._matches_column(perm.resource_path, schema, table)):
                    col_name = perm.resource_path.split(".")[-1]
                    masks[col_name] = perm.column_mask

        return masks

    def _matches_resource(self, pattern: str, schema: str, table: str) -> bool:
        """Check if a resource pattern matches schema.table."""
        parts = pattern.split(".")
        if len(parts) >= 2:
            schema_pattern = parts[-2]
            table_pattern = parts[-1]
            if schema_pattern == "*" or schema_pattern == schema:
                if table_pattern == "*" or table_pattern == table:
                    return True
        return False

    def _matches_column(self, resource_path: str, schema: str, table: str) -> bool:
        """Check if a column resource path matches schema.table."""
        parts = resource_path.split(".")
        if len(parts) >= 3:
            return (parts[-3] == schema and parts[-2] == table)
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
                roles_info.append({
                    "name": role.name,
                    "description": role.description,
                    "permission_count": len(role.permissions),
                })

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

        for role_name, role in self.roles.items():
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

    schemas = ["bronze", "silver", "gold", "sandbox"]
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
