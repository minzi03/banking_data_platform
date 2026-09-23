"""
Tests for governance.rbac — role-based access control.

Covers the three things the module actually decides:
  - has_access: whether a user reaches a schema/table, including role inheritance
  - get_masked_columns: which PII columns get a mask expression
  - get_user_info: what the module reports about a user

The masking tests matter most: these expressions are what protect PII, so a
silent change in matching logic would widen access to full_name/phone/email/cccd
without any test noticing.
"""

import pytest

from governance.rbac import (
    ROLES,
    USERS,
    AccessLevel,
    Permission,
    RBACManager,
    ResourceType,
    Role,
    User,
    check_access,
    get_masked_columns,
)


@pytest.fixture
def rbac():
    return RBACManager()


# ---------------------------------------------------------------------------
# has_access — direct grants
# ---------------------------------------------------------------------------


class TestHasAccessDirect:
    def test_admin_reads_everything(self, rbac):
        assert rbac.has_access("trino_admin", "gold", "mart_customer_360")
        assert rbac.has_access("trino_admin", "bronze", "core_customer")
        assert rbac.has_access("trino_admin", "sandbox", "scratch")

    def test_admin_writes_everything(self, rbac):
        assert rbac.has_access("trino_admin", "gold", "anything", "write")

    def test_etl_writes_bronze_silver_gold(self, rbac):
        for schema in ("bronze", "silver", "gold", "sandbox"):
            assert rbac.has_access("airflow_etl", schema, "t", "write"), schema

    def test_readonly_reads_gold_only(self, rbac):
        assert rbac.has_access("readonly_viewer", "gold", "mart_customer_360")
        assert not rbac.has_access("readonly_viewer", "silver", "dim_customer")
        assert not rbac.has_access("readonly_viewer", "bronze", "core_customer")

    def test_readonly_cannot_write(self, rbac):
        assert not rbac.has_access("readonly_viewer", "gold", "mart_customer_360", "write")

    def test_analytics_reads_gold_and_silver(self, rbac):
        assert rbac.has_access("analytics_report", "gold", "mart_customer_360")
        assert rbac.has_access("analytics_report", "silver", "dim_customer")

    def test_analytics_cannot_read_bronze(self, rbac):
        assert not rbac.has_access("analytics_report", "bronze", "core_customer")

    def test_analytics_cannot_write_gold(self, rbac):
        assert not rbac.has_access("analytics_report", "gold", "mart_customer_360", "write")


# ---------------------------------------------------------------------------
# has_access — inheritance, wildcards, unknown inputs
# ---------------------------------------------------------------------------


class TestHasAccessResolution:
    def test_parent_role_grants_are_inherited(self, rbac):
        """
        data_steward declares parent_roles=["analytics"] and adds only sandbox
        write. Everything analytics can read, data_steward must also read —
        through inheritance, not through duplicated permissions.
        """
        assert rbac.has_access("data_steward_user", "gold", "mart_customer_360")
        assert rbac.has_access("data_steward_user", "silver", "dim_customer")

    def test_own_permissions_apply_alongside_inherited_ones(self, rbac):
        assert rbac.has_access("data_steward_user", "sandbox", "scratch", "write")

    def test_inherited_read_does_not_become_write(self, rbac):
        """Inheritance must not escalate: analytics has no write anywhere."""
        assert not rbac.has_access("data_steward_user", "gold", "mart_customer_360", "write")
        assert not rbac.has_access("data_steward_user", "silver", "dim_customer", "write")

    def test_unknown_user_denied(self, rbac):
        assert not rbac.has_access("no_such_user", "gold", "mart_customer_360")

    def test_unknown_role_on_user_is_ignored(self, rbac, monkeypatch):
        """A user referencing a role that does not exist must not crash."""
        monkeypatch.setitem(rbac.users, "ghost", User(username="ghost", roles=["missing_role"]))
        assert not rbac.has_access("ghost", "gold", "mart_customer_360")

    def test_empty_role_list_denied(self, rbac, monkeypatch):
        monkeypatch.setitem(rbac.users, "roleless", User(username="roleless", roles=[]))
        assert not rbac.has_access("roleless", "gold", "t")

    def test_write_request_satisfied_by_admin_grant(self, rbac):
        """
        admin's permissions are AccessLevel.ADMIN. A write check must treat
        ADMIN as sufficient, otherwise the admin role silently loses write.
        """
        assert rbac.has_access("trino_admin", "gold", "t", "write")

    def test_access_type_defaults_to_read(self, rbac):
        assert rbac.has_access("readonly_viewer", "gold", "mart_customer_360")


class TestResourcePatternMatching:
    """_matches_resource compares the last two dot-separated segments."""

    def test_exact_schema_and_table(self, rbac):
        assert rbac._matches_resource("iceberg.gold.rfm_segment", "gold", "rfm_segment")

    def test_wildcard_table(self, rbac):
        assert rbac._matches_resource("iceberg.gold.*", "gold", "anything")

    def test_wildcard_schema_and_table(self, rbac):
        assert rbac._matches_resource("iceberg.*.*", "bronze", "core_customer")

    def test_schema_mismatch(self, rbac):
        assert not rbac._matches_resource("iceberg.gold.*", "silver", "dim_customer")

    def test_table_mismatch(self, rbac):
        assert not rbac._matches_resource("iceberg.gold.rfm_segment", "gold", "churn_prediction")

    def test_single_segment_pattern_never_matches(self, rbac):
        """A bare catalog name has no schema/table to compare."""
        assert not rbac._matches_resource("iceberg", "gold", "t")

    def test_pattern_shorter_than_two_segments_never_matches(self, rbac):
        assert not rbac._matches_resource("", "gold", "t")


# ---------------------------------------------------------------------------
# get_masked_columns — PII protection
# ---------------------------------------------------------------------------


PII_COLUMNS = {"cccd", "full_name", "phone", "email", "address", "date_of_birth"}


class TestMaskedColumns:
    """
    Mask trước đây trỏ vào gold.mart_customer_360.{full_name,phone,email,cccd} —
    bốn cột mà DDL Gold không có (Gold chỉ mang full_name_masked). Test cũ khoá
    đúng những đường dẫn đó nên xanh mà không bảo vệ gì. Giờ mask trỏ vào bảng
    Silver/Bronze thật sự mang PII; tests/governance/test_trino_access_control.py
    kiểm mọi cột bị che có trong DDL.
    """

    def test_analytics_gets_every_pii_mask_on_silver_customer(self, rbac):
        for table in ("dim_customer", "dim_customer_current"):
            masks = rbac.get_masked_columns("analytics_report", "silver", table)
            assert set(masks) == PII_COLUMNS, table

    def test_masks_are_non_empty_expressions(self, rbac):
        masks = rbac.get_masked_columns("analytics_report", "silver", "dim_customer")
        for col, expr in masks.items():
            assert expr.strip(), f"{col} has an empty mask expression"

    def test_masks_do_not_expose_full_value(self, rbac):
        """
        Each mask must redact. These strings are the actual protection, so a
        mask expression that returned the column unchanged would be a PII leak
        that no other test would catch.
        """
        masks = rbac.get_masked_columns("analytics_report", "silver", "dim_customer")
        for col in ("full_name", "phone", "email", "cccd"):
            assert "*" in masks[col], col
        assert "REDACTED" in masks["address"]
        assert masks["date_of_birth"].startswith("date_trunc('year'")

    def test_no_mask_targets_gold(self, rbac):
        """Gold không mang PII nguyên bản — mask ở đó là mask vào cột không tồn tại."""
        for user in ("analytics_report", "data_steward_user", "freshness_exporter"):
            assert rbac.get_masked_columns(user, "gold", "mart_customer_360") == {}, user

    def test_admin_gets_no_masks(self, rbac):
        """admin has no COLUMN permissions — masking is applied per role."""
        assert rbac.get_masked_columns("trino_admin", "silver", "dim_customer") == {}

    def test_etl_gets_no_masks(self, rbac):
        """Pipeline ghi Silver thì phải thấy giá trị thật."""
        assert rbac.get_masked_columns("airflow_etl", "silver", "dim_customer") == {}

    def test_masks_apply_only_to_the_declared_table(self, rbac):
        masks = rbac.get_masked_columns("analytics_report", "silver", "dim_account")
        assert masks == {}

    def test_masks_apply_only_to_the_declared_schema(self, rbac):
        masks = rbac.get_masked_columns("analytics_report", "gold", "dim_customer")
        assert masks == {}

    def test_observer_gets_bronze_masks_analytics_does_not(self, rbac):
        assert set(rbac.get_masked_columns("freshness_exporter", "bronze", "core_customer")) == PII_COLUMNS
        assert rbac.get_masked_columns("analytics_report", "bronze", "core_customer") == {}

    def test_unknown_user_gets_no_masks(self, rbac):
        assert rbac.get_masked_columns("no_such_user", "silver", "dim_customer") == {}

    def test_steward_inherits_masks(self, rbac):
        """
        Thay đổi CÓ CHỦ Ý so với trước. Bản cũ ghim "steward không kế thừa mask"
        với chú thích để thay đổi về kế thừa là có chủ ý. Lý do đổi: steward kế
        thừa quyền ĐỌC Silver của analytics (has_access đi theo parent_roles),
        nên nếu không kế thừa mask thì steward đọc được cccd gốc — và từ khi
        rules.json được Trino thực thi, đó là lỗ hổng thật chứ không chỉ là lệch
        trong API Python.
        """
        steward = rbac.get_masked_columns("data_steward_user", "silver", "dim_customer")
        analytics = rbac.get_masked_columns("analytics_report", "silver", "dim_customer")
        assert steward == analytics
        assert set(steward) == PII_COLUMNS

    def test_matches_column_requires_three_segments(self, rbac):
        assert rbac._matches_column("iceberg.gold.mart_customer_360.phone", "gold", "mart_customer_360")
        assert not rbac._matches_column("iceberg.gold.phone", "gold", "phone")


# ---------------------------------------------------------------------------
# get_user_info
# ---------------------------------------------------------------------------


class TestUserInfo:
    def test_reports_roles_and_groups(self, rbac):
        info = rbac.get_user_info("analytics_report")
        assert info["username"] == "analytics_report"
        assert [r["name"] for r in info["roles"]] == ["analytics"]
        assert info["groups"] == ["analysts"]

    def test_reports_permission_count(self, rbac):
        info = rbac.get_user_info("readonly_viewer")
        assert info["roles"][0]["permission_count"] == len(ROLES["readonly"].permissions)

    def test_unknown_user_returns_error(self, rbac):
        info = rbac.get_user_info("no_such_user")
        assert "error" in info
        assert "no_such_user" in info["error"]

    def test_missing_role_is_skipped(self, rbac, monkeypatch):
        monkeypatch.setitem(rbac.users, "ghost", User(username="ghost", roles=["nope"]))
        info = rbac.get_user_info("ghost")
        assert info["roles"] == []


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------


class TestConvenienceFunctions:
    def test_check_access_read(self):
        assert check_access("readonly_viewer", "gold", "rfm_segment") is True

    def test_check_access_denied(self):
        assert check_access("readonly_viewer", "bronze", "core_customer") is False

    def test_check_access_write_default_is_read(self):
        assert check_access("readonly_viewer", "gold", "rfm_segment", "read") is True
        assert check_access("readonly_viewer", "gold", "rfm_segment", "write") is False

    def test_get_masked_columns_function(self):
        masks = get_masked_columns("analytics_report", "silver", "dim_customer")
        assert set(masks) == PII_COLUMNS


# ---------------------------------------------------------------------------
# Role table contract
# ---------------------------------------------------------------------------


class TestRoleTable:
    def test_every_user_references_a_defined_role(self):
        for username, user in USERS.items():
            for role_name in user.roles:
                assert role_name in ROLES, f"{username} references undefined role {role_name}"

    def test_every_parent_role_is_defined(self):
        for name, role in ROLES.items():
            for parent in role.parent_roles:
                assert parent in ROLES, f"{name} inherits from undefined role {parent}"

    def test_every_role_name_matches_its_key(self):
        for key, role in ROLES.items():
            assert role.name == key

    def test_every_user_name_matches_its_key(self):
        for key, user in USERS.items():
            assert user.username == key

    def test_no_inheritance_cycles(self):
        def walk(name, seen):
            assert name not in seen, f"cycle through {name}"
            for parent in ROLES[name].parent_roles:
                walk(parent, seen | {name})

        for name in ROLES:
            walk(name, set())

    def test_every_permission_has_a_path_and_level(self):
        for name, role in ROLES.items():
            for perm in role.permissions:
                assert perm.resource_path, f"{name} has a permission with no path"
                assert isinstance(perm.access_level, AccessLevel)
                assert isinstance(perm.resource_type, ResourceType)

    def test_pii_masks_are_declared_as_column_permissions(self):
        """A mask expression only takes effect on a COLUMN resource."""
        for name, role in ROLES.items():
            for perm in role.permissions:
                if perm.column_mask:
                    assert perm.resource_type == ResourceType.COLUMN, (
                        f"{name}: {perm.resource_path} carries a mask but is "
                        f"{perm.resource_type.value}, so get_masked_columns skips it"
                    )


# ---------------------------------------------------------------------------
# Reporting helpers — smoke tests, these print rather than return
# ---------------------------------------------------------------------------


class TestReporting:
    def test_print_roles_summary_runs(self, rbac, capsys):
        rbac.print_roles_summary()
        out = capsys.readouterr().out
        assert "RBAC ROLES SUMMARY" in out
        assert "USERS SUMMARY" in out
        for name in ROLES:
            assert name in out

    def test_print_roles_summary_shows_masks(self, rbac, capsys):
        rbac.print_roles_summary()
        out = capsys.readouterr().out
        assert "[MASK:" in out

    def test_print_access_matrix_runs(self, capsys):
        from governance.rbac import print_access_matrix

        print_access_matrix()
        out = capsys.readouterr().out
        assert "ACCESS MATRIX" in out
        for schema in ("bronze", "silver", "gold", "sandbox"):
            assert schema in out
        assert "R/W" in out


# ---------------------------------------------------------------------------
# Dataclass defaults
# ---------------------------------------------------------------------------


class TestDataclassDefaults:
    def test_permission_defaults(self):
        perm = Permission(ResourceType.TABLE, "iceberg.gold.t", AccessLevel.READ)
        assert perm.column_mask == ""

    def test_role_defaults_are_not_shared(self):
        """
        field(default_factory=list) — two roles must not share one mutable list.
        A bare `= []` would make every role append into the same object.
        """
        a = Role(name="a", description="a")
        b = Role(name="b", description="b")
        a.permissions.append(Permission(ResourceType.TABLE, "iceberg.gold.t", AccessLevel.READ))
        assert b.permissions == []

    def test_user_defaults_are_not_shared(self):
        a = User(username="a")
        b = User(username="b")
        a.roles.append("admin")
        a.groups.append("admins")
        assert b.roles == []
        assert b.groups == []
