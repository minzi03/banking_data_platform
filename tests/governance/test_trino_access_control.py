"""
Contract tests cho access control của Trino.

Trước đây `docker/init_trino/access-control.properties` khai 195 dòng luật mà
không có tác dụng vì bốn lý do độc lập — không được mount, sai định dạng, không
có group nào, và che cột không tồn tại. Mỗi class dưới đây chặn một lý do tái
diễn:

  TestConfigIsLoaded      lý do 1–2: file đúng định dạng và được mount đúng chỗ
  TestRulesMatchRbac      rules.json là bản sinh từ governance/rbac.py, không drift
  TestMasksTargetRealColumns   lý do 4: mọi cột bị che có thật, mask đúng kiểu
  TestPolicy              hành vi: ai đọc/ghi được gì, PII có bị che không
  TestRbacAgreesWithRules rbac.py (Python) và rules.json (Trino) nói cùng một điều
  TestClientsUseTheirOwnUser   mỗi client khai đúng user của nó, không dùng admin

Lý do 3 (group) không còn: luật khớp theo tên user, không cần group provider.

Evaluator `_Rules` mô phỏng ngữ nghĩa file-based access control của Trino 443
(luật khớp đầu tiên, không khớp → từ chối). Đã đối chiếu với chính
FileBasedSystemAccessControl của Trino 443 trên toàn bộ ma trận probe — xem PR
thêm file này. Không cần stack; chạy trong unit CI.
"""

from __future__ import annotations

import configparser
import importlib.util
import itertools
import json
import re
from pathlib import Path

import pytest
import yaml

from governance.rbac import ROLES, USERS, RBACManager

REPO_ROOT = Path(__file__).resolve().parents[2]

_spec = importlib.util.spec_from_file_location(
    "generate_trino_access_control", REPO_ROOT / "scripts" / "generate_trino_access_control.py"
)
_generator = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_generator)
RULES_PATH = _generator.RULES_PATH
render = _generator.render

INIT_TRINO = REPO_ROOT / "docker" / "init_trino"
ACCESS_CONTROL_PROPERTIES = INIT_TRINO / "access-control.properties"

PII_COLUMNS = ("cccd", "full_name", "phone", "email", "address", "date_of_birth")
DATA_SCHEMAS = ("bronze", "silver", "silver_cdc", "gold", "sandbox", "serving", "semantic")

# User được phép thấy PII nguyên bản: vận hành (admin) và pipeline ghi dữ liệu.
RAW_PII_ROLES = {"admin", "etl_user"}


# ---------------------------------------------------------------------------
# Evaluator — ngữ nghĩa file-based access control của Trino 443
# ---------------------------------------------------------------------------


def _matches(rule: dict, key: str, value: str) -> bool:
    return re.fullmatch(rule.get(key, ".*"), value) is not None


class _Rules:
    def __init__(self, rules: dict):
        self.rules = rules

    def _first(self, section: str, user: str, **attrs: str) -> dict | None:
        for rule in self.rules.get(section, []):
            if _matches(rule, "user", user) and all(_matches(rule, k, v) for k, v in attrs.items()):
                return rule
        return None

    def catalog_access(self, user: str, catalog: str) -> str:
        rule = self._first("catalogs", user, catalog=catalog)
        if rule:
            return rule["allow"]
        # FileBasedSystemAccessControlModule (Trino 443) nối thêm một luật ẩn
        # vào CUỐI danh sách: mọi user → catalog `system`, ALL ("Hack to allow
        # Trino Admin…"). Luật khai trong rules.json đứng trước nên vẫn thắng.
        return "all" if catalog == "system" else "none"

    def _table_privileges(self, user: str, catalog: str, schema: str, table: str, needs_write: bool) -> set[str]:
        # checkTablePermission: SELECT cần catalog read-only; mọi privilege
        # khác cần catalog `all`.
        access = self.catalog_access(user, catalog)
        if access == "none" or (needs_write and access != "all"):
            return set()
        rule = self._first("tables", user, catalog=catalog, schema=schema, table=table)
        return set(rule["privileges"]) if rule else set()

    def _table_rule(self, user: str, catalog: str, schema: str, table: str) -> dict | None:
        if self.catalog_access(user, catalog) == "none":
            return None
        return self._first("tables", user, catalog=catalog, schema=schema, table=table)

    def can_select(self, user: str, catalog: str, schema: str, table: str) -> bool:
        return "SELECT" in self._table_privileges(user, catalog, schema, table, needs_write=False)

    def can_insert(self, user: str, catalog: str, schema: str, table: str) -> bool:
        return "INSERT" in self._table_privileges(user, catalog, schema, table, needs_write=True)

    def can_create_table(self, user: str, catalog: str, schema: str, table: str = "new_table") -> bool:
        # checkCanCreateTable / DropTable / RenameTable (Trino 443) đều gọi
        # checkTablePermission(OWNERSHIP): privilege trên BẢNG, không phải
        # quyền owner của schema.
        return "OWNERSHIP" in self._table_privileges(user, catalog, schema, table, needs_write=True)

    def owns_schema(self, user: str, catalog: str, schema: str) -> bool:
        # isSchemaOwner: cần catalog `all`, rồi luật schema khớp đầu tiên.
        # Quyết định CREATE / DROP SCHEMA.
        if self.catalog_access(user, catalog) != "all":
            return False
        rule = self._first("schemas", user, catalog=catalog, schema=schema)
        return bool(rule) and rule["owner"]

    def mask(self, user: str, catalog: str, schema: str, table: str, column: str) -> str | None:
        rule = self._table_rule(user, catalog, schema, table)
        for col in (rule or {}).get("columns", []):
            if col["name"] == column:
                return col.get("mask")
        return None

    def can_view_others_queries(self, user: str) -> bool:
        rule = self._first("queries", user)
        return bool(rule) and "view" in rule["allow"]


@pytest.fixture(scope="module")
def rules() -> _Rules:
    return _Rules(json.loads(RULES_PATH.read_text(encoding="utf-8")))


def _role_closure(username: str) -> set[str]:
    seen, pending = set(), list(USERS[username].roles)
    while pending:
        name = pending.pop()
        if name not in seen:
            seen.add(name)
            pending.extend(ROLES[name].parent_roles)
    return seen


# ---------------------------------------------------------------------------
# DDL: bảng lakehouse → cột → kiểu
# ---------------------------------------------------------------------------

_CREATE = re.compile(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.]+)\s*\((.*?)\n\s*\)", re.I | re.S)
_COLUMN = re.compile(r"\s*([a-z_][a-z0-9_]*)\s+([A-Z]+)", re.I)
_SKIP_DDL = {"04_ddl_bronze_cdc_old.sql"}


def _lakehouse_tables() -> dict[tuple[str, str], dict[str, str]]:
    tables: dict[tuple[str, str], dict[str, str]] = {}
    for path in sorted((REPO_ROOT / "docker" / "init_iceberg").glob("*.sql")):
        if path.name in _SKIP_DDL:
            continue
        for fqn, body in _CREATE.findall(path.read_text(encoding="utf-8")):
            parts = fqn.split(".")
            if len(parts) != 3:
                continue
            cols = {}
            for line in body.splitlines():
                m = _COLUMN.match(line.strip())
                if m and m.group(2).upper() not in {"PRIMARY", "CONSTRAINT"}:
                    cols[m.group(1).lower()] = m.group(2).upper()
            tables[(parts[1], parts[2])] = cols
    return tables


@pytest.fixture(scope="module")
def ddl() -> dict[tuple[str, str], dict[str, str]]:
    tables = _lakehouse_tables()
    assert ("silver", "dim_customer") in tables, "parser DDL không tìm thấy silver.dim_customer"
    return tables


# ---------------------------------------------------------------------------
# Lý do 1–2: file đúng định dạng và được mount
# ---------------------------------------------------------------------------


class TestConfigIsLoaded:
    def test_properties_select_the_file_plugin(self):
        parser = configparser.ConfigParser()
        parser.read_string("[root]\n" + ACCESS_CONTROL_PROPERTIES.read_text(encoding="utf-8"))
        props = dict(parser["root"])
        assert props.get("access-control.name") == "file"
        assert props.get("security.config-file") == "/etc/trino/rules.json"

    @pytest.mark.parametrize("compose", ["docker/docker-compose.yml", "docker/docker-compose.ci.yml"])
    def test_compose_mounts_both_files_where_trino_reads_them(self, compose):
        spec = yaml.safe_load((REPO_ROOT / compose).read_text(encoding="utf-8"))
        mounts = {}
        for volume in spec["services"]["trino"]["volumes"]:
            source, target = volume.split(":")[:2]
            mounts[target] = source
        assert mounts.get("/etc/trino/access-control.properties") == "./init_trino/access-control.properties"
        assert mounts.get("/etc/trino/rules.json") == "./init_trino/rules.json"
        assert mounts.get("/etc/trino/catalog") == "./init_trino/catalog"

    def test_terraform_mounts_match_compose(self):
        text = (REPO_ROOT / "terraform" / "services.tf").read_text(encoding="utf-8")
        trino = text[text.index('resource "docker_container" "trino"') :]
        trino = trino[: trino.index("\n}\n")]
        pairs = dict(
            (target, source)
            for source, target in re.findall(
                r'host_path\s*=\s*abspath\("\$\{path\.module\}/\.\./docker/(init_trino[^"]*)"\)\s*'
                r'container_path\s*=\s*"([^"]+)"',
                trino,
            )
        )
        assert pairs == {
            "/etc/trino/catalog": "init_trino/catalog",
            "/etc/trino/access-control.properties": "init_trino/access-control.properties",
            "/etc/trino/rules.json": "init_trino/rules.json",
        }


# ---------------------------------------------------------------------------
# rules.json là bản sinh
# ---------------------------------------------------------------------------


class TestRulesMatchRbac:
    def test_committed_rules_equal_generated(self):
        assert RULES_PATH.read_text(encoding="utf-8") == render(), (
            "docker/init_trino/rules.json lệch với governance/rbac.py — "
            "chạy: py -3 scripts/generate_trino_access_control.py"
        )

    def test_every_section_that_restricts_is_present(self):
        """Mục vắng mặt = cho phép tất cả (tài liệu Trino). Thiếu `queries` là
        mọi user xem và kill được truy vấn của người khác."""
        data = json.loads(RULES_PATH.read_text(encoding="utf-8"))
        for section in ("catalogs", "schemas", "tables", "queries", "system_information"):
            assert data.get(section), section

    def test_each_user_appears_in_exactly_one_catalog_block(self, rules):
        """Luật khớp đầu tiên: user nằm trong hai khối sẽ chỉ nhận khối đầu."""
        for user in USERS:
            regexes = {r["user"] for r in rules.rules["catalogs"] if re.fullmatch(r["user"], user)}
            assert len(regexes) == 1, f"{user} khớp {sorted(regexes)}"


# ---------------------------------------------------------------------------
# Lý do 4: mask trỏ vào cột có thật, đúng kiểu
# ---------------------------------------------------------------------------


class TestMasksTargetRealColumns:
    def _masked(self, rules):
        for rule in rules.rules["tables"]:
            for col in rule.get("columns", []):
                yield rule["schema"], rule["table"], col["name"], col["mask"]

    def test_there_are_masks(self, rules):
        assert list(self._masked(rules))

    def test_every_masked_column_exists_in_ddl(self, rules, ddl):
        for schema, table, column, _mask in self._masked(rules):
            assert (schema, table) in ddl, f"{schema}.{table} không có trong DDL"
            assert column in ddl[(schema, table)], f"{schema}.{table}.{column} không có trong DDL"

    def test_mask_returns_the_column_type(self, rules, ddl):
        """Trino từ chối mask lệch kiểu lúc query — lỗi đó chỉ lộ ra khi có
        người đọc bảng. Chặn trước ở đây cho các kiểu có trong mô hình."""
        for schema, table, column, mask in self._masked(rules):
            col_type = ddl[(schema, table)][column]
            if col_type == "DATE":
                assert mask.startswith("date_trunc("), f"{schema}.{table}.{column}: {mask}"
            elif col_type in {"BIGINT", "LONG"}:
                assert re.search(r"AS\s+BIGINT\)$", mask), f"{schema}.{table}.{column}: {mask}"
            else:
                assert col_type in {"STRING", "VARCHAR"}, f"{schema}.{table}.{column}: kiểu {col_type} chưa hỗ trợ"
                assert "%s" not in mask and column in mask or "AS VARCHAR" in mask, mask


# ---------------------------------------------------------------------------
# Hành vi
# ---------------------------------------------------------------------------


class TestPolicy:
    def test_no_one_outside_admin_and_etl_reads_raw_pii(self, rules, ddl):
        """Bất biến chính: mọi bảng có PII khách hàng, mọi user đọc được bảng đó
        mà không có role admin/etl_user, thì MỌI cột PII phải bị che."""
        pii_tables = {k: [c for c in PII_COLUMNS if c in cols] for k, cols in ddl.items()}
        pii_tables = {k: v for k, v in pii_tables.items() if len(v) >= 3}
        assert ("silver", "dim_customer") in pii_tables and ("bronze", "core_customer") in pii_tables

        checked = 0
        for user in USERS:
            if _role_closure(user) & RAW_PII_ROLES:
                continue
            for (schema, table), columns in pii_tables.items():
                if not rules.can_select(user, "iceberg", schema, table):
                    continue
                for column in columns:
                    checked += 1
                    assert rules.mask(user, "iceberg", schema, table, column), (
                        f"{user} đọc được {schema}.{table}.{column} KHÔNG che"
                    )
        assert checked, "không user nào đọc bảng PII — bất biến kiểm rỗng"

    def test_data_steward_inherits_masks(self, rules):
        """Trước đây mask không kế thừa: steward đọc Silver mà thấy cccd gốc."""
        assert rules.mask("data_steward_user", "iceberg", "silver", "dim_customer", "cccd")

    def test_admin_and_etl_see_raw_values(self, rules):
        for user in ("admin", "trino", "airflow_etl"):
            assert rules.can_select(user, "iceberg", "silver", "dim_customer")
            assert rules.mask(user, "iceberg", "silver", "dim_customer", "cccd") is None

    @pytest.mark.parametrize("user", ["superset", "customer_api", "streamlit", "ml"])
    def test_serving_consumers_read_serving_only(self, rules, user):
        for schema in DATA_SCHEMAS:
            assert rules.can_select(user, "iceberg", schema, "t") == (schema == "serving"), schema
            assert not rules.can_insert(user, "iceberg", schema, "t"), schema
            assert not rules.can_create_table(user, "iceberg", schema), schema

    def test_dbt_builds_serving_from_gold(self, rules):
        assert rules.can_select("dbt", "iceberg", "gold", "mart_customer_360")
        assert rules.can_create_table("dbt", "iceberg", "serving")
        assert rules.can_insert("dbt", "iceberg", "serving", "mart_customer_360_current")
        for schema in ("bronze", "silver", "silver_cdc", "sandbox"):
            assert not rules.can_select("dbt", "iceberg", schema, "t"), schema
        assert not rules.can_create_table("dbt", "iceberg", "gold")

    def test_every_declared_user_can_read_engine_metadata(self, rules):
        """dbt-trino join system.metadata.materialized_views ở MỌI lần list
        relation; thiếu quyền là mọi `dbt run` hỏng."""
        for user in USERS:
            assert rules.catalog_access(user, "system") != "none", user
            assert rules.can_select(user, "system", "metadata", "materialized_views"), user

    def test_observers_read_every_layer_with_pii_masked(self, rules):
        for user in ("freshness_exporter", "manifest_collector"):
            for schema in ("bronze", "silver", "silver_cdc", "gold", "serving"):
                assert rules.can_select(user, "iceberg", schema, "t"), (user, schema)
                assert not rules.can_insert(user, "iceberg", schema, "t"), (user, schema)
            assert rules.mask(user, "iceberg", "bronze", "core_customer", "cccd")

    def test_read_only_roles_cannot_write_anywhere(self, rules):
        for user in USERS:
            if rules.catalog_access(user, "iceberg") != "read-only":
                continue
            for schema in DATA_SCHEMAS:
                assert not rules.can_insert(user, "iceberg", schema, "t"), (user, schema)
                assert not rules.can_create_table(user, "iceberg", schema), (user, schema)

    def test_unknown_user_gets_nothing(self, rules):
        assert rules.catalog_access("nobody", "iceberg") == "none"
        for schema in DATA_SCHEMAS:
            assert not rules.can_select("nobody", "iceberg", schema, "t")
        # Catalog `system` thì Trino luôn mở (luật ẩn), nhưng không luật bảng
        # nào khớp user lạ — nên vẫn không đọc được bảng nào trong đó.
        for schema, table in (("metadata", "materialized_views"), ("runtime", "queries"), ("jdbc", "tables")):
            assert not rules.can_select("nobody", "system", schema, table), (schema, table)

    def test_only_dbt_etl_steward_and_admins_create_tables(self, rules):
        """CREATE TABLE cần OWNERSHIP trên bảng. Ghi lại ai đang có, để việc
        mở rộng quyền tạo bảng phải hiện ra trong diff của test này."""
        creators = {
            (user, schema)
            for user in USERS
            for schema in DATA_SCHEMAS
            if rules.can_create_table(user, "iceberg", schema)
        }
        by_user: dict[str, set[str]] = {}
        for user, schema in creators:
            by_user.setdefault(user, set()).add(schema)
        all_schemas = set(DATA_SCHEMAS)
        assert by_user == {
            "admin": all_schemas,
            "trino": all_schemas,
            "trino_admin": all_schemas,
            "airflow_etl": all_schemas,  # etl_user: TABLE iceberg.*.* WRITE
            "dbt": {"serving"},
            "data_steward_user": {"sandbox"},
        }

    def test_only_admins_see_other_users_queries(self, rules):
        for user in [*USERS, "nobody"]:
            assert rules.can_view_others_queries(user) == (
                "admin" in _role_closure(user) if user in USERS else False
            ), user


# ---------------------------------------------------------------------------
# rbac.py và rules.json nói cùng một điều
# ---------------------------------------------------------------------------


class TestRbacAgreesWithRules:
    """RBACManager là API Python; rules.json là thứ Trino thực thi. Nếu hai bên
    lệch, code nào hỏi RBACManager sẽ nhận câu trả lời khác với Trino."""

    @pytest.mark.parametrize(
        ("user", "schema"), list(itertools.product(sorted(USERS), ("bronze", "silver", "gold", "sandbox", "serving")))
    )
    def test_read_and_write_agree(self, rules, user, schema):
        rbac = RBACManager()
        assert rbac.has_access(user, schema, "t", "read") == rules.can_select(user, "iceberg", schema, "t")
        assert rbac.has_access(user, schema, "t", "write") == rules.can_insert(user, "iceberg", schema, "t")

    @pytest.mark.parametrize("user", sorted(USERS))
    def test_masks_agree(self, rules, user, ddl):
        rbac = RBACManager()
        for (schema, table), cols in ddl.items():
            if not rules.can_select(user, "iceberg", schema, table):
                continue
            expected = {c: m.replace("%s", c) for c, m in rbac.get_masked_columns(user, schema, table).items()}
            actual = {c: rules.mask(user, "iceberg", schema, table, c) for c in cols}
            assert {c: m for c, m in actual.items() if m} == expected, f"{user} {schema}.{table}"


# ---------------------------------------------------------------------------
# Mỗi client khai đúng user của nó
# ---------------------------------------------------------------------------

CLIENT_USERS = [
    ("dbt/profiles.yml", r"^\s*user:\s*(\S+)", "dbt"),
    ("docker/dbt/docker-compose.dbt.yml", r"DBT_TRINO_USER=(\S+)", "dbt"),
    ("docker/superset/add_trino_connection.py", r"trino://([^@]+)@", "superset"),
    ("api/main.py", r'"TRINO_USER",\s*"([^"]+)"', "customer_api"),
    ("docker/docker-compose.yml", r"TRINO_USER=(\S+)", "customer_api"),
    ("streamlit/app.py", r'\buser="([^"]+)"', "streamlit"),
    ("ml/pipeline/churn_prediction.py", r'\buser="([^"]+)"', "ml"),
    ("ml/pipeline/credit_scoring.py", r'\buser="([^"]+)"', "ml"),
    ("ml/monitoring/drift_detection.py", r'\buser="([^"]+)"', "ml"),
    ("docker/monitoring/exporters/freshness_exporter.py", r'"TRINO_USER",\s*"([^"]+)"', "freshness_exporter"),
    ("scripts/generate_metrics_manifest.py", r'\buser: str = "([^"]+)"', "manifest_collector"),
]


class TestClientsUseTheirOwnUser:
    @pytest.mark.parametrize(("path", "pattern", "expected"), CLIENT_USERS, ids=[c[0] for c in CLIENT_USERS])
    def test_client_declares_expected_user(self, path, pattern, expected):
        found = re.findall(pattern, (REPO_ROOT / path).read_text(encoding="utf-8"), flags=re.M)
        assert found, f"{path}: không tìm thấy khai báo user Trino"
        assert set(found) == {expected}, f"{path}: {found}"
        assert expected in USERS, f"{expected} chưa khai trong governance/rbac.py"

    def test_no_hardcoded_trino_header_user(self):
        text = (REPO_ROOT / "docker/monitoring/exporters/freshness_exporter.py").read_text(encoding="utf-8")
        assert re.findall(r'"X-Trino-User":\s*([^,}\n]+)', text) == ["TRINO_USER", "TRINO_USER"]
