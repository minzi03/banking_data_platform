"""
Tests for code_etl/serving/sync_postgres.py

Covers:
  - _pg_type: Spark → PostgreSQL type mapping (Decimal precision/scale, unsupported → raise)
  - load_config: YAML config shape
  - ensure_target_table: missing upsert key → raise; DDL built from Iceberg schema
  - parse_args: --only parsing

Không cần Spark/Postgres thật: bảng và connection được mock.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Stub CHỈ trong lúc exec rồi khôi phục — cùng khuôn tests/ops/test_iceberg_maintenance.py.
_STUBBED = {
    "pyspark": MagicMock(),
    "pyspark.sql": MagicMock(),
    "spark": MagicMock(),
    "spark.spark_session": MagicMock(),
}
_SAVED = {name: sys.modules.get(name) for name in _STUBBED}
sys.modules.update(_STUBBED)

_spec = importlib.util.spec_from_file_location(
    "sync_postgres_mod",
    str(PROJECT_ROOT / "code_etl" / "serving" / "sync_postgres.py"),
)
_imod = importlib.util.module_from_spec(_spec)
try:
    _spec.loader.exec_module(_imod)
finally:
    for _name, _previous in _SAVED.items():
        if _previous is None:
            sys.modules.pop(_name, None)
        else:
            sys.modules[_name] = _previous

_pg_type = _imod._pg_type
load_config = _imod.load_config
ensure_target_table = _imod.ensure_target_table
parse_args = _imod.parse_args
CONFIG_FILE = _imod.CONFIG_FILE


class FakeSparkType:
    def __init__(self, name):
        self.__class__.__name__ = name

    def __repr__(self):  # pragma: no cover - debug helper
        return f"FakeSparkType({type(self).__name__})"


def _fake_type(class_name, precision=None, scale=None):
    """Tạo đối tượng giả có __class__.__name__ đúng như Spark DataType."""
    cls = type(class_name, (), {})
    obj = cls()
    if precision is not None:
        obj.precision = precision
        obj.scale = scale
    return obj


class TestPgTypeMapping:
    def test_simple_types(self):
        assert _pg_type(_fake_type("StringType")) == "TEXT"
        assert _pg_type(_fake_type("LongType")) == "BIGINT"
        assert _pg_type(_fake_type("IntegerType")) == "INTEGER"
        assert _pg_type(_fake_type("DoubleType")) == "DOUBLE PRECISION"
        assert _pg_type(_fake_type("DateType")) == "DATE"
        assert _pg_type(_fake_type("TimestampType")) == "TIMESTAMP"
        assert _pg_type(_fake_type("BooleanType")) == "BOOLEAN"

    def test_timestamp_ntz_maps(self):
        """
        Trino trả `timestamp` (không zone) về Spark dưới dạng TimestampNTZType.
        Thiếu map này thì bảng serving có cột timestamp do dbt sinh (vd
        merchant_payment_notification_current) fail lúc sync — đã gặp thật.
        """
        assert _pg_type(_fake_type("TimestampNTZType")) == "TIMESTAMP"

    def test_decimal_keeps_precision_scale(self):
        assert _pg_type(_fake_type("DecimalType", precision=18, scale=2)) == "NUMERIC(18,2)"

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError, match="ArrayType"):
            _pg_type(_fake_type("ArrayType"))


class TestConfig:
    def test_config_loads(self):
        config = load_config(CONFIG_FILE)
        assert config["target_schema"] == "serving"
        assert len(config["tables"]) > 0

    def test_each_table_has_name_and_key(self):
        config = load_config(CONFIG_FILE)
        for t in config["tables"]:
            assert "name" in t and "key" in t, t
            assert isinstance(t["key"], list) and t["key"], t

    def test_customer_grain_tables_use_customer_id_key(self):
        config = load_config(CONFIG_FILE)
        by_name = {t["name"]: t for t in config["tables"]}
        assert by_name["mart_customer_360_current"]["key"] == ["customer_id"]
        assert by_name["loan_portfolio_risk_current"]["key"] == ["branch_code", "product_code"]
        assert by_name["fraud_risk_txn_current"]["key"] == ["txn_id"]


class TestEnsureTargetTable:
    @staticmethod
    def _spark_with_columns(cols):
        """Mock Spark sao cho spark.table(src).schema.fields trả về các cột cho trước."""
        spark = MagicMock()
        fields = []
        for name, dtype in cols:
            field = MagicMock()
            field.name = name
            field.dataType = dtype
            fields.append(field)
        spark.table.return_value.schema.fields = fields
        return spark

    def test_missing_key_raises(self):
        spark = self._spark_with_columns([("a", _fake_type("StringType"))])
        with patch.object(_imod, "_run_sql"), pytest.raises(ValueError, match="khoá upsert"):
            ensure_target_table(spark, "lakehouse.serving.x", "serving.x", ["customer_id"], {})

    def test_returns_column_names_in_order(self):
        cols = [
            ("customer_id", _fake_type("LongType")),
            ("cob_dt", _fake_type("DateType")),
            ("aum_total", _fake_type("DecimalType", precision=18, scale=2)),
        ]
        spark = self._spark_with_columns(cols)
        with patch.object(_imod, "_run_sql") as mock_sql:
            names = ensure_target_table(spark, "lakehouse.serving.x", "serving.x", ["customer_id"], {})
        assert names == ["customer_id", "cob_dt", "aum_total"]
        calls = [c[0][1] for c in mock_sql.call_args_list]
        ddl = next(c for c in calls if "CREATE TABLE" in c)
        assert "CREATE TABLE IF NOT EXISTS serving.x" in ddl
        assert 'PRIMARY KEY ("customer_id")' in ddl
        assert '"aum_total" NUMERIC(18,2)' in ddl
        # Index cho truy vấn app (new2 §4.3) — idempotent.
        idx = next(c for c in calls if "CREATE INDEX" in c)
        assert "IF NOT EXISTS" in idx
        assert "ON serving.x" in idx
        assert '("customer_id")' in idx

    def test_composite_key_index_name_bounded_length(self):
        cols = [
            ("merchant_id", _fake_type("LongType")),
            ("txn_year", _fake_type("IntegerType")),
            ("txn_month", _fake_type("IntegerType")),
        ]
        spark = self._spark_with_columns(cols)
        with patch.object(_imod, "_run_sql") as mock_sql:
            ensure_target_table(
                spark,
                "lakehouse.serving.merchant_ranking_current",
                "serving.merchant_ranking_current",
                ["merchant_id", "txn_year", "txn_month"],
                {},
            )
        idx = next(c[0][1] for c in mock_sql.call_args_list if "CREATE INDEX" in c[0][1])
        # Tên index phải <= 63 ký tự (giới hạn định danh PostgreSQL).
        name = idx.split("IF NOT EXISTS ")[1].split(" ")[0]
        assert len(name) <= 63, name


class TestParseArgs:
    def test_only_defaults_empty(self):
        with patch("sys.argv", ["sync_postgres.py", "--cob_dt", "2025-12-31"]):
            args = parse_args()
            assert args.only == ""
            assert args.cob_dt == "2025-12-31"

    def test_cob_dt_required(self):
        with patch("sys.argv", ["sync_postgres.py"]), pytest.raises(SystemExit):
            parse_args()
