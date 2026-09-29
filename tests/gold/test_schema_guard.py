"""
Guard schema trước khi ghi Gold (code_etl/shared/spark/schema_guard.py).

Chiều so sánh là KẾT QUẢ → BẢNG (khác drift DAG, so BẢNG → DDL):
- kết quả thiếu cột của bảng → BREAKING;
- đổi họ kiểu (số ↔ chuỗi, date ↔ timestamp) → BREAKING;
- số → số theo mọi chiều → ghi như trước, KHÔNG nới kiểu bảng (SUM trên
  DECIMAL(18,2) ra DECIMAL(28,2); nới theo kết quả sẽ kéo bảng lệch khỏi DDL);
- cột mới → ADD COLUMNS rồi ghi (trước đây INSERT_COLUMN_ARITY_MISMATCH, ALTER tay).
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "code_etl" / "shared"))

from spark.schema_guard import BreakingSchemaChange, assignable, guard_and_evolve, plan_write  # noqa: E402

TABLE = {"loan_id": "bigint", "dpd": "int", "amount": "decimal(18,2)", "bucket": "string", "cob_dt": "date"}


def test_identical_schema_needs_nothing():
    plan = plan_write(TABLE, dict(TABLE))
    assert not plan.breaking and plan.ddl("t") == []


@pytest.mark.parametrize(
    ("result", "table"),
    [
        ("decimal(28,2)", "decimal(18,2)"),  # SUM mở rộng precision — ghi như trước
        ("int", "bigint"),
        ("bigint", "int"),
        ("double", "decimal(18,2)"),
        ("varchar(10)", "string"),
    ],
)
def test_same_family_is_written_as_is(result, table):
    assert assignable(result, table)
    plan = plan_write({"c": table}, {"c": result})
    assert not plan.breaking and plan.ddl("t") == [], "không được tự đổi kiểu cột bảng"


def test_new_column_is_added_before_the_write():
    plan = plan_write(TABLE, {**TABLE, "is_npl": "int"})
    assert not plan.breaking
    assert plan.ddl("lakehouse.gold.x") == ["ALTER TABLE lakehouse.gold.x ADD COLUMNS (is_npl int)"]


@pytest.mark.parametrize(
    ("result", "why"),
    [
        ({k: v for k, v in TABLE.items() if k != "bucket"}, "mất cột"),
        ({**TABLE, "amount": "string"}, "số → chuỗi"),
        ({**TABLE, "bucket": "int"}, "chuỗi → số"),
        ({**TABLE, "cob_dt": "timestamp"}, "date → timestamp"),
    ],
)
def test_breaking_changes(result, why):
    assert plan_write(TABLE, result).breaking, why


def _spark_with_table(table_types):
    spark = MagicMock()
    spark.table.return_value.dtypes = list(table_types.items())
    return spark


def _result(result_types):
    df = MagicMock()
    df.dtypes = list(result_types.items())
    return df


def test_guard_raises_before_touching_the_table():
    spark = _spark_with_table(TABLE)
    with pytest.raises(BreakingSchemaChange, match="thiếu cột"):
        guard_and_evolve(spark, _result({k: v for k, v in TABLE.items() if k != "dpd"}), "t", MagicMock())
    spark.sql.assert_not_called()


def test_guard_adds_new_columns_only():
    spark = _spark_with_table(TABLE)
    guard_and_evolve(spark, _result({**TABLE, "is_npl": "int", "amount": "decimal(28,2)"}), "t", MagicMock())
    assert [c.args[0] for c in spark.sql.call_args_list] == ["ALTER TABLE t ADD COLUMNS (is_npl int)"]


@pytest.mark.integration
@pytest.mark.skipif(
    sys.platform == "win32", reason="tạo bảng Spark trên Windows cần winutils (HADOOP_HOME); chạy ở CI Linux"
)
def test_guard_on_real_spark(tmp_path_factory):
    """dtypes của Spark thật có đúng dạng guard đọc, và ADD COLUMNS chạy được."""
    pytest.importorskip("pyspark")
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("schema-guard")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("wh")))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        spark.sql("CREATE TABLE gold_x (loan_id BIGINT, amount DECIMAL(18,2)) USING parquet")
        result = spark.sql("SELECT CAST(1 AS BIGINT) AS loan_id, SUM(CAST(1 AS DECIMAL(18,2))) AS amount, 1 AS is_npl")
        plan = guard_and_evolve(spark, result, "gold_x", MagicMock())
        assert plan.add_columns == [("is_npl", "int")]
        assert [n for n, _ in spark.table("gold_x").dtypes] == ["loan_id", "amount", "is_npl"]
        assert dict(spark.table("gold_x").dtypes)["amount"] == "decimal(18,2)", "không được nới kiểu"

        with pytest.raises(BreakingSchemaChange):
            guard_and_evolve(spark, spark.sql("SELECT CAST(1 AS BIGINT) AS loan_id"), "gold_x", MagicMock())
    finally:
        spark.stop()
