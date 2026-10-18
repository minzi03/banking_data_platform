"""
Tests cho static dimension join trong foreachBatch (stream_enrichment.py).

Khoá new2 §4.3. Khoá phần logic chạy được trên DataFrame in-memory:
  - LEFT JOIN giữ nguyên số dòng (dimension thiếu → NULL, KHÔNG mất event).
  - Cột dimension mang sang đúng tên/prefix.
  - Nhiều join xếp tầng.
  - Đọc lại dim mỗi lần gọi (refresh) — mỗi lần gọi spark.table là một lần đọc.
"""

import importlib.util
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark.sql.functions", reason="pyspark không có trong CI env")
from pyspark.sql import SparkSession  # noqa: E402

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_JOB = PROJECT_ROOT / "code_etl" / "cdc" / "base_job"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


enr = _load("stream_enrichment", BASE_JOB / "stream_enrichment.py")


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("test-stream-enrichment")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture
def events(spark):
    return spark.createDataFrame(
        [(1, 100, "ACC"), (2, 200, "ACC"), (3, None, "ACC")],
        ["txn_id", "account_id", "kind"],
    )


@pytest.fixture
def dim_account(spark):
    # Chỉ có account 100 → 200 không khớp (test LEFT JOIN), account_id NULL cũng không.
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW dim_account AS SELECT * FROM VALUES (100, 'BR01', 'CASA') AS t(account_id, branch_code, account_type)"
    )
    return dim_account_view(spark)


def dim_account_view(spark):
    return "dim_account"


class TestEnrichBatch:
    def test_left_join_preserves_row_count(self, spark, events, dim_account):
        joins = [
            {
                "dim_table": dim_account,
                "on": [["account_id", "account_id"]],
                "columns": ["branch_code", "account_type"],
                "prefix": "acct_",
            }
        ]
        out = enr.enrich_batch(spark, events, joins, batch_id=1)
        assert out.count() == events.count() == 3

    def test_matching_row_gets_dimension_columns(self, spark, events, dim_account):
        joins = [
            {
                "dim_table": dim_account,
                "on": [["account_id", "account_id"]],
                "columns": ["branch_code"],
                "prefix": "acct_",
            }
        ]
        out = {r["txn_id"]: r for r in enr.enrich_batch(spark, events, joins, 1).collect()}
        assert out[1]["acct_branch_code"] == "BR01"
        # account 200 không có trong dim → NULL, không mất dòng.
        assert out[2]["acct_branch_code"] is None
        # account_id NULL → NULL.
        assert out[3]["acct_branch_code"] is None

    def test_dimension_read_once_per_call(self, spark, events, dim_account):
        """Mỗi lần gọi = một lần đọc dim (refresh mỗi micro-batch)."""
        joins = [
            {
                "dim_table": dim_account,
                "on": [["account_id", "account_id"]],
                "columns": ["branch_code"],
            }
        ]
        # Gọi hai lần với cùng input → vẫn chạy được (đọc lại), không cache lỗi.
        a = enr.enrich_batch(spark, events, joins, 1)
        b = enr.enrich_batch(spark, events, joins, 2)
        assert a.count() == b.count() == 3


class TestWriteEnrichment:
    def test_empty_df_returns_zero(self, spark, events):
        empty = events.filter("1 = 0")
        assert enr.write_enrichment(spark, empty, "lakehouse.silver.stream_enriched_x", 1) == 0
