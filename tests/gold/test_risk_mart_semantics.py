"""
Ngữ nghĩa của hai mart rủi ro — `aml_monitoring` và `loan_portfolio_risk`.

Hai lỗi, cùng một kiểu: cột trông hợp lý, không check nào đỏ, nhưng nói sai.

- `aml_monitoring` so `debit_credit` với 'CREDIT' / 'DEBIT', trong khi nguồn chỉ
  cho 'D' / 'C' (CHECK ở docker/init_postgres/01_ddl_core_banking.sql). Tổng
  credit/debit luôn bằng 0 — trong một CTE mà không cột nào ra tới SELECT cuối.
- `loan_portfolio_risk.npl_proxy` bằng đúng `total_outstanding`: đọc như dư nợ
  xấu, thực chất là tổng dư nợ.

Phần tĩnh chạy trong unit CI. Phần Spark (`-m integration`) chạy SQL THẬT từ
YAML trên dữ liệu in-memory, trong job Gold Spark Regression:

    pytest tests/gold/test_risk_mart_semantics.py -m integration
"""

from __future__ import annotations

import os
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ETL_ROOT = PROJECT_ROOT / "code_etl"
SOURCE_DDL = PROJECT_ROOT / "docker" / "init_postgres" / "01_ddl_core_banking.sql"
COB_DT = "2025-12-31"


def _job_sql() -> dict[str, str]:
    """SQL của mọi job YAML, bỏ comment `--` để comment không bị tính là code."""
    out = {}
    for path in sorted(ETL_ROOT.rglob("*.yml")):
        config = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(config, dict) and isinstance(config.get("sql"), str):
            sql = re.sub(r"--[^\n]*", "", config["sql"])
            out[path.relative_to(PROJECT_ROOT).as_posix()] = sql
    return out


JOB_SQL = _job_sql()


# ---------------------------------------------------------------------------
# Tĩnh — unit CI
# ---------------------------------------------------------------------------


def test_inputs_are_found():
    assert len(JOB_SQL) >= 40, "glob hỏng? quá ít job SQL"
    assert "code_etl/gold/risk/aml_monitoring.yml" in JOB_SQL


def test_source_allows_only_d_and_c():
    """Nếu nguồn đổi miền giá trị, test dưới phải đổi theo — không âm thầm lệch."""
    assert re.search(r"CHECK\s*\(\s*debit_credit\s+IN\s*\(\s*'D'\s*,\s*'C'\s*\)\s*\)", SOURCE_DDL.read_text())


def _debit_credit_literals(sql: str) -> list[str]:
    found = re.findall(r"debit_credit\s*(?:=|<>|!=)\s*'([^']*)'", sql, re.IGNORECASE)
    for group in re.findall(r"debit_credit\s+(?:NOT\s+)?IN\s*\(([^)]*)\)", sql, re.IGNORECASE):
        found += re.findall(r"'([^']*)'", group)
    return found


def test_debit_credit_is_compared_with_source_values():
    wrong = {
        path: sorted(set(lits) - {"D", "C"})
        for path, sql in JOB_SQL.items()
        if (lits := _debit_credit_literals(sql)) and set(lits) - {"D", "C"}
    }
    assert not wrong, f"debit_credit chỉ nhận 'D' / 'C' ở nguồn, nhưng SQL so với: {wrong}"


def test_literal_scanner_sees_the_old_bug():
    """Chứng minh scanner bắt được đúng dạng lỗi cũ — không pass vì mù."""
    old = "SUM(CASE WHEN debit_credit='CREDIT' THEN txn_amount ELSE 0 END)"
    assert _debit_credit_literals(old) == ["CREDIT"]


_CTE_NAME = re.compile(r"(?:\bWITH|,)\s*([a-z_][a-z0-9_]*)\s+AS\s*\(", re.IGNORECASE)


def _unused_ctes(sql: str) -> list[str]:
    return [
        name
        for name in dict.fromkeys(_CTE_NAME.findall(sql))
        if len(re.findall(rf"\b{re.escape(name)}\b", sql, re.IGNORECASE)) < 2
    ]


def test_no_gold_or_silver_cte_is_defined_but_never_read():
    """CTE không ai đọc là code chết — nơi `customer_stats` của AML tính sai mà không ai thấy."""
    unused = {path: names for path, sql in JOB_SQL.items() if (names := _unused_ctes(sql))}
    assert not unused, f"CTE được khai nhưng không được đọc: {unused}"


def test_cte_scanner_sees_an_unused_cte():
    sql = "WITH a AS (SELECT 1), dead AS (SELECT 2) SELECT * FROM a"
    assert _unused_ctes(sql) == ["dead"]


def test_npl_proxy_is_not_total_outstanding():
    sql = JOB_SQL["code_etl/gold/risk/loan_portfolio_risk.yml"]
    npl = re.search(r"([^\n]*)\bAS\s+npl_proxy\b", sql).group(1)
    assert "total_outstanding" not in npl, f"npl_proxy lại tính từ total_outstanding: {npl.strip()}"


# ---------------------------------------------------------------------------
# Chạy thật trên Spark — Gold Spark Regression
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="pyspark không có trong CI env")
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.appName("risk-mart-semantics")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _gold_sql(name: str) -> str:
    sys.path.insert(0, str(ETL_ROOT / "shared"))
    from utils.sql_renderer import render_sql

    config = yaml.safe_load(next((ETL_ROOT / "gold").glob(f"*/{name}")).read_text(encoding="utf-8"))
    return render_sql(config["sql"], {"cob_dt": COB_DT}).replace("lakehouse.silver.", "")


@pytest.mark.integration
def test_npl_proxy_sums_overdue_and_written_off_outstanding(spark):
    cob = date.fromisoformat(COB_DT)
    spark.createDataFrame(
        [
            ("B1", "LOAN001", 1, Decimal("1000"), Decimal("100"), "ACTIVE"),
            ("B1", "LOAN001", 2, Decimal("1000"), Decimal("50"), "OVERDUE"),
            ("B1", "LOAN001", 3, Decimal("1000"), Decimal("20"), "WRITTEN_OFF"),
            ("B1", "LOAN001", 4, Decimal("1000"), Decimal("0"), "CLOSED"),
        ],
        "branch_code string, product_code string, loan_id bigint, loan_amount decimal(18,2), "
        "outstanding_balance decimal(18,2), loan_status string",
    ).createOrReplaceTempView("dim_loan")
    spark.createDataFrame([("B1", "Chi nhánh 1")], "branch_code string, branch_name string").createOrReplaceTempView(
        "dim_branch"
    )
    spark.createDataFrame(
        [(10, 1, "PAID", Decimal("0"), cob)],
        "payment_id bigint, loan_id bigint, payment_status string, penalty decimal(18,2), cob_dt date",
    ).createOrReplaceTempView("fact_loan_payment")

    row = spark.sql(_gold_sql("loan_portfolio_risk.yml")).collect()[0]
    assert row.total_outstanding == Decimal("170.00")
    assert row.npl_proxy == Decimal("70.00"), "OVERDUE 50 + WRITTEN_OFF 20; ACTIVE và CLOSED không phải nợ xấu"


@pytest.mark.integration
def test_aml_runs_and_keeps_one_row_per_transaction(spark):
    """Gỡ CTE chết không được làm hỏng SQL hay nhân dòng."""
    cob = date.fromisoformat(COB_DT)
    ts = datetime(2025, 12, 30, 3, 0)
    spark.createDataFrame(
        [
            (1, 100, 7, ts, Decimal("60000000"), "TRANSFER_OUT", "D", Decimal("0"), "ATM", "", "", cob),
            (2, 100, 7, ts, Decimal("70000000"), "TRANSFER_OUT", "D", Decimal("0"), "POS", "", "", cob),
            (3, 100, 7, ts, Decimal("10000"), "DEPOSIT", "C", Decimal("0"), "BRANCH", "", "", cob),
        ],
        "txn_id bigint, account_id bigint, customer_id bigint, txn_date timestamp, txn_amount decimal(18,2), "
        "txn_type string, debit_credit string, balance_after decimal(18,2), channel string, description string, "
        "counter_account string, cob_dt date",
    ).createOrReplaceTempView("fact_txn_account")
    spark.createDataFrame(
        [(7, "RETAIL", 1)], "customer_id bigint, customer_segment string, is_current int"
    ).createOrReplaceTempView("dim_customer")
    spark.createDataFrame(
        [(100, "B1", 1)], "account_id bigint, branch_code string, is_current int"
    ).createOrReplaceTempView("dim_account")
    spark.createDataFrame(
        [(7, ts, 1, "SUCCESS", 0, None, cob)],
        "customer_id bigint, transaction_date timestamp, location_id bigint, status string, is_fraud int, "
        "fraud_reason string, cob_dt date",
    ).createOrReplaceTempView("fact_online_transaction")
    spark.createDataFrame(
        [(1, "HCM", 0)], "location_id bigint, state string, is_high_risk_area int"
    ).createOrReplaceTempView("dim_location")

    rows = spark.sql(_gold_sql("aml_monitoring.yml")).collect()
    assert sorted(r.txn_id for r in rows) == [1, 2, 3]
    assert all(r.structuring_flag == 1 for r in rows), "2 giao dịch 50–99,99tr trong ngày → structuring"
    assert "total_credit" not in rows[0].asDict()
