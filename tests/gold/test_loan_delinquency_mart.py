"""
Ngữ nghĩa của gold.loan_delinquency — chạy SQL thật của job trên Spark local.

Mỗi khoản vay trong fixture dựng cho MỘT câu hỏi:
- DPD lấy từ kỳ gần nhất có payment_date <= cob_dt; kỳ tương lai bị bỏ qua;
- prev_dpd là kỳ liền trước (cặp prev → current dựng nên ma trận roll rate);
- ranh giới nhóm nợ theo số ngày quá hạn (TT 11/2021): 9|10, 89|90, 179|180, 359|360;
- WRITTEN_OFF ra ngoài nhóm nợ / NPL;
- khoản chưa tới kỳ trả nào vẫn có một dòng, DPD 0;
- ever_30_plus / ever_90_plus nhớ đỉnh DPD dù khoản đã hồi.

Chạy trong CI job "Gold Spark Regression" (pyspark 3.5.3).
"""

from __future__ import annotations

import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ETL_ROOT = PROJECT_ROOT / "code_etl"
CONFIG = ETL_ROOT / "gold" / "risk" / "loan_delinquency.yml"
COB_DT = "2025-12-31"
COB = date.fromisoformat(COB_DT)


def _job_sql() -> str:
    sys.path.insert(0, str(ETL_ROOT / "shared"))
    from utils.sql_renderer import render_sql

    config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    return render_sql(config["sql"], {"cob_dt": COB_DT}).replace("lakehouse.silver.", "")


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="pyspark không có trong CI env")
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.appName("loan-delinquency-mart")
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


# loan_id → (loan_status, disbursement_date, [(payment_date, days_late), ...])
LOANS = {
    1: ("ACTIVE", date(2025, 1, 15), [(date(2025, 10, 31), 45), (date(2025, 11, 30), 0), (date(2026, 1, 31), 99)]),
    2: ("OVERDUE", date(2025, 6, 1), [(date(2025, 11, 30), 20), (date(2025, 12, 30), 50)]),
    3: ("WRITTEN_OFF", date(2023, 6, 1), []),
    4: ("ACTIVE", date(2025, 12, 20), []),
}
# Ranh giới nhóm nợ: (dpd, nhóm kỳ vọng) — mỗi dòng một khoản OVERDUE riêng.
BOUNDARIES = [(9, 1), (10, 2), (89, 2), (90, 3), (179, 3), (180, 4), (359, 4), (360, 5)]
FIRST_BOUNDARY_ID = 100


@pytest.fixture(scope="module")
def rows(spark):
    loans = dict(LOANS)
    for i, (dpd, _) in enumerate(BOUNDARIES):
        loans[FIRST_BOUNDARY_ID + i] = ("OVERDUE", date(2024, 1, 1), [(date(2025, 12, 1), dpd)])

    spark.createDataFrame(
        [
            (lid, 7, "B1", "LOAN001", status, Decimal("1000"), Decimal("500"), disb)
            for lid, (status, disb, _) in loans.items()
        ],
        "loan_id bigint, customer_id bigint, branch_code string, product_code string, loan_status string, "
        "loan_amount decimal(18,2), outstanding_balance decimal(18,2), disbursement_date date",
    ).createOrReplaceTempView("dim_loan")

    payments, pid = [], 1
    for lid, (_, _, schedule) in loans.items():
        for due, late in schedule:
            payments.append((pid, lid, due, late, COB))
            pid += 1
    # Một partition cob_dt khác: không được lọt vào kết quả.
    payments.append((pid, 1, date(2025, 12, 15), 300, date(2025, 12, 30)))
    spark.createDataFrame(
        payments, "payment_id bigint, loan_id bigint, payment_date date, days_late int, cob_dt date"
    ).createOrReplaceTempView("fact_loan_payment")

    return {r.loan_id: r for r in spark.sql(_job_sql()).collect()}


@pytest.mark.integration
def test_one_row_per_loan(rows):
    assert len(rows) == len(LOANS) + len(BOUNDARIES)


@pytest.mark.integration
def test_result_types_match_the_ddl(spark, rows):
    """ROUND(DECIMAL(18,2), 2) ra DECIMAL(19,2) — bảng tạo từ kết quả lệch DDL
    (drift DAG đo được trên lakehouse thật 2026-09-29). `rows` để các view tồn tại."""
    from governance.ddl_schema import declared_schemas, normalize_type

    actual = {c: normalize_type(t) for c, t in spark.sql(_job_sql()).dtypes}
    assert actual == declared_schemas()["lakehouse.gold.loan_delinquency"]


@pytest.mark.integration
def test_dpd_is_latest_installment_due_by_cob_dt(rows):
    loan = rows[1]
    # Kỳ 2026-01-31 (99 ngày) là tương lai so với cob_dt; kỳ 2025-12-15 thuộc partition khác.
    assert loan.dpd == 0 and loan.dpd_bucket == "CURRENT"
    assert loan.last_due_date == date(2025, 11, 30)
    assert loan.prev_dpd == 45 and loan.prev_dpd_bucket == "30-59"
    assert loan.debt_group == 1 and loan.is_npl == 0


@pytest.mark.integration
def test_ever_flags_remember_the_peak_after_cure(rows):
    loan = rows[1]
    assert loan.max_dpd_ever == 45
    assert loan.ever_30_plus == 1 and loan.ever_90_plus == 0


@pytest.mark.integration
def test_roll_pair_for_a_loan_rolling_forward(rows):
    loan = rows[2]
    assert (loan.prev_dpd_bucket, loan.dpd_bucket) == ("1-29", "30-59")
    assert loan.debt_group == 2 and loan.is_npl == 0


@pytest.mark.parametrize("offset", range(len(BOUNDARIES)))
@pytest.mark.integration
def test_debt_group_boundaries(rows, offset):
    dpd, group = BOUNDARIES[offset]
    loan = rows[FIRST_BOUNDARY_ID + offset]
    assert loan.dpd == dpd
    assert loan.debt_group == group, f"dpd={dpd}"
    assert loan.is_npl == (1 if group >= 3 else 0), f"dpd={dpd}"


@pytest.mark.integration
def test_written_off_is_outside_debt_groups(rows):
    loan = rows[3]
    assert loan.dpd is None and loan.debt_group is None
    assert loan.dpd_bucket == "WRITTEN_OFF" and loan.is_npl == 0


@pytest.mark.integration
def test_loan_without_due_installments_is_current(rows):
    loan = rows[4]
    assert loan.dpd == 0 and loan.dpd_bucket == "CURRENT" and loan.debt_group == 1
    assert loan.prev_dpd is None and loan.prev_dpd_bucket is None
    assert loan.months_on_book == 0 and loan.vintage_month == "2025-12"


@pytest.mark.integration
def test_months_on_book_and_vintage(rows):
    loan = rows[2]
    assert loan.vintage_month == "2025-06"
    # Số tháng TRÒN: 2025-06-01 → 2025-12-01 là 6 tháng, còn 30 ngày chưa đủ tháng thứ 7.
    assert loan.months_on_book == 6
    assert all(r.cob_dt == COB for r in rows.values())
