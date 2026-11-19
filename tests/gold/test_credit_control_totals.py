"""Static control-total contract for credit marts.

The delinquency mart is the source of record-level DPD; portfolio risk is the
branch/product aggregate. Their totals must reconcile for the same snapshot, or
a consumer sees two different loan books.
"""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
RISK = ROOT / "code_etl" / "gold" / "risk" / "loan_portfolio_risk.yml"
DELINQ = ROOT / "code_etl" / "gold" / "risk" / "loan_delinquency.yml"


def _sql(path: Path) -> str:
    return yaml.safe_load(path.read_text(encoding="utf-8"))["sql"]


def test_both_credit_marts_read_the_same_silver_snapshot():
    for path in (RISK, DELINQ):
        text = path.read_text(encoding="utf-8")
        assert "silver.dim_loan" in text
        assert "silver.fact_loan_payment" in text


def test_portfolio_risk_counts_and_sums_come_from_the_same_loan_grain():
    sql = _sql(RISK)
    assert "COUNT(DISTINCT loan_id)" in sql
    assert "COUNT(DISTINCT CASE WHEN loan_status='ACTIVE' THEN loan_id END)" in sql
    assert "SUM(loan_amount)" in sql
    assert "SUM(outstanding_balance)" in sql
    assert "GROUP BY branch_code, product_code" in sql


def test_delinquency_mart_keeps_one_row_per_loan_before_grouping():
    sql = _sql(DELINQ)
    assert "ROW_NUMBER() OVER (PARTITION BY loan_id" in sql
    assert "FROM l LEFT JOIN agg a ON l.loan_id = a.loan_id" in sql


def test_written_off_loans_are_excluded_from_debt_group_and_npl():
    sql = _sql(DELINQ)
    assert "loan_status = 'WRITTEN_OFF' THEN NULL" in sql
    assert "WHEN loan_status = 'WRITTEN_OFF' THEN 'WRITTEN_OFF'" in sql
    assert "CASE WHEN dpd >= 90 THEN 1 ELSE 0 END AS is_npl" in sql
