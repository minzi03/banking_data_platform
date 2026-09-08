# =============================================================================
# Airflow DAG: Regulatory Reporting
# =============================================================================
# Generates regulatory reports for banking compliance:
#   - BCBS 239 (Basel Committee on Banking Supervision)
#   - SBV/NHNN (State Bank of Vietnam)
#   - Internal DQ scorecards
#
# Flow:
#     wait_for_serving_complete(cob_dt)
#         ↓
#     generate_bcb239_report
#         ↓
#     generate_sbv_report
#         ↓
#     generate_dq_scorecard
#         ↓
#     log_results
# =============================================================================

import json
from datetime import timedelta
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl_flag import make_start_flag_task

# ─── Constants ────────────────────────────────────────────────────────────────
DAG_ID = "regulatory_reporting"
DATA_COB_DT = "{{ ds }}"
POSTGRES_CONN_ID = "postgres-etl"
SERVING_COMPLETE_FLAG = "SERVING_COMPLETE"
DQ_PASS_FLAG = "DQ_TEST_PASS"

DEFAULT_ARGS = {
    "owner": "compliance",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["compliance@banking.local"],
    "execution_timeout": timedelta(hours=2),
}


def _serving_complete_sql() -> str:
    return (
        "SELECT 1 FROM opslakehouse.flag_job_etl "
        f"WHERE job_name = '{SERVING_COMPLETE_FLAG}' "
        "  AND status = 'S' "
        f"  AND cob_dt = DATE '{DATA_COB_DT}' "
        "LIMIT 1"
    )


def generate_bcb239_report(**context):
    """Generate BCBS 239 risk data aggregation report."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    cob_dt = context["ds"]

    # BCBS 239 requires comprehensive risk data aggregation
    report_data = {
        "report_type": "BCBS239_RISK_DATA",
        "period": cob_dt,
        "generated_at": datetime.now().isoformat(),
        "metrics": {},
    }

    # 1. Credit Risk Data
    credit_risk_sql = f"""
    SELECT
        COUNT(DISTINCT l.customer_id) AS total_borrowers,
        SUM(l.loan_amount) AS total_exposure,
        SUM(CASE WHEN l.loan_status = 'OVERDUE' THEN l.outstanding_balance ELSE 0 END) AS overdue_amount,
        SUM(CASE WHEN l.loan_status = 'WRITTEN_OFF' THEN l.outstanding_balance ELSE 0 END) AS written_off_amount,
        COUNT(CASE WHEN l.loan_status = 'OVERDUE' THEN 1 END) AS overdue_count,
        COUNT(*) AS total_loans
    FROM iceberg.gold.fact_loan l
    WHERE l.cob_dt = DATE '{cob_dt}'
    """
    result = hook.get_first(credit_risk_sql)
    if result:
        report_data["metrics"]["credit_risk"] = {
            "total_borrowers": result[0],
            "total_exposure": float(result[1] or 0),
            "overdue_amount": float(result[2] or 0),
            "written_off_amount": float(result[3] or 0),
            "overdue_count": result[4],
            "total_loans": result[5],
            "npl_ratio": round((result[4] / result[5] * 100) if result[5] > 0 else 0, 2),
        }

    # 2. Deposit Data
    deposit_sql = f"""
    SELECT
        COUNT(DISTINCT a.customer_id) AS total_depositors,
        SUM(a.balance) AS total_deposits,
        COUNT(DISTINCT a.account_id) AS total_accounts
    FROM iceberg.gold.dim_account a
    WHERE a.cob_dt = DATE '{cob_dt}'
      AND a.account_type = 'TIME_DEPOSIT'
    """
    result = hook.get_first(deposit_sql)
    if result:
        report_data["metrics"]["deposits"] = {
            "total_depositors": result[0],
            "total_deposits": float(result[1] or 0),
            "total_accounts": result[2],
        }

    # 3. Transaction Volume
    txn_sql = f"""
    SELECT
        COUNT(*) AS total_transactions,
        SUM(txn_amount) AS total_volume,
        AVG(txn_amount) AS avg_transaction_size
    FROM iceberg.gold.fact_account_txn
    WHERE cob_dt = DATE '{cob_dt}'
    """
    result = hook.get_first(txn_sql)
    if result:
        report_data["metrics"]["transactions"] = {
            "total_transactions": result[0],
            "total_volume": float(result[1] or 0),
            "avg_transaction_size": float(result[2] or 0),
        }

    # Store report
    insert_sql = """
    INSERT INTO opslakehouse.regulatory_report
        (report_id, report_type, report_name, report_period_start, report_period_end,
         data_json, status, created_by, created_at)
    VALUES (
        (SELECT COALESCE(MAX(report_id), 0) + 1 FROM opslakehouse.regulatory_report),
        'BCBS239_RISK_DATA',
        'BCBS 239 Risk Data Aggregation - ' || %s,
        DATE %s,
        DATE %s,
        %s::jsonb,
        'DRAFT',
        'AIRFLOW_DAG',
        NOW()
    )
    """
    hook.run(insert_sql, parameters=(cob_dt, cob_dt, cob_dt, json.dumps(report_data)))
    return report_data


def generate_sbv_report(**context):
    """Generate SBV (State Bank of Vietnam) card transaction report."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    cob_dt = context["ds"]

    report_data = {
        "report_type": "SBV_CARD_TRANSACTION",
        "period": cob_dt,
        "generated_at": datetime.now().isoformat(),
        "metrics": {},
    }

    # Card transaction summary
    card_txn_sql = f"""
    SELECT
        COUNT(*) AS total_card_transactions,
        SUM(txn_amount) AS total_card_volume,
        COUNT(CASE WHEN is_fraud = 1 THEN 1 END) AS fraud_count,
        COUNT(CASE WHEN txn_type = 'PURCHASE' THEN 1 END) AS purchase_count,
        COUNT(CASE WHEN txn_type = 'CASH_ADVANCE' THEN 1 END) AS cash_advance_count
    FROM iceberg.gold.fact_card_txn
    WHERE cob_dt = DATE '{cob_dt}'
    """
    result = hook.get_first(card_txn_sql)
    if result:
        report_data["metrics"]["card_transactions"] = {
            "total_transactions": result[0],
            "total_volume": float(result[1] or 0),
            "fraud_count": result[2],
            "purchase_count": result[3],
            "cash_advance_count": result[4],
        }

    # Store report
    insert_sql = """
    INSERT INTO opslakehouse.regulatory_report
        (report_id, report_type, report_name, report_period_start, report_period_end,
         data_json, status, created_by, created_at)
    VALUES (
        (SELECT COALESCE(MAX(report_id), 0) + 1 FROM opslakehouse.regulatory_report),
        'SBV_CARD_TRANSACTION',
        'SBV Card Transaction Report - ' || %s,
        DATE %s,
        DATE %s,
        %s::jsonb,
        'DRAFT',
        'AIRFLOW_DAG',
        NOW()
    )
    """
    hook.run(insert_sql, parameters=(cob_dt, cob_dt, cob_dt, json.dumps(report_data)))
    return report_data


def generate_dq_scorecard(**context):
    """Generate Data Quality scorecard for all serving tables."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    cob_dt = context["ds"]

    serving_tables = [
        "mart_customer_360_current",
        "rfm_segment_current",
        "churn_prediction_current",
        "cross_sell_segment_current",
        "campaign_target_current",
        "customer_transaction_summary_current",
        "customer_balance_summary_current",
        "customer_card_summary_current",
        "customer_product_summary_current",
    ]

    scorecard_id = 1
    for table in serving_tables:
        # Completeness check
        completeness_sql = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(customer_id) AS non_null_customer_id,
            ROUND(COUNT(customer_id)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS completeness_score
        FROM iceberg.serving.{table}
        WHERE cob_dt = DATE '{cob_dt}'
        """
        result = hook.get_first(completeness_sql)
        if result and result[0] > 0:
            completeness = float(result[2] or 0)

            insert_sql = """
            INSERT INTO opslakehouse.dq_scorecard
                (scorecard_id, report_date, table_name, dimension,
                 score, threshold, total_records, valid_records, invalid_records,
                 created_at)
            VALUES (
                %s, DATE %s, %s, 'COMPLETENESS',
                %s, 95.00, %s, %s, %s - %s,
                NOW()
            )
            """
            hook.run(insert_sql, parameters=(
                scorecard_id, cob_dt, table,
                completeness, result[0], result[1], result[0], result[1],
            ))
            scorecard_id += 1

    return {"tables_scored": len(serving_tables), "scorecard_id": scorecard_id}


with DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Regulatory reporting for BCBS 239 and SBV compliance",
    schedule_interval="0 9 * * *",  # 09:00 daily (after DQ tests at 08:00)
    catchup=False,
    max_active_runs=1,
    tags=["compliance", "regulatory", "bcbs239", "sbv", "production"],
) as dag:

    start = make_start_flag_task("dag_start", DAG_ID, "compliance", dag, cob_dt=DATA_COB_DT)

    # ── 1. Wait for serving + DQ ────────────────────────────────────────
    wait_for_serving_complete = SqlSensor(
        task_id="wait_for_serving_complete",
        conn_id=POSTGRES_CONN_ID,
        sql=_serving_complete_sql(),
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
        dag=dag,
    )

    # ── 2. Generate Reports ─────────────────────────────────────────────
    gen_bcb239 = PythonOperator(
        task_id="generate_bcb239_report",
        python_callable=generate_bcb239_report,
        dag=dag,
    )

    gen_sbv = PythonOperator(
        task_id="generate_sbv_report",
        python_callable=generate_sbv_report,
        dag=dag,
    )

    gen_dq = PythonOperator(
        task_id="generate_dq_scorecard",
        python_callable=generate_dq_scorecard,
        dag=dag,
    )

    end = EmptyOperator(task_id="end", dag=dag)

    (
        start
        >> wait_for_serving_complete
        >> [gen_bcb239, gen_sbv, gen_dq]
        >> end
    )
