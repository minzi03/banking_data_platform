# =============================================================================
# Airflow DAG: dbt Data Quality Tests
# =============================================================================
# Runs comprehensive data quality tests on the serving layer.
# This DAG runs AFTER dbt_serving_publish to validate the published data.
#
# Flow:
#     wait_for_serving_complete(cob_dt)
#         ↓
#     dbt test --select serving (generic + singular tests)
#         ↓
#     log results to data_quality_log
# =============================================================================

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

from etl_flag import make_start_flag_task

# ─── Constants ────────────────────────────────────────────────────────────────
DAG_ID = "dbt_data_quality"
DATA_COB_DT = "{{ ds }}"
POSTGRES_CONN_ID = "postgres-etl"
DBT_EXEC = "/usr/bin/docker exec banking-dbt sh -lc"
DBT_DIR = "/usr/src/dbt"

SERVING_COMPLETE_FLAG = "SERVING_COMPLETE"
DQ_PASS_FLAG = "DQ_TEST_PASS"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=1),
}


def _serving_complete_sql() -> str:
    return (
        "SELECT 1 FROM opslakehouse.flag_job_etl "
        f"WHERE job_name = '{SERVING_COMPLETE_FLAG}' "
        "  AND status = 'S' "
        f"  AND cob_dt = DATE '{DATA_COB_DT}' "
        "LIMIT 1"
    )


with DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Data quality tests on serving layer (dbt test)",
    schedule_interval="0 8 * * *",  # sau serving publish 07:00
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "data-quality", "tests", "production"],
) as dag:

    start = make_start_flag_task("dag_start", DAG_ID, "data_quality", dag, cob_dt=DATA_COB_DT)

    # ── 1. Wait for serving publish ───────────────────────────────────────
    wait_for_serving_complete = SqlSensor(
        task_id="wait_for_serving_complete",
        conn_id=POSTGRES_CONN_ID,
        sql=_serving_complete_sql(),
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
        dag=dag,
    )

    # ── 2. Run generic tests (unique, not_null, accepted_values) ─────────
    dbt_test_generic = BashOperator(
        task_id="dbt_test_generic",
        bash_command=(
            f"{DBT_EXEC} \"cd {DBT_DIR} && dbt test --target docker "
            f"--select serving+ "
            f"--vars '{{\\\"cob_dt\\\": \\\"{DATA_COB_DT}\\\"}}'\""
        ),
        dag=dag,
    )

    # ── 3. Run singular tests (business rules) ───────────────────────────
    dbt_test_singular = BashOperator(
        task_id="dbt_test_singular",
        bash_command=(
            f"{DBT_EXEC} \"cd {DBT_DIR} && dbt test --target docker "
            f"--select assert_serving_snapshot_alignment "
            f"--select assert_customer_360_completeness "
            f"--select assert_rfm_scores_valid "
            f"--select assert_no_duplicate_customers "
            f"--select assert_balances_non_negative "
            f"--vars '{{\\\"cob_dt\\\": \\\"{DATA_COB_DT}\\\"}}'\""
        ),
        dag=dag,
    )

    # ── 4. Log DQ results ────────────────────────────────────────────────
    log_dq_results = BashOperator(
        task_id="log_dq_results",
        bash_command=(
            f"{DBT_EXEC} \"cd {DBT_DIR} && echo 'DQ tests passed for cob_dt={DATA_COB_DT}'\""
        ),
        dag=dag,
    )

    end = EmptyOperator(task_id="end", dag=dag)

    (
        start
        >> wait_for_serving_complete
        >> dbt_test_generic
        >> dbt_test_singular
        >> log_dq_results
        >> end
    )
