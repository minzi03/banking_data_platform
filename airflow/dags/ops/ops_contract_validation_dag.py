"""
Ops DAG — Contract Validation (post-Silver/Gold).
Validates output DataFrames against dataset contracts before downstream use.
Results written to opslakehouse.contract_validation_log.

Uses BashOperator + docker exec to run spark-submit on spark-worker
(same pattern as Bronze/Silver DAGs) to avoid missing Iceberg jars
on Airflow container.
"""

from datetime import timedelta

import pendulum
from cob_dt import COB_DT
from etl_flag import make_end_flag_task, make_start_flag_task, upstream_success_sql
from pipeline_run_audit import PipelineRun, connect_from_env, insert_run

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

DAG_ID = "ops_contract_validation_dag"
APPLICATION_PATH = "/opt/project/code_etl/shared/ops/contract_validation.py"
POSTGRES_ETL_CONN_ID = "postgres-etl"


def audit_run(status: str, **context) -> None:
    """Persist contract-validation lifecycle without logging contract payloads."""
    task_instance = context["task_instance"]
    start = task_instance.start_date or pendulum.now("Asia/Ho_Chi_Minh")
    completed = pendulum.now("Asia/Ho_Chi_Minh")
    connection = connect_from_env()
    try:
        insert_run(
            connection,
            PipelineRun(
                dag_id=DAG_ID,
                task_id=task_instance.task_id,
                status=status,
                cob_dt=pendulum.parse(COB_DT).date(),
                execution_time_s=max((completed - start).total_seconds(), 0),
                error_message=str(context.get("exception")) if status == "FAILED" else None,
                started_at=start,
                completed_at=completed,
            ),
        )
    finally:
        connection.close()


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Contract validation — Silver, Gold and serving snapshots of cob_dt",
    schedule_interval="0 9 * * *",  # Daily at 9:00 AM (Production - after DQ)
    catchup=False,
    max_active_runs=1,  # ghi bảng: hai lượt cùng cob_dt tranh nhau (PII 2026-09-30)
    max_active_tasks=4,
    tags=["ops", "governance", "contract-validation", "production"],
)

# ---------------------------------------------------------------------------
# Sensors — wait for upstream DAGs to finish
# ---------------------------------------------------------------------------
wait_silver = SqlSensor(
    task_id="wait_silver_all_dag",
    conn_id=POSTGRES_ETL_CONN_ID,
    sql=upstream_success_sql("silver_all_dag", COB_DT),
    poke_interval=120,
    timeout=7200,
    mode="reschedule",
    dag=dag,
)

wait_gold = SqlSensor(
    task_id="wait_gold_all_dag",
    conn_id=POSTGRES_ETL_CONN_ID,
    sql=upstream_success_sql("gold_all_dag", COB_DT),
    poke_interval=120,
    timeout=7200,
    mode="reschedule",
    dag=dag,
)

wait_serving = SqlSensor(
    task_id="wait_serving_complete",
    conn_id=POSTGRES_ETL_CONN_ID,
    sql=upstream_success_sql("SERVING_COMPLETE", COB_DT),
    poke_interval=120,
    timeout=7200,
    mode="reschedule",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Flag and audit lifecycle tasks
# ---------------------------------------------------------------------------
start = make_start_flag_task("start", DAG_ID, "ops", dag, cob_dt=COB_DT)
end = make_end_flag_task("end", DAG_ID, "ops", dag, cob_dt=COB_DT)

audit_started = PythonOperator(
    task_id="audit_pipeline_running",
    python_callable=audit_run,
    op_kwargs={"status": "RUNNING"},
    dag=dag,
)
audit_completed = PythonOperator(
    task_id="audit_pipeline_success",
    python_callable=audit_run,
    op_kwargs={"status": "SUCCESS"},
    trigger_rule="all_success",
    dag=dag,
)
audit_failed = PythonOperator(
    task_id="audit_pipeline_failed",
    python_callable=audit_run,
    op_kwargs={"status": "FAILED"},
    trigger_rule="one_failed",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Contract Validation Tasks — BashOperator + docker exec (same as Bronze/Silver DAGs)
# Layers: --layer silver, --layer gold, --layer serving.
# ---------------------------------------------------------------------------


def _validation_task(task_id: str, layer: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=(
            "/usr/bin/docker exec banking-spark-worker-1 "
            "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client "
            "--conf spark.driver.memory=512m "
            "--conf spark.executor.memory=768m "
            "--conf spark.executor.cores=1 "
            f"{APPLICATION_PATH} --cob_dt {COB_DT} --layer {layer}"
        ),
        dag=dag,
    )


validate_silver_contracts = _validation_task("validate_silver_contracts", "silver")
validate_gold_contracts = _validation_task("validate_gold_contracts", "gold")
validate_serving_contracts = _validation_task("validate_serving_contracts", "serving")

# ---------------------------------------------------------------------------
# Task Flow
# ---------------------------------------------------------------------------
(
    [wait_silver, wait_gold]
    >> audit_started
    >> start
    >> [validate_silver_contracts, validate_gold_contracts]
    >> end
    >> audit_completed
)
[start, wait_serving] >> validate_serving_contracts >> end
[validate_silver_contracts, validate_gold_contracts, validate_serving_contracts] >> audit_failed
