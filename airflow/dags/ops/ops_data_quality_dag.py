"""
Ops DAG — Data Quality Validation (post-Silver/Gold).
Chạy DQ checks trên Silver, Gold, và Bronze CDC tables.
Kết quả ghi vào opslakehouse.data_quality_log.

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

DAG_ID = "ops_data_quality_dag"
APPLICATION_PATH = "/opt/project/code_etl/shared/ops/data_quality.py"
POSTGRES_ETL_CONN_ID = "postgres-etl"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def audit_run(status: str, **context) -> None:
    """Persist DQ DAG lifecycle without storing DQ row payloads or credentials."""
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


dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Data Quality validation — Silver + Gold + Bronze CDC",
    schedule_interval="0 8 * * *",  # Daily at 8:00 AM (Production - after Gold)
    catchup=False,
    max_active_runs=1,  # ghi bảng: hai lượt cùng cob_dt tranh nhau (PII 2026-09-30)
    max_active_tasks=4,
    tags=["ops", "data-quality", "validation", "production"],
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

# ---------------------------------------------------------------------------
# Flag tasks and pipeline audit lifecycle
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
# DQ Check Tasks — BashOperator + docker exec (same as Bronze/Silver DAGs)
# ---------------------------------------------------------------------------
dq_silver = BashOperator(
    task_id="dq_silver_checks",
    bash_command=(
        "/usr/bin/docker exec banking-spark-worker-1 "
        "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client "
        "--conf spark.driver.memory=512m "
        "--conf spark.executor.memory=768m "
        "--conf spark.executor.cores=1 "
        f"{APPLICATION_PATH} --cob_dt {COB_DT} --layer silver"
    ),
    dag=dag,
)

dq_gold = BashOperator(
    task_id="dq_gold_checks",
    bash_command=(
        "/usr/bin/docker exec banking-spark-worker-1 "
        "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client "
        "--conf spark.driver.memory=512m "
        "--conf spark.executor.memory=768m "
        "--conf spark.executor.cores=1 "
        f"{APPLICATION_PATH} --cob_dt {COB_DT} --layer gold"
    ),
    dag=dag,
)

dq_bronze_cdc = BashOperator(
    task_id="dq_bronze_cdc_checks",
    bash_command=(
        "/usr/bin/docker exec banking-spark-worker-1 "
        "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client "
        "--conf spark.driver.memory=512m "
        "--conf spark.executor.memory=768m "
        "--conf spark.executor.cores=1 "
        f"{APPLICATION_PATH} --cob_dt {COB_DT} --layer bronze"
    ),
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task Flow
# ---------------------------------------------------------------------------
[wait_silver, wait_gold] >> audit_started >> start >> [dq_silver, dq_gold, dq_bronze_cdc] >> end >> audit_completed
[dq_silver, dq_gold, dq_bronze_cdc] >> audit_failed
