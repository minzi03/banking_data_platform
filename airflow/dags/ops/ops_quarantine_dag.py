"""
Ops DAG — Quarantine Checks (post-Silver/Gold).
Checks business rule violations and quarantines violating records.
Results written to lakehouse.quarantine.*
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum

from etl_flag import make_start_flag_task, make_end_flag_task

DAG_ID              = "ops_quarantine_dag"
APPLICATION_PATH    = "/opt/project/code_etl/shared/ops/quarantine.py"
POSTGRES_ETL_CONN_ID = "postgres-etl"
COB_DT              = "{{ ds }}"

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
    description="Quarantine checks — Business rule violations",
    schedule_interval="0 9 * * *",  # Daily at 9:00 AM (Production - after DQ)
    catchup=False,
    max_active_tasks=4,
    tags=["ops", "quarantine", "governance", "production"],
)

# ---------------------------------------------------------------------------
# Sensors — wait for upstream DAGs to finish
# ---------------------------------------------------------------------------
wait_silver = SqlSensor(
    task_id="wait_silver_all_dag",
    conn_id=POSTGRES_ETL_CONN_ID,
    sql=(
        "SELECT 1 FROM opslakehouse.flag_job_etl "
        "WHERE job_name = 'silver_all_dag' "
        "  AND status = 'S' "
        f"  AND cob_dt = '{COB_DT}' "
        "LIMIT 1"
    ),
    poke_interval=120,
    timeout=7200,
    mode="reschedule",
    dag=dag,
)

wait_gold = SqlSensor(
    task_id="wait_gold_all_dag",
    conn_id=POSTGRES_ETL_CONN_ID,
    sql=(
        "SELECT 1 FROM opslakehouse.flag_job_etl "
        "WHERE job_name = 'gold_all_dag' "
        "  AND status = 'S' "
        f"  AND cob_dt = '{COB_DT}' "
        "LIMIT 1"
    ),
    poke_interval=120,
    timeout=7200,
    mode="reschedule",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Flag tasks
# ---------------------------------------------------------------------------
start = make_start_flag_task("start", DAG_ID, "ops", dag, cob_dt=COB_DT)
end   = make_end_flag_task("end", DAG_ID, "ops", dag, cob_dt=COB_DT)

# ---------------------------------------------------------------------------
# Quarantine Check Tasks — BashOperator + docker exec
# ---------------------------------------------------------------------------
quarantine_silver = BashOperator(
    task_id="quarantine_silver_checks",
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

quarantine_gold = BashOperator(
    task_id="quarantine_gold_checks",
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

# ---------------------------------------------------------------------------
# Task Flow
# ---------------------------------------------------------------------------
[wait_silver, wait_gold] >> start >> [quarantine_silver, quarantine_gold] >> end
