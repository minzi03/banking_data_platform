"""
Ops DAG — Data Quality Validation (post-Silver/Gold).
Chạy DQ checks trên Silver, Gold, và Bronze CDC tables.
Kết quả ghi vào opslakehouse.data_quality_log.

Uses BashOperator + docker exec to run spark-submit on spark-worker
(same pattern as Bronze/Silver DAGs) to avoid missing Iceberg jars
on Airflow container.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum

from etl_flag import make_start_flag_task, make_end_flag_task, upstream_success_sql
from cob_dt import COB_DT

DAG_ID              = "ops_data_quality_dag"
APPLICATION_PATH    = "/opt/project/code_etl/shared/ops/data_quality.py"
POSTGRES_ETL_CONN_ID = "postgres-etl"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

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
# Flag tasks
# ---------------------------------------------------------------------------
start = make_start_flag_task("start", DAG_ID, "ops", dag, cob_dt=COB_DT)
end   = make_end_flag_task("end", DAG_ID, "ops", dag, cob_dt=COB_DT)

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
[wait_silver, wait_gold] >> start >> [dq_silver, dq_gold, dq_bronze_cdc] >> end
