"""
Ops DAG — Serving Sync (Iceberg serving → PostgreSQL real-time serving).

Context: ADR-0019. Tầng serving ghi vào Iceberg (`lakehouse.serving.*`) và đọc
qua Trino — đúng cho analytics lịch sử. Nghiệp vụ Merchant QR/POS cần thêm một
backend PostgreSQL cho point-lookup độ trễ thấp (app/dashboard real-time).

DAG này đồng bộ các bảng serving từ Iceberg sang PostgreSQL bằng upsert idempotent
(staging → ON CONFLICT DO UPDATE). Chạy sau khi tầng serving đã được build.
"""

from datetime import timedelta

import pendulum
from cob_dt import COB_DT
from etl_flag import make_end_flag_task, make_start_flag_task, upstream_success_sql

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

DAG_ID = "ops_serving_sync_dag"
APPLICATION_PATH = "/opt/project/code_etl/serving/sync_postgres.py"
POSTGRES_ETL_CONN_ID = "postgres-etl"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# spark-submit chạy TRONG spark-worker-1: image Airflow chỉ có pyspark wheel,
# không có Iceberg runtime jar → submit từ scheduler chết với "Cannot find
# catalog plugin class for catalog 'lakehouse'".
SPARK_SUBMIT = (
    "/usr/bin/docker exec banking-spark-worker-1 "
    "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client "
    "--conf spark.driver.memory=512m "
    "--conf spark.executor.memory=768m "
    "--conf spark.executor.cores=1"
)

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Sync Iceberg serving to PostgreSQL for low-latency app access",
    schedule_interval="0 23 * * *",  # Daily at 23:00 (after serving build)
    catchup=False,
    max_active_runs=1,  # ghi bảng: hai lượt cùng cob_dt tranh nhau
    max_active_tasks=1,
    tags=["ops", "serving", "adr-0019", "production"],
)

# ---------------------------------------------------------------------------
# Sensor — wait for the serving layer to be built (dbt serving run)
# ---------------------------------------------------------------------------
wait_serving = SqlSensor(
    task_id="wait_dbt_serving_publish",
    conn_id=POSTGRES_ETL_CONN_ID,
    sql=upstream_success_sql("dbt_serving_publish", COB_DT),
    poke_interval=120,
    timeout=7200,
    mode="reschedule",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Flag tasks
# ---------------------------------------------------------------------------
start = make_start_flag_task("start", DAG_ID, "ops", dag, cob_dt=COB_DT)
end = make_end_flag_task("end", DAG_ID, "ops", dag, cob_dt=COB_DT)

# ---------------------------------------------------------------------------
# Sync task
# ---------------------------------------------------------------------------
sync_serving = BashOperator(
    task_id="sync_serving_to_postgres",
    bash_command=f"{SPARK_SUBMIT} {APPLICATION_PATH} --cob_dt {COB_DT}",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task Flow
# ---------------------------------------------------------------------------
wait_serving >> start >> sync_serving >> end
