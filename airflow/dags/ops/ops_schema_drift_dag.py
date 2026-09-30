"""Ops DAG - Schema Drift Detection (post-Silver/Gold).

So schema thật của MỌI bảng Silver + Gold khai trong docker/init_iceberg/*.sql
với DDL, phân loại ADDITIVE / BREAKING (governance/schema_drift.py). Task fail
(exit 1) khi có thay đổi BREAKING: mất cột, đổi kiểu ngoài luật promotion của
Iceberg, hoặc bảng khai trong DDL mà không tồn tại.

Bản trước gọi schema_drift.py với --table/--columns trong khi file không có
entrypoint: job Spark thoát 0 và cả ba task luôn xanh mà không kiểm gì.
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum

from cob_dt import COB_DT
from etl_flag import upstream_success_sql

DAG_ID = "ops_schema_drift_dag"
APP = "/opt/project/governance/schema_drift.py"
PG = "postgres-etl"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

dag = DAG(DAG_ID, default_args=DEFAULT_ARGS,
    description="Schema drift detection - post-Silver/Gold",
    schedule_interval="0 9 * * *", catchup=False, max_active_runs=1,
    tags=["ops","schema-drift","production"])

wait_dq = SqlSensor(
    task_id="wait_dq",
    conn_id=PG,
    sql=upstream_success_sql("ops_data_quality_dag", COB_DT),
    poke_interval=60, timeout=3600, mode="reschedule", dag=dag)

check_schema_drift = BashOperator(
    task_id="check_schema_drift",
    bash_command=(
        "/usr/bin/docker exec -w /opt/project banking-spark-worker-1 /opt/spark/bin/spark-submit "
        "--master spark://spark-master:7077 --deploy-mode client --conf spark.driver.memory=512m "
        f"{APP} --layer silver --layer gold"
    ),
    dag=dag,
)

wait_dq >> check_schema_drift
