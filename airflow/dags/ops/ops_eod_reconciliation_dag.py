"""
Ops DAG — End-of-Day Reconciliation (streaming vs batch).

Tiêu chí nghiệm thu nghiệp vụ ngân hàng: số liệu đường streaming (Debezium CDC
→ Kafka → Bronze CDC → Silver Current) phải KHỚP số liệu đường batch (Spark JDBC
→ Bronze → Silver fact) cho cùng một cob_dt, và cả hai phải khớp nguồn sự thật
là Postgres core.

Chạy cuối ngày, SAU khi Silver và Gold đã xong. Exit non-zero nếu lệch quá
ngưỡng → DAG dừng → phát hiện silent corruption trước khi báo cáo được publish.
"""

from datetime import timedelta

import pendulum
from cob_dt import COB_DT
from etl_flag import make_end_flag_task, make_start_flag_task, upstream_success_sql

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

DAG_ID = "ops_eod_reconciliation_dag"
APPLICATION_PATH = "/opt/project/code_etl/shared/ops/eod_reconciliation.py"
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
# catalog plugin class for catalog 'lakehouse'". Cùng mô hình mọi Sprint DAG khác.
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
    description="End-of-day reconciliation — streaming vs batch must agree",
    schedule_interval="0 22 * * *",  # Daily at 22:00 (Production — end of business day)
    catchup=False,
    max_active_runs=1,  # ghi bảng: hai lượt cùng cob_dt tranh nhau
    max_active_tasks=1,
    tags=["ops", "reconciliation", "governance", "production"],
)

# ---------------------------------------------------------------------------
# Sensors — wait for both representations to be complete
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

# ---------------------------------------------------------------------------
# Flag tasks
# ---------------------------------------------------------------------------
start = make_start_flag_task("start", DAG_ID, "ops", dag, cob_dt=COB_DT)
end = make_end_flag_task("end", DAG_ID, "ops", dag, cob_dt=COB_DT)

# ---------------------------------------------------------------------------
# Reconciliation task
# ---------------------------------------------------------------------------
reconcile = BashOperator(
    task_id="reconcile_streaming_vs_batch",
    bash_command=f"{SPARK_SUBMIT} {APPLICATION_PATH} --cob_dt {COB_DT}",
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task Flow
# ---------------------------------------------------------------------------
wait_silver >> start >> reconcile >> end
