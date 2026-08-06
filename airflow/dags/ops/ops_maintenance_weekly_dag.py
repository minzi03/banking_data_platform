"""
Ops DAG — Iceberg Maintenance (weekly, Chủ nhật 02:00).
Chạy compact + expire_snapshots + remove_orphan_files cho tất cả bảng Iceberg.
"""

from datetime import timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

from etl_flag import make_start_flag_task, make_end_flag_task

DAG_ID            = "ops_maintenance_weekly_dag"
APPLICATION_PATH  = "/opt/project/code_etl/shared/ops/iceberg_maintenance.py"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

SPARK_CONF = {
    "spark.driver.memory":   "512m",
    "spark.executor.memory": "768m",
    "spark.executor.cores":  "1",
}

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Weekly Iceberg maintenance — compact, expire, orphan cleanup",
    schedule_interval="0 3 * * 0",   # Weekly on Sunday at 3:00 AM (Production)
    catchup=False,
    max_active_tasks=1,
    tags=["ops", "maintenance", "iceberg", "production"],
)

start = make_start_flag_task("start", DAG_ID, "ops", dag)

# Fact tables: compact + expire + orphan (nặng nhất)
maintain_fact = SparkSubmitOperator(
    task_id="maintain_fact_tables",
    application=APPLICATION_PATH,
    conn_id="spark_default",
    conf=SPARK_CONF,
    application_args=["--target", "fact", "--mode", "full"],
    execution_timeout=timedelta(hours=3),
    verbose=True,
    dag=dag,
)

# Mart/segment tables: compact + expire + orphan
maintain_mart = SparkSubmitOperator(
    task_id="maintain_mart_tables",
    application=APPLICATION_PATH,
    conn_id="spark_default",
    conf=SPARK_CONF,
    application_args=["--target", "mart", "--mode", "full"],
    execution_timeout=timedelta(hours=2),
    verbose=True,
    dag=dag,
)

# Dimension tables: expire only (ít thay đổi)
maintain_dim = SparkSubmitOperator(
    task_id="maintain_dim_tables",
    application=APPLICATION_PATH,
    conn_id="spark_default",
    conf=SPARK_CONF,
    application_args=["--target", "dim", "--mode", "expire_only"],
    execution_timeout=timedelta(hours=1),
    verbose=True,
    dag=dag,
)

end = make_end_flag_task("end", DAG_ID, "ops", dag)

start >> maintain_fact >> [maintain_mart, maintain_dim] >> end
