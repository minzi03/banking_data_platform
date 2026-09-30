"""
Ops DAG — Iceberg Maintenance (weekly, Chủ nhật 02:00).
Chạy compact + expire_snapshots + remove_orphan_files cho tất cả bảng Iceberg.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
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

# spark-submit chạy TRONG spark-worker-1 (có Iceberg jar + spark-defaults.conf).
# SparkSubmitOperator chạy spark-submit trong container Airflow — image đó chỉ có
# pyspark, không có Iceberg runtime, nên chết với "Cannot find catalog plugin
# class for catalog 'lakehouse'" (đúng lỗi cdc_consolidation_dag từng gặp).
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
    description="Weekly Iceberg maintenance — compact, expire, orphan cleanup",
    schedule_interval="0 3 * * 0",   # Weekly on Sunday at 3:00 AM (Production)
    catchup=False,
    max_active_runs=1,  # ghi bảng: hai lượt cùng cob_dt tranh nhau (PII 2026-09-30)
    max_active_tasks=1,
    tags=["ops", "maintenance", "iceberg", "production"],
)

start = make_start_flag_task("start", DAG_ID, "ops", dag)

# Fact tables: compact + expire + orphan (nặng nhất)
maintain_fact = BashOperator(
    task_id="maintain_fact_tables",
    bash_command=f"{SPARK_SUBMIT} {APPLICATION_PATH} --target fact --mode full",
    execution_timeout=timedelta(hours=3),
    dag=dag,
)

# Mart/segment tables: compact + expire + orphan
maintain_mart = BashOperator(
    task_id="maintain_mart_tables",
    bash_command=f"{SPARK_SUBMIT} {APPLICATION_PATH} --target mart --mode full",
    execution_timeout=timedelta(hours=2),
    dag=dag,
)

# Dimension tables: expire only (ít thay đổi)
maintain_dim = BashOperator(
    task_id="maintain_dim_tables",
    bash_command=f"{SPARK_SUBMIT} {APPLICATION_PATH} --target dim --mode expire_only",
    execution_timeout=timedelta(hours=1),
    dag=dag,
)

end = make_end_flag_task("end", DAG_ID, "ops", dag)

start >> maintain_fact >> [maintain_mart, maintain_dim] >> end
