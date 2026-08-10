"""
Ops DAG — PII Masking (daily).
Tạo/refresh các bảng masked sau khi gold layer hoàn tất.
sandbox.dim_customer_masked và sandbox.mart_customer_360_masked.
"""

from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum

from etl_flag import make_start_flag_task, make_end_flag_task

DAG_ID              = "ops_pii_masking_daily_dag"
APPLICATION_PATH    = "/opt/project/code_etl/shared/ops/pii_masking.py"
POSTGRES_ETL_CONN_ID = "postgres-etl"
COB_DT              = "{{ ds }}"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_CONF = {
    "spark.driver.memory":   "512m",
    "spark.executor.memory": "768m",
    "spark.executor.cores":  "1",
}

PII_ENV = {"PII_HASH_SALT": Variable.get("pii_hash_salt", default_var="banking-pii-salt-2025")}

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Daily PII masking — sandbox.dim_customer_masked + mart_customer_360_masked",
    schedule_interval="0 8 * * *",  # Daily at 8:00 AM (Production - after Gold)
    catchup=False,
    max_active_tasks=1,
    tags=["ops", "pii", "masking", "compliance", "production"],
)

# Wait for gold_mart360_dag
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

# Wait for silver_all_dag
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

start = make_start_flag_task("start", DAG_ID, "ops", dag, cob_dt=COB_DT)

mask_dim_customer = SparkSubmitOperator(
    task_id="mask_silver_dim_customer",
    application=APPLICATION_PATH,
    conn_id="spark_default",
    conf=SPARK_CONF,
    env_vars=PII_ENV,
    application_args=["--cob_dt", COB_DT, "--target", "dim_customer"],
    verbose=True,
    dag=dag,
)

mask_mart_360 = SparkSubmitOperator(
    task_id="mask_gold_mart_customer_360",
    application=APPLICATION_PATH,
    conn_id="spark_default",
    conf=SPARK_CONF,
    env_vars=PII_ENV,
    application_args=["--cob_dt", COB_DT, "--target", "mart_360"],
    verbose=True,
    dag=dag,
)

end = make_end_flag_task("end", DAG_ID, "ops", dag, cob_dt=COB_DT)

[wait_gold, wait_silver] >> start >> [mask_dim_customer, mask_mart_360] >> end
