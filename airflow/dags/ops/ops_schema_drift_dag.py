"""Ops DAG - Schema Drift Detection (post-Silver/Gold).
Detects schema changes by comparing current table schema against expected.
Runs after DQ validation as a safety check.
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum

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
    schedule_interval="0 9 * * *", catchup=False,
    tags=["ops","schema-drift","production"])

wait_dq = SqlSensor(
    task_id="wait_dq",
    conn_id=PG,
    sql='''SELECT 1 FROM opslakehouse.flag_job_etl WHERE job_name = 'ops_data_quality_dag' AND status = 'S' AND cob_dt = DATE '{{ ds }}' LIMIT 1''',
    poke_interval=60, timeout=3600, mode="reschedule", dag=dag)

CMDS = {
    "dim_customer": ["lakehouse.silver.dim_customer","customer_id","full_name","branch_code","customer_segment"],
    "dim_account": ["lakehouse.silver.dim_account","account_id","customer_id","product_code","balance"],
    "dim_loan": ["lakehouse.silver.dim_loan","loan_id","customer_id","loan_amount","outstanding_balance","loan_status"],
}

check_dim_customer = BashOperator(
    task_id="check_dim_customer",
    bash_command=f"/usr/bin/docker exec banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.memory=512m {APP} --table lakehouse.silver.dim_customer --columns customer_id,full_name,branch_code,customer_segment",
    dag=dag,
)
check_dim_account = BashOperator(
    task_id="check_dim_account",
    bash_command=f"/usr/bin/docker exec banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.memory=512m {APP} --table lakehouse.silver.dim_account --columns account_id,customer_id,product_code,balance",
    dag=dag,
)
check_dim_loan = BashOperator(
    task_id="check_dim_loan",
    bash_command=f"/usr/bin/docker exec banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.memory=512m {APP} --table lakehouse.silver.dim_loan --columns loan_id,customer_id,loan_amount,outstanding_balance,loan_status",
    dag=dag,
)

wait_dq >> check_dim_customer >> check_dim_account >> check_dim_loan
