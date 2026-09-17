"""Ops DAG - ML Churn Model Training (weekly)."""
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum

DAG_ID = "ops_ml_churn_dag"
APP = "/opt/project/ml/pipeline/churn_prediction.py"
PG = "postgres-etl"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

dag = DAG(DAG_ID, default_args=DEFAULT_ARGS,
    description="ML churn model training (weekly)",
    schedule_interval="0 10 * * 0", catchup=False,
    tags=["ops","ml","churn","production"])

wait_gold = SqlSensor(
    task_id="wait_gold", conn_id=PG,
    sql="SELECT 1 FROM opslakehouse.flag_job_etl WHERE job_name='gold_all_dag' AND status='S' LIMIT 1",
    poke_interval=120, timeout=7200, mode="reschedule", dag=dag)

train_churn = BashOperator(
    task_id="train_churn_model",
    bash_command=f"/usr/bin/docker exec banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.memory=1g {APP} --cob_dt {{{{ ds }}}}",
    dag=dag,
)

wait_gold >> train_churn
