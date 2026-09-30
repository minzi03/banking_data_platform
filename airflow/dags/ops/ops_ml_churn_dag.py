"""
Ops DAG - ML Churn Model Training (chạy tay).

Ngoài phạm vi đề bài (đề: Customer 360 + phân khúc bằng luật). Chạy tay vì image
spark-worker không cài phụ thuộc ML (ml/requirements.txt: mlflow, xgboost,
scikit-learn, trino) — cài chúng vào worker trước khi trigger:
    docker exec banking-spark-worker-1 pip install -r /opt/project/ml/requirements.txt
    airflow dags trigger ops_ml_churn_dag --conf '{"cob_dt": "YYYY-MM-DD"}'
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum

from cob_dt import COB_DT

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
    description="ML churn model training (manual; needs ml/requirements.txt on the worker)",
    schedule_interval=None, catchup=False,
    tags=["ops","ml","churn","manual"])

wait_gold = SqlSensor(
    task_id="wait_gold", conn_id=PG,
    sql=f"SELECT 1 FROM opslakehouse.flag_job_etl WHERE job_name='SERVING_COMPLETE' AND status='S' AND cob_dt = DATE '{COB_DT}' LIMIT 1",
    poke_interval=120, timeout=7200, mode="reschedule", dag=dag)

train_churn = BashOperator(
    task_id="train_churn_model",
    bash_command=f"/usr/bin/docker exec banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.memory=1g {APP} --cob_dt {COB_DT}",
    dag=dag,
)

wait_gold >> train_churn
