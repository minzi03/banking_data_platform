"""
Bronze ingestion DAG — digital_banking domain (5 tables).
Tasks discovered dynamically from code_etl/bronze/digital_banking/*.yml.
Uses BashOperator + docker exec to run spark-submit on spark-worker.
"""

import yaml
from pathlib import Path
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import pendulum

from jdbc_conn_utils import resolve_jdbc_conn
from etl_flag import make_start_flag_task, make_end_flag_task

DAG_ID            = "bronze_digital_banking_dag"
ETL_PATH          = Variable.get("ETL_PATH", default_var="/opt/project/code_etl")
SPARK_APPLICATION = f"{ETL_PATH}/bronze/base_job/ingestion_jdbc.py"
CONFIG_DIR        = Path(ETL_PATH) / "bronze" / "digital_banking"
CONN_ID           = "postgres-digital-banking"
COB_DT            = "{{ ds }}"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Bronze ingestion — digital_banking (PostgreSQL)",
    schedule_interval="0 2 * * *",  # Daily at 2:00 AM (Production)
    catchup=False,
    max_active_tasks=1,
    tags=["bronze", "digital_banking", "postgresql", "production"],
)

conn_tmpl = resolve_jdbc_conn(CONN_ID)

dag_start = make_start_flag_task("dag_start", DAG_ID, "bronze", dag, cob_dt=COB_DT)

with TaskGroup("ingest_all", dag=dag) as ingest_all:
    for config_file in sorted(CONFIG_DIR.glob("*.yml")):
        config     = yaml.safe_load(config_file.read_text())
        table_name = config["target"]["table"]
        remote_cfg = f"{CONFIG_DIR}/{config_file.name}"

        cmd = (
            f"/usr/bin/docker exec banking-spark-worker-1 "
            f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client "
            f"--conf spark.driver.memory=512m "
            f"--conf spark.executor.memory=768m "
            f"--conf spark.executor.cores=1 "
            f"{SPARK_APPLICATION} "
            f"--config {remote_cfg} "
            f"--cob_dt {COB_DT} "
            f"--jdbc_url '{conn_tmpl['jdbc_url']}' "
            f"--db_user {conn_tmpl['db_user']} "
            f"--db_password '{conn_tmpl['db_password']}'"
        )

        BashOperator(
            task_id=f"ingest_{table_name}",
            bash_command=cmd,
            dag=dag,
        )

dag_end = make_end_flag_task("dag_end", DAG_ID, "bronze", dag, cob_dt=COB_DT)

dag_start >> ingest_all >> dag_end
