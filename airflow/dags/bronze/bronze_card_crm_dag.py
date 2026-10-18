"""
Bronze ingestion DAG — card_crm domain (3 tables).
Tasks discovered dynamically from code_etl/bronze/card_crm/*.yml.
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

from jdbc_conn_utils import jdbc_jinja_args
from etl_flag import make_start_flag_task, make_end_flag_task
from cob_dt import BACKFILL_FROM_ARG, COB_DT

DAG_ID            = "bronze_card_crm_dag"
ETL_PATH          = Variable.get("ETL_PATH", default_var="/opt/project/code_etl")
SPARK_APPLICATION = f"{ETL_PATH}/bronze/base_job/ingestion_jdbc.py"
CONFIG_DIR        = Path(ETL_PATH) / "bronze" / "card_crm"
CONN_ID           = "postgres-card-crm"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Bronze ingestion — card_crm (PostgreSQL)",
    schedule_interval="0 2 * * *",  # Daily at 2:00 AM (Production)
    catchup=False,
    # Một lượt mỗi lúc: hai lượt cùng cob_dt (unpause tạo lượt theo lịch + trigger tay)
    # cùng overwritePartitions / MERGE SCD2 một bảng (đo 2026-09-30).
    max_active_runs=1,
    max_active_tasks=1,
    tags=["bronze", "card_crm", "postgresql", "production"],
)

# Template, render lúc task chạy — không query metadata DB lúc parse DAG,
# và password đi qua env (docker exec -e), không qua argv (ps / Spark UI).
jdbc = jdbc_jinja_args(CONN_ID)

dag_start = make_start_flag_task("dag_start", DAG_ID, "bronze", dag, cob_dt=COB_DT)

with TaskGroup("ingest_all", dag=dag) as ingest_all:
    for config_file in sorted(CONFIG_DIR.glob("*.yml")):
        config     = yaml.safe_load(config_file.read_text())
        table_name = config["target"]["table"]
        remote_cfg = f"{CONFIG_DIR}/{config_file.name}"

        cmd = (
            f"/usr/bin/docker exec -e DB_PASSWORD banking-spark-worker-1 "
            f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client "
            f"--conf spark.driver.memory=512m "
            f"--conf spark.executor.memory=768m "
            f"--conf spark.executor.cores=1 "
            f"{SPARK_APPLICATION} "
            f"--config {remote_cfg} "
            f"--cob_dt {COB_DT} "
            f"--jdbc_url '{jdbc['jdbc_url']}' "
            f"--db_user '{jdbc['db_user']}'"
        )
        # ADR-0018: lần nạp đầu truyền conf backfill_from; chỉ job incremental nhận tham số này.
        if config["load"]["strategy"] == "incremental":
            cmd += f" {BACKFILL_FROM_ARG}"

        BashOperator(
            task_id=f"ingest_{table_name}",
            bash_command=cmd,
            env={"DB_PASSWORD": jdbc["db_password"]},
            append_env=True,
            dag=dag,
        )

dag_end = make_end_flag_task("dag_end", DAG_ID, "bronze", dag, cob_dt=COB_DT)

dag_start >> ingest_all >> dag_end
