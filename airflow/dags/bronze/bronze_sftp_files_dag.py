"""
Bronze ingestion DAG — SFTP file source (POS/settlement files, khoá new2).

Luồng: SFTPSensor chờ file settlement xuất hiện trong landing zone → job nạp
(ingestion_sftp.py, submit qua docker exec vào spark-worker) tải file và ghi
vào Bronze Iceberg.

Chạy cùng khung Bronze DB. Sensor dùng kết nối Airflow `sftp_default` (khai
trong UI/env). Chờ theo pattern `pos_settlement_*.csv` — ngày nghiệp vụ lấy từ
cột settlement_date trong file (không theo ngày chạy), nên không cần path theo
ngày; xem load.business_date_column trong config.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor

from cob_dt import COB_DT
from etl_flag import make_end_flag_task, make_start_flag_task

DAG_ID = "bronze_sftp_files_dag"
ETL_PATH = "/opt/project/code_etl"
SPARK_APPLICATION = f"{ETL_PATH}/bronze/base_job/ingestion_sftp.py"
CONFIG = f"{ETL_PATH}/bronze/file_sources/pos_settlement.yml"
SFTP_CONN_ID = "sftp_default"

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
    description="Bronze ingestion — SFTP settlement files (file source)",
    schedule_interval="0 2 * * *",  # Daily at 2:00 AM (cùng khung Bronze DB)
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["bronze", "sftp", "file", "production"],
)

dag_start = make_start_flag_task("dag_start", DAG_ID, "bronze", dag, cob_dt=COB_DT)

# Chờ file settlement xuất hiện trong landing zone.
wait_file = SFTPSensor(
    task_id="wait_settlement_file",
    sftp_conn_id=SFTP_CONN_ID,
    path="/upload/pos_settlement_20251231.csv",
    poke_interval=60,
    timeout=3600,
    mode="reschedule",
    dag=dag,
)

# Tải + đọc file → Bronze. Credential truyền qua env (không qua argv).
ingest = BashOperator(
    task_id="ingest_pos_settlement",
    bash_command=(
        "/usr/bin/docker exec "
        "-e SFTP_HOST=sftp -e SFTP_PORT=22 "
        "-e SFTP_USER=sftp_user -e SFTP_PASSWORD=sftp_pass "
        "banking-spark-worker-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 "
        "--deploy-mode client "
        "--conf spark.driver.memory=512m --conf spark.executor.memory=768m --conf spark.executor.cores=1 "
        f"{SPARK_APPLICATION} --config {CONFIG} --cob_dt {COB_DT}"
    ),
    dag=dag,
)

dag_end = make_end_flag_task("dag_end", DAG_ID, "bronze", dag, cob_dt=COB_DT)

dag_start >> wait_file >> ingest >> dag_end
