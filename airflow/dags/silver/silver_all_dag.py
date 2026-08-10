"""
Silver layer DAG — tổng hợp 13 bảng silver (8 dims + 5 facts).

Luồng thực thi:
  1. dag_start ghi cờ R
  2. Kiểm tra 3 bronze DAGs đã hoàn thành (theo dag_id)
  3. Chạy song song 8 dim jobs (SCD1 + SCD2)
  4. Sau khi dims xong, chạy song parallel 5 fact jobs
  5. dag_end ghi cờ S
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup

from etl_flag import make_start_flag_task, make_end_flag_task

# ─── Constants ────────────────────────────────────────────────────────────────
DAG_ID           = "silver_all_dag"
DATA_COB_DT      = "{{ ds }}"
POSTGRES_CONN_ID = "postgres-etl"
SPARK_CONN_ID    = "spark_default"
SILVER_BASE      = "/opt/project/code_etl/silver"
SILVER_BASE_JOB  = f"{SILVER_BASE}/base_job"

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

# (table_name, job_script, config_file)
DIM_JOBS = [
    ("dim_branch",   "scd_type1.py", "dims/dim_branch.yml"),
    ("dim_product",  "scd_type1.py", "dims/dim_product.yml"),
    ("dim_card",     "scd_type1.py", "dims/dim_card.yml"),
    ("dim_employee", "scd_type1.py", "dims/dim_employee.yml"),
    ("dim_device",   "scd_type1.py", "dims/dim_device.yml"),
    ("dim_location", "scd_type1.py", "dims/dim_location.yml"),
    ("dim_customer", "scd_type2.py", "dims/dim_customer.yml"),
    ("dim_account",  "scd_type2.py", "dims/dim_account.yml"),
]

# (table_name, job_script, config_file)
FACT_JOBS = [
    ("fact_txn_account",          "fact_txn.py", "facts/fact_txn_account.yml"),
    ("fact_card_txn",             "fact_txn.py", "facts/fact_card_txn.yml"),
    ("fact_crm_interaction",      "fact_txn.py", "facts/fact_crm_interaction.yml"),
    ("fact_online_transaction",   "fact_txn.py", "facts/fact_online_transaction.yml"),
    ("fact_support_ticket",       "fact_txn.py", "facts/fact_support_ticket.yml"),
]

# Bronze DAG IDs cần check
BRONZE_DAG_IDS = [
    "bronze_core_banking_dag",
    "bronze_card_crm_dag",
    "bronze_digital_banking_dag",
]


def _check_dag_flag_sql(upstream_dag_id: str) -> str:
    return (
        "SELECT 1 FROM opslakehouse.flag_job_etl "
        f"WHERE job_name = '{upstream_dag_id}' "
        "  AND status = 'S' "
        f"  AND cob_dt = DATE '{DATA_COB_DT}' "
        "LIMIT 1"
    )


# ─── DAG ──────────────────────────────────────────────────────────────────────
dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Silver layer — 8 dims + 5 facts",
    schedule_interval="0 4 * * *",  # Daily at 4:00 AM (Production)
    catchup=False,
    max_active_tasks=1,
    tags=["silver", "all", "production"],
)

# ── 1. Cờ start ──────────────────────────────────────────────────────────────
dag_start = make_start_flag_task("dag_start", DAG_ID, "silver", dag, cob_dt=DATA_COB_DT)

# ── 2. Kiểm tra bronze DAGs ──────────────────────────────────────────────────
with TaskGroup("check_bronze", dag=dag) as check_bronze:
    for upstream_dag_id in BRONZE_DAG_IDS:
        SqlSensor(
            task_id=f"check_{upstream_dag_id}",
            conn_id=POSTGRES_CONN_ID,
            sql=_check_dag_flag_sql(upstream_dag_id),
            poke_interval=30,
            timeout=1800,
            mode="reschedule",
            dag=dag,
        )

# ── 3. Dim jobs ──────────────────────────────────────────────────────────────
with TaskGroup("dims", dag=dag) as dims_group:
    for table_name, script, config_file in DIM_JOBS:
        app = f"{SILVER_BASE_JOB}/{script}"
        cfg = f"{SILVER_BASE}/{config_file}"
        cmd = (
            f"/usr/bin/docker exec banking-spark-worker-1 "
            f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client "
            f"--conf spark.driver.memory=512m "
            f"--conf spark.executor.memory=768m "
            f"--conf spark.executor.cores=1 "
            f"{app} --config {cfg} --cob_dt {DATA_COB_DT}"
        )
        BashOperator(
            task_id=f"run_{table_name}",
            bash_command=cmd,
            dag=dag,
        )

# ── 4. Fact jobs ─────────────────────────────────────────────────────────────
with TaskGroup("facts", dag=dag) as facts_group:
    for table_name, script, config_file in FACT_JOBS:
        app = f"{SILVER_BASE_JOB}/{script}"
        cfg = f"{SILVER_BASE}/{config_file}"
        cmd = (
            f"/usr/bin/docker exec banking-spark-worker-1 "
            f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client "
            f"--conf spark.driver.memory=512m "
            f"--conf spark.executor.memory=768m "
            f"--conf spark.executor.cores=1 "
            f"{app} --config {cfg} --cob_dt {DATA_COB_DT}"
        )
        BashOperator(
            task_id=f"run_{table_name}",
            bash_command=cmd,
            dag=dag,
        )

# ── 5. Cờ end ────────────────────────────────────────────────────────────────
dag_end = make_end_flag_task("dag_end", DAG_ID, "silver", dag, cob_dt=DATA_COB_DT)

# ─── Dependencies ─────────────────────────────────────────────────────────────
dag_start >> check_bronze >> dims_group >> facts_group >> dag_end
