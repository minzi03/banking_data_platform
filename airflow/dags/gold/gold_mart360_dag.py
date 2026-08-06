"""
Gold layer DAG — tổng hợp 5 bảng mart360 + 3 segment + 1 time_analytics.

Luồng thực thi:
  1. dag_start ghi cờ R
  2. Kiểm tra silver_all_dag đã hoàn thành
  3. Chạy song parallel 9 Gold jobs (Phase 1 — independent)
  4. Chạy campaign_target (Phase 2 — depends on Phase 1)
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
DAG_ID           = "gold_all_dag"
DATA_COB_DT      = "{{ ds }}"
POSTGRES_CONN_ID = "postgres-etl"
SPARK_CONN_ID    = "spark_default"
GOLD_BASE        = "/opt/project/code_etl/gold"
GOLD_BASE_JOB    = f"{GOLD_BASE}/base_job"

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

# Phase 1: Independent Gold jobs (parallel-safe)
# (table_name, config_file)
PHASE1_JOBS = [
    # mart360
    ("mart_customer_360",            "mart360/customer_360.yml"),
    ("customer_balance_summary",     "mart360/customer_balance_summary.yml"),
    ("customer_transaction_summary", "mart360/customer_transaction_summary.yml"),
    ("customer_product_summary",     "mart360/customer_product_summary.yml"),
    ("customer_card_summary",        "mart360/customer_card_summary.yml"),
    # segments (independent)
    ("rfm_segment",          "segmentation/rfm_segment.yml"),
    ("churn_prediction",     "segmentation/churn_prediction.yml"),
    ("cross_sell_segment",   "segmentation/cross_sell_segment.yml"),
    # time_analytics
    ("branch_monthly_summary", "time_analytics/branch_monthly_summary.yml"),
]

# Phase 2: Depends on Phase 1
PHASE2_JOBS = [
    ("campaign_target", "segmentation/campaign_target.yml"),
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
    description="Gold layer — 5 mart360 + 4 segments + 1 time_analytics",
    schedule_interval="0 6 * * *",  # Daily at 6:00 AM (Production)
    catchup=False,
    max_active_tasks=1,
    tags=["gold", "all", "production"],
)

# ── 1. Cờ start ──────────────────────────────────────────────────────────────
dag_start = make_start_flag_task("dag_start", DAG_ID, "gold", dag, cob_dt=DATA_COB_DT)

# ── 2. Kiểm tra silver ──────────────────────────────────────────────────────
check_silver = SqlSensor(
    task_id="check_silver_all_dag",
    conn_id=POSTGRES_CONN_ID,
    sql=_check_dag_flag_sql("silver_all_dag"),
    poke_interval=30,
    timeout=1800,
    mode="reschedule",
    dag=dag,
)

# ── 3. Phase 1: Independent jobs ────────────────────────────────────────────
with TaskGroup("phase1_independent", dag=dag) as phase1_group:
    for table_name, config_file in PHASE1_JOBS:
        app = f"{GOLD_BASE_JOB}/gold_job.py"
        cfg = f"{GOLD_BASE}/{config_file}"
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

# ── 4. Phase 2: campaign_target (depends on Phase 1) ───────────────────────
with TaskGroup("phase2_dependent", dag=dag) as phase2_group:
    for table_name, config_file in PHASE2_JOBS:
        app = f"{GOLD_BASE_JOB}/gold_job.py"
        cfg = f"{GOLD_BASE}/{config_file}"
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
dag_end = make_end_flag_task("dag_end", DAG_ID, "gold", dag, cob_dt=DATA_COB_DT)

# ─── Dependencies ─────────────────────────────────────────────────────────────
dag_start >> check_silver >> phase1_group >> phase2_group >> dag_end
