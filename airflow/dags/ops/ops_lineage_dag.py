"""
Ops DAG — Lineage Emission (post-Silver/Gold).
Ghi các cạnh lineage mà job Silver/Gold khai trong YAML vào opslakehouse.lineage_log.
Không emit sang OpenMetadata.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
import pendulum

from etl_flag import make_start_flag_task, make_end_flag_task

DAG_ID              = "ops_lineage_dag"
POSTGRES_ETL_CONN_ID = "postgres-etl"
COB_DT              = "{{ ds }}"

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
    description="Lineage emission — Record and emit data lineage",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=4,
    tags=["ops", "lineage", "governance"],
)

# ---------------------------------------------------------------------------
# Sensors — wait for upstream DAGs to finish
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Flag tasks
# ---------------------------------------------------------------------------
start = make_start_flag_task("start", DAG_ID, "ops", dag, cob_dt=COB_DT)
end   = make_end_flag_task("end", DAG_ID, "ops", dag, cob_dt=COB_DT)

# ---------------------------------------------------------------------------
# Lineage Emission Task — PythonOperator
# ---------------------------------------------------------------------------
PROJECT_ROOT = "/opt/project"
LINEAGE_TABLE = "opslakehouse.lineage_log"


def emit_lineage(**context):
    """
    Ghi mọi cạnh lineage mà job Silver/Gold khai trong YAML vào lineage_log.

    Cạnh lấy từ governance.lineage.declared_edges — không còn danh sách viết
    tay (TD-13). Ghi lại cùng một dag_run thì thay thế, không nhân đôi: xoá
    cạnh cũ của run đó rồi chèn, trong một transaction.

    row_count và snapshot_id để NULL: task này không đo chúng, và một số 0
    giả sẽ đọc như "job ghi 0 dòng".
    """
    import os
    import sys

    sys.path.insert(0, PROJECT_ROOT)

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from governance.lineage import declared_edges

    edges = declared_edges(os.path.join(PROJECT_ROOT, "code_etl"))
    if not edges:
        raise RuntimeError("declared_edges trả về 0 cạnh — nghi đường dẫn code_etl sai")

    dag_id = context["dag"].dag_id
    dag_run_id = context["run_id"]
    rows = [(src, tgt, transform, dag_id, dag_run_id) for src, tgt, transform in edges]

    conn = PostgresHook(postgres_conn_id=POSTGRES_ETL_CONN_ID).get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {LINEAGE_TABLE} WHERE dag_id = %s AND dag_run_id = %s",
                (dag_id, dag_run_id),
            )
            cur.executemany(
                f"INSERT INTO {LINEAGE_TABLE} "
                "(source_table, target_table, transform_type, dag_id, dag_run_id, snapshot_id, row_count) "
                "VALUES (%s, %s, %s, %s, %s, NULL, NULL)",
                rows,
            )
    finally:
        conn.close()

    print(f"Wrote {len(rows)} lineage edges to {LINEAGE_TABLE} for run {dag_run_id}")
    return len(rows)


emit_lineage_task = PythonOperator(
    task_id="emit_lineage",
    python_callable=emit_lineage,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task Flow
# ---------------------------------------------------------------------------
[wait_silver, wait_gold] >> start >> emit_lineage_task >> end
