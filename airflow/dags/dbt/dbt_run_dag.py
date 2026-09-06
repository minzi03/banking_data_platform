# =============================================================================
# Airflow DAG: dbt Serving Publisher
# =============================================================================
# Ownership:
#     Spark      → build Bronze / Silver / historical Gold
#     dbt + Trino → publish tầng current-serving (iceberg.serving.*)
#
# DAG này CỐ Ý tách khỏi Gold DAG. Gold DAG dựng dữ liệu phân tích lịch sử;
# DAG này xuất bản tầng phục vụ. Hai trách nhiệm khác nhau, hai owner khác nhau.
# Ràng buộc duy nhất là: chỉ chạy khi Gold của CÙNG cob_dt đã hoàn tất.
#
# Flow:
#     wait_for_gold_complete(cob_dt)
#         ↓
#     validate_gold_sources        (smoke test — bắt sai catalog / source rỗng)
#         ↓
#     dbt build --select serving --vars cob_dt
#         ↓  (build, KHÔNG phải run — build gồm cả fail-loud tests)
#     write SERVING_COMPLETE flag  (chỉ khi build exit 0)
#
# COB_DT dùng MỘT biến duy nhất xuyên suốt: sensor check D thì dbt cũng build D.
# Trước đây DAG này chạy `dbt run --select semantic` không truyền cob_dt và
# không chờ Gold — tức có thể publish trước khi Gold xong, hoặc publish sai ngày.
# =============================================================================

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

from etl_flag import make_end_flag_task, make_start_flag_task

# ─── Constants ────────────────────────────────────────────────────────────────
DAG_ID = "dbt_serving_publish"
# Một biến duy nhất cho cả sensor lẫn dbt --vars. Không được để sensor check D
# còn dbt build D±1.
DATA_COB_DT = "{{ ds }}"
POSTGRES_CONN_ID = "postgres-etl"
# Image Airflow KHÔNG cài dbt (Dockerfile.airflow chỉ có pyspark + spark provider),
# nhưng CÓ docker CLI. Nên gọi dbt qua `docker exec` vào container dbt —
# đúng pattern mà DAG Silver/Gold đang dùng với spark-worker.
# DAG cũ chạy `cd /opt/project/dbt && dbt run` nên chưa từng thực thi được.
DBT_EXEC = "/usr/bin/docker exec banking-dbt sh -lc"
DBT_DIR = "/usr/src/dbt"

# Contract ở mức dữ liệu, không phải tên DAG: "toàn bộ Gold của cob_dt=D xong".
# Ghi bởi task cuối của gold_mart360_dag. Nếu Gold tách nhiều producer sau này,
# chỉ producer cuối ghi cờ này — DAG này không phải sửa.
GOLD_COMPLETE_FLAG = "GOLD_COMPLETE"
SERVING_COMPLETE_FLAG = "SERVING_COMPLETE"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "execution_timeout": timedelta(hours=1),
}


def _gold_complete_sql() -> str:
    return (
        "SELECT 1 FROM opslakehouse.flag_job_etl "
        f"WHERE job_name = '{GOLD_COMPLETE_FLAG}' "
        "  AND status = 'S' "
        f"  AND cob_dt = DATE '{DATA_COB_DT}' "
        "LIMIT 1"
    )


with DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Publish current-serving layer (iceberg.serving.*) qua dbt/Trino",
    schedule_interval="0 7 * * *",  # sau Gold 06:00
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "serving", "trino", "production"],
) as dag:

    start = make_start_flag_task("dag_start", DAG_ID, "serving", dag, cob_dt=DATA_COB_DT)

    # ── 1. Chờ Gold của ĐÚNG cob_dt này hoàn tất ─────────────────────────────
    # mode="reschedule" nhả worker slot trong lúc chờ.
    wait_for_gold_complete = SqlSensor(
        task_id="wait_for_gold_complete",
        conn_id=POSTGRES_CONN_ID,
        sql=_gold_complete_sql(),
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
        dag=dag,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f'{DBT_EXEC} "cd {DBT_DIR} && dbt deps"',
        dag=dag,
    )

    # ── 2. Smoke test connectivity ───────────────────────────────────────────
    # `dbt debug` chỉ mở kết nối và báo "All checks passed!" ngay cả khi profile
    # trỏ sai catalog — mọi model sau đó chết CATALOG_NOT_FOUND. Test này chạm
    # bảng Gold thật nên bắt được cả sai catalog lẫn source rỗng.
    validate_gold_sources = BashOperator(
        task_id="validate_gold_sources",
        bash_command=(
            f'{DBT_EXEC} "cd {DBT_DIR} && '
            'dbt test --target docker --select assert_gold_source_reachable"'
        ),
        dag=dag,
    )

    # ── 3. Build tầng serving ────────────────────────────────────────────────
    # `build` chứ KHÔNG phải `run`: build chạy model + toàn bộ test, trong đó có
    # assert_serving_snapshot_alignment (rỗng / nhiều cob_dt / sai cob_dt đều
    # FAIL). Dùng `run` sẽ publish được cả bảng rỗng mà vẫn báo thành công.
    dbt_build_serving = BashOperator(
        task_id="dbt_build_serving",
        bash_command=(
            f"{DBT_EXEC} \"cd {DBT_DIR} && dbt build --target docker "
            f"--select serving --vars '{{\\\"cob_dt\\\": \\\"{DATA_COB_DT}\\\"}}'\""
        ),
        dag=dag,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f'{DBT_EXEC} "cd {DBT_DIR} && dbt docs generate --target docker"',
        dag=dag,
    )

    # ── 4. Cờ SERVING_COMPLETE ───────────────────────────────────────────────
    # Chỉ được ghi khi dbt build exit 0 (mặc định trigger_rule=all_success).
    # Downstream (Superset refresh, manifest verification...) dựa vào cờ này.
    serving_complete = make_end_flag_task(
        "serving_complete", SERVING_COMPLETE_FLAG, "serving", dag, cob_dt=DATA_COB_DT
    )

    end = EmptyOperator(task_id="end", dag=dag)

    (
        start
        >> wait_for_gold_complete
        >> dbt_deps
        >> validate_gold_sources
        >> dbt_build_serving
        >> serving_complete
        >> dbt_docs
        >> end
    )
