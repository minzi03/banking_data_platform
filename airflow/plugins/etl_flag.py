"""
Reusable ETL flag utilities — ghi và check cờ theo dag_id.

Schema bảng flag_job_etl (PostgreSQL, schema opslakehouse):
    job_name    VARCHAR   — dag_id
    schema_name VARCHAR   — 'bronze' | 'silver' | 'gold' (informational)
    table_name  VARCHAR   — dag_id (trùng job_name, giữ để không NULL)
    status      CHAR(1)   — 'R' Running | 'S' Success
    start_time  TIMESTAMP — điền khi INSERT R
    end_time    TIMESTAMP — điền khi INSERT S
    cob_dt      DATE      — ngày xử lý (YYYY-MM-DD)

Quy tắc:
    - INSERT only, không bao giờ UPDATE
    - 1 DAG = 1 cặp cờ: INSERT R khi DAG bắt đầu, INSERT S khi DAG hoàn thành
    - Downstream DAGs check theo job_name (dag_id), không theo table_name
"""

from airflow.providers.postgres.operators.postgres import PostgresOperator

POSTGRES_ETL_CONN_ID = "postgres-etl"

_FLAG_SQL_RUNNING = """
    INSERT INTO opslakehouse.flag_job_etl
        (job_name, schema_name, table_name, status, start_time, end_time, cob_dt)
    VALUES
        (%(dag_id)s, %(layer)s, %(dag_id)s, 'R',
         (NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh'), NULL, %(cob_dt)s::date);
"""

_FLAG_SQL_SUCCESS = """
    INSERT INTO opslakehouse.flag_job_etl
        (job_name, schema_name, table_name, status, start_time, end_time, cob_dt)
    VALUES
        (%(dag_id)s, %(layer)s, %(dag_id)s, 'S',
         NULL, (NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh'), %(cob_dt)s::date);
"""


def make_start_flag_task(
    task_id: str,
    dag_id: str,
    layer: str,
    dag,
    cob_dt: str = "{{ ds }}",
) -> PostgresOperator:
    """INSERT một R (Running) flag row khi DAG bắt đầu."""
    return PostgresOperator(
        task_id=task_id,
        postgres_conn_id=POSTGRES_ETL_CONN_ID,
        sql=_FLAG_SQL_RUNNING,
        parameters={
            "dag_id": dag_id,
            "layer": layer,
            "cob_dt": cob_dt,
        },
        dag=dag,
    )


def make_end_flag_task(
    task_id: str,
    dag_id: str,
    layer: str,
    dag,
    cob_dt: str = "{{ ds }}",
) -> PostgresOperator:
    """INSERT một S (Success) flag row khi DAG hoàn thành."""
    return PostgresOperator(
        task_id=task_id,
        postgres_conn_id=POSTGRES_ETL_CONN_ID,
        sql=_FLAG_SQL_SUCCESS,
        parameters={
            "dag_id": dag_id,
            "layer": layer,
            "cob_dt": cob_dt,
        },
        dag=dag,
    )
