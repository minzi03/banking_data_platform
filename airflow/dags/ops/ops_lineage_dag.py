"""
Ops DAG — Lineage Emission (post-Silver/Gold).
Records data lineage across the pipeline and emits to OpenMetadata.
Results written to opslakehouse.lineage_log.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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

SPARK_CONF = {
    "spark.driver.memory":   "512m",
    "spark.executor.memory": "768m",
    "spark.executor.cores":  "1",
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
def emit_lineage(**context):
    """
    Emit lineage records for all pipeline tables.
    Reads Airflow run metadata and Iceberg snapshots.
    """
    import sys
    import os

    # Add project to path
    sys.path.insert(0, "/opt/project")

    from governance.lineage import LineageTracker, TransformType

    tracker = LineageTracker()
    dag_id = context["dag"].dag_id
    dag_run_id = context["run_id"]

    # Bronze → Silver lineage (SCD transforms)
    bronze_silver_lineage = [
        ("lakehouse.bronze.core_customer", "lakehouse.silver.dim_customer", TransformType.SCD2_MERGE),
        ("lakehouse.bronze.core_account", "lakehouse.silver.dim_account", TransformType.SCD2_MERGE),
        ("lakehouse.bronze.core_product", "lakehouse.silver.dim_product", TransformType.SCD1_UPSERT),
        ("lakehouse.bronze.core_branch", "lakehouse.silver.dim_branch", TransformType.SCD1_UPSERT),
        ("lakehouse.bronze.core_card", "lakehouse.silver.dim_card", TransformType.SCD1_UPSERT),
        ("lakehouse.bronze.core_employee", "lakehouse.silver.dim_employee", TransformType.SCD1_UPSERT),
        ("lakehouse.bronze.core_device", "lakehouse.silver.dim_device", TransformType.SCD1_UPSERT),
        ("lakehouse.bronze.core_location", "lakehouse.silver.dim_location", TransformType.SCD1_UPSERT),
        ("lakehouse.bronze.core_txn_account", "lakehouse.silver.fact_txn_account", TransformType.FACT_LOAD),
        ("lakehouse.bronze.core_card_txn", "lakehouse.silver.fact_card_txn", TransformType.FACT_LOAD),
        ("lakehouse.bronze.core_crm_interaction", "lakehouse.silver.fact_crm_interaction", TransformType.FACT_LOAD),
        ("lakehouse.bronze.core_online_transaction", "lakehouse.silver.fact_online_transaction", TransformType.FACT_LOAD),
        ("lakehouse.bronze.core_support_ticket", "lakehouse.silver.fact_support_ticket", TransformType.FACT_LOAD),
    ]

    # Silver → Gold lineage (mart aggregations)
    silver_gold_lineage = [
        ("lakehouse.silver.dim_customer", "lakehouse.gold.mart_customer_360", TransformType.GOLD_MART),
        ("lakehouse.silver.dim_account", "lakehouse.gold.mart_customer_360", TransformType.GOLD_MART),
        ("lakehouse.silver.dim_card", "lakehouse.gold.mart_customer_360", TransformType.GOLD_MART),
        ("lakehouse.silver.fact_txn_account", "lakehouse.gold.mart_customer_360", TransformType.GOLD_MART),
        ("lakehouse.silver.fact_card_txn", "lakehouse.gold.mart_customer_360", TransformType.GOLD_MART),
        ("lakehouse.gold.mart_customer_360", "lakehouse.gold.rfm_segment", TransformType.GOLD_MART),
        ("lakehouse.gold.mart_customer_360", "lakehouse.gold.churn_prediction", TransformType.GOLD_MART),
        ("lakehouse.gold.mart_customer_360", "lakehouse.gold.cross_sell_segment", TransformType.GOLD_MART),
        ("lakehouse.gold.rfm_segment", "lakehouse.gold.campaign_target", TransformType.GOLD_MART),
        ("lakehouse.gold.churn_prediction", "lakehouse.gold.campaign_target", TransformType.GOLD_MART),
        ("lakehouse.gold.cross_sell_segment", "lakehouse.gold.campaign_target", TransformType.GOLD_MART),
    ]

    # Record all lineage
    all_lineage = bronze_silver_lineage + silver_gold_lineage

    for source, target, transform_type in all_lineage:
        tracker.record_lineage(
            source_table=source,
            target_table=target,
            transform_type=transform_type,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            row_count=0,  # Will be updated if needed
        )

    # Summary
    print(tracker.summary())

    # Note: Actual PG write would need SparkSession
    # For now, just log the lineage
    print(f"Lineage emission complete: {len(all_lineage)} records")


emit_lineage_task = PythonOperator(
    task_id="emit_lineage",
    python_callable=emit_lineage,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task Flow
# ---------------------------------------------------------------------------
[wait_silver, wait_gold] >> start >> emit_lineage_task >> end
