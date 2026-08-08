"""
Airflow DAG: CDC Consolidation

Runs consolidation from Bronze CDC → Silver Current State.
Executes after cdc_streaming_pipeline has ingested new events.

Schedule: Every 10 minutes (or triggered manually)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Default args
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Config files
CONSOLIDATION_CONFIGS = [
    {
        "name": "customer",
        "config_path": "/opt/project/code_etl/cdc/consolidation/config/cdc_consolidation_customer.yml",
    },
    {
        "name": "account",
        "config_path": "/opt/project/code_etl/cdc/consolidation/config/cdc_consolidation_account.yml",
    },
]


def create_consolidation_task(config: dict) -> BashOperator:
    """Create a BashOperator for consolidation task."""
    return BashOperator(
        task_id=f"consolidate_{config['name']}",
        bash_command=f"""
            spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --conf spark.sql.shuffle.partitions=2 \
                --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
                --conf spark.sql.catalog.lakehouse.type=rest \
                --conf spark.sql.catalog.lakehouse.uri=http://iceberg-rest:8181 \
                --conf spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse/lakehouse \
                --conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
                --conf spark.sql.catalog.lakehouse.s3.endpoint=http://minio:9000 \
                --conf spark.sql.catalog.lakehouse.s3.path-style-access=true \
                --conf spark.sql.catalog.lakehouse.s3.access-key-id=minioadmin \
                --conf spark.sql.catalog.lakehouse.s3.secret-access-key=Minioadmin123 \
                /opt/project/code_etl/cdc/consolidation/cdc_consolidation.py \
                --config {config['config_path']}
        """,
        dag=None,  # Set by DAG definition
    )


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="cdc_consolidation_pipeline",
    default_args=default_args,
    description="Consolidate Bronze CDC → Silver Current State",
    schedule_interval="*/10 * * * *",  # Every 10 minutes
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "consolidation", "silver"],
) as dag:

    # Create consolidation tasks
    consolidate_customer = create_consolidation_task(CONSOLIDATION_CONFIGS[0])
    consolidate_account = create_consolidation_task(CONSOLIDATION_CONFIGS[1])

    # Task dependency: customer first, then account
    consolidate_customer >> consolidate_account
