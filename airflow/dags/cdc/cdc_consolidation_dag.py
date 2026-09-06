"""
Airflow DAG: CDC Consolidation

Runs consolidation from Bronze CDC → Silver Current State.
Executes after cdc_streaming_pipeline has ingested new events.

Schedule: Every 10 minutes (or triggered manually)

Submission model: `docker exec` into the Spark worker, matching every other
Spark DAG in this repo. Airflow's own image carries only the pyspark wheel —
no Iceberg runtime jars — so submitting from the scheduler container fails with
`Cannot find catalog plugin class for catalog 'lakehouse'`. Running inside the
worker also means the catalog / MinIO / timezone settings come from the image's
spark-defaults.conf instead of being re-declared (and credentials re-pasted)
here.

This cadence is part of the published CDC freshness number: end-to-end
freshness includes waiting for the next consolidation run. Changing the
schedule changes that number, so re-measure if you change it.
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


SPARK_SUBMIT = (
    "/usr/bin/docker exec banking-spark-worker-1 "
    "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client "
    "--conf spark.sql.shuffle.partitions=2 "
    "--conf spark.driver.memory=512m "
    "--conf spark.executor.memory=768m "
    "--conf spark.executor.cores=1"
)

CONSOLIDATION_APP = "/opt/project/code_etl/cdc/consolidation/cdc_consolidation.py"


def create_consolidation_task(config: dict) -> BashOperator:
    """Create a BashOperator for consolidation task."""
    return BashOperator(
        task_id=f"consolidate_{config['name']}",
        bash_command=(
            f"{SPARK_SUBMIT} {CONSOLIDATION_APP} --config {config['config_path']}"
        ),
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
