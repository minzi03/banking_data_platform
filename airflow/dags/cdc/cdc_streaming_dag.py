"""
Airflow DAG: CDC Streaming Pipeline

Starts Spark Structured Streaming jobs to consume CDC events from Kafka
and write to Iceberg Bronze tables.

Note: Streaming jobs run continuously. This DAG starts them and they
keep running until manually stopped or the cluster is restarted.

Schedule: Manual trigger only
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from pathlib import Path
import yaml


CONFIG_DIR = "/opt/project/code_etl/cdc/config"
SPARK_APP = "/opt/project/code_etl/cdc/base_job/cdc_streaming.py"
SPARK_CONN_ID = "spark_default"


def load_cdc_configs():
    """Load all CDC YAML configs and return list of (table_name, config) tuples."""
    configs = []
    config_dir = Path(CONFIG_DIR)
    for config_file in sorted(config_dir.glob("cdc_*.yml")):
        with open(config_file) as f:
            config = yaml.safe_load(f)
        table_name = config_file.stem.replace("cdc_", "")
        configs.append((table_name, config))
    return configs


# Load configs at DAG parse time
CDC_CONFIGS = load_cdc_configs()


with DAG(
    dag_id="cdc_streaming_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "streaming"],
    doc_md="""
    ## CDC Streaming Pipeline

    Starts Spark Structured Streaming jobs to consume CDC events from Kafka.

    **Important:** Streaming jobs run continuously once started.
    - To stop a streaming job, kill the Spark application via Spark Master UI
    - Streaming jobs will automatically restart on cluster restart

    ### Streaming Jobs:
    - core_account (30s trigger)
    - core_customer (30s trigger)
    - core_transaction (10s trigger)
    - card_account (30s trigger)
    - card_transaction (10s trigger)
    - online_transaction (10s trigger)
    """,
) as dag:

    # Start marker
    start = EmptyOperator(task_id="start")

    # Create streaming tasks for each CDC config
    streaming_tasks = []
    for table_name, config in CDC_CONFIGS:
        kafka_topic = config["kafka"]["topic"]
        target_table = f"{config['target']['catalog']}.{config['target']['schema']}.{config['target']['table']}"
        checkpoint = config["kafka"]["checkpoint_location"]
        trigger_interval = config["kafka"].get("trigger_interval", "30 seconds")

        # Use SparkSubmitOperator for streaming jobs
        # Note: Streaming jobs need to run in background and not block Airflow
        # We use a BashOperator with nohup to run in background
        cmd = (
            f"/usr/bin/docker exec -d banking-spark-worker-1 "
            f"/opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 "
            f"--deploy-mode client "
            f"--name cdc_{table_name} "
            f"--conf spark.driver.memory=512m "
            f"--conf spark.executor.memory=768m "
            f"--conf spark.executor.cores=1 "
            f"--conf spark.sql.streaming.checkpointLocation={checkpoint} "
            f"--conf spark.sql.shuffle.partitions=4 "
            f"{SPARK_APP} "
            f"--config {CONFIG_DIR}/cdc_{table_name}.yml "
            f"--kafka_bootstrap kafka:9092"
        )

        task = BashOperator(
            task_id=f"stream_{table_name}",
            bash_command=cmd,
            doc_md=f"Streaming job for {kafka_topic} → {target_table}",
        )
        streaming_tasks.append(task)

    # End marker
    end = EmptyOperator(task_id="end")

    # Dependencies
    start >> streaming_tasks >> end


# =============================================================================
# Helper DAG: Stop all streaming jobs
# =============================================================================

with DAG(
    dag_id="cdc_streaming_stop_all",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "streaming", "admin"],
    doc_md="Stop all CDC streaming jobs",
) as stop_dag:

    stop_all = BashOperator(
        task_id="stop_all_streaming",
        bash_command=(
            "/usr/bin/docker exec banking-spark-worker-1 "
            "bash -c '"
            "for app in $(/opt/spark/bin/spark-history-server --history 2>/dev/null "
            "  || /opt/spark/sbin/stop-slave.sh 2>/dev/null "
            "  || true); do "
            "  echo \"Stopping $app\"; "
            "done; "
            "# Kill any running spark-submit streaming apps by name pattern "
            "pkill -f 'cdc_' 2>/dev/null || true; "
            "# Also try graceful stop via Spark REST API "
            "for app_id in $(curl -s http://spark-master:8080/json/ 2>/dev/null "
            "  | python3 -c 'import sys,json; [print(a[\"id\"]) for a in json.load(sys.stdin).get(\"activeapps\",[])]' 2>/dev/null || true); do "
            "  echo \"Killing $app_id\"; "
            "  curl -s -X POST \"http://spark-master:8080/cluster/kill/?id=$app_id\" 2>/dev/null; "
            "done; "
            "echo 'Stop signal sent to all streaming jobs'"
            "' || echo 'No streaming jobs found'"
        ),
    )
