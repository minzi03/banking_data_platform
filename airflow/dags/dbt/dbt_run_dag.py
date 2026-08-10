# =============================================================================
# Airflow DAG: dbt Run
# Business Logic: Execute dbt semantic layer on Gold tables
# Pattern: dbt + Airflow integration
# =============================================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Default arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# DAG definition
with DAG(
    dag_id='dbt_run',
    default_args=default_args,
    description='Execute dbt semantic layer for Banking Data Platform',
    schedule_interval='0 7 * * *',  # Daily at 7:00 AM (Production - after Gold)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'semantic', 'banking', 'production'],
) as dag:

    # Start
    start = EmptyOperator(
        task_id='start',
        dag=dag,
    )

    # Install dbt packages
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/project/dbt && dbt deps',
        dag=dag,
    )

    # Run semantic models (ephemeral on Gold tables)
    dbt_run_semantic = BashOperator(
        task_id='dbt_run_semantic',
        bash_command='cd /opt/project/dbt && dbt run --select semantic',
        dag=dag,
    )

    # Run tests (source tests on Gold tables)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/project/dbt && dbt test',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # Generate docs
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /opt/project/dbt && dbt docs generate',
        dag=dag,
    )

    # End
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # Task dependencies
    start >> dbt_deps
    dbt_deps >> dbt_run_semantic
    dbt_run_semantic >> dbt_test
    dbt_test >> dbt_docs
    dbt_docs >> end
