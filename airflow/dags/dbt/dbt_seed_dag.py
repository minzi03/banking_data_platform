# =============================================================================
# Airflow DAG: dbt Seed
# Business Logic: Load seed data for dbt
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
    dag_id='dbt_seed',
    default_args=default_args,
    description='Load seed data for dbt models',
    schedule_interval='@once',  # Run once
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'seed', 'banking'],
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

    # Load seeds
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /opt/project/dbt && dbt seed',
        dag=dag,
    )

    # End
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # Task dependencies
    start >> dbt_deps >> dbt_seed >> end
