"""
Airflow DAG: CDC Register Connectors

Registers Debezium CDC connectors for PostgreSQL databases.
Run this DAG once to setup CDC infrastructure.

Schedule: Manual trigger only
"""

import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
import requests
import json


DEBEZIUM_URL = "http://debezium:8083"
CDC_DB_USER = os.environ.get("CDC_DB_USER", "cdc_user")
CDC_DB_PASSWORD = os.environ.get("CDC_DB_PASSWORD", "CDCPassword123")


def wait_for_debezium():
    """Wait for Debezium to be ready."""
    import time
    timeout = 120
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = requests.get(f"{DEBEZIUM_URL}/connectors", timeout=5)
            if response.status_code == 200:
                print(f"✓ Debezium is ready")
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(2)
    raise Exception(f"Debezium not ready after {timeout}s")


def register_connector(connector_name: str, config: dict):
    """Register a single Debezium connector."""
    url = f"{DEBEZIUM_URL}/connectors/{connector_name}/config"
    response = requests.put(
        url,
        json=config,
        headers={"Content-Type": "application/json"},
        timeout=30
    )
    if response.status_code in [200, 201]:
        print(f"✓ Connector '{connector_name}' registered successfully")
    else:
        raise Exception(f"Failed to register connector '{connector_name}': {response.status_code} {response.text}")


def get_core_banking_config():
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": CDC_DB_USER,
        "database.password": CDC_DB_PASSWORD,
        "database.dbname": "banking_db",
        "topic.prefix": "postgresql.banking",
        "plugin.name": "pgoutput",
        "schema.include.list": "core_banking",
        "table.include.list": (
            "core_banking.customer,"
            "core_banking.account,"
            "core_banking.branch,"
            "core_banking.employee,"
            "core_banking.loan,"
            "core_banking.txn_account"
        ),
        "slot.name": "debezium_core_banking",
        "publication.name": "debezium_pub_core",
        "publication.autocreate.mode": "disabled",
        "heartbeat.interval.ms": "10000",
        "snapshot.mode": "always",
        "tombstones.on.delete": "true",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.add.fields": "op,ts_ms,source.ts_ms"
    }


def get_card_crm_config():
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": CDC_DB_USER,
        "database.password": CDC_DB_PASSWORD,
        "database.dbname": "banking_db",
        "topic.prefix": "postgresql.banking",
        "plugin.name": "pgoutput",
        "schema.include.list": "card_crm",
        "table.include.list": (
            "card_crm.card,"
            "card_crm.card_txn,"
            "card_crm.crm_interaction"
        ),
        "slot.name": "debezium_card_crm",
        "publication.name": "debezium_pub_card",
        "publication.autocreate.mode": "disabled",
        "heartbeat.interval.ms": "10000",
        "snapshot.mode": "always",
        "tombstones.on.delete": "true",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.add.fields": "op,ts_ms,source.ts_ms"
    }


def get_digital_banking_config():
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": CDC_DB_USER,
        "database.password": CDC_DB_PASSWORD,
        "database.dbname": "banking_db",
        "topic.prefix": "postgresql.banking",
        "plugin.name": "pgoutput",
        "schema.include.list": "digital_banking",
        "table.include.list": (
            "digital_banking.online_transaction,"
            "digital_banking.device,"
            "digital_banking.support_ticket"
        ),
        "slot.name": "debezium_digital_banking",
        "publication.name": "debezium_pub_digital",
        "publication.autocreate.mode": "disabled",
        "heartbeat.interval.ms": "10000",
        "snapshot.mode": "always",
        "tombstones.on.delete": "true",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.add.fields": "op,ts_ms,source.ts_ms"
    }


with DAG(
    dag_id="cdc_register_connectors",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "setup"],
    doc_md="""
    ## CDC Register Connectors

    Registers Debezium CDC connectors for PostgreSQL databases:
    - core_banking (6 tables)
    - card_crm (3 tables)
    - digital_banking (3 tables)

    Run this DAG once to setup CDC infrastructure.
    """,
) as dag:

    # Task 1: Wait for Debezium
    wait_debezium = PythonOperator(
        task_id="wait_for_debezium",
        python_callable=wait_for_debezium,
    )

    # Task 2: Register core_banking connector
    register_core_banking = PythonOperator(
        task_id="register_core_banking",
        python_callable=register_connector,
        op_args=["banking-core-banking", get_core_banking_config()],
    )

    # Task 3: Register card_crm connector
    register_card_crm = PythonOperator(
        task_id="register_card_crm",
        python_callable=register_connector,
        op_args=["banking-card-crm", get_card_crm_config()],
    )

    # Task 4: Register digital_banking connector
    register_digital_banking = PythonOperator(
        task_id="register_digital_banking",
        python_callable=register_connector,
        op_args=["banking-digital-banking", get_digital_banking_config()],
    )

    # Task 5: Verify connectors
    verify_connectors = BashOperator(
        task_id="verify_connectors",
        bash_command='curl -sf http://debezium:8083/connectors | python3 -m json.tool',
    )

    # Dependencies
    wait_debezium >> [register_core_banking, register_card_crm, register_digital_banking] >> verify_connectors
