#!/usr/bin/env python3
"""
Debezium Connector Registration Script — Banking Data Platform

Registers CDC connectors for PostgreSQL databases:
- core_banking (8 tables)
- card_crm (3 tables)
- digital_banking (5 tables)

Usage:
    python register_connectors.py [--debezium-url http://debezium:8083]
"""

import argparse
import json
import requests
import time
from typing import Dict, List


def wait_for_debezium(url: str, timeout: int = 60) -> bool:
    """Wait for Debezium to be ready."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = requests.get(f"{url}/connectors", timeout=5)
            if response.status_code == 200:
                print(f"✓ Debezium is ready at {url}")
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(2)
    print(f"✗ Debezium not ready after {timeout}s")
    return False


def create_connector(url: str, connector_name: str, config: Dict) -> bool:
    """Create or update a Debezium connector."""
    api_url = f"{url}/connectors/{connector_name}/config"

    try:
        response = requests.put(
            api_url,
            json=config,
            headers={"Content-Type": "application/json"},
            timeout=30
        )

        if response.status_code in [200, 201]:
            print(f"✓ Connector '{connector_name}' created/updated successfully")
            return True
        else:
            print(f"✗ Failed to create connector '{connector_name}': {response.status_code}")
            print(f"  Response: {response.text}")
            return False
    except Exception as e:
        print(f"✗ Error creating connector '{connector_name}': {e}")
        return False


def get_connector_status(url: str, connector_name: str) -> Dict:
    """Get connector status."""
    try:
        response = requests.get(f"{url}/connectors/{connector_name}/status", timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return {}


def main():
    parser = argparse.ArgumentParser(description="Register Debezium CDC connectors")
    parser.add_argument(
        "--debezium-url",
        default="http://debezium:8083",
        help="Debezium Connect URL (default: http://debezium:8083)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print connector configs without registering"
    )
    args = parser.parse_args()

    debezium_url = args.debezium_url.rstrip("/")

    # =========================================================================
    # Connector configurations
    # =========================================================================

    connectors = [
        {
            "name": "banking-core-banking",
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": "postgres",
                "database.port": "5432",
                "database.user": "cdc_user",
                "database.password": "CDCPassword123",
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
        },
        {
            "name": "banking-card-crm",
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": "postgres",
                "database.port": "5432",
                "database.user": "cdc_user",
                "database.password": "CDCPassword123",
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
        },
        {
            "name": "banking-digital-banking",
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": "postgres",
                "database.port": "5432",
                "database.user": "cdc_user",
                "database.password": "CDCPassword123",
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
        }
    ]

    # =========================================================================
    # Dry run: print configs
    # =========================================================================

    if args.dry_run:
        print("=" * 80)
        print("DRY RUN — Connector Configurations")
        print("=" * 80)
        for connector in connectors:
            print(f"\n--- {connector['name']} ---")
            print(json.dumps(connector["config"], indent=2))
        return

    # =========================================================================
    # Wait for Debezium
    # =========================================================================

    print("=" * 80)
    print("Registering Debezium CDC Connectors")
    print("=" * 80)

    if not wait_for_debezium(debezium_url):
        print("Exiting due to Debezium unavailability")
        return 1

    # =========================================================================
    # Register connectors
    # =========================================================================

    success_count = 0
    for connector in connectors:
        if create_connector(debezium_url, connector["name"], connector["config"]):
            success_count += 1

    # =========================================================================
    # Summary
    # =========================================================================

    print("\n" + "=" * 80)
    print(f"Summary: {success_count}/{len(connectors)} connectors registered successfully")
    print("=" * 80)

    # Print status
    print("\nConnector Status:")
    for connector in connectors:
        status = get_connector_status(debezium_url, connector["name"])
        connector_status = status.get("connector", {}).get("state", "UNKNOWN")
        print(f"  {connector['name']}: {connector_status}")

    return 0 if success_count == len(connectors) else 1


if __name__ == "__main__":
    exit(main())
