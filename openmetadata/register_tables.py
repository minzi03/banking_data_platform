#!/usr/bin/env python3
"""
Register all tables in OpenMetadata for Banking Data Platform.

This script registers tables in OpenMetadata catalog for:
- Bronze layer (15 batch + 6 CDC tables)
- Silver layer (13 tables)
- Gold layer (19 tables)

Usage:
    python register_tables.py
"""

import requests
import json
from typing import Dict, List, Optional

# OpenMetadata configuration
OM_BASE_URL = "http://localhost:8585"
OM_EMAIL = "admin@open-metadata.org"
OM_PASSWORD = "YWRtaW4="  # Base64 encoded "admin"

# Service configuration
SERVICE_NAME = "TrinoLakehouse"
DATABASE_NAME = "lakehouse"


def get_jwt_token() -> str:
    """Get JWT token from OpenMetadata."""
    url = f"{OM_BASE_URL}/api/v1/users/login"
    payload = {
        "email": OM_EMAIL,
        "password": OM_PASSWORD
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()["accessToken"]


def get_headers(token: str) -> Dict:
    """Get headers with JWT token."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def get_schema_id(token: str, schema_name: str) -> Optional[str]:
    """Get schema ID by name."""
    url = f"{OM_BASE_URL}/api/v1/databaseSchemas"
    params = {
        "database": f"{SERVICE_NAME}.{DATABASE_NAME}",
        "limit": 100
    }
    response = requests.get(url, headers=get_headers(token), params=params)
    response.raise_for_status()

    for schema in response.json().get("data", []):
        if schema["name"] == schema_name:
            return schema["id"]
    return None


def register_table(token: str, schema_name: str, table_name: str, description: str = "") -> Dict:
    """Register a table in OpenMetadata."""
    schema_id = get_schema_id(token, schema_name)
    if not schema_id:
        print(f"  ❌ Schema {schema_name} not found")
        return None

    url = f"{OM_BASE_URL}/api/v1/tables"
    payload = {
        "name": table_name,
        "displayName": table_name.replace("_", " ").title(),
        "description": description or f"Table {table_name} in {schema_name} layer",
        "databaseSchema": {
            "id": schema_id
        }
    }

    response = requests.post(url, headers=get_headers(token), json=payload)
    if response.status_code == 200 or response.status_code == 201:
        print(f"  ✅ Registered: {schema_name}.{table_name}")
        return response.json()
    else:
        print(f"  ❌ Failed to register {schema_name}.{table_name}: {response.status_code}")
        return None


def main():
    """Main function to register all tables."""
    print("=" * 60)
    print("OpenMetadata Table Registration")
    print("=" * 60)

    # Get JWT token
    print("\n1. Getting JWT token...")
    token = get_jwt_token()
    print("   ✅ Token obtained")

    # Define tables to register
    tables = {
        "bronze": [
            # Core Banking
            ("core_banking_customer", "Core banking customer data"),
            ("core_banking_account", "Core banking account data"),
            ("core_banking_branch", "Core banking branch data"),
            ("core_banking_employee", "Core banking employee data"),
            ("core_banking_loan", "Core banking loan data"),
            ("core_banking_txn_account", "Core banking account transactions"),
            ("core_banking_product", "Core banking product data"),
            ("core_banking_deposit", "Core banking deposit data"),
            # Card CRM
            ("card_crm_card", "Card CRM card data"),
            ("card_crm_card_txn", "Card CRM card transactions"),
            ("card_crm_crm_interaction", "Card CRM interactions"),
            # Digital Banking
            ("digital_banking_device", "Digital banking device data"),
            ("digital_banking_location", "Digital banking location data"),
            ("digital_banking_online_transaction", "Digital banking online transactions"),
            ("digital_banking_support_ticket", "Digital banking support tickets"),
            # CDC Tables
            ("core_customer_cdc", "CDC - Core customer real-time updates"),
            ("core_account_cdc", "CDC - Core account real-time updates"),
            ("core_transaction_cdc", "CDC - Core transaction real-time updates"),
            ("card_account_cdc", "CDC - Card account real-time updates"),
            ("card_transaction_cdc", "CDC - Card transaction real-time updates"),
            ("online_transaction_cdc", "CDC - Online transaction real-time updates"),
        ],
        "silver": [
            # Dimensions
            ("dim_branch", "SCD Type 1 - Branch dimension"),
            ("dim_product", "SCD Type 1 - Product dimension"),
            ("dim_employee", "SCD Type 1 - Employee dimension"),
            ("dim_card", "SCD Type 1 - Card dimension"),
            ("dim_device", "SCD Type 1 - Device dimension"),
            ("dim_location", "SCD Type 1 - Location dimension"),
            ("dim_customer", "SCD Type 2 - Customer dimension (history)"),
            ("dim_account", "SCD Type 2 - Account dimension (history)"),
            # Facts
            ("fact_txn_account", "Daily account transaction facts"),
            ("fact_card_txn", "Daily card transaction facts"),
            ("fact_crm_interaction", "Daily CRM interaction facts"),
            ("fact_online_transaction", "Daily online transaction facts"),
            ("fact_support_ticket", "Daily support ticket facts"),
        ],
        "gold": [
            # History tables
            ("mart_customer_360", "Customer 360 history"),
            ("rfm_segment", "RFM segmentation history"),
            ("churn_prediction", "Churn prediction history"),
            ("campaign_target", "Campaign targeting history"),
            ("cross_sell_segment", "Cross-sell segmentation history"),
            ("customer_balance_summary", "Customer balance summary history"),
            ("customer_transaction_summary", "Customer transaction summary history"),
            ("customer_product_summary", "Customer product summary history"),
            ("customer_card_summary", "Customer card summary history"),
            # Current tables (snapshots)
            ("mart_customer_360_current", "Customer 360 current snapshot"),
            ("rfm_segment_current", "RFM segmentation current"),
            ("churn_prediction_current", "Churn prediction current"),
            ("campaign_target_current", "Campaign targeting current"),
            ("cross_sell_segment_current", "Cross-sell segmentation current"),
            ("customer_balance_summary_current", "Customer balance summary current"),
            ("customer_transaction_summary_current", "Customer transaction summary current"),
            ("customer_product_summary_current", "Customer product summary current"),
            ("customer_card_summary_current", "Customer card summary current"),
        ],
    }

    # Register tables
    total_registered = 0
    for schema_name, table_list in tables.items():
        print(f"\n2. Registering {schema_name.upper()} layer tables...")
        for table_name, description in table_list:
            result = register_table(token, schema_name, table_name, description)
            if result:
                total_registered += 1

    print("\n" + "=" * 60)
    print(f"Registration complete: {total_registered} tables registered")
    print("=" * 60)


if __name__ == "__main__":
    main()
