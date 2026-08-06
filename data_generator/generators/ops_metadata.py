"""
Ops Metadata Generator — source_table_registry
Populates the source table registry with metadata for all source tables.
"""


def generate_source_registry() -> list[tuple]:
    """
    Generate source_table_registry rows.
    Each row maps a source table to its Iceberg Bronze/Silver target.
    """
    from datetime import datetime
    now = datetime.now()

    tables = [
        # (schema, table, source_type, jdbc_conn_id, bronze_table, silver_table, is_active)
        ("core_banking", "branch", "postgresql", "postgres-banking",
         "lakehouse.bronze.core_branch", "lakehouse.silver.dim_branch", 1),
        ("core_banking", "product", "postgresql", "postgres-banking",
         "lakehouse.bronze.core_product", "lakehouse.silver.dim_product", 1),
        ("core_banking", "customer", "postgresql", "postgres-banking",
         "lakehouse.bronze.core_customer", "lakehouse.silver.dim_customer", 1),
        ("core_banking", "account", "postgresql", "postgres-banking",
         "lakehouse.bronze.core_account", "lakehouse.silver.dim_account", 1),
        ("core_banking", "deposit", "postgresql", "postgres-banking",
         "lakehouse.bronze.core_deposit", "lakehouse.silver.dim_deposit", 1),
        ("core_banking", "loan", "postgresql", "postgres-banking",
         "lakehouse.bronze.core_loan", "lakehouse.silver.dim_loan", 1),
        ("core_banking", "txn_account", "postgresql", "postgres-banking",
         "lakehouse.bronze.core_txn_account", "lakehouse.silver.fact_txn_account", 1),
        ("core_banking", "employee", "postgresql", "postgres-banking",
         "lakehouse.bronze.core_employee", "lakehouse.silver.dim_employee", 1),
        ("card_crm", "card", "postgresql", "postgres-banking",
         "lakehouse.bronze.card_card", "lakehouse.silver.dim_card", 1),
        ("card_crm", "card_txn", "postgresql", "postgres-banking",
         "lakehouse.bronze.card_txn", "lakehouse.silver.fact_card_txn", 1),
        ("card_crm", "crm_interaction", "postgresql", "postgres-banking",
         "lakehouse.bronze.crm_interaction", "lakehouse.silver.fact_crm_interaction", 1),
        ("digital_banking", "device", "postgresql", "postgres-banking",
         "lakehouse.bronze.digi_device", None, 1),
        ("digital_banking", "location", "postgresql", "postgres-banking",
         "lakehouse.bronze.digi_location", None, 1),
        ("digital_banking", "online_transaction", "postgresql", "postgres-banking",
         "lakehouse.bronze.digi_online_txn", "lakehouse.silver.fact_online_txn", 1),
        ("digital_banking", "support_ticket", "postgresql", "postgres-banking",
         "lakehouse.bronze.digi_support_ticket", "lakehouse.silver.fact_support_ticket", 1),
        ("digital_banking", "mcc_code", "postgresql", "postgres-banking",
         "lakehouse.bronze.digi_mcc_code", None, 1),
    ]

    rows = []
    for t in tables:
        rows.append((
            t[0],  # schema_name
            t[1],  # table_name
            t[2],  # source_type
            t[3],  # jdbc_conn_id
            t[4],  # bronze_table
            t[5],  # silver_table
            t[6],  # is_active
            now,   # last_updated
        ))
    return rows
