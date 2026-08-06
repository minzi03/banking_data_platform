#!/usr/bin/env python3
"""
Seed Data Generator — Banking Data Platform
Main orchestrator: reads config, calls generators, writes to PostgreSQL.

Usage:
    python generate_all.py
    python generate_all.py --config config/seed_config.yaml
    python generate_all.py --host localhost --port 5432
"""

import argparse
import logging
import os
import sys
import time
import yaml
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from connectors.postgres_writer import PostgresWriter
from generators.core_banking import (
    generate_branches, generate_products, generate_customers,
    generate_accounts, generate_deposits, generate_loans,
    generate_txn_account, generate_employees,
)
from generators.card_crm import generate_cards, generate_card_txn, generate_crm_interactions
from generators.digital_banking import (
    generate_devices, generate_locations, generate_online_transactions,
    generate_support_tickets, generate_mcc_codes,
)
from generators.ops_metadata import generate_source_registry

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("seed_generator")


def load_config(config_path: str) -> dict:
    """Load YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="Banking Data Platform — Seed Data Generator")
    parser.add_argument("--config", default=str(Path(__file__).parent / "config" / "seed_config.yaml"),
                        help="Path to seed_config.yaml")
    parser.add_argument("--host", default=os.environ.get("POSTGRES_HOST", "postgres"),
                        help="PostgreSQL host (default: postgres)")
    parser.add_argument("--port", type=int, default=int(os.environ.get("POSTGRES_PORT", 5432)),
                        help="PostgreSQL port (default: 5432)")
    parser.add_argument("--dbname", default=os.environ.get("POSTGRES_DB", "banking_db"),
                        help="PostgreSQL database (default: banking_db)")
    parser.add_argument("--user", default=os.environ.get("POSTGRES_USER", "banking_admin"),
                        help="PostgreSQL user (default: banking_admin)")
    parser.add_argument("--password", default=os.environ.get("POSTGRES_PASSWORD", "BankingAdmin123"),
                        help="PostgreSQL password")
    parser.add_argument("--truncate", action="store_true",
                        help="Truncate all tables before inserting (clear old data)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info(" Banking Data Platform — Seed Data Generator")
    logger.info("=" * 60)
    logger.info("Config: %s", args.config)
    logger.info("Target: %s@%s:%d/%s", args.user, args.host, args.port, args.dbname)

    config = load_config(args.config)
    cb_cfg = config["core_banking"]
    cc_cfg = config["card_crm"]
    db_cfg = config["digital_banking"]

    # Connect to PostgreSQL
    writer = PostgresWriter(args.host, args.port, args.dbname, args.user, args.password)
    writer.connect()

    start_time = time.time()

    try:
        # Disable triggers for faster bulk load
        writer.disable_triggers("all")

        # Truncate all tables if --truncate flag is set
        if args.truncate:
            logger.info("Truncating all tables...")
            writer.truncate_all()

        # ── Core Banking (8 tables) ─────────────────────────────────────
        logger.info("")
        logger.info("═══ CORE BANKING ═══")

        # 1. Branch
        logger.info("[1/8] Generating branches...")
        branches = generate_branches(cb_cfg["branch"]["row_count"], cb_cfg["branch"])
        branch_codes = [r[0] for r in branches]
        writer.write_rows("core_banking", "branch", [
            "branch_code", "branch_name", "region", "city", "district",
            "address", "manager_name", "open_date", "status", "last_updated"
        ], branches)

        # 2. Product
        logger.info("[2/8] Generating products...")
        products = generate_products(cb_cfg["product"])
        product_codes = [r[0] for r in products]
        writer.write_rows("core_banking", "product", [
            "product_code", "product_name", "product_group", "product_type",
            "currency", "is_active", "launch_date", "last_updated"
        ], products)

        # 3. Customer
        logger.info("[3/8] Generating customers...")
        customers = generate_customers(cb_cfg["customer"]["row_count"], cb_cfg["customer"], branch_codes)
        customer_ids = [r[0] for r in customers]
        writer.write_rows("core_banking", "customer", [
            "customer_id", "cccd", "full_name", "gender", "date_of_birth",
            "phone", "email", "address", "city", "district", "branch_code",
            "customer_segment", "kyc_status", "register_date", "is_active", "last_updated"
        ], customers)

        # 4. Account
        logger.info("[4/8] Generating accounts...")
        accounts = generate_accounts(
            cb_cfg["account"]["row_count"], cb_cfg["account"],
            customer_ids, branch_codes, product_codes
        )
        account_ids = [r[0] for r in accounts]
        # Build account_id -> customer_id map
        account_customer_map = {r[0]: r[2] for r in accounts}
        writer.write_rows("core_banking", "account", [
            "account_id", "account_no", "customer_id", "product_code", "branch_code",
            "account_type", "currency", "balance", "open_date", "close_date",
            "status", "last_updated"
        ], accounts)

        # 5. Deposit
        logger.info("[5/8] Generating deposits...")
        deposits = generate_deposits(
            cb_cfg["deposit"]["row_count"], cb_cfg["deposit"],
            customer_ids, product_codes
        )
        writer.write_rows("core_banking", "deposit", [
            "deposit_id", "account_id", "customer_id", "product_code",
            "principal_amount", "interest_rate", "term_months",
            "open_date", "maturity_date", "status", "last_updated"
        ], deposits)

        # 6. Loan
        logger.info("[6/8] Generating loans...")
        loans = generate_loans(
            cb_cfg["loan"]["row_count"], cb_cfg["loan"],
            customer_ids, branch_codes, product_codes
        )
        writer.write_rows("core_banking", "loan", [
            "loan_id", "customer_id", "product_code", "branch_code",
            "loan_amount", "outstanding_balance", "interest_rate", "term_months",
            "disbursement_date", "maturity_date", "loan_status", "last_updated"
        ], loans)

        # 7. TXN Account (largest table)
        logger.info("[7/8] Generating account transactions (this may take a while)...")
        txns = generate_txn_account(
            cb_cfg["txn_account"]["row_count"], cb_cfg["txn_account"],
            account_ids, account_customer_map
        )
        writer.write_rows("core_banking", "txn_account", [
            "txn_id", "account_id", "customer_id", "txn_date", "txn_amount",
            "txn_type", "debit_credit", "balance_after", "channel",
            "description", "counter_account", "created_ts", "last_updated"
        ], txns)

        # 8. Employee
        logger.info("[8/8] Generating employees...")
        employees = generate_employees(cb_cfg["employee"]["row_count"], cb_cfg["employee"], branch_codes)
        writer.write_rows("core_banking", "employee", [
            "employee_id", "full_name", "branch_code", "role",
            "hire_date", "salary", "status", "last_updated"
        ], employees)

        # ── Card & CRM (3 tables) ───────────────────────────────────────
        logger.info("")
        logger.info("═══ CARD & CRM ═══")

        # 9. Card
        logger.info("[9/11] Generating cards...")
        cards = generate_cards(
            cc_cfg["card"]["row_count"], cc_cfg["card"],
            customer_ids, account_ids, product_codes
        )
        # Extract card data for card_txn generator
        card_data = [(r[0], r[2], r[5], r[10]) for r in cards]  # (card_id, customer_id, card_type, status)
        writer.write_rows("card_crm", "card", [
            "card_id", "card_no_masked", "customer_id", "account_id",
            "product_code", "card_type", "card_brand", "credit_limit",
            "issue_date", "expiry_date", "status", "last_updated"
        ], cards)

        # 10. Card TXN
        logger.info("[10/11] Generating card transactions...")
        card_txns = generate_card_txn(cc_cfg["card_txn"]["row_count"], cc_cfg["card_txn"], card_data)
        writer.write_rows("card_crm", "card_txn", [
            "txn_id", "card_id", "customer_id", "txn_date", "txn_amount",
            "txn_type", "currency", "merchant_name", "merchant_category",
            "channel", "status", "created_ts", "last_updated"
        ], card_txns)

        # 11. CRM Interaction
        logger.info("[11/11] Generating CRM interactions...")
        crm = generate_crm_interactions(cc_cfg["crm_interaction"]["row_count"], cc_cfg["crm_interaction"], customer_ids)
        writer.write_rows("card_crm", "crm_interaction", [
            "interaction_id", "customer_id", "interaction_date", "channel",
            "direction", "subject", "category", "status", "assigned_to",
            "satisfaction_score", "created_ts", "last_updated"
        ], crm)

        # ── Digital Banking (5 tables) ──────────────────────────────────
        logger.info("")
        logger.info("═══ DIGITAL BANKING ═══")

        # 12. Device
        logger.info("[12/16] Generating devices...")
        devices = generate_devices(db_cfg["device"]["row_count"], db_cfg["device"], customer_ids)
        device_ids = [r[0] for r in devices]
        writer.write_rows("digital_banking", "device", [
            "device_id", "customer_id", "device_type", "device_fingerprint",
            "operating_system", "ip_address", "is_trusted", "first_seen",
            "last_seen", "last_updated"
        ], devices)

        # 13. Location
        logger.info("[13/16] Generating locations...")
        locations = generate_locations(db_cfg["location"]["row_count"], db_cfg["location"])
        location_ids = [r[0] for r in locations]
        writer.write_rows("digital_banking", "location", [
            "location_id", "merchant_name", "merchant_category", "city",
            "state", "latitude", "longitude", "is_high_risk_area", "last_updated"
        ], locations)

        # 14. Online Transaction
        logger.info("[14/16] Generating online transactions (this may take a while)...")
        online_txns = generate_online_transactions(
            db_cfg["online_transaction"]["row_count"], db_cfg["online_transaction"],
            customer_ids, device_ids, location_ids
        )
        writer.write_rows("digital_banking", "online_transaction", [
            "transaction_id", "account_id", "device_id", "location_id",
            "customer_id", "transaction_type", "channel", "amount", "currency",
            "is_fraud", "fraud_reason", "status", "transaction_date",
            "created_ts", "last_updated"
        ], online_txns)

        # 15. Support Ticket
        logger.info("[15/16] Generating support tickets...")
        tickets = generate_support_tickets(db_cfg["support_ticket"]["row_count"], db_cfg["support_ticket"], customer_ids)
        writer.write_rows("digital_banking", "support_ticket", [
            "ticket_id", "customer_id", "issue_type", "priority", "status",
            "date_opened", "date_resolved", "resolution_time_hrs",
            "satisfaction_score", "last_updated"
        ], tickets)

        # 16. MCC Code
        logger.info("[16/16] Generating MCC codes...")
        mcc = generate_mcc_codes(db_cfg["mcc_code"])
        writer.write_rows("digital_banking", "mcc_code", [
            "mcc_code", "description", "category_group", "is_high_risk", "last_updated"
        ], mcc)

        # ── Ops Metadata ────────────────────────────────────────────────
        logger.info("")
        logger.info("═══ OPS METADATA ═══")
        logger.info("[+] Populating source_table_registry...")
        registry = generate_source_registry()
        writer.write_rows("opslakehouse", "source_table_registry", [
            "schema_name", "table_name", "source_type", "jdbc_conn_id",
            "bronze_table", "silver_table", "is_active", "last_updated"
        ], registry)

        # Re-enable triggers
        writer.enable_triggers()

        # ── Summary ─────────────────────────────────────────────────────
        elapsed = time.time() - start_time
        logger.info("")
        logger.info("=" * 60)
        logger.info(" SEED DATA GENERATION COMPLETE")
        logger.info("=" * 60)
        logger.info(" Elapsed: %.1f seconds", elapsed)
        logger.info("")

        # Print row counts
        logger.info(" Row Counts:")
        logger.info(" ─" * 30)
        tables = [
            ("core_banking", "branch"), ("core_banking", "product"),
            ("core_banking", "customer"), ("core_banking", "account"),
            ("core_banking", "deposit"), ("core_banking", "loan"),
            ("core_banking", "txn_account"), ("core_banking", "employee"),
            ("card_crm", "card"), ("card_crm", "card_txn"),
            ("card_crm", "crm_interaction"),
            ("digital_banking", "device"), ("digital_banking", "location"),
            ("digital_banking", "online_transaction"),
            ("digital_banking", "support_ticket"), ("digital_banking", "mcc_code"),
            ("opslakehouse", "source_table_registry"),
        ]
        total_rows = 0
        for schema, table in tables:
            count = writer.get_row_count(schema, table)
            total_rows += count
            logger.info("   %s.%-25s %s rows", schema, table, f"{count:>10,}")
        logger.info(" ─" * 30)
        logger.info("   TOTAL                           %s rows", f"{total_rows:>10,}")
        logger.info("")

    except Exception as e:
        logger.error("SEED GENERATION FAILED: %s", str(e), exc_info=True)
        writer.enable_triggers()
        sys.exit(1)

    finally:
        writer.close()


if __name__ == "__main__":
    main()
