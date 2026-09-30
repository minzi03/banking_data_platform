#!/usr/bin/env python3
from __future__ import annotations
"""
Seed Data Generator — Banking Data Platform
Main orchestrator: reads config, calls generators, writes to PostgreSQL.

Usage:
    python generate_all.py
    python generate_all.py --config config/seed_config.yaml
    python generate_all.py --host localhost --port 5432
    python generate_all.py --scale 0.01      # CI smoke test — tiny volume
"""

import argparse
import logging
import os
import sys
import time
import yaml
from datetime import date
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from connectors.postgres_writer import PostgresWriter
from connectors.csv_writer import CsvWriter
from generators.core_banking import (
    generate_branches, generate_products, generate_customers,
    generate_accounts, generate_deposits, generate_loans,
    generate_txn_account, generate_employees,
    generate_loan_payments, generate_standing_orders,
    REGION_CITIES,
)
from generators.card_crm import generate_cards, generate_card_txn, generate_crm_interactions
from generators.digital_banking import (
    generate_devices, generate_locations, generate_online_transactions,
    generate_support_tickets, generate_mcc_codes, generate_merchants,
)
from generators.ops_metadata import generate_source_registry
from generators.timeline import current_as_of, set_as_of
from generators.aml_generator import (
    generate_aml_rules, generate_aml_alerts, generate_aml_customer_risk,
    get_aml_rules_columns, get_aml_alerts_columns, get_aml_customer_risk_columns,
)

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


# Bảng lớn được sinh và ghi theo khối: RAM tỉ lệ với kích thước khối, không với --scale.
# Đo 2026-09-30: ~384 byte/dòng giao dịch; ở --scale 10 giữ cả bốn bảng lớn trong RAM
# là ~14 GB (máy dev còn ~7 GB trống, container airflow-scheduler giới hạn 1,3 GB).
CHUNK_ROWS = 500_000


def write_chunked(writer, csv_writer, schema: str, table: str, columns: list[str], total: int, make_chunk):
    """Ghi `total` dòng theo khối; make_chunk(start_id, count) trả về list tuple."""
    for start in range(1, total + 1, CHUNK_ROWS):
        rows = make_chunk(start, min(CHUNK_ROWS, total - start + 1))
        writer.write_rows(schema, table, columns, rows)
        if csv_writer:
            csv_writer.write_rows(schema, table, columns, rows, append=start > 1)


def apply_scale(config: dict, scale: float) -> dict:
    """Apply a scale factor to all table row_counts.

    Preserves ratios between tables. Reference/dimension tables keep a
    minimum of 10 rows to ensure lookups still work in tests.
    """
    import copy
    config = copy.deepcopy(config)

    REFERENCE_TABLES = {
        "branch", "product", "employee", "mcc_code", "source_table_registry",
    }
    MIN_ROWS = 10

    for section in ("core_banking", "card_crm", "digital_banking"):
        if section not in config:
            continue
        for table_name, table_cfg in config[section].items():
            if isinstance(table_cfg, dict) and "row_count" in table_cfg:
                original = table_cfg["row_count"]
                if table_name in REFERENCE_TABLES:
                    table_cfg["row_count"] = max(MIN_ROWS, int(original * scale))
                else:
                    table_cfg["row_count"] = max(1, int(original * scale))
    return config


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
    parser.add_argument("--password", default=os.environ.get("POSTGRES_PASSWORD"),
                        help="PostgreSQL password (mặc định: $POSTGRES_PASSWORD — không có giá trị viết cứng)")
    parser.add_argument("--truncate", action="store_true",
                        help="Truncate all tables before inserting (clear old data)")
    parser.add_argument("--csv-dir", default=None,
                        help="Also export to CSV files in this directory (e.g., ./output/csv)")
    parser.add_argument("--scale", type=float, default=float(os.environ.get("SEED_SCALE", 1.0)),
                        help="Scale factor applied to every table's row_count "
                             "(e.g. 0.01 for CI smokes). Ratios between tables are preserved. "
                             "Reference tables keep at least 10 rows.")
    parser.add_argument("--as-of", default=os.environ.get("SEED_AS_OF", date.today().isoformat()),
                        help="Ngày giao dịch mới nhất (YYYY-MM-DD), mặc định hôm nay. Đặt bằng "
                             "cob_dt sẽ nạp — mọi ngày sinh ra được dời theo mốc này, nên cửa sổ "
                             "'30 ngày gần nhất' của Gold có dữ liệu. Xem generators/timeline.py.")
    args = parser.parse_args()
    if not args.password:
        parser.error("thiếu mật khẩu PostgreSQL: đặt POSTGRES_PASSWORD (xem RUNBOOK §2) hoặc --password")
    set_as_of(date.fromisoformat(args.as_of))

    logger.info("=" * 60)
    logger.info(" Banking Data Platform — Seed Data Generator")
    logger.info("=" * 60)
    logger.info("Config: %s", args.config)
    logger.info("Target: %s@%s:%d/%s", args.user, args.host, args.port, args.dbname)
    if args.scale != 1.0:
        logger.info("Scale factor: %s (small-volume mode)", args.scale)
    logger.info("As-of: %s (giao dịch mới nhất; nên bằng cob_dt sẽ nạp)", current_as_of())

    config = load_config(args.config)
    if args.scale != 1.0:
        config = apply_scale(config, args.scale)
    cb_cfg = config["core_banking"]
    cc_cfg = config["card_crm"]
    db_cfg = config["digital_banking"]

    # Connect to PostgreSQL
    writer = PostgresWriter(args.host, args.port, args.dbname, args.user, args.password)
    writer.connect()

    # Optional CSV writer
    csv_writer = CsvWriter(args.csv_dir) if args.csv_dir else None

    start_time = time.time()

    try:
        # Schema của Postgres có sẵn có thể cũ hơn DDL (DDL chỉ chạy trên volume
        # rỗng) — đưa nó lên trước khi ghi, nếu không INSERT sẽ chết ở cột mới.
        writer.apply_migrations(Path(__file__).resolve().parent / "migrations")

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
        logger.info("[1/10] Generating branches...")
        branches = generate_branches(cb_cfg["branch"]["row_count"], cb_cfg["branch"])
        branch_codes = [r[0] for r in branches]
        # Build branch_code → city mapping for geographic consistency
        branch_city_map = {r[0]: r[3] for r in branches}  # (code, name, region, city, ...)
        writer.write_rows("core_banking", "branch", [
            "branch_code", "branch_name", "region", "city", "district",
            "address", "manager_name", "open_date", "status", "last_updated"
        ], branches)
        if csv_writer:
            csv_writer.write_rows("core_banking", "branch", [
                "branch_code", "branch_name", "region", "city", "district",
                "address", "manager_name", "open_date", "status", "last_updated"
            ], branches)

        # 2. Product
        logger.info("[2/10] Generating products...")
        products = generate_products(cb_cfg["product"])
        product_codes = [r[0] for r in products]
        writer.write_rows("core_banking", "product", [
            "product_code", "product_name", "product_group", "product_type",
            "currency", "is_active", "launch_date", "last_updated"
        ], products)
        if csv_writer:
            csv_writer.write_rows("core_banking", "product", [
                "product_code", "product_name", "product_group", "product_type",
                "currency", "is_active", "launch_date", "last_updated"
            ], products)

        # 3. Customer
        logger.info("[3/10] Generating customers...")
        customers = generate_customers(
            cb_cfg["customer"]["row_count"], cb_cfg["customer"],
            branch_codes, branch_city_map
        )
        customer_ids = [r[0] for r in customers]
        writer.write_rows("core_banking", "customer", [
            "customer_id", "cccd", "full_name", "gender", "date_of_birth",
            "phone", "email", "address", "city", "district", "branch_code",
            "customer_segment", "kyc_status", "register_date", "is_active", "last_updated"
        ], customers)
        if csv_writer:
            csv_writer.write_rows("core_banking", "customer", [
                "customer_id", "cccd", "full_name", "gender", "date_of_birth",
                "phone", "email", "address", "city", "district", "branch_code",
                "customer_segment", "kyc_status", "register_date", "is_active", "last_updated"
            ], customers)

        # 4. Account
        logger.info("[4/10] Generating accounts...")
        accounts = generate_accounts(
            cb_cfg["account"]["row_count"], cb_cfg["account"],
            customer_ids, branch_codes, product_codes
        )
        account_ids = [r[0] for r in accounts]
        # Build account_id → customer_id map and balance map for txn simulation
        account_customer_map = {r[0]: r[2] for r in accounts}
        account_balances = {r[0]: float(r[7]) for r in accounts}  # r[7] = balance
        writer.write_rows("core_banking", "account", [
            "account_id", "account_no", "customer_id", "product_code", "branch_code",
            "account_type", "currency", "balance", "open_date", "close_date",
            "status", "last_updated"
        ], accounts)
        if csv_writer:
            csv_writer.write_rows("core_banking", "account", [
                "account_id", "account_no", "customer_id", "product_code", "branch_code",
                "account_type", "currency", "balance", "open_date", "close_date",
                "status", "last_updated"
            ], accounts)

        # 5. Deposit
        logger.info("[5/10] Generating deposits...")
        deposits = generate_deposits(
            cb_cfg["deposit"]["row_count"], cb_cfg["deposit"],
            customer_ids, product_codes
        )
        writer.write_rows("core_banking", "deposit", [
            "deposit_id", "account_id", "customer_id", "product_code",
            "principal_amount", "interest_rate", "term_months",
            "open_date", "maturity_date", "status", "last_updated"
        ], deposits)
        if csv_writer:
            csv_writer.write_rows("core_banking", "deposit", [
                "deposit_id", "account_id", "customer_id", "product_code",
                "principal_amount", "interest_rate", "term_months",
                "open_date", "maturity_date", "status", "last_updated"
            ], deposits)

        # 6. Loan
        logger.info("[6/10] Generating loans...")
        loans = generate_loans(
            cb_cfg["loan"]["row_count"], cb_cfg["loan"],
            customer_ids, branch_codes, product_codes
        )
        writer.write_rows("core_banking", "loan", [
            "loan_id", "customer_id", "product_code", "branch_code",
            "loan_amount", "outstanding_balance", "interest_rate", "term_months",
            "disbursement_date", "maturity_date", "loan_status", "last_updated"
        ], loans)
        if csv_writer:
            csv_writer.write_rows("core_banking", "loan", [
                "loan_id", "customer_id", "product_code", "branch_code",
                "loan_amount", "outstanding_balance", "interest_rate", "term_months",
                "disbursement_date", "maturity_date", "loan_status", "last_updated"
            ], loans)

        # 7. Loan Payment (amortization schedule)
        logger.info("[7/10] Generating loan payments...")
        # Theo khối khoản vay (~30 kỳ mỗi khoản): lịch trả của một khoản không bị cắt đôi.
        loans_per_chunk = max(1, CHUNK_ROWS // 30)
        next_payment_id = 1
        for k in range(0, len(loans), loans_per_chunk):
            loan_payments = generate_loan_payments(
                loans[k:k + loans_per_chunk], cb_cfg.get("loan_payment", {}), start_payment_id=next_payment_id
            )
            next_payment_id += len(loan_payments)
            writer.write_rows("core_banking", "loan_payment", [
                "payment_id", "loan_id", "payment_date", "scheduled_amount",
                "amount_paid", "principal_component", "interest_component",
                "penalty", "outstanding_after", "days_late",
                "payment_method", "payment_status", "late_payment_flag", "last_updated"
            ], loan_payments)
            if csv_writer:
                csv_writer.write_rows("core_banking", "loan_payment", [
                    "payment_id", "loan_id", "payment_date", "scheduled_amount",
                    "amount_paid", "principal_component", "interest_component",
                    "penalty", "outstanding_after", "days_late",
                    "payment_method", "payment_status", "late_payment_flag", "last_updated"
                ], loan_payments, append=k > 0)

        # 8. Standing Order
        logger.info("[8/10] Generating standing orders...")
        standing_orders = generate_standing_orders(
            cb_cfg["standing_order"]["row_count"], cb_cfg["standing_order"],
            account_ids, account_customer_map
        )
        writer.write_rows("core_banking", "standing_order", [
            "order_id", "account_id", "customer_id", "order_type",
            "beneficiary_name", "beneficiary_account", "amount",
            "frequency", "next_execute_date", "status", "created_date",
            "last_updated"
        ], standing_orders)
        if csv_writer:
            csv_writer.write_rows("core_banking", "standing_order", [
                "order_id", "account_id", "customer_id", "order_type",
                "beneficiary_name", "beneficiary_account", "amount",
                "frequency", "next_execute_date", "status", "created_date",
                "last_updated"
            ], standing_orders)

        # 9. TXN Account (largest table — uses balance simulation)
        logger.info("[9/10] Generating account transactions (this may take a while)...")
        # Một dict số dư dùng chung cho mọi khối: balance_after nối tiếp qua các khối.
        balances = dict(account_balances)
        write_chunked(
            writer, csv_writer, "core_banking", "txn_account", [
                "txn_id", "account_id", "customer_id", "txn_date", "txn_amount",
                "txn_type", "debit_credit", "balance_after", "channel",
                "description", "counter_account", "created_ts", "last_updated"
            ],
            cb_cfg["txn_account"]["row_count"],
            lambda start, n: generate_txn_account(
                n, cb_cfg["txn_account"], account_ids, account_customer_map, account_balances,
                start_id=start, running_balances=balances,
            ),
        )

        # 10. Employee
        logger.info("[10/10] Generating employees...")
        employees = generate_employees(cb_cfg["employee"]["row_count"], cb_cfg["employee"], branch_codes)
        writer.write_rows("core_banking", "employee", [
            "employee_id", "full_name", "branch_code", "role",
            "hire_date", "salary", "status", "last_updated"
        ], employees)
        if csv_writer:
            csv_writer.write_rows("core_banking", "employee", [
                "employee_id", "full_name", "branch_code", "role",
                "hire_date", "salary", "status", "last_updated"
            ], employees)

        # ── Pre-generate MCC codes (needed by Card & CRM and Digital Banking) ──
        logger.info("")
        logger.info("═══ MCC CODES (shared reference) ═══")
        mcc = generate_mcc_codes(db_cfg["mcc_code"])
        mcc_code_list = [r[0] for r in mcc]
        writer.write_rows("digital_banking", "mcc_code", [
            "mcc_code", "description", "category_group", "is_high_risk", "last_updated"
        ], mcc)
        if csv_writer:
            csv_writer.write_rows("digital_banking", "mcc_code", [
                "mcc_code", "description", "category_group", "is_high_risk", "last_updated"
            ], mcc)

        # ── Card & CRM (3 tables) ───────────────────────────────────────
        logger.info("")
        logger.info("═══ CARD & CRM ═══")

        # Card
        logger.info("  Generating cards...")
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
        if csv_writer:
            csv_writer.write_rows("card_crm", "card", [
                "card_id", "card_no_masked", "customer_id", "account_id",
                "product_code", "card_type", "card_brand", "credit_limit",
                "issue_date", "expiry_date", "status", "last_updated"
            ], cards)

        # Card TXN
        logger.info("  Generating card transactions...")
        write_chunked(
            writer, csv_writer, "card_crm", "card_txn", [
                "txn_id", "card_id", "customer_id", "txn_date", "txn_amount",
                "txn_type", "currency", "merchant_name", "merchant_category",
                "mcc_code", "channel", "status", "entry_mode", "decline_reason", "processing_time_ms",
                "reference_number", "created_ts", "last_updated"
            ],
            cc_cfg["card_txn"]["row_count"],
            lambda start, n: generate_card_txn(n, cc_cfg["card_txn"], card_data, mcc_code_list, start_id=start),
        )

        # CRM Interaction
        logger.info("  Generating CRM interactions...")
        crm = generate_crm_interactions(cc_cfg["crm_interaction"]["row_count"], cc_cfg["crm_interaction"], customer_ids)
        writer.write_rows("card_crm", "crm_interaction", [
            "interaction_id", "customer_id", "interaction_date", "channel",
            "direction", "subject", "category", "status", "assigned_to",
            "satisfaction_score", "created_ts", "last_updated"
        ], crm)
        if csv_writer:
            csv_writer.write_rows("card_crm", "crm_interaction", [
                "interaction_id", "customer_id", "interaction_date", "channel",
                "direction", "subject", "category", "status", "assigned_to",
                "satisfaction_score", "created_ts", "last_updated"
            ], crm)

        # ── Digital Banking (5 tables) ──────────────────────────────────
        logger.info("")
        logger.info("═══ DIGITAL BANKING ═══")

        # Device
        logger.info("  Generating devices...")
        devices = generate_devices(db_cfg["device"]["row_count"], db_cfg["device"], customer_ids)
        device_ids = [r[0] for r in devices]
        writer.write_rows("digital_banking", "device", [
            "device_id", "customer_id", "device_type", "device_fingerprint",
            "operating_system", "ip_address", "is_trusted", "first_seen",
            "last_seen", "last_updated"
        ], devices)
        if csv_writer:
            csv_writer.write_rows("digital_banking", "device", [
                "device_id", "customer_id", "device_type", "device_fingerprint",
                "operating_system", "ip_address", "is_trusted", "first_seen",
                "last_seen", "last_updated"
            ], devices)

        # Location
        logger.info("  Generating locations...")
        locations = generate_locations(db_cfg["location"]["row_count"], db_cfg["location"])
        location_ids = [r[0] for r in locations]
        # Build list of high-risk location IDs for fraud correlation
        high_risk_location_ids = [r[0] for r in locations if r[7] == 1]  # r[7] = is_high_risk_area
        writer.write_rows("digital_banking", "location", [
            "location_id", "merchant_name", "merchant_category", "city",
            "state", "latitude", "longitude", "is_high_risk_area", "last_updated"
        ], locations)
        if csv_writer:
            csv_writer.write_rows("digital_banking", "location", [
                "location_id", "merchant_name", "merchant_category", "city",
                "state", "latitude", "longitude", "is_high_risk_area", "last_updated"
            ], locations)

        # Online Transaction
        logger.info("  Generating online transactions (this may take a while)...")
        write_chunked(
            writer, csv_writer, "digital_banking", "online_transaction", [
                "transaction_id", "account_id", "device_id", "location_id",
                "customer_id", "transaction_type", "channel", "amount", "currency",
                "is_fraud", "fraud_reason", "status", "transaction_date",
                "created_ts", "last_updated"
            ],
            db_cfg["online_transaction"]["row_count"],
            lambda start, n: generate_online_transactions(
                n, db_cfg["online_transaction"], customer_ids, device_ids, location_ids,
                high_risk_location_ids, start_id=start,
            ),
        )

        # Support Ticket
        logger.info("  Generating support tickets...")
        tickets = generate_support_tickets(db_cfg["support_ticket"]["row_count"], db_cfg["support_ticket"], customer_ids)
        writer.write_rows("digital_banking", "support_ticket", [
            "ticket_id", "customer_id", "issue_type", "priority", "status",
            "date_opened", "date_resolved", "resolution_time_hrs",
            "satisfaction_score", "last_updated"
        ], tickets)
        if csv_writer:
            csv_writer.write_rows("digital_banking", "support_ticket", [
                "ticket_id", "customer_id", "issue_type", "priority", "status",
                "date_opened", "date_resolved", "resolution_time_hrs",
                "satisfaction_score", "last_updated"
            ], tickets)

        # Merchant
        logger.info("  Generating merchants...")
        merchants = generate_merchants(
            db_cfg.get("merchant", {}).get("row_count", 2000),
            db_cfg.get("merchant", {}),
            mcc_code_list,
            db_cfg.get("location", {}).get("cities")
        )
        writer.write_rows("digital_banking", "merchant", [
            "merchant_id", "merchant_name", "merchant_category", "mcc_code",
            "city", "state", "risk_category", "is_active", "last_updated"
        ], merchants)
        if csv_writer:
            csv_writer.write_rows("digital_banking", "merchant", [
                "merchant_id", "merchant_name", "merchant_category", "mcc_code",
                "city", "state", "risk_category", "is_active", "last_updated"
            ], merchants)

        # ── Ops Metadata ────────────────────────────────────────────────
        logger.info("")
        logger.info("═══ OPS METADATA ═══")
        logger.info("  Populating source_table_registry...")
        registry = generate_source_registry()
        writer.write_rows("opslakehouse", "source_table_registry", [
            "schema_name", "table_name", "source_type", "jdbc_conn_id",
            "bronze_table", "silver_table", "is_active", "last_updated"
        ], registry)
        if csv_writer:
            csv_writer.write_rows("opslakehouse", "source_table_registry", [
                "schema_name", "table_name", "source_type", "jdbc_conn_id",
                "bronze_table", "silver_table", "is_active", "last_updated"
            ], registry)

        # ── AML (Anti-Money Laundering) ────────────────────────────────
        logger.info("")
        logger.info("═══ AML (ANTI-MONEY LAUNDERING) ═══")

        # AML Rules
        logger.info("  Generating AML rules...")
        aml_rules = generate_aml_rules()
        writer.write_rows("core_banking", "aml_rule", get_aml_rules_columns(), aml_rules)
        if csv_writer:
            csv_writer.write_rows("core_banking", "aml_rule", get_aml_rules_columns(), aml_rules)

        # AML Alerts
        logger.info("  Generating AML alerts...")
        aml_alerts = generate_aml_alerts(
            num_alerts=500,
            max_customer_id=len(customer_ids),
            max_txn_id=cb_cfg["txn_account"]["row_count"],
            max_employee_id=cb_cfg["employee"]["row_count"],
        )
        writer.write_rows("core_banking", "aml_alert", get_aml_alerts_columns(), aml_alerts)
        if csv_writer:
            csv_writer.write_rows("core_banking", "aml_alert", get_aml_alerts_columns(), aml_alerts)

        # AML Customer Risk Profiles
        logger.info("  Generating AML customer risk profiles...")
        aml_risk = generate_aml_customer_risk(
            num_customers=200,
            max_customer_id=len(customer_ids),
        )
        writer.write_rows("core_banking", "aml_customer_risk", get_aml_customer_risk_columns(), aml_risk)
        if csv_writer:
            csv_writer.write_rows("core_banking", "aml_customer_risk", get_aml_customer_risk_columns(), aml_risk)

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
            ("core_banking", "loan_payment"), ("core_banking", "standing_order"),
            ("core_banking", "txn_account"), ("core_banking", "employee"),
            ("card_crm", "card"), ("card_crm", "card_txn"),
            ("card_crm", "crm_interaction"),
            ("digital_banking", "device"), ("digital_banking", "location"),
            ("digital_banking", "online_transaction"),
            ("digital_banking", "support_ticket"), ("digital_banking", "mcc_code"),
            ("digital_banking", "merchant"),
            ("opslakehouse", "source_table_registry"),
            ("core_banking", "aml_rule"),
            ("core_banking", "aml_alert"),
            ("core_banking", "aml_customer_risk"),
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
