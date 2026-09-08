#!/usr/bin/env python3
"""Insert missing seed data directly into PostgreSQL."""
import psycopg2
import random
from datetime import datetime, timedelta

conn = psycopg2.connect(host="postgres", port=5432, dbname="banking_db",
                        user="banking_admin", password="BankingAdmin123")
conn.autocommit = True
cur = conn.cursor()

# 1. MCC codes (already 109 in table, but merchant needs some)
# 2. Merchant (~2000)
print("Inserting merchants...")
cities = ["Hà Nội", "TP.HCM", "Đà Nẵng", "Hải Phòng", "Cần Thơ", "Nha Trang", "Huế", "Vũng Tàu", "Biên Hòa", "Buôn Ma Thuột"]
categories = ["GROCERY", "RESTAURANT", "TRAVEL", "ECOM", "FUEL", "EDUCATION", "HEALTHCARE", "ENTERTAINMENT", "FASHION", "UTILITIES"]
risk_levels = ["LOW", "MEDIUM", "HIGH"]
risk_weights = [0.7, 0.2, 0.1]
merchants = []
for i in range(1, 2001):
    cat = random.choice(categories)
    city = random.choice(cities)
    merchants.append((
        i, f"Merchant_{i:04d}", cat, f"{cat[:3]}{random.randint(1000,9999)}",
        city, city, random.choices(risk_levels, weights=risk_weights, k=1)[0],
        1, datetime.now()
    ))
cur.executemany(
    "INSERT INTO digital_banking.merchant (merchant_id, merchant_name, merchant_category, "
    "mcc_code, city, state, risk_category, is_active, last_updated) "
    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", merchants)
print(f"  Merchants: {len(merchants)} rows")

# 3. AML Rules (~30)
print("Inserting AML rules...")
cur.execute("""
INSERT INTO core_banking.aml_rule (rule_id, rule_name, rule_code, rule_type, description,
    threshold, threshold_currency, window_hours, severity, is_active, regulatory_ref, created_by, created_at)
VALUES
    (1, 'Large Cash Transaction', 'AML-R001', 'LARGE_CASH', 'Cash transaction above threshold', 2000000000, 'VND', 24, 'HIGH', 1, 'SBV-CIRC35', 'SYSTEM', NOW()),
    (2, 'Velocity Check', 'AML-R002', 'VELOCITY', 'Too many transactions in short period', 50, NULL, 1, 'MEDIUM', 1, 'BCBS239', 'SYSTEM', NOW()),
    (3, 'Structuring', 'AML-R003', 'STRUCTURING', 'Multiple just-below-threshold transactions', 1900000000, 'VND', 24, 'HIGH', 1, 'BSA', 'SYSTEM', NOW()),
    (4, 'High Risk Country', 'AML-R004', 'GEOGRAPHIC', 'Transaction involving high-risk jurisdiction', NULL, NULL, 72, 'MEDIUM', 1, 'FATF', 'SYSTEM', NOW()),
    (5, 'Unusual Pattern', 'AML-R005', 'BEHAVIORAL', 'Transaction pattern deviating from customer baseline', NULL, NULL, 168, 'LOW', 1, 'INTERNAL', 'SYSTEM', NOW()),
    (6, 'PEP Screening', 'AML-R006', 'SCREENING', 'Politically Exposed Person match', NULL, NULL, 24, 'HIGH', 1, 'FATF', 'SYSTEM', NOW()),
    (7, 'Sanctions Hit', 'AML-R007', 'SCREENING', 'Sanctions list match', NULL, NULL, 4, 'CRITICAL', 1, 'OFAC', 'SYSTEM', NOW()),
    (8, 'Dormant Account Activity', 'AML-R008', 'BEHAVIORAL', 'Sudden activity on dormant account', 180, NULL, 24, 'MEDIUM', 1, 'INTERNAL', 'SYSTEM', NOW()),
    (9, 'Round Amount Pattern', 'AML-R009', 'STRUCTURING', 'Multiple round-amount transactions', 10, NULL, 24, 'LOW', 1, 'INTERNAL', 'SYSTEM', NOW()),
    (10, 'Cross-Border Frequency', 'AML-R010', 'GEOGRAPHIC', 'Frequent cross-border transfers', 5, NULL, 168, 'MEDIUM', 1, 'SBV-CIRC35', 'SYSTEM', NOW())
ON CONFLICT DO NOTHING
""")
print(f"  AML Rules: 10 rows")

# 4. AML Alerts (~500)
print("Inserting AML alerts...")
statuses = ['OPEN', 'IN_REVIEW', 'ESCALATED', 'CLOSED', 'FALSE_POSITIVE']
alert_types = ['LARGE_CASH', 'VELOCITY', 'STRUCTURING', 'UNUSUAL_PATTERN', 'DORMANT_ACCOUNT']
risk_categories = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
channels = ['MOBILE_APP', 'WEB', 'BRANCH', 'ATM', 'POS']
for i in range(1, 501):
    cust_id = random.randint(1, 10000)
    txn_id = random.randint(1, 1200000)
    created = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 364))
    cur.execute("""
        INSERT INTO core_banking.aml_alert
        (alert_id, alert_number, rule_id, transaction_id, customer_id, account_id,
         alert_type, risk_score, risk_category, description, txn_amount, txn_date,
         channel, status, priority, due_date, notes, ctr_required, sar_filed)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (
        i, f"ALT-{i:06d}", random.randint(1, 10), txn_id, cust_id, random.randint(1, 30000),
        random.choice(alert_types), round(random.uniform(10, 100), 1),
        random.choice(risk_categories), f"Alert for customer {cust_id}",
        round(random.uniform(1000000, 5000000000), 2), created,
        random.choice(channels), random.choice(statuses),
        random.choice(['LOW', 'MEDIUM', 'HIGH']), created + timedelta(days=random.randint(1, 30)),
        None, 1 if random.random() < 0.3 else 0, 0
    ))
print(f"  AML Alerts: 500 rows")

# 5. AML Customer Risk (~200)
print("Inserting AML customer risk profiles...")
risk_levels_aml = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
risk_weights_aml = [0.5, 0.3, 0.15, 0.05]
for i in range(1, 201):
    cust_id = random.randint(1, 10000)
    rl = random.choices(risk_levels_aml, weights=risk_weights_aml, k=1)[0]
    cur.execute("""
        INSERT INTO core_banking.aml_customer_risk
        (customer_id, risk_level, risk_score, peps_flag, sanctions_flag, adverse_media_flag,
         total_alerts, open_alerts, last_alert_date, last_review_date, next_review_date, edd_required)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (
        cust_id, rl, random.randint(1, 100),
        1 if random.random() < 0.02 else 0,
        1 if random.random() < 0.01 else 0,
        1 if random.random() < 0.05 else 0,
        random.randint(0, 10), random.randint(0, 3),
        datetime(2025, 1, 1) + timedelta(days=random.randint(0, 364)),
        datetime(2025, 6, 1) + timedelta(days=random.randint(0, 180)),
        datetime(2025, 12, 1) + timedelta(days=random.randint(0, 90)),
        1 if rl in ('HIGH', 'CRITICAL') else 0
    ))
print(f"  AML Customer Risk: 200 rows")

# 6. Source Table Registry (~19)
print("Inserting source table registry...")
registry = [
    ('core_banking', 'branch', 'postgresql', 'postgres-etl', 'core_branch', 'dim_branch', 1, datetime.now()),
    ('core_banking', 'product', 'postgresql', 'postgres-etl', 'core_product', 'dim_product', 1, datetime.now()),
    ('core_banking', 'customer', 'postgresql', 'postgres-etl', 'core_customer', 'dim_customer', 1, datetime.now()),
    ('core_banking', 'account', 'postgresql', 'postgres-etl', 'core_account', 'dim_account', 1, datetime.now()),
    ('core_banking', 'deposit', 'postgresql', 'postgres-etl', 'core_deposit', None, 1, datetime.now()),
    ('core_banking', 'loan', 'postgresql', 'postgres-etl', 'core_loan', None, 1, datetime.now()),
    ('core_banking', 'txn_account', 'postgresql', 'postgres-etl', 'core_txn_account', 'fact_txn_account', 1, datetime.now()),
    ('core_banking', 'employee', 'postgresql', 'postgres-etl', 'core_employee', 'dim_employee', 1, datetime.now()),
    ('card_crm', 'card', 'postgresql', 'postgres-etl', 'core_card', 'dim_card', 1, datetime.now()),
    ('card_crm', 'card_txn', 'postgresql', 'postgres-etl', 'core_card_txn', 'fact_card_txn', 1, datetime.now()),
    ('card_crm', 'crm_interaction', 'postgresql', 'postgres-etl', 'core_crm_interaction', 'fact_crm_interaction', 1, datetime.now()),
    ('digital_banking', 'device', 'postgresql', 'postgres-etl', 'core_device', 'dim_device', 1, datetime.now()),
    ('digital_banking', 'location', 'postgresql', 'postgres-etl', 'core_location', 'dim_location', 1, datetime.now()),
    ('digital_banking', 'online_transaction', 'postgresql', 'postgres-etl', 'core_online_transaction', 'fact_online_transaction', 1, datetime.now()),
    ('digital_banking', 'support_ticket', 'postgresql', 'postgres-etl', 'core_support_ticket', 'fact_support_ticket', 1, datetime.now()),
    ('digital_banking', 'mcc_code', 'postgresql', 'postgres-etl', 'core_mcc_code', None, 1, datetime.now()),
    ('digital_banking', 'merchant', 'postgresql', 'postgres-etl', None, None, 1, datetime.now()),
    ('core_banking', 'loan_payment', 'postgresql', 'postgres-etl', None, None, 1, datetime.now()),
    ('core_banking', 'standing_order', 'postgresql', 'postgres-etl', None, None, 1, datetime.now()),
]
cur.executemany(
    "INSERT INTO opslakehouse.source_table_registry "
    "(schema_name, table_name, source_type, jdbc_conn_id, bronze_table, silver_table, is_active, last_updated) "
    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", registry)
print(f"  Source Registry: {len(registry)} rows")

cur.close()
conn.close()
print("\nDone! All missing data inserted.")
