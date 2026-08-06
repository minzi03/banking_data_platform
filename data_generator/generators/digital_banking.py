"""
Digital Banking Generator — 5 tables
Generates realistic data for: device, location, online_transaction,
support_ticket, mcc_code
"""

import random
import uuid
from datetime import datetime, timedelta


OS_OPTIONS = ["iOS", "Android", "Windows", "macOS", "Linux"]
MERCHANT_NAMES = [
    "VinMart", "Circle K", "Highlands Coffee", "The Coffee House",
    "Shopee", "Lazada", "Tiki", "Grab", "Be Group",
    "VietJet Air", "Bamboo Airways", "Vietnam Airlines",
    "FPT Shop", "Thế Giới Di Động", "CellphoneS",
    "VinFast", "McDonald's", "KFC", "Pizza Hut",
    "CGV Cinema", "Vinmec Hospital", "Dien May Xanh",
    "PNJ", "Nike Store", "Adidas Store",
    "Nha Sach Fahasa", "Song Hong Books", "The Gioi Di Dong",
    "Phong Vu", "An Phat", "Hoang Ha Mobile",
    "Co.opmart", "Big C", "Lotte Mart", "AEON Mall",
    "Starbucks", "Gong Cha", "Tocotoco", "Highlands Coffee",
    "Uber Eats", "Shopee Food", "Grab Food", "Baemin",
]
CITIES = [
    "Hanoi", "HCM", "Da Nang", "Hai Phong", "Can Tho",
    "Bien Hoa", "Nha Trang", "Vung Tau", "Hue", "Quy Nhon",
]
ISSUE_TYPES = ["TRANSACTION_DISPUTE", "ACCOUNT_ACCESS", "CARD_BLOCK",
               "GENERAL_INQUIRY", "FEEDBACK"]
ISSUE_SUBJECTS = {
    "TRANSACTION_DISPUTE": ["Wrong amount charged", "Duplicate charge", "Unauthorized transaction", "Refund not received"],
    "ACCOUNT_ACCESS": ["Cannot login", "Forgot password", "Account locked", "OTP not received"],
    "CARD_BLOCK": ["Card stolen", "Card lost", "Suspicious activity", "Fraud alert"],
    "GENERAL_INQUIRY": ["Balance inquiry", "Statement request", "Fee schedule", "Interest rate"],
    "FEEDBACK": ["Service quality", "App experience", "Branch feedback", "Staff feedback"],
}


def generate_devices(count: int, config: dict, customer_ids: list[int]) -> list[tuple]:
    """Generate device data."""
    rows = []
    type_dist = config.get("type_distribution", {"MOBILE": 0.65, "TABLET": 0.15, "DESKTOP": 0.20})
    os_options = config.get("os_options", OS_OPTIONS)
    trusted_rate = config.get("trusted_rate", 0.40)

    device_types = list(type_dist.keys())
    dt_weights = list(type_dist.values())

    os_map = {"MOBILE": ["iOS", "Android"], "TABLET": ["iOS", "Android"], "DESKTOP": ["Windows", "macOS", "Linux"]}

    for i in range(1, count + 1):
        cust_id = random.choice(customer_ids)
        device_type = random.choices(device_types, weights=dt_weights)[0]
        os_list = os_map.get(device_type, os_options)
        operating_system = random.choice(os_list)
        fingerprint = str(uuid.uuid4())[:16]
        ip = f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
        is_trusted = 1 if random.random() < trusted_rate else 0
        first_seen = _random_datetime("2022-01-01", "2024-12-31")
        last_seen = _random_datetime(first_seen.strftime("%Y-%m-%d"), "2025-12-31")

        rows.append((
            i,
            cust_id,
            device_type,
            fingerprint,
            operating_system,
            ip,
            is_trusted,
            first_seen,
            last_seen,
            datetime.now(),
        ))
    return rows


def generate_locations(count: int, config: dict) -> list[tuple]:
    """Generate merchant location data."""
    rows = []
    cities = config.get("cities", CITIES)
    high_risk_rate = config.get("high_risk_rate", 0.05)
    merchant_cats = config.get("merchant_categories", ["grocery", "restaurant", "travel"])

    for i in range(1, count + 1):
        city = random.choice(cities)
        merchant = random.choice(MERCHANT_NAMES)
        cat = random.choice(merchant_cats)
        is_high_risk = 1 if random.random() < high_risk_rate else 0

        # Approximate coordinates for Vietnamese cities
        lat_base = {"Hanoi": 21.0, "HCM": 10.8, "Da Nang": 16.0, "Hai Phong": 20.8,
                    "Can Tho": 10.0, "Bien Hoa": 10.9, "Nha Trang": 12.2,
                    "Vung Tau": 10.3, "Hue": 16.5, "Quy Nhon": 13.8}.get(city, 10.8)
        lon_base = {"Hanoi": 105.8, "HCM": 106.7, "Da Nang": 108.2, "Hai Phong": 106.7,
                    "Can Tho": 105.8, "Bien Hoa": 106.8, "Nha Trang": 109.2,
                    "Vung Tau": 107.1, "Hue": 107.6, "Quy Nhon": 109.2}.get(city, 106.7)

        rows.append((
            i,
            merchant,
            cat,
            city,
            city,  # state = city for Vietnam
            round(lat_base + random.uniform(-0.1, 0.1), 7),
            round(lon_base + random.uniform(-0.1, 0.1), 7),
            is_high_risk,
            datetime.now(),
        ))
    return rows


def generate_online_transactions(count: int, config: dict, customer_ids: list[int],
                                  device_ids: list[int], location_ids: list[int]) -> list[tuple]:
    """Generate online transaction data."""
    rows = []
    type_dist = config.get("type_distribution", {})
    channel_dist = config.get("channel_distribution", {})
    status_dist = config.get("status_distribution", {})
    fraud_rate = config.get("fraud_rate", 0.008)
    amount_range = config.get("amount_range", [10000, 100000000])

    txn_types = list(type_dist.keys())
    txn_weights = list(type_dist.values())
    channels = list(channel_dist.keys())
    ch_weights = list(channel_dist.values())
    statuses = list(status_dist.keys())
    s_weights = list(status_dist.values())

    fraud_reasons = [
        "Unusual location", "Velocity check failed", "Amount exceeds limit",
        "Known fraud pattern", "Device fingerprint mismatch",
    ]

    for i in range(1, count + 1):
        cust_id = random.choice(customer_ids)
        device_id = random.choice(device_ids) if device_ids else None
        location_id = random.choice(location_ids) if location_ids else None
        txn_type = random.choices(txn_types, weights=txn_weights)[0]
        channel = random.choices(channels, weights=ch_weights)[0]
        status = random.choices(statuses, weights=s_weights)[0]
        amount = round(random.uniform(amount_range[0], amount_range[1]), 2)
        is_fraud = 1 if random.random() < fraud_rate else 0
        fraud_reason = random.choice(fraud_reasons) if is_fraud else None
        txn_date = _random_datetime("2025-06-01", "2026-08-01")

        rows.append((
            i,
            None,  # account_id (nullable)
            device_id,
            location_id,
            cust_id,
            txn_type,
            channel,
            amount,
            "VND",
            is_fraud,
            fraud_reason,
            status,
            txn_date,
            txn_date,  # created_ts
            datetime.now(),
        ))

        if i % 100000 == 0:
            print(f"    ... {i:,}/{count:,} online transactions generated")
    return rows


def generate_support_tickets(count: int, config: dict, customer_ids: list[int]) -> list[tuple]:
    """Generate support ticket data."""
    rows = []
    issue_types = config.get("issue_types", ISSUE_TYPES)
    priority_dist = config.get("priority_distribution", {})
    status_dist = config.get("status_distribution", {})
    satisfaction_range = config.get("satisfaction_range", [1, 5])

    priorities = list(priority_dist.keys())
    p_weights = list(priority_dist.values())
    statuses = list(status_dist.keys())
    s_weights = list(status_dist.values())

    for i in range(1, count + 1):
        cust_id = random.choice(customer_ids)
        issue = random.choice(issue_types)
        priority = random.choices(priorities, weights=p_weights)[0]
        status = random.choices(statuses, weights=s_weights)[0]
        subject = random.choice(ISSUE_SUBJECTS.get(issue, ["General inquiry"]))

        date_opened = _random_datetime("2024-01-01", "2025-12-31")
        date_resolved = None
        resolution_hrs = None
        satisfaction = None

        if status in ("RESOLVED", "CLOSED"):
            resolution_hrs = round(random.uniform(0.5, 72.0), 2)
            date_resolved = date_opened + timedelta(hours=resolution_hrs)
            satisfaction = random.randint(satisfaction_range[0], satisfaction_range[1])
        elif status == "IN_PROGRESS":
            resolution_hrs = round(random.uniform(0.1, 24.0), 2)

        rows.append((
            i,
            cust_id,
            issue,
            priority,
            status,
            date_opened,
            date_resolved,
            resolution_hrs,
            satisfaction,
            datetime.now(),
        ))
    return rows


def generate_mcc_codes(config: dict) -> list[tuple]:
    """Generate MCC code data from config."""
    rows = []
    codes = config.get("codes", [])

    for c in codes:
        rows.append((
            c["mcc"],
            c["desc"],
            c["group"],
            c["risk"],
            datetime.now(),
        ))

    # Fill up to 109 with generated codes if needed
    existing = len(rows)
    if existing < 109:
        groups = ["RETAIL", "FOOD", "TRAVEL", "SERVICES", "UTILITIES"]
        descs = ["General Retail", "Restaurant", "Gas Station", "Hotel",
                 "Airline", "Telecom", "Healthcare", "Education", "Entertainment"]
        for i in range(existing + 1, 110):
            code = f"{random.randint(1000, 9999)}"
            rows.append((
                code,
                random.choice(descs),
                random.choice(groups),
                1 if random.random() < 0.1 else 0,
                datetime.now(),
            ))
    return rows


# ── Helpers ──────────────────────────────────────────────────────────────────

def _random_datetime(start_str: str, end_str: str) -> datetime:
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    delta = (end - start).total_seconds()
    if delta <= 0:
        return start
    return start + timedelta(seconds=random.randint(0, int(delta)))
