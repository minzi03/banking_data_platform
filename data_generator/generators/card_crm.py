"""
Card & CRM Generator — 3 tables
Generates realistic data for: card, card_txn, crm_interaction
"""

import random
from datetime import datetime, timedelta
from typing import Any


MERCHANT_NAMES = [
    "VinMart", "Circle K", "Highlands Coffee", "The Coffee House",
    "Shopee", "Lazada", "Tiki", "Grab", "Be Group",
    "VietJet Air", "Bamboo Airways", "Vietnam Airlines",
    "PTI Insurance", "Prudential Vietnam", "Manulife",
    "FPT Shop", "Thế Giới Di Động", "CellphoneS",
    "VinFast", "Toyota Vietnam", "Honda Vietnam",
    "McDonald's", "KFC", "Pizza Hut", "Subway",
    "CGV Cinema", "Galaxy Cinema", "Lotteria",
    "Vinmec Hospital", "FPT Hospital", "BV Mat Trung Uong",
    "Dien May Xanh", "PNJ", "Nike Store", "Adidas Store",
    "Nha Sach Fahasa", "Song Hong Books",
]
SUBJECTS_COMPLAINT = [
    "Wrong charge", "Failed transaction", "Card blocked unexpectedly",
    " Unauthorized transaction", "Fee dispute", "Statement error",
    "Late payment penalty dispute", "Interest rate discrepancy",
]
SUBJECTS_INQUIRY = [
    "Balance inquiry", "Statement request", "Card limit increase",
    "Interest rate question", "Account opening", "Card replacement",
    "PIN reset", "Foreign transaction fee",
]
SUBJECTS_CAMPAIGN = [
    "Credit card promotion", "Savings rate offer", "Loan pre-approval",
    "Insurance bundle", "Reward points campaign", "Referral bonus",
]
SUBJECTS_CROSS_SELL = [
    "Credit card offer", "Personal loan offer", "Insurance product",
    "Investment fund", "Premium account upgrade",
]
SUBJECTS_RETENTION = [
    "Win-back call", "Churn prevention", "Loyalty reward",
    "Account closure survey", "Service recovery",
]


def generate_cards(count: int, config: dict, customer_ids: list[int],
                   account_ids: list[int], product_codes: list[str]) -> list[tuple]:
    """Generate card data."""
    rows = []
    type_dist = config.get("type_distribution", {"DEBIT": 0.55, "CREDIT": 0.40, "PREPAID": 0.05})
    brand_dist = config.get("brand_distribution", {"VISA": 0.40, "MASTER": 0.30, "JCB": 0.15, "NAPAS": 0.15})
    limit_range = config.get("credit_limit_range", [5000000, 200000000])
    status_dist = config.get("status_distribution", {"ACTIVE": 0.75, "BLOCKED": 0.05, "EXPIRED": 0.12, "CLOSED": 0.08})
    expiry_range = config.get("expiry_months_range", [12, 60])

    card_types = list(type_dist.keys())
    ct_weights = list(type_dist.values())
    brands = list(brand_dist.keys())
    br_weights = list(brand_dist.values())
    statuses = list(status_dist.keys())
    s_weights = list(status_dist.values())

    card_products = [p for p in product_codes if p.startswith("CRD")]
    used_numbers = set()

    for i in range(1, count + 1):
        cust_id = random.choice(customer_ids)
        card_type = random.choices(card_types, weights=ct_weights)[0]
        brand = random.choices(brands, weights=br_weights)[0]
        status = random.choices(statuses, weights=s_weights)[0]

        # Generate unique masked card number
        prefix = random.randint(4000, 5999)
        suffix = random.randint(1000, 9999)
        masked = f"{prefix}****{suffix}"
        while masked in used_numbers:
            suffix = random.randint(1000, 9999)
            masked = f"{prefix}****{suffix}"
        used_numbers.add(masked)

        issue_date = _random_date("2020-01-01", "2025-06-30")
        issue_dt = datetime.strptime(issue_date, "%Y-%m-%d")
        expiry_months = random.randint(expiry_range[0], expiry_range[1])
        expiry_date = (issue_dt + timedelta(days=expiry_months * 30)).strftime("%Y-%m-%d")

        # Only CREDIT cards have credit_limit
        credit_limit = None
        if card_type == "CREDIT":
            credit_limit = round(random.uniform(limit_range[0], limit_range[1]), 2)

        # Debit cards link to an account
        acct_id = None
        if card_type == "DEBIT":
            acct_id = random.choice(account_ids)

        product = random.choice(card_products) if card_products else (
            "CRD004" if card_type == "DEBIT" else "CRD001"
        )

        rows.append((
            i,
            masked,
            cust_id,
            acct_id,
            product,
            card_type,
            brand,
            credit_limit,
            issue_date,
            expiry_date,
            status,
            datetime.now(),
        ))
    return rows


def generate_card_txn(count: int, config: dict, card_data: list[tuple]) -> list[tuple]:
    """
    Generate card transaction data.
    card_data: list of (card_id, customer_id, card_type, status) tuples
    """
    rows = []
    type_dist = config.get("type_distribution", {"PURCHASE": 0.70, "CASH_ADVANCE": 0.15, "REFUND": 0.10, "REVERSAL": 0.05})
    channel_dist = config.get("channel_distribution", {"POS": 0.45, "ECOM": 0.40, "ATM": 0.15})
    status_dist = config.get("status_distribution", {"SUCCESS": 0.90, "FAILED": 0.07, "PENDING": 0.03})
    amount_range = config.get("amount_range", [50000, 50000000])
    merchant_cats = config.get("merchant_categories", ["GROCERY", "RESTAURANT", "TRAVEL", "ECOM"])

    # Build lookup: card_id -> (customer_id, card_type)
    active_cards = [(c[0], c[1]) for c in card_data if c[3] == "ACTIVE"]

    txn_types = list(type_dist.keys())
    txn_weights = list(type_dist.values())
    channels = list(channel_dist.keys())
    ch_weights = list(channel_dist.values())
    statuses = list(status_dist.keys())
    s_weights = list(status_dist.values())

    for i in range(1, count + 1):
        card_id, cust_id = random.choice(active_cards) if active_cards else (1, 1)
        txn_type = random.choices(txn_types, weights=txn_weights)[0]
        channel = random.choices(channels, weights=ch_weights)[0]
        status = random.choices(statuses, weights=s_weights)[0]
        amount = round(random.uniform(amount_range[0], amount_range[1]), 2)
        merchant = random.choice(MERCHANT_NAMES)
        merchant_cat = random.choice(merchant_cats)
        txn_date = _random_datetime("2025-06-01", "2026-08-01")

        # Refunds and reversals have negative amounts
        if txn_type in ("REFUND", "REVERSAL"):
            amount = -amount

        rows.append((
            i,
            card_id,
            cust_id,
            txn_date,
            amount,
            txn_type,
            "VND",
            merchant,
            merchant_cat,
            channel,
            status,
            txn_date,  # created_ts
            datetime.now(),
        ))

        if i % 100000 == 0:
            print(f"    ... {i:,}/{count:,} card transactions generated")
    return rows


def generate_crm_interactions(count: int, config: dict, customer_ids: list[int]) -> list[tuple]:
    """Generate CRM interaction data."""
    rows = []
    channel_dist = config.get("channel_distribution", {})
    direction_dist = config.get("direction_distribution", {})
    category_dist = config.get("category_distribution", {})
    status_dist = config.get("status_distribution", {})

    channels = list(channel_dist.keys())
    ch_weights = list(channel_dist.values())
    directions = list(direction_dist.keys())
    dir_weights = list(direction_dist.values())
    categories = list(category_dist.keys())
    cat_weights = list(category_dist.values())
    statuses = list(status_dist.keys())
    s_weights = list(status_dist.values())

    subject_map = {
        "COMPLAINT": SUBJECTS_COMPLAINT,
        "INQUIRY": SUBJECTS_INQUIRY,
        "CAMPAIGN": SUBJECTS_CAMPAIGN,
        "CROSS_SELL": SUBJECTS_CROSS_SELL,
        "RETENTION": SUBJECTS_RETENTION,
    }

    for i in range(1, count + 1):
        cust_id = random.choice(customer_ids)
        channel = random.choices(channels, weights=ch_weights)[0]
        direction = random.choices(directions, weights=dir_weights)[0]
        category = random.choices(categories, weights=cat_weights)[0]
        status = random.choices(statuses, weights=s_weights)[0]
        subject = random.choice(subject_map.get(category, ["General inquiry"]))
        assigned = f"Agent_{random.randint(1, 50):03d}" if status != "OPEN" else None

        # Satisfaction score: higher for resolved, lower for open/complaints
        if status == "RESOLVED":
            satisfaction = random.choices([3, 4, 5], weights=[0.2, 0.5, 0.3])[0]
        elif status == "OPEN":
            satisfaction = None
        else:
            satisfaction = random.choices([1, 2, 3, 4, 5], weights=[0.15, 0.25, 0.30, 0.20, 0.10])[0]

        interaction_date = _random_datetime("2024-01-01", "2025-12-31")

        rows.append((
            i,
            cust_id,
            interaction_date,
            channel,
            direction,
            subject,
            category,
            status,
            assigned,
            satisfaction,
            interaction_date,  # created_ts
            datetime.now(),
        ))
    return rows


# ── Helpers ──────────────────────────────────────────────────────────────────

def _random_date(start_str: str, end_str: str) -> str:
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    delta = (end - start).days
    if delta <= 0:
        return start_str
    return (start + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")


def _random_datetime(start_str: str, end_str: str) -> datetime:
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    delta = (end - start).total_seconds()
    return start + timedelta(seconds=random.randint(0, int(delta)))
