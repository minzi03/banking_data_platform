"""
Core Banking Generator — 8 tables
Generates realistic banking data for: branch, product, customer, account,
deposit, loan, txn_account, employee
"""

import random
import string
from datetime import datetime, timedelta
from typing import Any

# Vietnamese names
FIRST_NAMES_MALE = [
    "Nguyen Van", "Tran Minh", "Le Hong", "Pham Duc", "Hoang Anh",
    "Vo Thanh", "Phan Tuan", "Do Minh", "Ngo Quoc", "Dang Khoa",
    "Bui Van", "Duong Ngoc", "Ly Hai", "Ho Thanh", "Nguyen Xuan",
]
FIRST_NAMES_FEMALE = [
    "Nguyen Thi", "Tran Thi", "Le Thi", "Pham Thi", "Hoang Thi",
    "Vo Thi", "Phan Thi", "Do Thi", "Ngo Thi", "Dang Thi",
    "Bui Thi", "Duong Thi", "Ly Thi", "Ho Thi", "Nguyen Mai",
]
LAST_NAMES = [
    "An", "Binh", "Cam", "Dung", "Giang", "Ha", "Huong", "Khanh",
    "Lan", "Linh", "Mai", "Nga", "Oanh", "Phuong", "Quynh",
    "Son", "Tam", "Thao", "Thu", "Trang", "Tuyen", "Uyen", "Yen",
    "Chieu", "Duc", "Fu", "Giang", "Hai", "Kien", "Long",
]
CITIES = [
    "Hanoi", "HCM", "Da Nang", "Hai Phong", "Can Tho",
    "Bien Hoa", "Nha Trang", "Vung Tau", "Hue", "Quy Nhon",
]
DISTRICTS = [
    "District 1", "District 2", "District 3", "District 4", "District 5",
    "District 7", "District 10", "District 12", "Binh Thanh", "Tan Binh",
    "Phu Nhuan", "Go Vap", "Thu Duc", "Nha Be", "Binh Chanh",
]
CHANNELS = ["BRANCH", "ATM", "INTERNET_BANKING", "MOBILE_BANKING", "POS"]
TXN_TYPES = ["DEPOSIT", "WITHDRAWAL", "TRANSFER_IN", "TRANSFER_OUT", "FEE", "INTEREST"]
ROLES = ["TELLER", "MANAGER", "ANALYST", "DIRECTOR"]


def generate_branches(count: int, config: dict) -> list[tuple]:
    """Generate branch data."""
    rows = []
    regions = config.get("regions", ["NORTH", "CENTRAL", "SOUTH"])
    weights = config.get("region_weights", [0.35, 0.20, 0.45])
    statuses = config.get("status", ["ACTIVE", "CLOSED"])
    s_weights = config.get("status_weights", [0.92, 0.08])

    for i in range(1, count + 1):
        code = f"BR{i:03d}"
        region = random.choices(regions, weights=weights)[0]
        city = random.choice(CITIES)
        district = random.choice(DISTRICTS)
        status = random.choices(statuses, weights=s_weights)[0]
        open_date = _random_date("2000-01-01", "2024-12-31")
        manager = _random_name("M") if random.random() < 0.8 else None

        rows.append((
            code,
            f"Chi nhanh {city} {district}",
            region,
            city,
            district,
            f"{random.randint(1, 200)} {random.choice(['Le Loi', 'Nguyen Hue', 'Tran Hung Dao', 'Le Duan', 'Vo Thi Sau'])}, {district}",
            manager,
            open_date,
            status,
            datetime.now(),
        ))
    return rows


def generate_products(config: dict) -> list[tuple]:
    """Generate product data from config."""
    rows = []
    products = config.get("products", [])

    for p in products:
        rows.append((
            p["code"],
            p["name"],
            p["group"],
            p["type"],
            p["currency"],
            1,  # is_active
            _random_date("2015-01-01", "2024-12-31"),
            datetime.now(),
        ))
    return rows


def generate_customers(count: int, config: dict, branch_codes: list[str]) -> list[tuple]:
    """Generate customer data."""
    rows = []
    seg_dist = config.get("segment_distribution", {"RETAIL": 0.70, "PRIORITY": 0.22, "VIP": 0.08})
    kyc_dist = config.get("kyc_distribution", {"VERIFIED": 0.85, "PENDING": 0.10, "REJECTED": 0.05})
    active_rate = config.get("active_rate", 0.94)
    cities = config.get("cities", CITIES)

    segments = list(seg_dist.keys())
    seg_weights = list(seg_dist.values())
    kycs = list(kyc_dist.keys())
    kyc_weights = list(kyc_dist.values())

    for i in range(1, count + 1):
        gender = random.choices(["M", "F", "O"], weights=[0.52, 0.46, 0.02])[0]
        name = _random_name(gender)
        dob = _random_date("1955-01-01", "2005-12-31")
        city = random.choice(cities)
        cccd = f"{random.randint(100000000000, 999999999999)}"
        phone = f"0{random.choice([3, 5, 7, 8, 9])}{random.randint(10000000, 99999999)}"
        email = f"{name.lower().replace(' ', '.')}_{i}@email.com"
        segment = random.choices(segments, weights=seg_weights)[0]
        kyc = random.choices(kycs, weights=kyc_weights)[0]
        branch = random.choice(branch_codes)

        rows.append((
            i,
            cccd,
            name,
            gender,
            dob,
            phone,
            email,
            f"{random.randint(1, 100)} {random.choice(['Le Loi', 'Nguyen Hue', 'Tran Phu'])}",
            city,
            random.choice(DISTRICTS),
            branch,
            segment,
            kyc,
            _random_date("2015-01-01", "2025-06-30"),
            1 if random.random() < active_rate else 0,
            datetime.now(),
        ))
    return rows


def generate_accounts(count: int, config: dict, customer_ids: list[int],
                      branch_codes: list[str], product_codes: list[str]) -> list[tuple]:
    """Generate account data."""
    rows = []
    type_dist = config.get("account_type_distribution", {"CASA": 0.55, "TIME_DEPOSIT": 0.45})
    status_dist = config.get("status_distribution", {"ACTIVE": 0.80, "CLOSED": 0.15, "FROZEN": 0.05})

    acc_types = list(type_dist.keys())
    acc_weights = list(type_dist.values())
    statuses = list(status_dist.keys())
    s_weights = list(status_dist.values())

    # Filter products by group
    deposit_products = [p for p in product_codes if p.startswith("SAV") or p.startswith("CASA")]

    for i in range(1, count + 1):
        cust_id = random.choice(customer_ids)
        acc_type = random.choices(acc_types, weights=acc_weights)[0]
        status = random.choices(statuses, weights=s_weights)[0]
        product = random.choice(deposit_products) if acc_type == "TIME_DEPOSIT" else "CASA001"

        if acc_type == "CASA":
            balance = random.randint(100000, 500000000)
        else:
            balance = random.randint(10000000, 1000000000)

        open_date = _random_date("2018-01-01", "2025-06-30")
        close_date = None
        if status == "CLOSED":
            close_date = _random_date(open_date, "2025-12-31")

        rows.append((
            i,
            f"ACC{i:08d}",
            cust_id,
            product,
            random.choice(branch_codes),
            acc_type,
            "VND",
            balance,
            open_date,
            close_date,
            status,
            datetime.now(),
        ))
    return rows


def generate_deposits(count: int, config: dict, customer_ids: list[int],
                      product_codes: list[str]) -> list[tuple]:
    """Generate deposit (savings certificate) data."""
    rows = []
    terms = config.get("term_options", [1, 3, 6, 12, 24, 36])
    rate_range = config.get("rate_range", [3.0, 8.5])
    principal_range = config.get("principal_range", [5000000, 1000000000])
    status_dist = config.get("status_distribution", {"ACTIVE": 0.60, "MATURED": 0.30, "EARLY_WITHDRAWN": 0.10})

    sav_products = [p for p in product_codes if p.startswith("SAV")]
    statuses = list(status_dist.keys())
    s_weights = list(status_dist.values())

    for i in range(1, count + 1):
        cust_id = random.choice(customer_ids)
        term = random.choice(terms)
        rate = round(random.uniform(rate_range[0], rate_range[1]), 2)
        principal = random.randint(principal_range[0], principal_range[1])
        open_date = _random_date("2020-01-01", "2025-06-30")
        open_dt = datetime.strptime(open_date, "%Y-%m-%d")
        maturity_date = (open_dt + timedelta(days=term * 30)).strftime("%Y-%m-%d")
        status = random.choices(statuses, weights=s_weights)[0]

        rows.append((
            i,
            None,  # account_id (standalone savings)
            cust_id,
            random.choice(sav_products) if sav_products else "SAV001",
            principal,
            rate,
            term,
            open_date,
            maturity_date,
            status,
            datetime.now(),
        ))
    return rows


def generate_loans(count: int, config: dict, customer_ids: list[int],
                   branch_codes: list[str], product_codes: list[str]) -> list[tuple]:
    """Generate loan data."""
    rows = []
    amount_range = config.get("loan_amount_range", [10000000, 5000000000])
    rate_range = config.get("rate_range", [6.0, 15.0])
    terms = config.get("term_options", [6, 12, 24, 36, 60, 120])
    status_dist = config.get("status_distribution", {"ACTIVE": 0.55, "CLOSED": 0.30, "OVERDUE": 0.10, "WRITTEN_OFF": 0.05})

    loan_products = [p for p in product_codes if p.startswith("LOAN")]
    statuses = list(status_dist.keys())
    s_weights = list(status_dist.values())

    for i in range(1, count + 1):
        cust_id = random.choice(customer_ids)
        amount = random.randint(amount_range[0], amount_range[1])
        rate = round(random.uniform(rate_range[0], rate_range[1]), 2)
        term = random.choice(terms)
        status = random.choices(statuses, weights=s_weights)[0]
        disb_date = _random_date("2020-01-01", "2025-03-31")
        disb_dt = datetime.strptime(disb_date, "%Y-%m-%d")
        mat_date = (disb_dt + timedelta(days=term * 30)).strftime("%Y-%m-%d")

        if status == "CLOSED":
            outstanding = 0
        elif status == "WRITTEN_OFF":
            outstanding = round(amount * random.uniform(0.3, 0.8), 2)
        else:
            outstanding = round(amount * random.uniform(0.2, 0.95), 2)

        rows.append((
            i,
            cust_id,
            random.choice(loan_products) if loan_products else "LOAN001",
            random.choice(branch_codes),
            amount,
            outstanding,
            rate,
            term,
            disb_date,
            mat_date,
            status,
            datetime.now(),
        ))
    return rows


def generate_txn_account(count: int, config: dict, account_ids: list[int],
                         customer_map: dict) -> list[tuple]:
    """
    Generate account transaction data.
    customer_map: {account_id: customer_id}
    """
    rows = []
    type_dist = config.get("type_distribution", {})
    dc_dist = config.get("dc_distribution", {"D": 0.55, "C": 0.45})
    channel_dist = config.get("channel_distribution", {})
    amount_range = config.get("amount_range", [10000, 500000000])

    txn_types = list(type_dist.keys())
    txn_weights = list(type_dist.values())
    channels = list(channel_dist.keys())
    ch_weights = list(channel_dist.values())
    dcs = list(dc_dist.keys())
    dc_weights = list(dc_dist.values())

    for i in range(1, count + 1):
        acct_id = random.choice(account_ids)
        cust_id = customer_map.get(acct_id, 1)
        txn_type = random.choices(txn_types, weights=txn_weights)[0]
        dc = random.choices(dcs, weights=dc_weights)[0]
        channel = random.choices(channels, weights=ch_weights)[0]
        amount = round(random.uniform(amount_range[0], amount_range[1]), 2)
        txn_date = _random_datetime("2025-06-01", "2026-08-01")
        balance_after = round(random.uniform(100000, 500000000), 2)

        rows.append((
            i,
            acct_id,
            cust_id,
            txn_date,
            amount,
            txn_type,
            dc,
            balance_after,
            channel,
            f"Transaction {txn_type}",
            None,  # counter_account
            txn_date,  # created_ts
            datetime.now(),
        ))

        if i % 100000 == 0:
            print(f"    ... {i:,}/{count:,} transactions generated")
    return rows


def generate_employees(count: int, config: dict, branch_codes: list[str]) -> list[tuple]:
    """Generate employee data."""
    rows = []
    role_dist = config.get("role_distribution", {"TELLER": 0.40, "MANAGER": 0.20, "ANALYST": 0.30, "DIRECTOR": 0.10})
    salary_range = config.get("salary_range", [8000000, 80000000])
    active_rate = config.get("active_rate", 0.90)

    roles = list(role_dist.keys())
    r_weights = list(role_dist.values())

    for i in range(1, count + 1):
        gender = random.choices(["M", "F"], weights=[0.55, 0.45])[0]
        name = _random_name(gender)
        role = random.choices(roles, weights=r_weights)[0]
        salary = random.randint(salary_range[0], salary_range[1])
        status = "ACTIVE" if random.random() < active_rate else "TERMINATED"
        hire_date = _random_date("2010-01-01", "2025-06-30")

        rows.append((
            i,
            name,
            random.choice(branch_codes),
            role,
            hire_date,
            salary,
            status,
            datetime.now(),
        ))
    return rows


# ── Helpers ──────────────────────────────────────────────────────────────────

def _random_name(gender: str) -> str:
    """Generate a Vietnamese name."""
    if gender == "M":
        first = random.choice(FIRST_NAMES_MALE)
    else:
        first = random.choice(FIRST_NAMES_FEMALE)
    last = random.choice(LAST_NAMES)
    return f"{first} {last}"


def _random_date(start_str: str, end_str: str) -> str:
    """Generate a random date string YYYY-MM-DD."""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    delta = (end - start).days
    if delta <= 0:
        return start_str
    rand_days = random.randint(0, delta)
    return (start + timedelta(days=rand_days)).strftime("%Y-%m-%d")


def _random_datetime(start_str: str, end_str: str) -> datetime:
    """Generate a random datetime."""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    delta = (end - start).total_seconds()
    rand_secs = random.randint(0, int(delta))
    return start + timedelta(seconds=rand_secs)
