from __future__ import annotations
"""
Core Banking Generator — 8 tables
Generates realistic banking data for: branch, product, customer, account,
deposit, loan, txn_account, employee
"""

import random
import string
from datetime import datetime, timedelta
from typing import Any

# Vietnamese names — expanded pool for 10K+ customer uniqueness
FIRST_NAMES_MALE = [
    "Nguyen Van", "Tran Minh", "Le Hong", "Pham Duc", "Hoang Anh",
    "Vo Thanh", "Phan Tuan", "Do Minh", "Ngo Quoc", "Dang Khoa",
    "Bui Van", "Duong Ngoc", "Ly Hai", "Ho Thanh", "Nguyen Xuan",
    "Nguyen Tien", "Tran Ngoc", "Le Trung", "Pham Hieu", "Hoang Nam",
    "Vo Dinh", "Phan Duc", "Do Quang", "Ngo Anh", "Dang Ba",
    "Bui Duc", "Duong Tuan", "Ly Minh", "Ho Ngoc", "Nguyen The",
    "Tran Duc", "Le Duy", "Pham Tien", "Hoang Duc", "Vo Xuan",
    "Phan Hoang", "Do Anh", "Ngo Van", "Dang Trung", "Bui Anh",
    "Duong Van", "Ly Duc", "Ho Tuan", "Nguyen Phuc", "Tran Khoi",
    "Le Son", "Pham Phuong", "Hoang Long", "Vo Quoc", "Phan Ngoc",
]
FIRST_NAMES_FEMALE = [
    "Nguyen Thi", "Tran Thi", "Le Thi", "Pham Thi", "Hoang Thi",
    "Vo Thi", "Phan Thi", "Do Thi", "Ngo Thi", "Dang Thi",
    "Bui Thi", "Duong Thi", "Ly Thi", "Ho Thi", "Nguyen Mai",
    "Nguyen Thanh", "Tran Thanh", "Le Thanh", "Pham Thanh", "Hoang Thanh",
    "Vo Ngoc", "Phan Ngoc", "Do Ngoc", "Ngo Thanh", "Dang Ngoc",
    "Bui Ngoc", "Duong Thanh", "Ly Thi Mai", "Ho Ngoc", "Nguyen Cam",
    "Tran Cam", "Le Ha", "Pham Ha", "Hoang Lan", "Vo Lan",
    "Phan Lan", "Do Linh", "Ngo Linh", "Dang Ha", "Bui Lan",
    "Duong Linh", "Ly Ha", "Ho Mai", "Nguyen Oanh", "Tran Oanh",
    "Le Quynh", "Pham Quynh", "Hoang Trang", "Vo Trang", "Phan Trang",
]
LAST_NAMES = [
    "An", "Bao", "Cam", "Chieu", "Cuong", "Dung", "Duc", "Duy",
    "Giang", "Ha", "Hai", "Hien", "Hoang", "Huong", "Huy",
    "Khanh", "Kien", "Khoa", "Lam", "Lan", "Linh", "Long",
    "Mai", "Minh", "My", "Nga", "Nghia", "Ngoc", "Nhat",
    "Oanh", "Phong", "Phuong", "Quan", "Quoc", "Quynh",
    "Son", "Son", "Suong", "Tam", "Thao", "Thu", "Thuy",
    "Tien", "Tra", "Tram", "Trang", "Tuyen", "Tuyet",
    "Uyen", "Yen", "Yen",
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

# Geographic mapping: region → cities (Vietnamese banking geography)
REGION_CITIES = {
    "NORTH": ["Hanoi", "Hai Phong", "Ha Long", "Bac Ninh", "Nam Dinh"],
    "CENTRAL": ["Da Nang", "Hue", "Nha Trang", "Quy Nhon", "Vinh"],
    "SOUTH": ["HCM", "Can Tho", "Bien Hoa", "Vung Tau", "My Tho"],
}

# City → districts mapping (realistic Vietnamese districts)
CITY_DISTRICTS = {
    "Hanoi": ["Ba Dinh", "Hoan Kiem", "Hai Ba Trung", "Dong Da", "Cau Giay",
              "Thanh Xuan", "Ha Dong", "Long Bien", "Nam Tu Liem", "Bac Tu Liem"],
    "HCM": ["District 1", "District 3", "District 5", "Binh Thanh", "Tan Binh",
            "Phu Nhuan", "Thu Duc", "Go Vap", "District 7", "Nha Be"],
    "Da Nang": ["Hai Chau", "Thanh Khe", "Son Tra", "Ngu Hanh Son", "Lien Chieu"],
    "Hai Phong": ["Hong Bang", "Le Chan", "Ngo Quyen", "Hai An", "Toan Thang"],
    "Can Tho": ["Ninh Kieu", "Binh Thuy", "Cai Rang", "O Mon", "Thot Not"],
    "Bien Hoa": ["Tan Hiep", "Quang Vinh", "Buu Hoa", "Hiep Hoa", "Trang Dai"],
    "Nha Trang": ["Loc Tho", "Vinh Thanh", "Vinh Nguyen", "Phuoc Hai", "Xuong Huan"],
    "Vung Tau": ["Ward 1", "Ward 2", "Ward 5", "Thang Nhat", "Rach Dua"],
    "Hue": ["Thuong Hoa", "Kim Long", "Vinh Ninh", "Phuoc Vinh", "An Cuu"],
    "Quy Nhon": ["Nguyen Van Cu", "Tran Hung Dao", "Bach Dang", "Le Hong Phong"],
}
# Fallback districts for cities not in the map
DEFAULT_DISTRICTS = ["District 1", "District 2", "District 3", "Ward 1", "Ward 2"]


def generate_branches(count: int, config: dict) -> list[tuple]:
    """
    Generate branch data with geographic consistency.
    Branches are assigned to cities that match their region.
    """
    rows = []
    regions = config.get("regions", ["NORTH", "CENTRAL", "SOUTH"])
    weights = config.get("region_weights", [0.35, 0.20, 0.45])
    statuses = config.get("status", ["ACTIVE", "CLOSED"])
    s_weights = config.get("status_weights", [0.92, 0.08])

    for i in range(1, count + 1):
        code = f"BR{i:03d}"
        region = random.choices(regions, weights=weights)[0]
        # Pick city from region
        region_cities = REGION_CITIES.get(region, CITIES)
        city = random.choice(region_cities)
        # Pick district from city
        city_districts = CITY_DISTRICTS.get(city, DEFAULT_DISTRICTS)
        district = random.choice(city_districts)
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


def generate_customers(count: int, config: dict, branch_codes: list[str],
                       branch_city_map: dict | None = None) -> list[tuple]:
    """
    Generate customer data with geographic consistency.

    Args:
        branch_city_map: {branch_code: city} mapping for geographic assignment.
                         If provided, customers in a city are assigned to branches in that city.
    """
    rows = []
    seg_dist = config.get("segment_distribution", {"RETAIL": 0.70, "PRIORITY": 0.22, "VIP": 0.08})
    kyc_dist = config.get("kyc_distribution", {"VERIFIED": 0.85, "PENDING": 0.10, "REJECTED": 0.05})
    active_rate = config.get("active_rate", 0.94)
    cities = config.get("cities", CITIES)

    segments = list(seg_dist.keys())
    seg_weights = list(seg_dist.values())
    kycs = list(kyc_dist.keys())
    kyc_weights = list(kyc_dist.values())

    # Build city → branches mapping if provided
    city_branches = {}
    if branch_city_map:
        for b_code, b_city in branch_city_map.items():
            city_branches.setdefault(b_city, []).append(b_code)

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

        # Geographic consistency: assign branch matching customer's city
        if city_branches and city in city_branches:
            branch = random.choice(city_branches[city])
        else:
            branch = random.choice(branch_codes)

        # Pick district from city
        city_districts_list = CITY_DISTRICTS.get(city, DEFAULT_DISTRICTS)

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
            random.choice(city_districts_list),
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
                         customer_map: dict, account_balances: dict | None = None) -> list[tuple]:
    """
    Generate account transaction data with seasonal patterns and balance simulation.

    Args:
        count: Number of transactions to generate
        config: Configuration dict
        account_ids: List of valid account IDs
        customer_map: {account_id: customer_id}
        account_balances: {account_id: initial_balance} for balance simulation
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

    # Balance simulation: track running balance per account
    running_balances = {}
    if account_balances:
        running_balances = dict(account_balances)

    for i in range(1, count + 1):
        acct_id = random.choice(account_ids)
        cust_id = customer_map.get(acct_id, 1)
        txn_type = random.choices(txn_types, weights=txn_weights)[0]
        dc = random.choices(dcs, weights=dc_weights)[0]
        channel = random.choices(channels, weights=ch_weights)[0]
        amount = round(random.uniform(amount_range[0], amount_range[1]), 2)
        # Use seasonal datetime for realistic patterns
        txn_date = _random_datetime_seasonal("2025-06-01", "2026-08-01")

        # Balance simulation
        if running_balances:
            current_balance = running_balances.get(acct_id, 1000000)
            if dc == "C":
                # Credit: deposit, transfer_in, interest
                new_balance = round(current_balance + amount, 2)
            else:
                # Debit: withdrawal, transfer_out, fee
                new_balance = round(current_balance - amount, 2)
                # Prevent negative balance — clamp to minimum
                if new_balance < 0:
                    new_balance = round(random.uniform(10000, 50000), 2)
            running_balances[acct_id] = new_balance
            balance_after = new_balance
        else:
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


def generate_loan_payments(loan_data: list[tuple], config: dict) -> list[tuple]:
    """
    Generate loan payment (amortization) schedule for active/closed loans.

    Uses standard amortization formula:
        monthly_payment = P * r(1+r)^n / ((1+r)^n - 1)
        where P = principal, r = monthly rate, n = term months

    Enhanced with: payment_method, penalty, payment_status, days_late
    (inspired by Data13 reference dataset)

    Args:
        loan_data: list of loan tuples from generate_loans()
        config: loan_payment config dict
    """
    late_rate = config.get("late_payment_rate", 0.05)
    missed_rate = config.get("missed_payment_rate", 0.02)
    rows = []
    payment_id = 1

    payment_methods = ["BANK_TRANSFER", "CASH", "CHEQUE", "DEBIT_CARD", "MOBILE_APP"]
    method_weights = [0.40, 0.10, 0.05, 0.15, 0.30]

    for loan in loan_data:
        # loan tuple: (loan_id, customer_id, product_code, branch_code,
        #              loan_amount, outstanding_balance, interest_rate, term_months,
        #              disbursement_date, maturity_date, loan_status, last_updated)
        loan_id = loan[0]
        principal = float(loan[4])
        annual_rate = float(loan[6])
        term_months = int(loan[7])
        disb_date = datetime.strptime(loan[8], "%Y-%m-%d")
        loan_status = loan[10]

        # Skip WRITTEN_OFF loans
        if loan_status == "WRITTEN_OFF":
            continue

        monthly_rate = annual_rate / 12.0 / 100.0

        # Calculate monthly payment (annuity formula)
        if monthly_rate == 0:
            monthly_payment = principal / term_months
        else:
            factor = (1 + monthly_rate) ** term_months
            monthly_payment = principal * (monthly_rate * factor) / (factor - 1)

        # Generate payments up to current date (or full term for CLOSED loans)
        today = datetime(2026, 8, 1)
        if loan_status == "CLOSED":
            months_to_generate = term_months
        else:
            months_elapsed = (today.year - disb_date.year) * 12 + (today.month - disb_date.month)
            months_to_generate = min(months_elapsed, term_months)

        outstanding = principal
        for month_idx in range(1, months_to_generate + 1):
            payment_date = disb_date + timedelta(days=month_idx * 30)
            scheduled_amount = round(monthly_payment, 2)

            # Calculate interest component for this month
            interest_component = round(outstanding * monthly_rate, 2)
            principal_component = round(monthly_payment - interest_component, 2)

            # Don't exceed outstanding
            if principal_component > outstanding:
                principal_component = round(outstanding, 2)
                interest_component = round(monthly_payment - principal_component, 2) if monthly_payment > principal_component else 0

            # Determine payment status
            roll = random.random()
            if roll < missed_rate and loan_status != "CLOSED":
                # Missed payment
                payment_status = "MISSED"
                days_late = random.randint(30, 90)
                penalty = round(scheduled_amount * 0.05, 2)  # 5% penalty
                amount_paid = 0
                outstanding = round(outstanding, 2)  # outstanding doesn't change
            elif roll < late_rate and loan_status != "CLOSED":
                # Late payment
                payment_status = "LATE"
                days_late = random.randint(1, 30)
                penalty = round(scheduled_amount * 0.02, 2)  # 2% late fee
                amount_paid = round(scheduled_amount + penalty, 2)
                outstanding = round(max(0, outstanding - principal_component), 2)
            else:
                # On-time payment
                payment_status = "PAID"
                days_late = 0
                penalty = 0
                amount_paid = round(principal_component + interest_component, 2)
                outstanding = round(max(0, outstanding - principal_component), 2)

            payment_method = random.choices(payment_methods, weights=method_weights)[0]

            rows.append((
                payment_id,
                loan_id,
                payment_date.strftime("%Y-%m-%d"),
                scheduled_amount,
                amount_paid,
                principal_component,
                interest_component,
                penalty,
                outstanding,
                days_late,
                payment_method,
                payment_status,
                1 if payment_status == "LATE" else 0,  # late_payment_flag
                datetime.now(),
            ))
            payment_id += 1

        if payment_id % 50000 == 0:
            print(f"    ... {payment_id:,} loan payments generated")

    return rows


def generate_standing_orders(count: int, config: dict,
                             account_ids: list[int], customer_map: dict) -> list[tuple]:
    """
    Generate standing order (recurring payment) data.

    Common Vietnamese scenarios: electricity (EVN), water (Sawaco),
    internet (FPT/Viettel/VNPT), insurance, salary transfers.
    """
    freq_dist = config.get("frequency_distribution", {"MONTHLY": 0.60, "WEEKLY": 0.25, "QUARTERLY": 0.15})
    status_dist = config.get("status_distribution", {"ACTIVE": 0.70, "PAUSED": 0.15, "CANCELLED": 0.15})

    freqs = list(freq_dist.keys())
    f_weights = list(freq_dist.values())
    statuses = list(status_dist.keys())
    s_weights = list(status_dist.values())

    # Vietnamese bill payment scenarios
    billers = [
        ("EVN - Dien luc", "BILL_PAYMENT"),
        ("Sawaco - Nuoc sach", "BILL_PAYMENT"),
        ("FPT Telecom", "BILL_PAYMENT"),
        ("Viettel Telecom", "BILL_PAYMENT"),
        ("VNPT Viettel", "BILL_PAYMENT"),
        ("Bao Viet Nhan Tho", "BILL_PAYMENT"),
        ("Prudential Vietnam", "BILL_PAYMENT"),
        ("Manulife Vietnam", "BILL_PAYMENT"),
        ("Cong ty Môi trường", "BILL_PAYMENT"),
        ("Quy Prudential", "TRANSFER"),
        ("Cong ty TNHH ABC", "TRANSFER"),
        ("Cong ty XYZ Corp", "TRANSFER"),
        ("Thanh toan khoan vay", "LOAN_PAYMENT"),
    ]

    rows = []
    for i in range(1, count + 1):
        acct_id = random.choice(account_ids)
        cust_id = customer_map.get(acct_id, 1)
        order_type = random.choices(["BILL_PAYMENT", "TRANSFER", "LOAN_PAYMENT"],
                                    weights=[0.50, 0.30, 0.20])[0]
        frequency = random.choices(freqs, weights=f_weights)[0]
        status = random.choices(statuses, weights=s_weights)[0]

        # Pick a biller matching order type
        matching_billers = [b for b in billers if b[1] == order_type]
        if matching_billers:
            beneficiary_name, _ = random.choice(matching_billers)
        else:
            beneficiary_name = random.choice(billers)[0]

        # Amount depends on type and frequency
        if order_type == "BILL_PAYMENT":
            if "Dien" in beneficiary_name:
                amount = round(random.uniform(200000, 3000000), 2)  # Electricity 200K-3M VND
            elif "Nuoc" in beneficiary_name:
                amount = round(random.uniform(100000, 800000), 2)   # Water 100K-800K
            elif "Telecom" in beneficiary_name or "VNPT" in beneficiary_name:
                amount = round(random.uniform(100000, 500000), 2)   # Internet/phone 100K-500K
            else:
                amount = round(random.uniform(200000, 2000000), 2)  # Insurance 200K-2M
        elif order_type == "TRANSFER":
            amount = round(random.uniform(1000000, 50000000), 2)    # Salary/transfer 1M-50M
        else:  # LOAN_PAYMENT
            amount = round(random.uniform(2000000, 30000000), 2)    # Loan payment 2M-30M

        created_date = _random_date("2022-01-01", "2025-12-31")
        created_dt = datetime.strptime(created_date, "%Y-%m-%d")

        # next_execute_date: based on frequency
        if frequency == "WEEKLY":
            next_execute = created_dt + timedelta(days=random.randint(1, 7))
        elif frequency == "QUARTERLY":
            next_execute = created_dt + timedelta(days=random.randint(1, 90))
        else:  # MONTHLY
            next_execute = created_dt + timedelta(days=random.randint(1, 30))

        rows.append((
            i,
            acct_id,
            cust_id,
            order_type,
            beneficiary_name,
            f"VN{random.randint(100000000, 999999999)}",  # beneficiary account
            amount,
            frequency,
            next_execute.strftime("%Y-%m-%d"),
            status,
            created_date,
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


def _random_datetime_seasonal(start_str: str, end_str: str) -> datetime:
    """
    Generate a random datetime with realistic banking patterns:
    - 65% weekday, 20% Saturday, 15% Sunday
    - Hour peaks: 9-11am (salary/payments), 7-9pm (mobile banking)
    - Monthly: higher volume on 1st-5th (salary) and 15th (mid-month)
    """
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    delta_days = (end - start).days
    if delta_days <= 0:
        return start

    # Pick a random day, biased toward weekdays
    rand_day = random.randint(0, delta_days)
    dt = start + timedelta(days=rand_day)
    weekday = dt.weekday()  # 0=Mon, 6=Sun

    # Re-roll day of week with bias: 65% weekday, 20% Sat, 15% Sun
    day_roll = random.random()
    if day_roll < 0.65:
        # Weekday — if currently weekend, shift to nearest weekday
        if weekday >= 5:
            shift = random.choice([-(weekday - 4), (7 - weekday)])
            dt = dt + timedelta(days=shift)
    elif day_roll < 0.85:
        # Saturday
        while dt.weekday() != 5:
            dt = dt + timedelta(days=1)
    else:
        # Sunday
        while dt.weekday() != 6:
            dt = dt + timedelta(days=1)

    # Hour distribution: peaks at 9-11am and 7-9pm
    hour_weights = {
        0: 0.01, 1: 0.005, 2: 0.005, 3: 0.005, 4: 0.005, 5: 0.01,
        6: 0.02, 7: 0.04, 8: 0.08,
        9: 0.12, 10: 0.14, 11: 0.10,
        12: 0.06, 13: 0.05, 14: 0.06, 15: 0.05, 16: 0.04,
        17: 0.03, 18: 0.04,
        19: 0.06, 20: 0.05, 21: 0.03,
        22: 0.01, 23: 0.005,
    }
    hours = list(hour_weights.keys())
    h_weights = list(hour_weights.values())
    hour = random.choices(hours, weights=h_weights)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return dt.replace(hour=hour, minute=minute, second=second)


def _random_date_seasonal_month(start_str: str, end_str: str) -> str:
    """
    Generate a random date biased toward salary periods (1st-5th, 15th)
    and month-end (25th-30th). Used for loan payments, standing orders.
    """
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    delta_days = (end - start).days
    if delta_days <= 0:
        return start_str

    dt = start + timedelta(days=random.randint(0, delta_days))

    # Bias toward salary periods: 1st-5th or 15th or 25th-28th
    day_weights = {
        1: 0.08, 2: 0.06, 3: 0.05, 4: 0.04, 5: 0.04,
        15: 0.08,
        25: 0.04, 26: 0.04, 27: 0.03, 28: 0.03,
    }
    if dt.day in day_weights:
        # Higher chance of keeping this date
        if random.random() < 0.7:
            return dt.strftime("%Y-%m-%d")

    return dt.strftime("%Y-%m-%d")
