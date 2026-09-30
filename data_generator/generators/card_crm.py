from __future__ import annotations
"""
Card & CRM Generator — 3 tables
Generates realistic data for: card, card_txn, crm_interaction
"""

import random
from datetime import datetime, timedelta
from typing import Any

from .amounts import amount_sampler
from .timeline import business_to_utc, shift


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

# merchant_category của card_txn → MCC. Mọi mã phải có trong mcc_code.codes của
# seed_config (tests/data_generator/test_mcc_code_uniqueness.py kiểm). Bản trước
# có 5422 (không có trong bảng MCC) và 3000/3351/3501 — mã riêng của MỘT hãng
# bay / hãng thuê xe / chuỗi khách sạn, không phải mã nhóm ngành.
CARD_CATEGORY_MCC = {
    "GROCERY": ["5411", "5499"],
    "RESTAURANT": ["5812", "5814"],
    "TRAVEL": ["4511", "7512", "7011", "4121"],
    "ECOM": ["5999", "5732"],
    "FUEL": ["5541"],
    "EDUCATION": ["8299"],
    "HEALTHCARE": ["8011", "8041", "8062"],
    "ENTERTAINMENT": ["7832", "7996", "7995"],
    "UTILITIES": ["4814", "4899"],
    "FASHION": ["5691", "5651"],
}

# Cách dùng thẻ theo kênh. Giao dịch có mặt thẻ (POS, ATM): chip / quẹt theo tỷ
# lệ của bộ tham khảo Xóm Bank (112.114 chip : 27.327 swipe ≈ 80 : 20). ECOM
# luôn là ONLINE.
CARD_PRESENT_ENTRY_MODE = {"CHIP": 112_114, "SWIPE": 27_327}

# Lý do từ chối cho giao dịch FAILED, trọng số = số lần đếm được trong
# 157.224 giao dịch của Xóm Bank, TÁCH THEO cách dùng thẻ — nên lỗi PIN chỉ có
# khi có mặt thẻ, lỗi CVV / ngày hết hạn / số thẻ chỉ có khi online, đúng như dữ
# liệu thật. Lỗi tổ hợp giữ nguyên (nhãn nối bằng dấu phẩy, sắp theo tên).
# Bỏ "Bad Zipcode" (17 lần): kiểm địa chỉ AVS của Mỹ, không dùng ở Việt Nam.
DECLINE_REASON_WEIGHTS = {
    "CHIP": {
        "INSUFFICIENT_FUNDS": 1281,
        "WRONG_PIN": 291,
        "TECHNICAL_ERROR": 228,
        "INSUFFICIENT_FUNDS,TECHNICAL_ERROR": 1,
        "INSUFFICIENT_FUNDS,WRONG_PIN": 1,
        "TECHNICAL_ERROR,WRONG_PIN": 1,
    },
    "SWIPE": {
        "INSUFFICIENT_FUNDS": 321,
        "WRONG_PIN": 97,
        "TECHNICAL_ERROR": 49,
        "INSUFFICIENT_FUNDS,WRONG_PIN": 5,
        "INSUFFICIENT_FUNDS,TECHNICAL_ERROR": 1,
    },
    "ONLINE": {
        "INSUFFICIENT_FUNDS": 158,
        "INVALID_CARD_NUMBER": 93,
        "INVALID_EXPIRY": 73,
        "WRONG_CVV": 65,
        "TECHNICAL_ERROR": 47,
        "INSUFFICIENT_FUNDS,WRONG_CVV": 3,
        "INVALID_CARD_NUMBER,WRONG_CVV": 2,
        "INSUFFICIENT_FUNDS,INVALID_CARD_NUMBER": 1,
        "INVALID_CARD_NUMBER,TECHNICAL_ERROR": 1,
        "INVALID_EXPIRY,WRONG_CVV": 1,
        "INSUFFICIENT_FUNDS,TECHNICAL_ERROR": 1,
    },
}


def _weighted(weights: dict[str, int]) -> str:
    return random.choices(list(weights), weights=list(weights.values()))[0]


def card_entry_mode(channel: str) -> str:
    return "ONLINE" if channel == "ECOM" else _weighted(CARD_PRESENT_ENTRY_MODE)


def decline_reason(status: str, entry_mode: str) -> str | None:
    """Chỉ giao dịch FAILED có lý do; SUCCESS / PENDING là NULL."""
    return _weighted(DECLINE_REASON_WEIGHTS[entry_mode]) if status == "FAILED" else None


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

        issue_date = _random_date(shift("2020-01-01"), shift("2025-06-30"))
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


def generate_card_txn(count: int, config: dict, card_data: list[tuple],
                      mcc_codes: list[str] | None = None, start_id: int = 1) -> list[tuple]:
    """
    Generate card transaction data with merchant details.

    card_data: list of (card_id, customer_id, card_type, status) tuples
    mcc_codes: list of valid MCC code strings from digital_banking.mcc_code

    Enhanced with: processing_time_ms, reference_number, mcc_code FK
    """
    rows = []
    type_dist = config.get("type_distribution", {"PURCHASE": 0.70, "CASH_ADVANCE": 0.15, "REFUND": 0.10, "REVERSAL": 0.05})
    channel_dist = config.get("channel_distribution", {"POS": 0.45, "ECOM": 0.40, "ATM": 0.15})
    status_dist = config.get("status_distribution", {"SUCCESS": 0.90, "FAILED": 0.07, "PENDING": 0.03})
    amount_range = config.get("amount_range", [50000, 50000000])
    # Chi tiêu thẻ lệch phải: nhiều giao dịch nhỏ, ít giao dịch lớn.
    sample_amount = amount_sampler(
        config,
        {"median": 500_000, "p99": 20_000_000,
         "min": amount_range[0], "max": amount_range[1]},
    )
    merchant_cats = config.get("merchant_categories", ["GROCERY", "RESTAURANT", "TRAVEL", "ECOM"])

    # Build lookup: card_id -> (customer_id, card_type)
    active_cards = [(c[0], c[1]) for c in card_data if c[3] == "ACTIVE"]

    txn_types = list(type_dist.keys())
    txn_weights = list(type_dist.values())
    channels = list(channel_dist.keys())
    ch_weights = list(channel_dist.values())
    statuses = list(status_dist.keys())
    s_weights = list(status_dist.values())

    # start_id: sinh theo khối (generate_all ở --scale lớn) mà txn_id / reference_number
    # vẫn liên tục và duy nhất.
    for i in range(start_id, start_id + count):
        card_id, cust_id = random.choice(active_cards) if active_cards else (1, 1)
        txn_type = random.choices(txn_types, weights=txn_weights)[0]
        channel = random.choices(channels, weights=ch_weights)[0]
        status = random.choices(statuses, weights=s_weights)[0]
        amount = sample_amount()
        merchant = random.choice(MERCHANT_NAMES)
        merchant_cat = random.choice(merchant_cats)
        txn_date = _random_datetime_seasonal(shift("2025-06-01"), shift("2026-08-01"))

        # Refunds and reversals have negative amounts
        if txn_type in ("REFUND", "REVERSAL"):
            amount = -amount

        # MCC khớp merchant_category. Bản trước có 20% giao dịch bốc MCC bất kỳ dù
        # category đã có mã — MCC mâu thuẫn category là dữ liệu sai, không phải
        # nhiễu thực tế. Chỉ category chưa có mã mới lấy mã bất kỳ trong bảng.
        mcc = None
        if mcc_codes:
            cat_mcns = CARD_CATEGORY_MCC.get(merchant_cat, [])
            mcc = random.choice(cat_mcns) if cat_mcns else random.choice(mcc_codes)

        # Processing time: POS fastest (50-200ms), ECOM slower (200-2000ms), ATM medium
        if channel == "POS":
            proc_time = random.randint(50, 300)
        elif channel == "ATM":
            proc_time = random.randint(500, 2000)
        else:
            proc_time = random.randint(200, 3000)

        ref_number = f"CDN{i:010d}"
        entry_mode = card_entry_mode(channel)

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
            mcc,
            channel,
            status,
            entry_mode,
            decline_reason(status, entry_mode),
            proc_time,
            ref_number,
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

        interaction_date = _random_datetime(shift("2024-08-01"), shift("2026-08-01"))

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


def _random_datetime_seasonal(start_str: str, end_str: str) -> datetime:
    """Generate datetime with realistic banking hour/day-of-week patterns."""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    delta_days = (end - start).days
    if delta_days <= 0:
        return start

    dt = start + timedelta(days=random.randint(0, delta_days))
    weekday = dt.weekday()

    # Bias toward weekdays
    day_roll = random.random()
    if day_roll < 0.65:
        if weekday >= 5:
            shift = random.choice([-(weekday - 4), (7 - weekday)])
            dt = dt + timedelta(days=shift)
    elif day_roll < 0.85:
        while dt.weekday() != 5:
            dt = dt + timedelta(days=1)
    else:
        while dt.weekday() != 6:
            dt = dt + timedelta(days=1)

    # Dời sang thứ Bảy / Chủ nhật / thứ Hai chỉ đi TỚI, nên có thể vượt `end` tới
    # 6 ngày. Khi `end` là cob_dt (timeline) thì đó là giao dịch trong tương lai.
    # Lùi đúng một tuần: giữ thứ trong tuần, không vượt mốc.
    if dt > end:
        dt -= timedelta(days=7)

    # Hour peaks (giờ VN): 9-11am and 7-9pm for card spending
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

    result = dt.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))
    # `result` là giờ đồng hồ Việt Nam; timestamp lưu ở UTC (ADR-0004). Đổi ở
    # đây. Vì dt <= end, instant muộn nhất là 23:59:59 giờ VN ngày `end`
    # (= 16:59:59 UTC) — không cần kẹp thêm như khi giờ VN bị ghi như UTC.
    return business_to_utc(result)
