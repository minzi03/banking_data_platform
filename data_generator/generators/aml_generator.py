"""
from __future__ import annotations
AML (Anti-Money Laundering) Data Generator
Generates realistic AML rules, alerts, and customer risk profiles.
"""

import random
import string
from datetime import datetime, timedelta
from typing import List, Tuple

# ── AML Rules ───────────────────────────────────────────────────────────────
AML_RULES = [
    # VELOCITY rules
    {
        "rule_name": "High Frequency Transactions",
        "rule_code": "AML-VEL-001",
        "rule_type": "VELOCITY",
        "description": "More than 10 transactions within 1 hour",
        "threshold": 10,
        "window_hours": 1,
        "severity": "HIGH",
        "regulatory_ref": "TT 35/2019/TT-NHNN",
    },
    {
        "rule_name": "Daily Transaction Limit Breach",
        "rule_code": "AML-VEL-002",
        "rule_type": "VELOCITY",
        "description": "More than 20 transactions in a single day",
        "threshold": 20,
        "window_hours": 24,
        "severity": "MEDIUM",
        "regulatory_ref": "TT 35/2019/TT-NHNN",
    },
    {
        "rule_name": "Rapid Fund Transfer",
        "rule_code": "AML-VEL-003",
        "rule_type": "VELOCITY",
        "description": "Multiple transfers within 30 minutes",
        "threshold": 5,
        "window_hours": 1,
        "severity": "HIGH",
        "regulatory_ref": "TT 35/2019/TT-NHNN",
    },

    # AMOUNT rules
    {
        "rule_name": "Large Cash Transaction",
        "rule_code": "AML-AMT-001",
        "rule_type": "AMOUNT",
        "description": "Single cash transaction exceeding 500M VND",
        "threshold": 500000000,
        "window_hours": 0,
        "severity": "HIGH",
        "regulatory_ref": "Nghị định 11/2023/NĐ-CP",
    },
    {
        "rule_name": "CTR Threshold",
        "rule_code": "AML-AMT-002",
        "rule_type": "AMOUNT",
        "description": "Currency Transaction Report threshold (2B VND)",
        "threshold": 2000000000,
        "window_hours": 0,
        "severity": "CRITICAL",
        "regulatory_ref": "TT 35/2019/TT-NHNN",
    },
    {
        "rule_name": "Cross-Border Transfer Alert",
        "rule_code": "AML-AMT-003",
        "rule_type": "AMOUNT",
        "description": "International transfer exceeding 1B VND",
        "threshold": 1000000000,
        "window_hours": 0,
        "severity": "HIGH",
        "regulatory_ref": "TT 19/2014/TT-NHNN",
    },

    # STRUCTURING rules
    {
        "rule_name": "Structuring Detection",
        "rule_code": "AML-STR-001",
        "rule_type": "STRUCTURING",
        "description": "Multiple transactions just below 500M threshold",
        "threshold": 490000000,
        "window_hours": 24,
        "severity": "HIGH",
        "regulatory_ref": "TT 35/2019/TT-NHNN",
    },
    {
        "rule_name": "Smurfing Pattern",
        "rule_code": "AML-STR-002",
        "rule_type": "STRUCTURING",
        "description": "Multiple accounts used for same beneficiary",
        "threshold": 3,
        "window_hours": 48,
        "severity": "CRITICAL",
        "regulatory_ref": "Nghị định 11/2023/NĐ-CP",
    },

    # GEOGRAPHIC rules
    {
        "rule_name": "High Risk Country Transfer",
        "rule_code": "AML-GEO-001",
        "rule_type": "GEOGRAPHIC",
        "description": "Transfer to/from FATF grey/blacklist countries",
        "threshold": 100000000,
        "window_hours": 0,
        "severity": "CRITICAL",
        "regulatory_ref": "FATF Standards",
    },
    {
        "rule_name": "Unusual Location Pattern",
        "rule_code": "AML-GEO-002",
        "rule_type": "GEOGRAPHIC",
        "description": "Transactions from multiple distant locations within short window",
        "threshold": 500,
        "window_hours": 4,
        "severity": "MEDIUM",
        "regulatory_ref": "TT 35/2019/TT-NHNN",
    },

    # PATTERN rules
    {
        "rule_name": "Round Amount Pattern",
        "rule_code": "AML-PAT-001",
        "rule_type": "PATTERN",
        "description": "Multiple round-amount transactions (10M, 50M, 100M)",
        "threshold": 5,
        "window_hours": 72,
        "severity": "MEDIUM",
        "regulatory_ref": "Internal Policy",
    },
    {
        "rule_name": "Dormant Account Activation",
        "rule_code": "AML-PAT-002",
        "rule_type": "PATTERN",
        "description": "Sudden high activity on dormant account (>90 days inactive)",
        "threshold": 90,
        "window_hours": 0,
        "severity": "HIGH",
        "regulatory_ref": "TT 35/2019/TT-NHNN",
    },
    {
        "rule_name": "Night Time Transactions",
        "rule_code": "AML-PAT-003",
        "rule_type": "PATTERN",
        "description": "Multiple transactions between midnight and 5 AM",
        "threshold": 3,
        "window_hours": 24,
        "severity": "LOW",
        "regulatory_ref": "Internal Policy",
    },

    # PEPS rules
    {
        "rule_name": "PEPS Transaction Monitoring",
        "rule_code": "AML-PEP-001",
        "rule_type": "PEPS",
        "description": "Enhanced monitoring for Politically Exposed Persons",
        "threshold": 100000000,
        "window_hours": 0,
        "severity": "HIGH",
        "regulatory_ref": "TT 35/2019/TT-NHNN",
    },
]

# ── Alert Types ──────────────────────────────────────────────────────────────
ALERT_TYPES = [
    "LARGE_CASH_TRANSACTION",
    "VELOCITY_BREACH",
    "STRUCTURING_DETECTED",
    "GEOGRAPHIC_ANOMALY",
    "UNUSUAL_PATTERN",
    "PEPS_MATCH",
    "SANCTIONS_MATCH",
    "CTR_REQUIRED",
    "SAR_REQUIRED",
]

# ── Fraud Reasons (from existing online_transaction patterns) ────────────────
FRAUD_DESCRIPTIONS = [
    "Unusual location detected — customer normally transacts in Hanoi but transaction in HCM",
    "Velocity check failed — 15 transactions in 30 minutes exceeds threshold",
    "Amount exceeds limit — single transaction 800M VND exceeds 500M threshold",
    "Known fraud pattern match — structuring behavior detected across 3 accounts",
    "Device fingerprint mismatch — new device used for high-value transfer",
    "Geo-anomaly detected — transaction from IP in different country than usual",
    "Multiple failed attempts followed by successful large transfer",
    "Round amount pattern — 5 consecutive 100M VND transfers within 2 hours",
    "Dormant account sudden activity — 500M deposited after 120 days inactive",
    "Night time anomaly — 3 large transfers between 2-4 AM",
]

# ── Evidence Templates ──────────────────────────────────────────────────────
def _generate_evidence(alert_type: str, txn_amount: float) -> dict:
    """Generate realistic evidence JSON for alert."""
    evidence = {
        "alert_type": alert_type,
        "detection_timestamp": datetime.now().isoformat(),
        "rule_engine_version": "2.1.0",
    }

    if alert_type in ["VELOCITY_BREACH", "LARGE_CASH_TRANSACTION"]:
        evidence["transaction_count"] = random.randint(5, 20)
        evidence["time_window_hours"] = random.choice([1, 4, 24])
        evidence["total_amount"] = txn_amount * random.uniform(1.5, 5.0)

    elif alert_type == "STRUCTURING_DETECTED":
        evidence["related_transactions"] = random.randint(3, 8)
        evidence["individual_amounts"] = [
            round(random.uniform(400000000, 499000000), 2)
            for _ in range(random.randint(3, 5))
        ]

    elif alert_type == "GEOGRAPHIC_ANOMALY":
        evidence["transaction_location"] = random.choice(["HCM", "Da Nang", "Can Tho"])
        evidence["usual_location"] = "Hanoi"
        evidence["distance_km"] = random.randint(600, 1200)
        evidence["time_since_last_txn_hours"] = random.randint(1, 6)

    elif alert_type == "UNUSUAL_PATTERN":
        evidence["pattern_type"] = random.choice([
            "round_amount", "dormant_activation", "night_time"
        ])
        evidence["confidence_score"] = round(random.uniform(0.7, 0.95), 2)

    return evidence


def generate_aml_rules() -> List[Tuple]:
    """Generate AML detection rules."""
    rules = []
    for i, rule in enumerate(AML_RULES, 1):
        rules.append((
            i,  # rule_id
            rule["rule_name"],
            rule["rule_code"],
            rule["rule_type"],
            rule["description"],
            rule["threshold"],
            "VND",
            rule["window_hours"],
            rule["severity"],
            1,  # is_active
            rule["regulatory_ref"],
            "SYSTEM",
            datetime.now(),
            datetime.now(),
        ))
    return rules


def generate_aml_alerts(
    num_alerts: int = 500,
    max_customer_id: int = 10000,
    max_txn_id: int = 1200000,
    max_employee_id: int = 1800,
) -> List[Tuple]:
    """Generate realistic AML alerts."""
    alerts = []
    statuses = ["OPEN", "IN_REVIEW", "ESCALATED", "STR_FALSE_POSITIVE", "STR_FILED", "CLOSED"]
    status_weights = [0.20, 0.15, 0.10, 0.25, 0.05, 0.25]
    priorities = ["LOW", "MEDIUM", "HIGH"]
    priority_weights = [0.30, 0.50, 0.20]

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 12, 31)
    date_range = (end_date - start_date).days

    used_alert_numbers = set()

    for i in range(1, num_alerts + 1):
        # Generate unique alert number
        alert_date = start_date + timedelta(days=random.randint(0, date_range))
        alert_number = f"AML-{alert_date.strftime('%Y%m%d')}-{i:05d}"
        while alert_number in used_alert_numbers:
            alert_number = f"AML-{alert_date.strftime('%Y%m%d')}-{random.randint(1, 99999):05d}"
        used_alert_numbers.add(alert_number)

        rule_id = random.randint(1, len(AML_RULES))
        rule = AML_RULES[rule_id - 1]
        alert_type = random.choice(ALERT_TYPES)

        # Risk score based on severity
        severity_scores = {"LOW": (0, 30), "MEDIUM": (31, 60), "HIGH": (61, 80), "CRITICAL": (81, 100)}
        score_range = severity_scores[rule["severity"]]
        risk_score = round(random.uniform(score_range[0], score_range[1]), 2)

        if risk_score <= 30:
            risk_category = "LOW"
        elif risk_score <= 60:
            risk_category = "MEDIUM"
        elif risk_score <= 80:
            risk_category = "HIGH"
        else:
            risk_category = "CRITICAL"

        txn_amount = round(random.uniform(10000000, 2000000000), 2)
        txn_date = alert_date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        status = random.choices(statuses, weights=status_weights, k=1)[0]
        priority = random.choices(priorities, weights=priority_weights, k=1)[0]

        # Resolve date (only for closed/filed alerts)
        resolved_at = None
        if status in ["CLOSED", "STR_FILED", "STR_FALSE_POSITIVE"]:
            resolved_at = alert_date + timedelta(days=random.randint(1, 30))

        # CTR/SAR flags
        ctr_required = 1 if txn_amount >= 2000000000 else 0
        sar_filed = 1 if status == "STR_FILED" else 0
        sar_reference = f"SAR-{alert_date.strftime('%Y')}-{random.randint(1000, 9999)}" if sar_filed else None

        alerts.append((
            i,  # alert_id
            alert_number,
            rule_id,
            random.randint(1, max_txn_id),  # transaction_id (nullable in DDL)
            None,  # card_txn_id
            random.randint(1, max_customer_id),  # customer_id
            None,  # account_id
            alert_type,
            risk_score,
            risk_category,
            random.choice(FRAUD_DESCRIPTIONS),
            _generate_evidence(alert_type, txn_amount),
            txn_amount,
            txn_date,
            random.choice(["BRANCH", "ATM", "INTERNET_BANKING", "MOBILE_BANKING"]),
            status,
            random.randint(1, max_employee_id) if status != "OPEN" else None,  # analyst_id
            priority,
            alert_date + timedelta(days=random.randint(3, 14)),  # due_date
            None,  # notes
            ctr_required,
            sar_filed,
            sar_reference,
            alert_date,
            alert_date,
            resolved_at,
        ))

    return alerts


def generate_aml_customer_risk(num_customers: int = 200, max_customer_id: int = 10000) -> List[Tuple]:
    """Generate AML customer risk profiles for a subset of customers."""
    profiles = []
    risk_levels = ["STANDARD", "ELEVATED", "HIGH", "PROHIBITED"]
    risk_weights = [0.60, 0.25, 0.12, 0.03]
    source_of_wealth_options = [
        "Employment", "Business Income", "Investment Returns", "Inheritance",
        "Real Estate", "Pension", "Other", "Unknown"
    ]

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 12, 31)
    date_range = (end_date - start_date).days

    for i in range(1, num_customers + 1):
        customer_id = random.randint(1, max_customer_id)
        risk_level = random.choices(risk_levels, weights=risk_weights, k=1)[0]

        if risk_level == "STANDARD":
            risk_score = round(random.uniform(0, 20), 2)
        elif risk_level == "ELEVATED":
            risk_score = round(random.uniform(21, 50), 2)
        elif risk_level == "HIGH":
            risk_score = round(random.uniform(51, 80), 2)
        else:
            risk_score = round(random.uniform(81, 100), 2)

        peps_flag = 1 if risk_level in ["HIGH", "PROHIBITED"] and random.random() < 0.1 else 0
        sanctions_flag = 1 if risk_level == "PROHIBITED" and random.random() < 0.3 else 0
        adverse_media_flag = 1 if risk_level in ["HIGH", "PROHIBITED"] and random.random() < 0.15 else 0

        total_alerts = random.randint(0, 15) if risk_level != "STANDARD" else random.randint(0, 3)
        open_alerts = random.randint(0, min(total_alerts, 5))
        last_alert_date = (start_date + timedelta(days=random.randint(0, date_range))).date() if total_alerts > 0 else None

        edd_required = 1 if risk_level in ["HIGH", "PROHIBITED"] else 0
        next_review_date = datetime.now().date() + timedelta(days=random.choice([30, 90, 180, 365]))

        profiles.append((
            customer_id,
            risk_level,
            risk_score,
            peps_flag,
            sanctions_flag,
            adverse_media_flag,
            total_alerts,
            open_alerts,
            last_alert_date,
            None,  # last_review_date
            next_review_date,
            edd_required,
            f"Enhanced Due Diligence required for {risk_level} risk customer" if edd_required else None,
            random.choice(source_of_wealth_options),
            f"Regular {random.choice(['salary', 'business', 'investment'])} transactions expected",
            start_date + timedelta(days=random.randint(0, date_range)),
            datetime.now(),
        ))

    return profiles


def get_aml_rules_columns() -> List[str]:
    """Return column names for aml_rule table."""
    return [
        "rule_id", "rule_name", "rule_code", "rule_type", "description",
        "threshold", "threshold_currency", "window_hours", "severity",
        "is_active", "regulatory_ref", "created_by", "created_at", "updated_at",
    ]


def get_aml_alerts_columns() -> List[str]:
    """Return column names for aml_alert table."""
    return [
        "alert_id", "alert_number", "rule_id", "transaction_id", "card_txn_id",
        "customer_id", "account_id", "alert_type", "risk_score", "risk_category",
        "description", "evidence_json", "txn_amount", "txn_date", "channel",
        "status", "analyst_id", "priority", "due_date", "notes",
        "ctr_required", "sar_filed", "sar_reference",
        "created_at", "updated_at", "resolved_at",
    ]


def get_aml_customer_risk_columns() -> List[str]:
    """Return column names for aml_customer_risk table."""
    return [
        "customer_id", "risk_level", "risk_score", "peps_flag", "sanctions_flag",
        "adverse_media_flag", "total_alerts", "open_alerts", "last_alert_date",
        "last_review_date", "next_review_date", "edd_required", "edd_reason",
        "source_of_wealth", "expected_activity", "created_at", "updated_at",
    ]
