# Data Vault 2.0 — Mapping Document

> **Date**: 2026-09-07
> **Purpose**: Document how current star schema maps to Data Vault 2.0
> **Reference**: Required by OCB Bank, KienlongBank in banking JDs

---

## 1. Overview

This document provides a reference mapping from the project's current **Kimball Star Schema** (Bronze→Silver→Gold) to **Data Vault 2.0** methodology. While the project uses Kimball for its simplicity and analytical performance, understanding Data Vault 2.0 is valuable for:

- Banking interviews (OCB, KienlongBank explicitly require DV2.0)
- Enterprise-scale data architecture discussions
- Regulatory compliance scenarios (audit trail, historization)

---

## 2. Current Architecture vs Data Vault 2.0

| Aspect | Current (Kimball) | Data Vault 2.0 |
|--------|-------------------|----------------|
| **Methodology** | Dimensional Modeling | Hybrid (Hubs + Links + Satellites) |
| **Schema** | Star Schema (facts + dims) | Raw Vault + Business Vault + Marts |
| **Historization** | SCD Type 1/2 | Satellites with load dates |
| **Loading** | Truncate & Reload / SCD | Insert-only (append-only) |
| **Flexibility** | Schema changes require DDL | Additive (new Satellites) |
| **Auditability** | Limited | Full audit trail (load_date, record_source) |
| **Performance** | Optimized for reads | Requires PIT/Bridge for queries |
| **Best For** | Analytics, BI | Enterprise, compliance, audit |

---

## 3. Hub, Link, Satellite Mapping

### 3.1 Hubs (Business Keys)

| Hub | Business Key | Source Table | Record Sources |
|-----|-------------|--------------|----------------|
| **Hub_Customer** | customer_id (CCCD) | customer | core_banking.customer |
| **Hub_Account** | account_no | account | core_banking.account |
| **Hub_Product** | product_code | product | core_banking.product |
| **Hub_Branch** | branch_code | branch | core_banking.branch |
| **Hub_Employee** | employee_id | employee | core_banking.employee |
| **Hub_Card** | card_no_masked | card | card_crm.card |
| **Hub_Device** | device_fingerprint | device | digital_banking.device |
| **Hub_Loan** | loan_id | loan | core_banking.loan |
| **Hub_Transaction** | txn_id | txn_account | core_banking.txn_account |

### 3.2 Links (Relationships)

| Link | From Hub | To Hub | Description |
|------|----------|--------|-------------|
| **Link_Customer_Account** | Hub_Customer | Hub_Account | Customer owns account |
| **Link_Account_Product** | Hub_Account | Hub_Product | Account has product |
| **Link_Customer_Branch** | Hub_Customer | Hub_Branch | Customer registered at branch |
| **Link_Account_Branch** | Hub_Account | Hub_Branch | Account opened at branch |
| **Link_Customer_Card** | Hub_Customer | Hub_Card | Customer has card |
| **Link_Card_Account** | Hub_Card | Hub_Account | Card linked to account |
| **Link_Customer_Device** | Hub_Customer | Hub_Device | Customer uses device |
| **Link_Loan_Customer** | Hub_Loan | Hub_Customer | Loan belongs to customer |
| **Link_Transaction_Account** | Hub_Transaction | Hub_Account | Transaction on account |

### 3.3 Satellites (Descriptive Attributes)

| Satellite | Hub/Link | Key Attributes | SCD Type |
|-----------|----------|----------------|----------|
| **Sat_Customer_Personal** | Hub_Customer | full_name, gender, dob, phone, email, address | SCD2 |
| **Sat_Customer_Status** | Hub_Customer | segment, kyc_status, is_active | SCD2 |
| **Sat_Account_Details** | Hub_Account | account_type, currency, balance, status | SCD2 |
| **Sat_Product_Details** | Hub_Product | product_name, group, type, currency | SCD1 |
| **Sat_Branch_Details** | Hub_Branch | branch_name, region, city, address | SCD1 |
| **Sat_Employee_Details** | Hub_Employee | full_name, role, salary, status | SCD2 |
| **Sat_Card_Details** | Hub_Card | card_type, brand, credit_limit, status | SCD2 |
| **Sat_Loan_Details** | Hub_Loan | amount, rate, term, status | SCD2 |
| **Sat_Transaction Details** | Hub_Transaction | amount, type, channel, description | Append-only |

### 3.4 PIT Tables (Point-in-Time)

| PIT | Hub | Satellites | Purpose |
|-----|-----|------------|---------|
| **PIT_Customer_Daily** | Hub_Customer | Sat_Customer_Personal, Sat_Customer_Status | Daily customer snapshot |
| **PIT_Account_Monthly** | Hub_Account | Sat_Account_Details | Monthly account snapshot |
| **PIT_Loan_Quarterly** | Hub_Loan | Sat_Loan_Details | Quarterly loan snapshot |

### 3.5 Bridge Tables

| Bridge | Links | Purpose |
|--------|-------|---------|
| **Bridge_Customer_Account** | Link_Customer_Account, Account_Product | Customer-account-product relationship |
| **Bridge_Customer_Card** | Link_Customer_Card, Card_Account | Customer-card-account relationship |

---

## 4. Example DDL (Data Vault 2.0)

### 4.1 Hub: Customer

```sql
CREATE TABLE vault.hub_customer (
    hub_customer_hk    BYTEINT NOT NULL,  -- Hash Key
    customer_bk        VARCHAR(20) NOT NULL,  -- Business Key (CCCD)
    record_source      VARCHAR(100) NOT NULL,
    load_date          TIMESTAMP NOT NULL,
    PRIMARY KEY (hub_customer_hk)
);
```

### 4.2 Satellite: Customer Personal

```sql
CREATE TABLE vault.sat_customer_personal (
    hub_customer_hk    BYTEINT NOT NULL,
    load_date          TIMESTAMP NOT NULL,
    load_end_date      TIMESTAMP,
    hash_diff          BYTEINT NOT NULL,  -- Hash of descriptive columns
    full_name          VARCHAR(200),
    gender             CHAR(1),
    date_of_birth      DATE,
    phone              VARCHAR(20),
    email              VARCHAR(200),
    address            VARCHAR(500),
    city               VARCHAR(100),
    district           VARCHAR(100),
    record_source      VARCHAR(100) NOT NULL,
    PRIMARY KEY (hub_customer_hk, load_date),
    FOREIGN KEY (hub_customer_hk) REFERENCES vault.hub_customer(hub_customer_hk)
);
```

### 4.3 Link: Customer Account

```sql
CREATE TABLE vault.link_customer_account (
    link_customer_account_hk  BYTEINT NOT NULL,
    hub_customer_hk           BYTEINT NOT NULL,
    hub_account_hk            BYTEINT NOT NULL,
    record_source             VARCHAR(100) NOT NULL,
    load_date                 TIMESTAMP NOT NULL,
    PRIMARY KEY (link_customer_account_hk),
    FOREIGN KEY (hub_customer_hk) REFERENCES vault.hub_customer(hub_customer_hk),
    FOREIGN KEY (hub_account_hk) REFERENCES vault.hub_account(hub_account_hk)
);
```

---

## 5. Hash Key Generation

Data Vault 2.0 uses SHA-256 hash keys for indexing:

```python
import hashlib

def generate_hash_key(*values) -> str:
    """Generate SHA-256 hash key from business key values."""
    combined = "|".join(str(v) for v in values)
    return hashlib.sha256(combined.encode()).hexdigest()

# Example:
# hub_customer_hk = generate_hash_key("001234567890")  # CCCD
# link_customer_account_hk = generate_hash_key(customer_hk, account_hk)
# hash_diff = generate_hash_key(full_name, gender, dob, phone, email)
```

---

## 6. When to Use Data Vault 2.0

| Scenario | Recommendation |
|----------|----------------|
| **Banking regulatory compliance** | ✅ Use DV2.0 (audit trail required) |
| **Enterprise data warehouse** | ✅ Use DV2.0 (multiple source systems) |
| **Analytics/BI project** | ❌ Use Kimball (simpler, faster) |
| **Small team / MVP** | ❌ Use Kimball (less overhead) |
| **Multiple source systems merging** | ✅ Use DV2.0 (natural fit) |
| **Real-time CDC ingestion** | ✅ Use DV2.0 (append-only) |
| **Interview discussion** | ✅ Know both, explain trade-offs |

---

## 7. References

- **OCB Bank JD**: "Data Vault 2.0 — Raw Vault + Business Vault + PIT/Bridge"
- **KienlongBank JD**: "Data Vault methodology for data warehouse"
- **DAMA-DMBOK**: Data Management Body of Knowledge
- **Dan Linstedt**: "Data Vault 2.0" — The official methodology
- **Scalefree**: Data Vault modeling patterns and best practices
