-- =============================================================================
-- Data Vault 2.0 — Example DDL
-- Banking Data Platform
-- =============================================================================
-- This is a REFERENCE implementation showing how to map the current Kimball
-- star schema to Data Vault 2.0. It covers the Customer domain only.
--
-- In production, this would be created in a separate 'vault' schema.
-- =============================================================================

-- ── Schema ──────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS vault;

-- ── Hash Key Function ───────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION vault.generate_hash_key(VARIADIC vals TEXT[])
RETURNS TEXT AS $$
DECLARE
    combined TEXT;
BEGIN
    SELECT string_agg(v, '|' ORDER BY ordinality)
    INTO combined
    FROM unnest(vals) WITH ORDINALITY AS t(v, ordinality);
    RETURN encode(sha256(combined::bytea), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- =============================================================================
-- HUBS (Business Keys)
-- =============================================================================

-- Hub: Customer
CREATE TABLE IF NOT EXISTS vault.hub_customer (
    hub_customer_hk    TEXT NOT NULL PRIMARY KEY,
    customer_bk        VARCHAR(20) NOT NULL,  -- CCCD (business key)
    record_source      VARCHAR(100) NOT NULL DEFAULT 'core_banking.customer',
    load_date          TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Hub: Account
CREATE TABLE IF NOT EXISTS vault.hub_account (
    hub_account_hk     TEXT NOT NULL PRIMARY KEY,
    account_no         VARCHAR(20) NOT NULL,  -- Account number
    record_source      VARCHAR(100) NOT NULL DEFAULT 'core_banking.account',
    load_date          TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Hub: Product
CREATE TABLE IF NOT EXISTS vault.hub_product (
    hub_product_hk     TEXT NOT NULL PRIMARY KEY,
    product_code       VARCHAR(10) NOT NULL,
    record_source      VARCHAR(100) NOT NULL DEFAULT 'core_banking.product',
    load_date          TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Hub: Branch
CREATE TABLE IF NOT EXISTS vault.hub_branch (
    hub_branch_hk      TEXT NOT NULL PRIMARY KEY,
    branch_code        VARCHAR(10) NOT NULL,
    record_source      VARCHAR(100) NOT NULL DEFAULT 'core_banking.branch',
    load_date          TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- SATELLITES (Descriptive Attributes — SCD2)
-- =============================================================================

-- Satellite: Customer Personal Info (SCD2)
CREATE TABLE IF NOT EXISTS vault.sat_customer_personal (
    hub_customer_hk    TEXT NOT NULL,
    load_date          TIMESTAMP NOT NULL,
    load_end_date      TIMESTAMP,
    hash_diff          TEXT NOT NULL,
    full_name          VARCHAR(200),
    gender             CHAR(1),
    date_of_birth      DATE,
    phone              VARCHAR(20),
    email              VARCHAR(200),
    address            VARCHAR(500),
    city               VARCHAR(100),
    district           VARCHAR(100),
    record_source      VARCHAR(100) NOT NULL DEFAULT 'core_banking.customer',
    PRIMARY KEY (hub_customer_hk, load_date),
    FOREIGN KEY (hub_customer_hk) REFERENCES vault.hub_customer(hub_customer_hk)
);

-- Satellite: Customer Status (SCD2)
CREATE TABLE IF NOT EXISTS vault.sat_customer_status (
    hub_customer_hk    TEXT NOT NULL,
    load_date          TIMESTAMP NOT NULL,
    load_end_date      TIMESTAMP,
    hash_diff          TEXT NOT NULL,
    customer_segment   VARCHAR(20),
    kyc_status         VARCHAR(20),
    is_active          SMALLINT,
    register_date      DATE,
    record_source      VARCHAR(100) NOT NULL DEFAULT 'core_banking.customer',
    PRIMARY KEY (hub_customer_hk, load_date),
    FOREIGN KEY (hub_customer_hk) REFERENCES vault.hub_customer(hub_customer_hk)
);

-- Satellite: Account Details (SCD2)
CREATE TABLE IF NOT EXISTS vault.sat_account_details (
    hub_account_hk     TEXT NOT NULL,
    load_date          TIMESTAMP NOT NULL,
    load_end_date      TIMESTAMP,
    hash_diff          TEXT NOT NULL,
    account_type       VARCHAR(20),
    currency           VARCHAR(3),
    balance            NUMERIC(18,2),
    open_date          DATE,
    close_date         DATE,
    status             VARCHAR(20),
    record_source      VARCHAR(100) NOT NULL DEFAULT 'core_banking.account',
    PRIMARY KEY (hub_account_hk, load_date),
    FOREIGN KEY (hub_account_hk) REFERENCES vault.hub_account(hub_account_hk)
);

-- Satellite: Branch Details (SCD1 — overwrite)
CREATE TABLE IF NOT EXISTS vault.sat_branch_details (
    hub_branch_hk      TEXT NOT NULL,
    load_date          TIMESTAMP NOT NULL,
    hash_diff          TEXT NOT NULL,
    branch_name        VARCHAR(200),
    region             VARCHAR(20),
    city               VARCHAR(100),
    district           VARCHAR(100),
    address            VARCHAR(500),
    manager_name       VARCHAR(100),
    status             VARCHAR(20),
    record_source      VARCHAR(100) NOT NULL DEFAULT 'core_banking.branch',
    PRIMARY KEY (hub_branch_hk, load_date),
    FOREIGN KEY (hub_branch_hk) REFERENCES vault.hub_branch(hub_branch_hk)
);

-- =============================================================================
-- LINKS (Relationships)
-- =============================================================================

-- Link: Customer-Account
CREATE TABLE IF NOT EXISTS vault.link_customer_account (
    link_customer_account_hk  TEXT NOT NULL PRIMARY KEY,
    hub_customer_hk           TEXT NOT NULL,
    hub_account_hk            TEXT NOT NULL,
    record_source             VARCHAR(100) NOT NULL DEFAULT 'core_banking.account',
    load_date                 TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (hub_customer_hk) REFERENCES vault.hub_customer(hub_customer_hk),
    FOREIGN KEY (hub_account_hk) REFERENCES vault.hub_account(hub_account_hk)
);

-- Link: Account-Product
CREATE TABLE IF NOT EXISTS vault.link_account_product (
    link_account_product_hk   TEXT NOT NULL PRIMARY KEY,
    hub_account_hk            TEXT NOT NULL,
    hub_product_hk            TEXT NOT NULL,
    record_source             VARCHAR(100) NOT NULL DEFAULT 'core_banking.account',
    load_date                 TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (hub_account_hk) REFERENCES vault.hub_account(hub_account_hk),
    FOREIGN KEY (hub_product_hk) REFERENCES vault.hub_product(hub_product_hk)
);

-- Link: Customer-Branch
CREATE TABLE IF NOT EXISTS vault.link_customer_branch (
    link_customer_branch_hk   TEXT NOT NULL PRIMARY KEY,
    hub_customer_hk           TEXT NOT NULL,
    hub_branch_hk             TEXT NOT NULL,
    record_source             VARCHAR(100) NOT NULL DEFAULT 'core_banking.customer',
    load_date                 TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (hub_customer_hk) REFERENCES vault.hub_customer(hub_customer_hk),
    FOREIGN KEY (hub_branch_hk) REFERENCES vault.hub_branch(hub_branch_hk)
);

-- =============================================================================
-- PIT TABLES (Point-in-Time — for efficient querying)
-- =============================================================================

-- PIT: Customer Daily (snapshot of all satellites)
CREATE TABLE IF NOT EXISTS vault.pit_customer_daily (
    hub_customer_hk    TEXT NOT NULL,
    snapshot_date      DATE NOT NULL,
    sat_customer_personalLD  TIMESTAMP,  -- Load date of this satellite
    sat_customer_statusLD    TIMESTAMP,
    record_source      VARCHAR(100),
    PRIMARY KEY (hub_customer_hk, snapshot_date)
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_sat_customer_personal_hk ON vault.sat_customer_personal(hub_customer_hk);
CREATE INDEX IF NOT EXISTS idx_sat_customer_status_hk ON vault.sat_customer_status(hub_customer_hk);
CREATE INDEX IF NOT EXISTS idx_sat_account_details_hk ON vault.sat_account_details(hub_account_hk);
CREATE INDEX IF NOT EXISTS idx_link_customer_account_cust ON vault.link_customer_account(hub_customer_hk);
CREATE INDEX IF NOT EXISTS idx_link_customer_account_acct ON vault.link_customer_account(hub_account_hk);
