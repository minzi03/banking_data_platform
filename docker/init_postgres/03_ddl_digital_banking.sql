-- =============================================================================
-- DDL: Digital Banking Schema (PostgreSQL 15)
-- Schema: digital_banking (5 tables)
-- Purpose: Online/digital channel data for Bronze ingestion
-- Rules: Every table has last_updated TIMESTAMP + BEFORE UPDATE trigger
-- Source: dataset_thamkhao (archive 10 — modern fintech dataset)
-- =============================================================================

-- =============================================================================
-- Trigger function
-- =============================================================================
CREATE OR REPLACE FUNCTION digital_banking.set_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 1. DEVICE (customer devices — mobile/web)
-- =============================================================================
CREATE TABLE IF NOT EXISTS digital_banking.device (
    device_id           BIGINT          NOT NULL,
    customer_id         BIGINT          NOT NULL,        -- logical FK -> core_banking.customer
    device_type         VARCHAR(30)     NOT NULL,        -- MOBILE / TABLET / DESKTOP
    device_fingerprint  VARCHAR(200),                    -- unique device fingerprint
    operating_system    VARCHAR(50),                    -- iOS / Android / Windows / macOS
    ip_address          VARCHAR(45),                    -- IPv4 or IPv6
    is_trusted          SMALLINT        NOT NULL DEFAULT 0,  -- 0/1
    first_seen          TIMESTAMP,
    last_seen           TIMESTAMP,
    last_updated        TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_device PRIMARY KEY (device_id),
    CONSTRAINT chk_device_type CHECK (device_type IN ('MOBILE', 'TABLET', 'DESKTOP')),
    CONSTRAINT chk_device_trusted CHECK (is_trusted IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_device_customer ON digital_banking.device(customer_id);
CREATE INDEX IF NOT EXISTS idx_device_upd ON digital_banking.device(last_updated);

DROP TRIGGER IF EXISTS trg_device_last_upd ON digital_banking.device;
CREATE TRIGGER trg_device_last_upd
    BEFORE UPDATE ON digital_banking.device
    FOR EACH ROW EXECUTE FUNCTION digital_banking.set_last_updated();

-- =============================================================================
-- 2. LOCATION (merchant locations)
-- =============================================================================
CREATE TABLE IF NOT EXISTS digital_banking.location (
    location_id         BIGINT          NOT NULL,
    merchant_name       VARCHAR(200)    NOT NULL,
    merchant_category   VARCHAR(100),                   -- grocery, restaurant, travel, etc.
    city                VARCHAR(100),
    state               VARCHAR(100),
    latitude            NUMERIC(10,7),
    longitude           NUMERIC(10,7),
    is_high_risk_area   SMALLINT        NOT NULL DEFAULT 0,  -- 0/1
    last_updated        TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_location PRIMARY KEY (location_id),
    CONSTRAINT chk_location_risk CHECK (is_high_risk_area IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_location_city ON digital_banking.location(city);
CREATE INDEX IF NOT EXISTS idx_location_upd ON digital_banking.location(last_updated);

DROP TRIGGER IF EXISTS trg_location_last_upd ON digital_banking.location;
CREATE TRIGGER trg_location_last_upd
    BEFORE UPDATE ON digital_banking.location
    FOR EACH ROW EXECUTE FUNCTION digital_banking.set_last_updated();

-- =============================================================================
-- 3. ONLINE_TRANSACTION (digital channel transactions) — large (~3M rows)
-- =============================================================================
CREATE TABLE IF NOT EXISTS digital_banking.online_transaction (
    transaction_id      BIGINT          NOT NULL,
    account_id          BIGINT,                         -- FK -> core_banking.account (nullable for card-only txns)
    device_id           BIGINT,                         -- FK -> digital_banking.device
    location_id         BIGINT,                         -- FK -> digital_banking.location
    customer_id         BIGINT          NOT NULL,        -- denormalized
    transaction_type    VARCHAR(30)     NOT NULL,        -- PURCHASE / TRANSFER / PAYMENT / WITHDRAWAL / TOP_UP
    channel             VARCHAR(20)     NOT NULL,        -- MOBILE_APP / WEB / API / POS
    amount              NUMERIC(18,2)   NOT NULL,
    currency            CHAR(3)         NOT NULL DEFAULT 'VND',
    is_fraud            SMALLINT        NOT NULL DEFAULT 0,  -- 0/1
    fraud_reason        VARCHAR(200),
    status              VARCHAR(20)     NOT NULL DEFAULT 'SUCCESS',  -- SUCCESS / FAILED / PENDING
    transaction_date    TIMESTAMP       NOT NULL,
    created_ts          TIMESTAMP       NOT NULL,
    last_updated        TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_online_txn PRIMARY KEY (transaction_id),
    CONSTRAINT chk_otxn_type CHECK (transaction_type IN ('PURCHASE', 'TRANSFER', 'PAYMENT', 'WITHDRAWAL', 'TOP_UP')),
    CONSTRAINT chk_otxn_channel CHECK (channel IN ('MOBILE_APP', 'WEB', 'API', 'POS')),
    CONSTRAINT chk_otxn_fraud CHECK (is_fraud IN (0, 1)),
    CONSTRAINT chk_otxn_status CHECK (status IN ('SUCCESS', 'FAILED', 'PENDING')),
    CONSTRAINT chk_otxn_amount CHECK (amount > 0)
);

CREATE INDEX IF NOT EXISTS idx_otxn_customer_date ON digital_banking.online_transaction(customer_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_otxn_account_date ON digital_banking.online_transaction(account_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_otxn_fraud ON digital_banking.online_transaction(is_fraud);
CREATE INDEX IF NOT EXISTS idx_otxn_upd ON digital_banking.online_transaction(last_updated);

DROP TRIGGER IF EXISTS trg_online_txn_last_upd ON digital_banking.online_transaction;
CREATE TRIGGER trg_online_txn_last_upd
    BEFORE UPDATE ON digital_banking.online_transaction
    FOR EACH ROW EXECUTE FUNCTION digital_banking.set_last_updated();

-- =============================================================================
-- 4. SUPPORT_TICKET (customer support tickets)
-- =============================================================================
CREATE TABLE IF NOT EXISTS digital_banking.support_ticket (
    ticket_id           BIGINT          NOT NULL,
    customer_id         BIGINT          NOT NULL,
    issue_type          VARCHAR(50)     NOT NULL,        -- TRANSACTION_DISPUTE / ACCOUNT_ACCESS / CARD_BLOCK / GENERAL_INQUIRY / FEEDBACK
    priority            VARCHAR(10)     NOT NULL DEFAULT 'MEDIUM',  -- LOW / MEDIUM / HIGH / URGENT
    status              VARCHAR(20)     NOT NULL,        -- OPEN / IN_PROGRESS / RESOLVED / CLOSED
    date_opened         TIMESTAMP       NOT NULL,
    date_resolved       TIMESTAMP,
    resolution_time_hrs NUMERIC(8,2),                   -- auto-calculated
    satisfaction_score  SMALLINT,                        -- 1-5 (nullable)
    last_updated        TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_support_ticket PRIMARY KEY (ticket_id),
    CONSTRAINT chk_st_issue_type CHECK (issue_type IN ('TRANSACTION_DISPUTE', 'ACCOUNT_ACCESS', 'CARD_BLOCK', 'GENERAL_INQUIRY', 'FEEDBACK')),
    CONSTRAINT chk_st_priority CHECK (priority IN ('LOW', 'MEDIUM', 'HIGH', 'URGENT')),
    CONSTRAINT chk_st_status CHECK (status IN ('OPEN', 'IN_PROGRESS', 'RESOLVED', 'CLOSED')),
    CONSTRAINT chk_st_score CHECK (satisfaction_score IS NULL OR satisfaction_score BETWEEN 1 AND 5)
);

CREATE INDEX IF NOT EXISTS idx_st_customer ON digital_banking.support_ticket(customer_id);
CREATE INDEX IF NOT EXISTS idx_st_status ON digital_banking.support_ticket(status);
CREATE INDEX IF NOT EXISTS idx_st_upd ON digital_banking.support_ticket(last_updated);

DROP TRIGGER IF EXISTS trg_support_ticket_last_upd ON digital_banking.support_ticket;
CREATE TRIGGER trg_support_ticket_last_upd
    BEFORE UPDATE ON digital_banking.support_ticket
    FOR EACH ROW EXECUTE FUNCTION digital_banking.set_last_updated();

-- =============================================================================
-- 5. MCC_CODE (Merchant Category Codes)
-- =============================================================================
CREATE TABLE IF NOT EXISTS digital_banking.mcc_code (
    mcc_code            VARCHAR(10)     NOT NULL,
    description         VARCHAR(200)    NOT NULL,
    category_group      VARCHAR(50)     NOT NULL,        -- RETAIL / FOOD / TRAVEL / SERVICES / UTILITIES
    is_high_risk        SMALLINT        NOT NULL DEFAULT 0,  -- 0/1
    last_updated        TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_mcc_code PRIMARY KEY (mcc_code),
    CONSTRAINT chk_mcc_risk CHECK (is_high_risk IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_mcc_group ON digital_banking.mcc_code(category_group);
CREATE INDEX IF NOT EXISTS idx_mcc_upd ON digital_banking.mcc_code(last_updated);

DROP TRIGGER IF EXISTS trg_mcc_code_last_upd ON digital_banking.mcc_code;
CREATE TRIGGER trg_mcc_code_last_upd
    BEFORE UPDATE ON digital_banking.mcc_code
    FOR EACH ROW EXECUTE FUNCTION digital_banking.set_last_updated();
