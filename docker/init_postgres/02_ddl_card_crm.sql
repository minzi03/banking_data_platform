-- =============================================================================
-- DDL: Card & CRM Schema (PostgreSQL 15)
-- Schema: card_crm (3 tables)
-- Purpose: Source data for Bronze ingestion pipeline
-- Rules: Every table has last_updated TIMESTAMP + BEFORE UPDATE trigger
-- =============================================================================

-- =============================================================================
-- Trigger function
-- =============================================================================
CREATE OR REPLACE FUNCTION card_crm.set_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 1. CARD (card information)
-- =============================================================================
CREATE TABLE card_crm.card (
    card_id         BIGINT          NOT NULL,
    card_no_masked  VARCHAR(19)     NOT NULL,       -- format: 4111****1234
    customer_id     BIGINT          NOT NULL,        -- logical FK -> core_banking.customer
    account_id      BIGINT,                          -- logical FK -> core_banking.account (for debit cards)
    product_code    VARCHAR(20)     NOT NULL,
    card_type       VARCHAR(20)     NOT NULL,        -- DEBIT / CREDIT / PREPAID
    card_brand      VARCHAR(20)     NOT NULL,        -- VISA / MASTER / JCB / NAPAS
    credit_limit    NUMERIC(18,2),                   -- only for CREDIT cards
    issue_date      DATE            NOT NULL,
    expiry_date     DATE            NOT NULL,
    status          VARCHAR(20)     NOT NULL,        -- ACTIVE / BLOCKED / EXPIRED / CLOSED
    last_updated    TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_card PRIMARY KEY (card_id),
    CONSTRAINT uq_card_no_masked UNIQUE (card_no_masked),
    CONSTRAINT chk_card_type CHECK (card_type IN ('DEBIT', 'CREDIT', 'PREPAID')),
    CONSTRAINT chk_card_brand CHECK (card_brand IN ('VISA', 'MASTER', 'JCB', 'NAPAS')),
    CONSTRAINT chk_card_status CHECK (status IN ('ACTIVE', 'BLOCKED', 'EXPIRED', 'CLOSED')),
    CONSTRAINT chk_credit_limit CHECK (
        (card_type = 'CREDIT' AND credit_limit IS NOT NULL AND credit_limit > 0)
        OR (card_type <> 'CREDIT' AND credit_limit IS NULL)
    )
);

CREATE INDEX idx_card_customer ON card_crm.card(customer_id);
CREATE INDEX idx_card_status ON card_crm.card(status);
CREATE INDEX idx_card_last_upd ON card_crm.card(last_updated);

CREATE TRIGGER trg_card_last_upd
    BEFORE UPDATE ON card_crm.card
    FOR EACH ROW EXECUTE FUNCTION card_crm.set_last_updated();

-- =============================================================================
-- 2. CARD_TXN (card transactions) — large table (~500K rows/month)
-- =============================================================================
CREATE TABLE card_crm.card_txn (
    txn_id              BIGINT          NOT NULL,
    card_id             BIGINT          NOT NULL,
    customer_id         BIGINT          NOT NULL,        -- denormalized
    txn_date            TIMESTAMP       NOT NULL,
    txn_amount          NUMERIC(18,2)   NOT NULL,
    txn_type            VARCHAR(20)     NOT NULL,        -- PURCHASE / CASH_ADVANCE / REFUND / REVERSAL
    currency            CHAR(3)         NOT NULL DEFAULT 'VND',
    merchant_name       VARCHAR(200),
    merchant_category   VARCHAR(50),                    -- GROCERY / RESTAURANT / TRAVEL / ECOM / FUEL / EDUCATION
    channel             VARCHAR(20)     NOT NULL,        -- POS / ECOM / ATM
    status              VARCHAR(20)     NOT NULL,        -- SUCCESS / FAILED / PENDING
    created_ts          TIMESTAMP       NOT NULL,
    last_updated        TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_card_txn PRIMARY KEY (txn_id),
    CONSTRAINT fk_card_txn_card FOREIGN KEY (card_id) REFERENCES card_crm.card(card_id),
    CONSTRAINT chk_ct_txn_type CHECK (txn_type IN ('PURCHASE', 'CASH_ADVANCE', 'REFUND', 'REVERSAL')),
    CONSTRAINT chk_ct_txn_channel CHECK (channel IN ('POS', 'ECOM', 'ATM')),
    CONSTRAINT chk_ct_txn_status CHECK (status IN ('SUCCESS', 'FAILED', 'PENDING')),
    CONSTRAINT chk_ct_txn_amount CHECK (txn_amount <> 0)
);

CREATE INDEX idx_card_txn_card_date ON card_crm.card_txn(card_id, txn_date);
CREATE INDEX idx_card_txn_cust_date ON card_crm.card_txn(customer_id, txn_date);
CREATE INDEX idx_card_txn_last_upd ON card_crm.card_txn(last_updated);

CREATE TRIGGER trg_card_txn_last_upd
    BEFORE UPDATE ON card_crm.card_txn
    FOR EACH ROW EXECUTE FUNCTION card_crm.set_last_updated();

-- =============================================================================
-- 3. CRM_INTERACTION (CRM interactions) — ~50K rows/month
-- =============================================================================
CREATE TABLE card_crm.crm_interaction (
    interaction_id      BIGINT          NOT NULL,
    customer_id         BIGINT          NOT NULL,
    interaction_date    TIMESTAMP       NOT NULL,
    channel             VARCHAR(20)     NOT NULL,        -- CALL / EMAIL / CHAT / BRANCH / SMS
    direction           VARCHAR(10)     NOT NULL,        -- INBOUND / OUTBOUND
    subject             VARCHAR(500),
    category            VARCHAR(30)     NOT NULL,        -- COMPLAINT / INQUIRY / CAMPAIGN / CROSS_SELL / RETENTION
    status              VARCHAR(20)     NOT NULL,        -- OPEN / RESOLVED / PENDING
    assigned_to         VARCHAR(100),
    satisfaction_score  SMALLINT,                        -- 1-5 rating (nullable)
    created_ts          TIMESTAMP       NOT NULL,
    last_updated        TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_crm_interaction PRIMARY KEY (interaction_id),
    CONSTRAINT chk_crm_channel CHECK (channel IN ('CALL', 'EMAIL', 'CHAT', 'BRANCH', 'SMS')),
    CONSTRAINT chk_crm_direction CHECK (direction IN ('INBOUND', 'OUTBOUND')),
    CONSTRAINT chk_crm_category CHECK (category IN ('COMPLAINT', 'INQUIRY', 'CAMPAIGN', 'CROSS_SELL', 'RETENTION')),
    CONSTRAINT chk_crm_status CHECK (status IN ('OPEN', 'RESOLVED', 'PENDING')),
    CONSTRAINT chk_crm_score CHECK (satisfaction_score IS NULL OR satisfaction_score BETWEEN 1 AND 5)
);

CREATE INDEX idx_crm_customer_date ON card_crm.crm_interaction(customer_id, interaction_date);
CREATE INDEX idx_crm_category ON card_crm.crm_interaction(category);
CREATE INDEX idx_crm_last_upd ON card_crm.crm_interaction(last_updated);

CREATE TRIGGER trg_crm_interaction_last_upd
    BEFORE UPDATE ON card_crm.crm_interaction
    FOR EACH ROW EXECUTE FUNCTION card_crm.set_last_updated();
