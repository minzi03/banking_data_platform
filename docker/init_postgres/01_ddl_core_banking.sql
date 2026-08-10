-- =============================================================================
-- DDL: Core Banking Schema (PostgreSQL 15)
-- Schema: core_banking (8 tables)
-- Purpose: Primary source data for Bronze ingestion pipeline
-- Rules: Every table has last_updated TIMESTAMP + BEFORE UPDATE trigger
-- =============================================================================

-- =============================================================================
-- Trigger function: auto-update last_updated on any UPDATE
-- =============================================================================
CREATE OR REPLACE FUNCTION core_banking.set_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 1. BRANCH (bank branches)
-- =============================================================================
CREATE TABLE IF NOT EXISTS core_banking.branch (
    branch_code     VARCHAR(10)     NOT NULL,
    branch_name     VARCHAR(200)    NOT NULL,
    region          VARCHAR(20)     NOT NULL,       -- NORTH / CENTRAL / SOUTH
    city            VARCHAR(100)    NOT NULL,
    district        VARCHAR(100),
    address         VARCHAR(500),
    manager_name    VARCHAR(200),
    open_date       DATE,
    status          VARCHAR(20)     NOT NULL,       -- ACTIVE / CLOSED
    last_updated    TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_branch PRIMARY KEY (branch_code),
    CONSTRAINT chk_branch_region CHECK (region IN ('NORTH', 'CENTRAL', 'SOUTH')),
    CONSTRAINT chk_branch_status CHECK (status IN ('ACTIVE', 'CLOSED'))
);

DROP TRIGGER IF EXISTS trg_branch_last_upd ON core_banking.branch;
CREATE TRIGGER trg_branch_last_upd
    BEFORE UPDATE ON core_banking.branch
    FOR EACH ROW EXECUTE FUNCTION core_banking.set_last_updated();

-- =============================================================================
-- 2. PRODUCT (banking products)
-- =============================================================================
CREATE TABLE IF NOT EXISTS core_banking.product (
    product_code    VARCHAR(20)     NOT NULL,
    product_name    VARCHAR(200)    NOT NULL,
    product_group   VARCHAR(20)     NOT NULL,       -- DEPOSIT / LOAN / CARD
    product_type    VARCHAR(30)     NOT NULL,       -- CASA / SAVINGS / PERSONAL_LOAN / MORTGAGE / CREDIT_CARD / DEBIT_CARD
    currency        CHAR(3)         NOT NULL,       -- VND / USD
    is_active       SMALLINT        NOT NULL,       -- 0 / 1
    launch_date     DATE,
    last_updated    TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_product PRIMARY KEY (product_code),
    CONSTRAINT chk_product_group CHECK (product_group IN ('DEPOSIT', 'LOAN', 'CARD')),
    CONSTRAINT chk_product_type CHECK (product_type IN ('CASA', 'SAVINGS', 'PERSONAL_LOAN', 'MORTGAGE', 'CREDIT_CARD', 'DEBIT_CARD')),
    CONSTRAINT chk_product_active CHECK (is_active IN (0, 1))
);

DROP TRIGGER IF EXISTS trg_product_last_upd ON core_banking.product;
CREATE TRIGGER trg_product_last_upd
    BEFORE UPDATE ON core_banking.product
    FOR EACH ROW EXECUTE FUNCTION core_banking.set_last_updated();

-- =============================================================================
-- 3. CUSTOMER (customers)
-- =============================================================================
CREATE TABLE IF NOT EXISTS core_banking.customer (
    customer_id         BIGINT          NOT NULL,
    cccd                VARCHAR(12),                    -- National ID (12 digits)
    full_name           VARCHAR(200)    NOT NULL,
    gender              CHAR(1)         NOT NULL,       -- M / F / O
    date_of_birth       DATE            NOT NULL,
    phone               VARCHAR(15),                    -- 0xxxxxxxxx
    email               VARCHAR(200),
    address             VARCHAR(500),
    city                VARCHAR(100),
    district            VARCHAR(100),
    branch_code         VARCHAR(10),                    -- FK -> branch
    customer_segment    VARCHAR(20)     NOT NULL,       -- RETAIL / PRIORITY / VIP
    kyc_status          VARCHAR(20)     NOT NULL,       -- PENDING / VERIFIED / REJECTED
    register_date       DATE            NOT NULL,
    is_active           SMALLINT        NOT NULL,       -- 0 / 1
    last_updated        TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_customer PRIMARY KEY (customer_id),
    CONSTRAINT uq_customer_cccd UNIQUE (cccd),
    CONSTRAINT fk_customer_branch FOREIGN KEY (branch_code) REFERENCES core_banking.branch(branch_code),
    CONSTRAINT chk_customer_gender CHECK (gender IN ('M', 'F', 'O')),
    CONSTRAINT chk_customer_segment CHECK (customer_segment IN ('RETAIL', 'PRIORITY', 'VIP')),
    CONSTRAINT chk_customer_kyc CHECK (kyc_status IN ('PENDING', 'VERIFIED', 'REJECTED')),
    CONSTRAINT chk_customer_active CHECK (is_active IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_customer_branch ON core_banking.customer(branch_code);
CREATE INDEX IF NOT EXISTS idx_customer_segment ON core_banking.customer(customer_segment);
CREATE INDEX IF NOT EXISTS idx_customer_upd ON core_banking.customer(last_updated);

DROP TRIGGER IF EXISTS trg_customer_last_upd ON core_banking.customer;
CREATE TRIGGER trg_customer_last_upd
    BEFORE UPDATE ON core_banking.customer
    FOR EACH ROW EXECUTE FUNCTION core_banking.set_last_updated();

-- =============================================================================
-- 4. ACCOUNT (deposits & current accounts)
-- =============================================================================
CREATE TABLE IF NOT EXISTS core_banking.account (
    account_id      BIGINT          NOT NULL,
    account_no      VARCHAR(20)     NOT NULL,
    customer_id     BIGINT          NOT NULL,
    product_code    VARCHAR(20)     NOT NULL,
    branch_code     VARCHAR(10)     NOT NULL,
    account_type    VARCHAR(20)     NOT NULL,       -- CASA / TIME_DEPOSIT
    currency        CHAR(3)         NOT NULL DEFAULT 'VND',
    balance         NUMERIC(18,2)   NOT NULL DEFAULT 0,
    open_date       DATE            NOT NULL,
    close_date      DATE,
    status          VARCHAR(20)     NOT NULL,       -- ACTIVE / CLOSED / FROZEN
    last_updated    TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_account PRIMARY KEY (account_id),
    CONSTRAINT uq_account_no UNIQUE (account_no),
    CONSTRAINT fk_account_customer FOREIGN KEY (customer_id) REFERENCES core_banking.customer(customer_id),
    CONSTRAINT fk_account_product FOREIGN KEY (product_code) REFERENCES core_banking.product(product_code),
    CONSTRAINT fk_account_branch FOREIGN KEY (branch_code) REFERENCES core_banking.branch(branch_code),
    CONSTRAINT chk_account_type CHECK (account_type IN ('CASA', 'TIME_DEPOSIT')),
    CONSTRAINT chk_account_status CHECK (status IN ('ACTIVE', 'CLOSED', 'FROZEN'))
);

CREATE INDEX IF NOT EXISTS idx_account_customer ON core_banking.account(customer_id);
CREATE INDEX IF NOT EXISTS idx_account_upd ON core_banking.account(last_updated);

DROP TRIGGER IF EXISTS trg_account_last_upd ON core_banking.account;
CREATE TRIGGER trg_account_last_upd
    BEFORE UPDATE ON core_banking.account
    FOR EACH ROW EXECUTE FUNCTION core_banking.set_last_updated();

-- =============================================================================
-- 5. DEPOSIT (savings certificates)
-- =============================================================================
CREATE TABLE IF NOT EXISTS core_banking.deposit (
    deposit_id          BIGINT          NOT NULL,
    account_id          BIGINT,                         -- FK -> account (can be NULL for standalone savings)
    customer_id         BIGINT          NOT NULL,
    product_code        VARCHAR(20)     NOT NULL,
    principal_amount    NUMERIC(18,2)   NOT NULL,
    interest_rate       NUMERIC(5,2)    NOT NULL,       -- e.g. 5.50 = 5.5%/year
    term_months         SMALLINT        NOT NULL,       -- 1/3/6/12/24/36
    open_date           DATE            NOT NULL,
    maturity_date       DATE            NOT NULL,
    status              VARCHAR(20)     NOT NULL,       -- ACTIVE / MATURED / EARLY_WITHDRAWN
    last_updated        TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_deposit PRIMARY KEY (deposit_id),
    CONSTRAINT fk_deposit_account FOREIGN KEY (account_id) REFERENCES core_banking.account(account_id),
    CONSTRAINT fk_deposit_customer FOREIGN KEY (customer_id) REFERENCES core_banking.customer(customer_id),
    CONSTRAINT fk_deposit_product FOREIGN KEY (product_code) REFERENCES core_banking.product(product_code),
    CONSTRAINT chk_deposit_status CHECK (status IN ('ACTIVE', 'MATURED', 'EARLY_WITHDRAWN')),
    CONSTRAINT chk_deposit_rate CHECK (interest_rate > 0),
    CONSTRAINT chk_deposit_term CHECK (term_months IN (1, 3, 6, 12, 24, 36))
);

CREATE INDEX IF NOT EXISTS idx_deposit_customer ON core_banking.deposit(customer_id);
CREATE INDEX IF NOT EXISTS idx_deposit_upd ON core_banking.deposit(last_updated);

DROP TRIGGER IF EXISTS trg_deposit_last_upd ON core_banking.deposit;
CREATE TRIGGER trg_deposit_last_upd
    BEFORE UPDATE ON core_banking.deposit
    FOR EACH ROW EXECUTE FUNCTION core_banking.set_last_updated();

-- =============================================================================
-- 6. LOAN (loans)
-- =============================================================================
CREATE TABLE IF NOT EXISTS core_banking.loan (
    loan_id             BIGINT          NOT NULL,
    customer_id         BIGINT          NOT NULL,
    product_code        VARCHAR(20)     NOT NULL,
    branch_code         VARCHAR(10)     NOT NULL,
    loan_amount         NUMERIC(18,2)   NOT NULL,       -- original loan amount
    outstanding_balance NUMERIC(18,2)   NOT NULL,       -- current outstanding balance
    interest_rate       NUMERIC(5,2)    NOT NULL,
    term_months         SMALLINT        NOT NULL,
    disbursement_date   DATE            NOT NULL,
    maturity_date       DATE            NOT NULL,
    loan_status         VARCHAR(20)     NOT NULL,       -- ACTIVE / CLOSED / OVERDUE / WRITTEN_OFF
    last_updated        TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_loan PRIMARY KEY (loan_id),
    CONSTRAINT fk_loan_customer FOREIGN KEY (customer_id) REFERENCES core_banking.customer(customer_id),
    CONSTRAINT fk_loan_product FOREIGN KEY (product_code) REFERENCES core_banking.product(product_code),
    CONSTRAINT fk_loan_branch FOREIGN KEY (branch_code) REFERENCES core_banking.branch(branch_code),
    CONSTRAINT chk_loan_status CHECK (loan_status IN ('ACTIVE', 'CLOSED', 'OVERDUE', 'WRITTEN_OFF')),
    CONSTRAINT chk_loan_amount CHECK (loan_amount > 0)
);

CREATE INDEX IF NOT EXISTS idx_loan_customer ON core_banking.loan(customer_id);
CREATE INDEX IF NOT EXISTS idx_loan_status ON core_banking.loan(loan_status);
CREATE INDEX IF NOT EXISTS idx_loan_upd ON core_banking.loan(last_updated);

DROP TRIGGER IF EXISTS trg_loan_last_upd ON core_banking.loan;
CREATE TRIGGER trg_loan_last_upd
    BEFORE UPDATE ON core_banking.loan
    FOR EACH ROW EXECUTE FUNCTION core_banking.set_last_updated();

-- =============================================================================
-- 7. TXN_ACCOUNT (account transactions) — largest table (~1M+ rows/month)
-- =============================================================================
CREATE TABLE IF NOT EXISTS core_banking.txn_account (
    txn_id          BIGINT          NOT NULL,
    account_id      BIGINT          NOT NULL,
    customer_id     BIGINT          NOT NULL,           -- denormalized for query speed
    txn_date        TIMESTAMP       NOT NULL,
    txn_amount      NUMERIC(18,2)   NOT NULL,
    txn_type        VARCHAR(30)     NOT NULL,           -- DEPOSIT / WITHDRAWAL / TRANSFER_IN / TRANSFER_OUT / FEE / INTEREST
    debit_credit    CHAR(1)         NOT NULL,           -- D (Debit) / C (Credit)
    balance_after   NUMERIC(18,2)   NOT NULL,
    channel         VARCHAR(20)     NOT NULL,           -- BRANCH / ATM / INTERNET_BANKING / MOBILE_BANKING / POS
    description     VARCHAR(500),
    counter_account VARCHAR(20),                        -- counterparty account (for transfers)
    created_ts      TIMESTAMP       NOT NULL,
    last_updated    TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_txn_account PRIMARY KEY (txn_id),
    CONSTRAINT fk_txn_account_acct FOREIGN KEY (account_id) REFERENCES core_banking.account(account_id),
    CONSTRAINT chk_txn_type CHECK (txn_type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER_IN', 'TRANSFER_OUT', 'FEE', 'INTEREST')),
    CONSTRAINT chk_txn_dc CHECK (debit_credit IN ('D', 'C')),
    CONSTRAINT chk_txn_channel CHECK (channel IN ('BRANCH', 'ATM', 'INTERNET_BANKING', 'MOBILE_BANKING', 'POS'))
);

-- Indexes for incremental ingest and customer-level queries
CREATE INDEX IF NOT EXISTS idx_txn_acct_date ON core_banking.txn_account(account_id, txn_date);
CREATE INDEX IF NOT EXISTS idx_txn_cust_date ON core_banking.txn_account(customer_id, txn_date);
CREATE INDEX IF NOT EXISTS idx_txn_upd ON core_banking.txn_account(last_updated);

DROP TRIGGER IF EXISTS trg_txn_account_last_upd ON core_banking.txn_account;
CREATE TRIGGER trg_txn_account_last_upd
    BEFORE UPDATE ON core_banking.txn_account
    FOR EACH ROW EXECUTE FUNCTION core_banking.set_last_updated();

-- =============================================================================
-- 8. EMPLOYEE (bank employees)
-- =============================================================================
CREATE TABLE IF NOT EXISTS core_banking.employee (
    employee_id     BIGINT          NOT NULL,
    full_name       VARCHAR(200)    NOT NULL,
    branch_code     VARCHAR(10)     NOT NULL,
    role            VARCHAR(50)     NOT NULL,           -- TELLER / MANAGER / ANALYST / DIRECTOR
    hire_date       DATE            NOT NULL,
    salary          NUMERIC(12,2)   NOT NULL,
    status          VARCHAR(20)     NOT NULL,           -- ACTIVE / TERMINATED
    last_updated    TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT pk_employee PRIMARY KEY (employee_id),
    CONSTRAINT fk_employee_branch FOREIGN KEY (branch_code) REFERENCES core_banking.branch(branch_code),
    CONSTRAINT chk_employee_role CHECK (role IN ('TELLER', 'MANAGER', 'ANALYST', 'DIRECTOR')),
    CONSTRAINT chk_employee_status CHECK (status IN ('ACTIVE', 'TERMINATED'))
);

CREATE INDEX IF NOT EXISTS idx_employee_branch ON core_banking.employee(branch_code);
CREATE INDEX IF NOT EXISTS idx_employee_upd ON core_banking.employee(last_updated);

DROP TRIGGER IF EXISTS trg_employee_last_upd ON core_banking.employee;
CREATE TRIGGER trg_employee_last_upd
    BEFORE UPDATE ON core_banking.employee
    FOR EACH ROW EXECUTE FUNCTION core_banking.set_last_updated();
