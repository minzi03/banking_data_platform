-- Minimal isolated source schema for TD-22; never mounted by the shared stack.
CREATE SCHEMA IF NOT EXISTS digital_banking;

CREATE TABLE IF NOT EXISTS digital_banking.online_transaction (
    transaction_id BIGINT PRIMARY KEY,
    account_id BIGINT,
    device_id BIGINT,
    location_id BIGINT,
    customer_id BIGINT NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    channel VARCHAR(20) NOT NULL,
    amount NUMERIC(18,2) NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'VND',
    is_fraud SMALLINT NOT NULL DEFAULT 0,
    fraud_reason VARCHAR(200),
    status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',
    merchant_id BIGINT,
    payment_channel VARCHAR(10),
    refund_flag SMALLINT NOT NULL DEFAULT 0,
    transaction_date TIMESTAMP NOT NULL,
    created_ts TIMESTAMP NOT NULL,
    last_updated TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE USER cdc_user WITH PASSWORD 'merchant_e2e_cdc_only';
GRANT CONNECT ON DATABASE merchant_e2e TO cdc_user;
GRANT USAGE ON SCHEMA digital_banking TO cdc_user;
GRANT SELECT ON digital_banking.online_transaction TO cdc_user;
ALTER TABLE digital_banking.online_transaction REPLICA IDENTITY FULL;
ALTER SYSTEM SET wal_level = 'logical';
CREATE PUBLICATION merchant_e2e_publication FOR TABLE digital_banking.online_transaction;
