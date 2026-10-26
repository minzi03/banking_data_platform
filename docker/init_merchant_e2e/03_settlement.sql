-- =============================================================================
-- Isolated TD-22 settlement feed fixture for EOD settlement reconciliation.
--
-- The shared project reads settlement from an SFTP landing zone into
-- lakehouse.bronze.pos_settlement. The isolated E2E stack has no SFTP service,
-- so this seed creates the same contract table directly in Postgres and provides
-- a deterministic fixture whose gross_amount and txn_count agree with the
-- transaction cohort seeded by 01_schema.sql for cob_dt 2026-10-18.
--
-- This fixture is intentionally bounded to the deterministic E2E cohort and is
-- NOT a claim about the production SFTP transport.
-- =============================================================================

CREATE TABLE IF NOT EXISTS digital_banking.pos_settlement (
    settlement_id    BIGINT,
    merchant_id      BIGINT,
    settlement_date  DATE,
    acquirer_ref     VARCHAR(50),
    gross_amount     DECIMAL(18,2),
    fee_amount       DECIMAL(18,2),
    net_amount       DECIMAL(18,2),
    currency         VARCHAR(10),
    txn_count        BIGINT,
    status           VARCHAR(20)
);

INSERT INTO digital_banking.pos_settlement
    (settlement_id, merchant_id, settlement_date, acquirer_ref, gross_amount, fee_amount, net_amount, currency, txn_count, status)
VALUES
    (900001, 900001, DATE '2026-10-18', 'ACQ-E2E-000001', 10000.00, 50.00, 9950.00, 'VND', 1, 'SETTLED'),
    (900002, 900002, DATE '2026-10-18', 'ACQ-E2E-000002', 30000.00, 150.00, 29850.00, 'VND', 2, 'SETTLED');
