-- =============================================================================
-- CDC Setup — Banking Data Platform
-- Enable logical replication for Debezium CDC connector
-- =============================================================================

-- Enable logical replication for CDC
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;

-- Create CDC user with replication privileges
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'cdc_user') THEN
        CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'CDCPassword123';
    END IF;
END
$$;

-- Grant privileges to CDC user
GRANT USAGE ON SCHEMA core_banking TO cdc_user;
GRANT USAGE ON SCHEMA card_crm TO cdc_user;
GRANT USAGE ON SCHEMA digital_banking TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA core_banking TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA card_crm TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA digital_banking TO cdc_user;

-- Grant replication privilege
ALTER ROLE cdc_user WITH REPLICATION;

-- Create publication for Debezium
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'debezium_pub') THEN
        CREATE PUBLICATION debezium_pub FOR ALL TABLES;
    END IF;
END
$$;

COMMENT ON ROLE cdc_user IS 'CDC user for Debezium connector with replication privileges';
