-- =============================================================================
-- CDC Setup — Banking Data Platform
-- Enable logical replication for Debezium CDC connector
-- =============================================================================

ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'cdc_user') THEN
        CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'CDCPassword123';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA core_banking TO cdc_user;
GRANT USAGE ON SCHEMA card_crm TO cdc_user;
GRANT USAGE ON SCHEMA digital_banking TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA core_banking TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA card_crm TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA digital_banking TO cdc_user;
ALTER ROLE cdc_user WITH REPLICATION;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'debezium_pub') THEN
        CREATE PUBLICATION debezium_pub FOR ALL TABLES;
    END IF;
END
$$;

-- Connector-specific publications. These are created on a clean database;
-- the reconciliation command below is rerunnable and adds missing tables to
-- existing publications without dropping slots or historical offsets.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'debezium_pub_core') THEN
        CREATE PUBLICATION debezium_pub_core FOR TABLE
            core_banking.customer, core_banking.account, core_banking.branch,
            core_banking.employee, core_banking.loan, core_banking.txn_account;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'debezium_pub_card') THEN
        CREATE PUBLICATION debezium_pub_card FOR TABLE
            card_crm.card, card_crm.card_txn, card_crm.crm_interaction;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'debezium_pub_digital') THEN
        CREATE PUBLICATION debezium_pub_digital FOR TABLE
            digital_banking.online_transaction, digital_banking.device,
            digital_banking.support_ticket;
    END IF;
END
$$;

-- Reconcile existing publications on reruns. CREATE IF NOT EXISTS does not add
-- tables to a publication that was created by an older deployment.
DO $$
DECLARE
    item TEXT[];
    fqtn TEXT;
    pub TEXT;
    schema_name TEXT;
    table_name TEXT;
BEGIN
    FOREACH item SLICE 1 IN ARRAY ARRAY[
        ARRAY['debezium_pub_core', 'core_banking.customer'],
        ARRAY['debezium_pub_core', 'core_banking.account'],
        ARRAY['debezium_pub_core', 'core_banking.branch'],
        ARRAY['debezium_pub_core', 'core_banking.employee'],
        ARRAY['debezium_pub_core', 'core_banking.loan'],
        ARRAY['debezium_pub_core', 'core_banking.txn_account'],
        ARRAY['debezium_pub_card', 'card_crm.card'],
        ARRAY['debezium_pub_card', 'card_crm.card_txn'],
        ARRAY['debezium_pub_card', 'card_crm.crm_interaction'],
        ARRAY['debezium_pub_digital', 'digital_banking.online_transaction'],
        ARRAY['debezium_pub_digital', 'digital_banking.device'],
        ARRAY['debezium_pub_digital', 'digital_banking.support_ticket']
    ] LOOP
        pub := item[1];
        fqtn := item[2];
        schema_name := split_part(fqtn, '.', 1);
        table_name := split_part(fqtn, '.', 2);
        IF NOT EXISTS (
            SELECT 1 FROM pg_publication_tables
            WHERE pubname = pub AND schemaname = schema_name AND tablename = table_name
        ) THEN
            EXECUTE format('ALTER PUBLICATION %I ADD TABLE %I.%I', pub, schema_name, table_name);
        END IF;
    END LOOP;
END
$$;

COMMENT ON ROLE cdc_user IS 'CDC user for Debezium connector with replication privileges';
