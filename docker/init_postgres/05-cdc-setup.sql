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

-- =============================================================================
-- Per-connector publications — BẮT BUỘC để CDC khởi động được
-- =============================================================================
-- code_etl/cdc/register_connectors.py khai báo:
--     publication.name           = debezium_pub_core | _card | _digital
--     publication.autocreate.mode = disabled
-- Trước đây file này chỉ tạo `debezium_pub`, nên trên môi trường SẠCH cả 3
-- connector chết ngay:
--     ConnectException: Publication autocreation is disabled,
--                       please create one and restart the connector.
-- Nguy hiểm hơn: connector state vẫn báo RUNNING trong khi task = FAILED và
-- 0 topic được tạo — nhìn `/connectors/<name>/status` ở mức connector sẽ tưởng
-- mọi thứ ổn. Đã runtime-proven trong lần clean rebuild.
--
-- Danh sách bảng phải khớp ĐÚNG table.include.list của từng connector (6/3/3).
-- =============================================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'debezium_pub_core') THEN
        CREATE PUBLICATION debezium_pub_core FOR TABLE
            core_banking.customer,
            core_banking.account,
            core_banking.branch,
            core_banking.employee,
            core_banking.loan,
            core_banking.txn_account;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'debezium_pub_card') THEN
        CREATE PUBLICATION debezium_pub_card FOR TABLE
            card_crm.card,
            card_crm.card_txn,
            card_crm.crm_interaction;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'debezium_pub_digital') THEN
        CREATE PUBLICATION debezium_pub_digital FOR TABLE
            digital_banking.online_transaction,
            digital_banking.device,
            digital_banking.support_ticket;
    END IF;
END
$$;

COMMENT ON ROLE cdc_user IS 'CDC user for Debezium connector with replication privileges';
