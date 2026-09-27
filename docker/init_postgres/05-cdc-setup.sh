#!/bin/bash
# =============================================================================
# CDC Setup — Banking Data Platform (chạy một lần khi Postgres khởi tạo)
# =============================================================================
# Từng là 05-cdc-setup.sql với mật khẩu cdc_user viết cứng. Thành .sh để mật
# khẩu đến từ CDC_DB_PASSWORD (docker/.env, container postgres nhận qua env_file).
# Thiếu biến (vd. compose CI) thì role tồn tại nhưng NOLOGIN — không có mặc định.
# File được docker-entrypoint SOURCE: không dùng `exit`.

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
-- Enable logical replication for Debezium CDC connector
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
-- Trần WAL mà MỘT replication slot được giữ. Mặc định -1 = không giới hạn: slot
-- không ai đọc giữ WAL mãi. Đo 2026-09-27: ba slot mồ côi (không code nào dùng
-- tên đó) giữ 3.6 GB và tăng theo mỗi lần seed. Vượt trần thì slot bị vô hiệu
-- (wal_status = lost) thay vì làm đầy đĩa — Debezium báo lỗi rõ, đăng ký lại
-- connector sẽ snapshot lại. Offset Debezium nằm trong anonymous volume của Kafka
-- (mất khi `docker compose down`); slot cũ không giữ gì mà snapshot mới không có.
ALTER SYSTEM SET max_slot_wal_keep_size = '4GB';

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'cdc_user') THEN
        -- NOLOGIN, không mật khẩu. LOGIN + mật khẩu chỉ được đặt bên dưới, từ
        -- CDC_DB_PASSWORD. Bản cũ viết cứng mật khẩu ngay tại đây.
        CREATE ROLE cdc_user WITH REPLICATION NOLOGIN;
    END IF;
END
$$;

-- psql không thay biến trong khối $$…$$, nên mật khẩu đặt ngoài DO. \getenv đọc
-- env mà không đưa giá trị lên dòng lệnh; :'cdc_pw' tự quote đúng.
\getenv cdc_pw CDC_DB_PASSWORD
\if :{?cdc_pw}
ALTER ROLE cdc_user WITH LOGIN PASSWORD :'cdc_pw';
\else
\echo 'CDC_DB_PASSWORD chưa đặt: cdc_user giữ NOLOGIN — Debezium sẽ không đăng nhập được.'
\endif

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
SQL
