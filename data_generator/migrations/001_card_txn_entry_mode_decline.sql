-- =============================================================================
-- 001 — card_crm.card_txn: entry_mode + decline_reason
-- =============================================================================
-- docker/init_postgres/*.sql chỉ chạy trên volume RỖNG. Cột mới trong
-- 02_ddl_card_crm.sql không bao giờ tới một Postgres đã có dữ liệu, và
-- generator sẽ chết ở INSERT vì cột không tồn tại. generate_all.py chạy mọi file
-- trong thư mục này trước khi ghi; mọi câu lệnh phải idempotent — trên volume
-- mới (DDL đã có cột + constraint) file này không đổi gì.
--
-- Constraint thêm NOT VALID: dòng cũ (FAILED mà chưa có lý do) không bị kiểm,
-- dòng ghi mới thì bị kiểm. Tên constraint phải trùng với 02_ddl_card_crm.sql.

ALTER TABLE card_crm.card_txn ADD COLUMN IF NOT EXISTS entry_mode VARCHAR(10);
ALTER TABLE card_crm.card_txn ADD COLUMN IF NOT EXISTS decline_reason VARCHAR(100);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_ct_entry_mode') THEN
        ALTER TABLE card_crm.card_txn
            ADD CONSTRAINT chk_ct_entry_mode CHECK (entry_mode IN ('CHIP', 'SWIPE', 'ONLINE')) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_ct_decline_reason') THEN
        ALTER TABLE card_crm.card_txn
            ADD CONSTRAINT chk_ct_decline_reason CHECK ((status = 'FAILED') = (decline_reason IS NOT NULL)) NOT VALID;
    END IF;
END
$$;
