-- =============================================================================
-- 002 — digital_banking.online_transaction: merchant QR/POS payment fields
-- =============================================================================
-- Mở rộng giao dịch online thành giao dịch thanh toán merchant (QR/POS) theo
-- nghiệp vụ khoá new2 (Merchant Acquiring). Ba cột mới:
--
--   merchant_id      FK -> digital_banking.merchant. NULL với giao dịch không
--                    qua merchant (chuyển khoản, rút tiền...).
--   payment_channel  'QR' | 'POS' | NULL — kênh chấp nhận thanh toán tại merchant.
--   refund_flag      0/1 — dùng cho KPI refund rate (tỷ lệ hoàn tiền).
--
-- docker/init_postgres/*.sql chỉ chạy trên volume RỖNG; cột thêm ở đây mới tới
-- được một Postgres đã có dữ liệu. Mọi câu lệnh phải idempotent.
--
-- Constraint thêm NOT VALID: dòng cũ (merchant_id NULL) không bị kiểm, dòng ghi
-- mới thì bị kiểm. Tên constraint phải trùng với 03_ddl_digital_banking.sql.

ALTER TABLE digital_banking.online_transaction
    ADD COLUMN IF NOT EXISTS merchant_id BIGINT;
ALTER TABLE digital_banking.online_transaction
    ADD COLUMN IF NOT EXISTS payment_channel VARCHAR(10);
ALTER TABLE digital_banking.online_transaction
    ADD COLUMN IF NOT EXISTS refund_flag SMALLINT NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_otxn_merchant') THEN
        ALTER TABLE digital_banking.online_transaction
            ADD CONSTRAINT fk_otxn_merchant
            FOREIGN KEY (merchant_id) REFERENCES digital_banking.merchant(merchant_id) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_otxn_payment_channel') THEN
        ALTER TABLE digital_banking.online_transaction
            ADD CONSTRAINT chk_otxn_payment_channel
            CHECK (payment_channel IN ('QR', 'POS')) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_otxn_refund_flag') THEN
        ALTER TABLE digital_banking.online_transaction
            ADD CONSTRAINT chk_otxn_refund_flag CHECK (refund_flag IN (0, 1)) NOT VALID;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_otxn_merchant ON digital_banking.online_transaction(merchant_id);
CREATE INDEX IF NOT EXISTS idx_otxn_payment_channel ON digital_banking.online_transaction(payment_channel);
