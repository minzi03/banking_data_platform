"""
PII Masking Job
---------------
Tạo các Iceberg tables cho phép team Marketing/CRM
truy vấn dữ liệu mà không lộ thông tin cá nhân nhạy cảm.

Quy tắc masking:
  full_name  : "Nguyễn Văn An" → "Nguyễn V** A**" (giữ họ, che phần giữa)
  phone      : "0912345678" → "091****678"
  email      : "an.nguyen@gmail.com" → "a*****n@gmail.com"
  cccd       : SHA256(cccd + salt) — deterministic để vẫn có thể join
  address    : chỉ giữ city + district, ẩn số nhà/đường

Output tables:
  sandbox.dim_customer_masked       (từ silver.dim_customer)
  sandbox.mart_customer_360_masked  (từ gold.mart_customer_360)
"""

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from spark.spark_session import get_spark_session

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("pii_masking")

# Salt cho hash PII — phải set PII_HASH_SALT env var trước khi chạy
_pii_salt_raw = os.environ.get("PII_HASH_SALT")
if not _pii_salt_raw:
    raise EnvironmentError("PII_HASH_SALT environment variable is required but not set")
PII_SALT = _pii_salt_raw
PII_SALT_SQL = PII_SALT.replace("'", "''")


def _mask_name_udf() -> str:
    """Trả về biểu thức SQL mask tên tiếng Việt."""
    return """
        CASE
            WHEN full_name IS NULL THEN NULL
            WHEN size(split(full_name, ' ')) >= 3 THEN
                concat(
                    split(full_name, ' ')[0], ' ',
                    substr(split(full_name, ' ')[1], 1, 1), '** ',
                    substr(split(full_name, ' ')[size(split(full_name, ' ')) - 1], 1, 1), '**'
                )
            WHEN size(split(full_name, ' ')) = 2 THEN
                concat(split(full_name, ' ')[0], ' ', substr(split(full_name, ' ')[1], 1, 1), '**')
            ELSE
                concat(substr(full_name, 1, 1), '**')
        END AS full_name_masked
    """


def create_masked_dim_customer(spark, cob_dt: str) -> None:
    """Tạo bảng sandbox.dim_customer_masked với PII đã được mask."""
    logger.info("Creating sandbox.dim_customer_masked for cob_dt=%s", cob_dt)
    spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.sandbox")
    spark.sql(f"""
        CREATE OR REPLACE TABLE lakehouse.sandbox.dim_customer_masked
        USING iceberg
        PARTITIONED BY (is_current)
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
            customer_sk,
            customer_id,
            {_mask_name_udf()},
            gender,
            FLOOR(DATEDIFF(DATE '{cob_dt}', date_of_birth) / 365 / 10) * 10 AS age_group_decade,
            CASE WHEN phone IS NULL THEN NULL
                 ELSE concat(substr(phone, 1, 3), '****', substr(phone, 8)) END AS phone_masked,
            CASE WHEN email IS NULL THEN NULL
                 ELSE concat(
                     substr(split(email, '@')[0], 1, 1),
                     '*****',
                     substr(split(email, '@')[0], length(split(email, '@')[0])),
                     '@',
                     split(email, '@')[1]
                 ) END AS email_masked,
            sha2(concat(cccd, '{PII_SALT_SQL}'), 256)  AS cccd_hash,
            city,
            district,
            branch_code,
            customer_segment,
            kyc_status,
            register_date,
            is_active,
            effective_from,
            effective_to,
            is_current,
            last_updated
        FROM lakehouse.silver.dim_customer
    """)


def create_masked_mart_customer_360(spark, cob_dt: str) -> None:
    """Tạo bảng sandbox.mart_customer_360_masked với PII đã được mask."""
    logger.info("Creating sandbox.mart_customer_360_masked for cob_dt=%s", cob_dt)
    spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.sandbox")
    spark.sql(f"""
        CREATE OR REPLACE TABLE lakehouse.sandbox.mart_customer_360_masked
        USING iceberg
        PARTITIONED BY (days(cob_dt))
        TBLPROPERTIES ('format-version' = '2')
        AS
        SELECT
            customer_id,
            customer_sk,
            full_name_masked,
            age,
            gender,
            primary_branch_code,
            customer_segment,
            kyc_status,
            register_date,
            total_accounts,
            total_cards,
            total_loans,
            has_credit_card,
            has_savings,
            has_loan,
            total_deposit_balance,
            total_loan_outstanding,
            aum_total,
            aum_bucket,
            txn_count_30d,
            txn_amount_30d,
            last_txn_date,
            days_since_last_txn,
            primary_channel,
            interaction_count_90d,
            last_interaction_date,
            rfm_recency_score,
            rfm_frequency_score,
            rfm_monetary_score,
            rfm_segment,
            churn_flag,
            cross_sell_credit_card_flag,
            cob_dt
        FROM lakehouse.gold.mart_customer_360
        WHERE cob_dt = DATE '{cob_dt}'
    """)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PII Masking Job")
    parser.add_argument("--cob_dt", required=True, help="Ngày xử lý YYYY-MM-DD")
    parser.add_argument(
        "--target",
        choices=["dim_customer", "mart_360", "all"],
        default="all",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    spark = get_spark_session("pii_masking")

    if args.target in ("dim_customer", "all"):
        create_masked_dim_customer(spark, args.cob_dt)

    if args.target in ("mart_360", "all"):
        create_masked_mart_customer_360(spark, args.cob_dt)

    logger.info("PII masking completed for cob_dt=%s", args.cob_dt)


if __name__ == "__main__":
    main()
