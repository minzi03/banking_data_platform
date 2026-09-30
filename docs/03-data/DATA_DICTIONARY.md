# Data Dictionary

> ⚠️ **File này được SINH TỰ ĐỘNG. Đừng sửa tay.**
>
> Sinh bởi `scripts/generate_data_dictionary.py` từ DDL (`docker/init_*/`)
> và data contract (`governance/datasets/`). Sửa tay sẽ bị
> `tests/governance/test_data_dictionary_current.py` bắt.
>
> Sinh lại: `py -3 scripts/generate_data_dictionary.py`

**93 bảng · 1256 cột · 25 bảng có data contract · 35 cột nghi chứa PII**

## Mục lục

| Tầng | Số bảng | Số cột |
|---|---:|---:|
| [bronze](#bronze) | 27 | 400 |
| [silver](#silver) | 17 | 231 |
| [gold](#gold) | 15 | 243 |
| [meta](#meta) | 1 | 5 |
| [card_crm](#card_crm) | 3 | 42 |
| [core_banking](#core_banking) | 14 | 176 |
| [digital_banking](#digital_banking) | 6 | 58 |
| [opslakehouse](#opslakehouse) | 10 | 101 |

---

## bronze

### `lakehouse.bronze.card_account_cdc`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `card_id` | `BIGINT` |  |  |
| `card_no_masked` | `VARCHAR(50)` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `product_code` | `VARCHAR(20)` |  |  |
| `card_type` | `VARCHAR(50)` |  |  |
| `card_brand` | `VARCHAR(20)` |  |  |
| `credit_limit` | `DECIMAL(18,2)` |  |  |
| `issue_date` | `BIGINT` |  |  |
| `expiry_date` | `BIGINT` |  |  |
| `status` | `VARCHAR(20)` |  |  |
| `last_updated` | `BIGINT` |  |  |
| `__cdc_operation` | `VARCHAR(10)` |  |  |
| `__cdc_timestamp` | `TIMESTAMP` |  |  |
| `__cdc_timestamp_ms` | `BIGINT` |  |  |
| `__spark_batch_id` | `BIGINT` |  |  |
| `__ingestion_time` | `TIMESTAMP` |  |  |
| `__kafka_partition` | `INT` |  |  |
| `__kafka_offset` | `BIGINT` |  |  |

<sub>Nguồn DDL: `04_ddl_bronze_cdc.sql`</sub>

### `lakehouse.bronze.card_transaction_cdc`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `txn_id` | `BIGINT` |  |  |
| `card_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `txn_date` | `BIGINT` |  |  |
| `txn_amount` | `DECIMAL(18,2)` |  |  |
| `txn_type` | `VARCHAR(50)` |  |  |
| `currency` | `VARCHAR(10)` |  |  |
| `merchant_name` | `VARCHAR(255)` |  |  |
| `merchant_category` | `VARCHAR(50)` |  |  |
| `channel` | `VARCHAR(50)` |  |  |
| `status` | `VARCHAR(20)` |  |  |
| `created_ts` | `BIGINT` |  |  |
| `last_updated` | `BIGINT` |  |  |
| `__cdc_operation` | `VARCHAR(10)` |  |  |
| `__cdc_timestamp` | `TIMESTAMP` |  |  |
| `__cdc_timestamp_ms` | `BIGINT` |  |  |
| `__spark_batch_id` | `BIGINT` |  |  |
| `__ingestion_time` | `TIMESTAMP` |  |  |
| `__kafka_partition` | `INT` |  |  |
| `__kafka_offset` | `BIGINT` |  |  |

<sub>Nguồn DDL: `04_ddl_bronze_cdc.sql`</sub>

### `lakehouse.bronze.core_account`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `account_id` | `BIGINT` |  |  |
| `account_no` | `STRING` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `product_code` | `STRING` |  |  |
| `branch_code` | `STRING` |  |  |
| `account_type` | `STRING` |  |  |
| `currency` | `STRING` |  |  |
| `balance` | `DECIMAL(18,2)` |  |  |
| `open_date` | `DATE` |  |  |
| `close_date` | `DATE` |  |  |
| `status` | `STRING` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_account_cdc`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `account_id` | `BIGINT` |  |  |
| `account_no` | `VARCHAR(50)` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `product_code` | `VARCHAR(20)` |  |  |
| `branch_code` | `VARCHAR(10)` |  |  |
| `account_type` | `VARCHAR(20)` |  |  |
| `currency` | `VARCHAR(10)` |  |  |
| `balance` | `DECIMAL(18,2)` |  |  |
| `open_date` | `BIGINT` |  |  |
| `close_date` | `BIGINT` |  |  |
| `status` | `VARCHAR(20)` |  |  |
| `last_updated` | `BIGINT` |  |  |
| `__cdc_operation` | `VARCHAR(10)` |  |  |
| `__cdc_timestamp` | `TIMESTAMP` |  |  |
| `__cdc_timestamp_ms` | `BIGINT` |  |  |
| `__spark_batch_id` | `BIGINT` |  |  |
| `__ingestion_time` | `TIMESTAMP` |  |  |
| `__kafka_partition` | `INT` |  |  |
| `__kafka_offset` | `BIGINT` |  |  |

<sub>Nguồn DDL: `04_ddl_bronze_cdc.sql`</sub>

### `lakehouse.bronze.core_aml_alert`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `alert_id` | `BIGINT` |  |  |
| `alert_number` | `STRING` |  |  |
| `rule_id` | `BIGINT` |  |  |
| `transaction_id` | `BIGINT` |  |  |
| `card_txn_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `alert_type` | `STRING` |  |  |
| `risk_score` | `DECIMAL(5,2)` |  |  |
| `risk_category` | `STRING` |  |  |
| `description` | `STRING` |  |  |
| `evidence_json` | `STRING` |  |  |
| `txn_amount` | `DECIMAL(18,2)` |  |  |
| `txn_date` | `TIMESTAMP` |  |  |
| `channel` | `STRING` |  |  |
| `status` | `STRING` |  |  |
| `analyst_id` | `BIGINT` |  |  |
| `priority` | `STRING` |  |  |
| `due_date` | `DATE` |  |  |
| `notes` | `STRING` |  |  |
| `ctr_required` | `INT` |  |  |
| `sar_filed` | `INT` |  |  |
| `sar_reference` | `STRING` |  |  |
| `created_at` | `TIMESTAMP` |  |  |
| `updated_at` | `TIMESTAMP` |  |  |
| `resolved_at` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_aml_customer_risk`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `risk_level` | `STRING` |  |  |
| `risk_score` | `DECIMAL(5,2)` |  |  |
| `peps_flag` | `INT` |  |  |
| `sanctions_flag` | `INT` |  |  |
| `adverse_media_flag` | `INT` |  |  |
| `total_alerts` | `INT` |  |  |
| `open_alerts` | `INT` |  |  |
| `last_alert_date` | `DATE` |  |  |
| `last_review_date` | `DATE` |  |  |
| `next_review_date` | `DATE` |  |  |
| `edd_required` | `INT` |  |  |
| `edd_reason` | `STRING` |  |  |
| `source_of_wealth` | `STRING` |  |  |
| `expected_activity` | `STRING` |  |  |
| `created_at` | `TIMESTAMP` |  |  |
| `updated_at` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_aml_rule`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `rule_id` | `BIGINT` |  |  |
| `rule_name` | `STRING` |  |  |
| `rule_code` | `STRING` |  |  |
| `rule_type` | `STRING` |  |  |
| `description` | `STRING` |  |  |
| `threshold` | `DECIMAL(18,2)` |  |  |
| `threshold_currency` | `STRING` |  |  |
| `window_hours` | `INT` |  |  |
| `severity` | `STRING` |  |  |
| `is_active` | `INT` |  |  |
| `regulatory_ref` | `STRING` |  |  |
| `created_by` | `STRING` |  |  |
| `created_at` | `TIMESTAMP` |  |  |
| `updated_at` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_branch`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `branch_code` | `STRING` |  |  |
| `branch_name` | `STRING` |  |  |
| `region` | `STRING` |  |  |
| `city` | `STRING` |  |  |
| `district` | `STRING` |  |  |
| `address` | `STRING` | ⚠️ |  |
| `manager_name` | `STRING` |  |  |
| `open_date` | `DATE` |  |  |
| `status` | `STRING` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_card`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `card_id` | `BIGINT` |  |  |
| `card_no_masked` | `STRING` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `product_code` | `STRING` |  |  |
| `card_type` | `STRING` |  |  |
| `card_brand` | `STRING` |  |  |
| `credit_limit` | `DECIMAL(18,2)` |  |  |
| `issue_date` | `DATE` |  |  |
| `expiry_date` | `DATE` |  |  |
| `status` | `STRING` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_card_txn`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `txn_id` | `BIGINT` |  |  |
| `card_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `txn_date` | `TIMESTAMP` |  |  |
| `txn_amount` | `DECIMAL(18,2)` |  |  |
| `txn_type` | `STRING` |  |  |
| `currency` | `STRING` |  |  |
| `merchant_name` | `STRING` |  |  |
| `merchant_category` | `STRING` |  |  |
| `channel` | `STRING` |  |  |
| `status` | `STRING` |  |  |
| `entry_mode` | `STRING` |  |  |
| `decline_reason` | `STRING` |  |  |
| `created_ts` | `TIMESTAMP` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_crm_interaction`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `interaction_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `interaction_date` | `TIMESTAMP` |  |  |
| `channel` | `STRING` |  |  |
| `direction` | `STRING` |  |  |
| `subject` | `STRING` |  |  |
| `category` | `STRING` |  |  |
| `status` | `STRING` |  |  |
| `assigned_to` | `STRING` |  |  |
| `satisfaction_score` | `INT` |  |  |
| `created_ts` | `TIMESTAMP` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_customer`

Raw customer records ingested from core_banking.customer via batch JDBC. Append-only, immutable, day-partitioned.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: important · **DAG**: `bronze_core_banking_dag`

**Tối thiểu**: 5,000 dòng · **Freshness**: 48h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `cccd` | `STRING` |  |  |
| `full_name` | `STRING` | ⚠️ |  |
| `gender` | `STRING` |  |  |
| `date_of_birth` | `DATE` | ⚠️ |  |
| `phone` | `STRING` | ⚠️ |  |
| `email` | `STRING` | ⚠️ |  |
| `address` | `STRING` | ⚠️ |  |
| `city` | `STRING` |  |  |
| `district` | `STRING` |  |  |
| `branch_code` | `STRING` |  |  |
| `customer_segment` | `STRING` |  |  |
| `kyc_status` | `STRING` |  |  |
| `register_date` | `DATE` |  |  |
| `is_active` | `INT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_customer_cdc`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `cccd` | `VARCHAR(12)` |  |  |
| `full_name` | `VARCHAR(200)` | ⚠️ |  |
| `gender` | `VARCHAR(10)` |  |  |
| `date_of_birth` | `BIGINT` | ⚠️ |  |
| `phone` | `VARCHAR(15)` | ⚠️ |  |
| `email` | `VARCHAR(200)` | ⚠️ |  |
| `address` | `VARCHAR(500)` | ⚠️ |  |
| `city` | `VARCHAR(100)` |  |  |
| `district` | `VARCHAR(100)` |  |  |
| `branch_code` | `VARCHAR(10)` |  |  |
| `customer_segment` | `VARCHAR(20)` |  |  |
| `kyc_status` | `VARCHAR(20)` |  |  |
| `register_date` | `BIGINT` |  |  |
| `is_active` | `VARCHAR(10)` |  |  |
| `last_updated` | `BIGINT` |  |  |
| `__cdc_operation` | `VARCHAR(10)` |  |  |
| `__cdc_timestamp` | `TIMESTAMP` |  |  |
| `__cdc_timestamp_ms` | `BIGINT` |  |  |
| `__spark_batch_id` | `BIGINT` |  |  |
| `__ingestion_time` | `TIMESTAMP` |  |  |
| `__kafka_partition` | `INT` |  |  |
| `__kafka_offset` | `BIGINT` |  |  |

<sub>Nguồn DDL: `04_ddl_bronze_cdc.sql`</sub>

### `lakehouse.bronze.core_deposit`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `deposit_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `product_code` | `STRING` |  |  |
| `principal_amount` | `DECIMAL(18,2)` |  |  |
| `interest_rate` | `DECIMAL(5,2)` |  |  |
| `term_months` | `INT` |  |  |
| `open_date` | `DATE` |  |  |
| `maturity_date` | `DATE` |  |  |
| `status` | `STRING` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_device`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `device_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `device_type` | `STRING` |  |  |
| `device_fingerprint` | `STRING` |  |  |
| `operating_system` | `STRING` |  |  |
| `ip_address` | `STRING` | ⚠️ |  |
| `is_trusted` | `INT` |  |  |
| `first_seen` | `TIMESTAMP` |  |  |
| `last_seen` | `TIMESTAMP` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_employee`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `employee_id` | `BIGINT` |  |  |
| `full_name` | `STRING` | ⚠️ |  |
| `branch_code` | `STRING` |  |  |
| `role` | `STRING` |  |  |
| `hire_date` | `DATE` |  |  |
| `salary` | `DECIMAL(12,2)` |  |  |
| `status` | `STRING` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_loan`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `loan_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `product_code` | `STRING` |  |  |
| `branch_code` | `STRING` |  |  |
| `loan_amount` | `DECIMAL(18,2)` |  |  |
| `outstanding_balance` | `DECIMAL(18,2)` |  |  |
| `interest_rate` | `DECIMAL(5,2)` |  |  |
| `term_months` | `INT` |  |  |
| `disbursement_date` | `DATE` |  |  |
| `maturity_date` | `DATE` |  |  |
| `loan_status` | `STRING` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_location`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `location_id` | `BIGINT` |  |  |
| `merchant_name` | `STRING` |  |  |
| `merchant_category` | `STRING` |  |  |
| `city` | `STRING` |  |  |
| `state` | `STRING` |  |  |
| `latitude` | `DECIMAL(10,7)` |  |  |
| `longitude` | `DECIMAL(10,7)` |  |  |
| `is_high_risk_area` | `INT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_mcc_code`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `mcc_code` | `STRING` |  |  |
| `description` | `STRING` |  |  |
| `category_group` | `STRING` |  |  |
| `is_high_risk` | `INT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_merchant`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `merchant_id` | `BIGINT` |  |  |
| `merchant_name` | `STRING` |  |  |
| `merchant_category` | `STRING` |  |  |
| `mcc_code` | `STRING` |  |  |
| `city` | `STRING` |  |  |
| `state` | `STRING` |  |  |
| `risk_category` | `STRING` |  |  |
| `is_active` | `INT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_online_transaction`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `transaction_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `device_id` | `BIGINT` |  |  |
| `location_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `transaction_type` | `STRING` |  |  |
| `channel` | `STRING` |  |  |
| `amount` | `DECIMAL(18,2)` |  |  |
| `currency` | `STRING` |  |  |
| `is_fraud` | `INT` |  |  |
| `fraud_reason` | `STRING` |  |  |
| `status` | `STRING` |  |  |
| `transaction_date` | `TIMESTAMP` |  |  |
| `created_ts` | `TIMESTAMP` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_product`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `product_code` | `STRING` |  |  |
| `product_name` | `STRING` |  |  |
| `product_group` | `STRING` |  |  |
| `product_type` | `STRING` |  |  |
| `currency` | `STRING` |  |  |
| `is_active` | `INT` |  |  |
| `launch_date` | `DATE` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_standing_order`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `order_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `order_type` | `STRING` |  |  |
| `beneficiary_name` | `STRING` |  |  |
| `beneficiary_account` | `STRING` |  |  |
| `amount` | `DECIMAL(18,2)` |  |  |
| `frequency` | `STRING` |  |  |
| `next_execute_date` | `DATE` |  |  |
| `status` | `STRING` |  |  |
| `created_date` | `DATE` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_support_ticket`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `ticket_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `issue_type` | `STRING` |  |  |
| `priority` | `STRING` |  |  |
| `status` | `STRING` |  |  |
| `date_opened` | `TIMESTAMP` |  |  |
| `date_resolved` | `TIMESTAMP` |  |  |
| `resolution_time_hrs` | `DECIMAL(8,2)` |  |  |
| `satisfaction_score` | `INT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.core_transaction_cdc`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `txn_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `txn_date` | `BIGINT` |  |  |
| `txn_amount` | `DECIMAL(18,2)` |  |  |
| `txn_type` | `VARCHAR(50)` |  |  |
| `debit_credit` | `VARCHAR(10)` |  |  |
| `balance_after` | `DECIMAL(18,2)` |  |  |
| `channel` | `VARCHAR(50)` |  |  |
| `description` | `VARCHAR(500)` |  |  |
| `counter_account` | `VARCHAR(50)` |  |  |
| `created_ts` | `BIGINT` |  |  |
| `last_updated` | `BIGINT` |  |  |
| `__cdc_operation` | `VARCHAR(10)` |  |  |
| `__cdc_timestamp` | `TIMESTAMP` |  |  |
| `__cdc_timestamp_ms` | `BIGINT` |  |  |
| `__spark_batch_id` | `BIGINT` |  |  |
| `__ingestion_time` | `TIMESTAMP` |  |  |
| `__kafka_partition` | `INT` |  |  |
| `__kafka_offset` | `BIGINT` |  |  |

<sub>Nguồn DDL: `04_ddl_bronze_cdc.sql`</sub>

### `lakehouse.bronze.core_txn_account`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `txn_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `txn_date` | `TIMESTAMP` |  |  |
| `txn_amount` | `DECIMAL(18,2)` |  |  |
| `txn_type` | `STRING` |  |  |
| `debit_credit` | `STRING` |  |  |
| `balance_after` | `DECIMAL(18,2)` |  |  |
| `channel` | `STRING` |  |  |
| `description` | `STRING` |  |  |
| `counter_account` | `STRING` |  |  |
| `created_ts` | `TIMESTAMP` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `01_ddl_bronze.sql`</sub>

### `lakehouse.bronze.online_transaction_cdc`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `transaction_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `device_id` | `BIGINT` |  |  |
| `location_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `transaction_type` | `VARCHAR(50)` |  |  |
| `channel` | `VARCHAR(50)` |  |  |
| `amount` | `DECIMAL(18,2)` |  |  |
| `currency` | `VARCHAR(10)` |  |  |
| `is_fraud` | `VARCHAR(10)` |  |  |
| `fraud_reason` | `VARCHAR(500)` |  |  |
| `status` | `VARCHAR(20)` |  |  |
| `transaction_date` | `BIGINT` |  |  |
| `created_ts` | `BIGINT` |  |  |
| `last_updated` | `BIGINT` |  |  |
| `__cdc_operation` | `VARCHAR(10)` |  |  |
| `__cdc_timestamp` | `TIMESTAMP` |  |  |
| `__cdc_timestamp_ms` | `BIGINT` |  |  |
| `__spark_batch_id` | `BIGINT` |  |  |
| `__ingestion_time` | `TIMESTAMP` |  |  |
| `__kafka_partition` | `INT` |  |  |
| `__kafka_offset` | `BIGINT` |  |  |

<sub>Nguồn DDL: `04_ddl_bronze_cdc.sql`</sub>

---

## silver

### `lakehouse.silver.dim_account`

Cleansed account dimension with slowly changing dimension type 2 (SCD2) history tracking. Links customers to their banking products and provides a full audit trail of account status and balance changes over time.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 5,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `account_sk` | `STRING` |  |  |
| `account_id` | `BIGINT` |  |  |
| `account_no` | `STRING` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `product_code` | `STRING` |  |  |
| `branch_code` | `STRING` |  |  |
| `account_type` | `STRING` |  |  |
| `currency` | `STRING` |  |  |
| `balance` | `DECIMAL(18,2)` |  |  |
| `open_date` | `DATE` |  |  |
| `close_date` | `DATE` |  |  |
| `status` | `STRING` |  |  |
| `effective_from` | `DATE` |  |  |
| `effective_to` | `DATE` |  |  |
| `is_current` | `INT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.dim_account_current`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `account_id` | `BIGINT` |  |  |
| `account_no` | `VARCHAR(50)` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `product_code` | `VARCHAR(20)` |  |  |
| `branch_code` | `VARCHAR(10)` |  |  |
| `account_type` | `VARCHAR(20)` |  |  |
| `currency` | `VARCHAR(10)` |  |  |
| `balance` | `DECIMAL(18,2)` |  |  |
| `open_date` | `DATE` |  |  |
| `close_date` | `DATE` |  |  |
| `status` | `VARCHAR(20)` |  |  |
| `__cdc_operation` | `VARCHAR(10)` |  |  |
| `__cdc_timestamp` | `TIMESTAMP` |  |  |
| `__cdc_timestamp_ms` | `BIGINT` |  |  |
| `__source_spark_batch_id` | `BIGINT` |  |  |
| `__consolidated_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `06_ddl_silver_cdc_current.sql`</sub>

### `lakehouse.silver.dim_branch`

Reference dimension for bank branches. Contains branch metadata including region, city, and branch manager information. Used for geographic analytics and performance reporting across the branch network.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 5 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `branch_sk` | `STRING` |  |  |
| `branch_code` | `STRING` |  |  |
| `branch_name` | `STRING` |  |  |
| `region` | `STRING` |  |  |
| `city` | `STRING` |  |  |
| `district` | `STRING` |  |  |
| `address` | `STRING` | ⚠️ |  |
| `manager_name` | `STRING` |  |  |
| `open_date` | `DATE` |  |  |
| `status` | `STRING` |  |  |
| `effective_from` | `DATE` |  |  |
| `effective_to` | `DATE` |  |  |
| `is_current` | `INT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.dim_card`

Cleansed card dimension linking customers and accounts to their payment cards. Card numbers are masked for security compliance. Provides card type, expiry, and status information for downstream transaction analytics.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 1,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `card_id` | `BIGINT` |  |  |
| `card_no_masked` | `STRING` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `product_code` | `STRING` |  |  |
| `card_type` | `STRING` |  |  |
| `card_brand` | `STRING` |  |  |
| `credit_limit` | `DECIMAL(18,2)` |  |  |
| `issue_date` | `DATE` |  |  |
| `expiry_date` | `DATE` |  |  |
| `status` | `STRING` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.dim_customer`

Cleansed and deduplicated customer dimension with slowly changing dimension type 2 (SCD2) history tracking. Provides the single source of truth for customer attributes across the data platform.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 5,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_sk` | `STRING` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `cccd` | `STRING` |  |  |
| `full_name` | `STRING` | ⚠️ |  |
| `gender` | `STRING` |  |  |
| `date_of_birth` | `DATE` | ⚠️ |  |
| `phone` | `STRING` | ⚠️ |  |
| `email` | `STRING` | ⚠️ |  |
| `address` | `STRING` | ⚠️ |  |
| `city` | `STRING` |  |  |
| `district` | `STRING` |  |  |
| `branch_code` | `STRING` |  |  |
| `customer_segment` | `STRING` |  |  |
| `kyc_status` | `STRING` |  |  |
| `register_date` | `DATE` |  |  |
| `is_active` | `INT` |  |  |
| `effective_from` | `DATE` |  |  |
| `effective_to` | `DATE` |  |  |
| `is_current` | `INT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.dim_customer_current`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `cccd` | `VARCHAR(12)` |  |  |
| `full_name` | `VARCHAR(200)` | ⚠️ |  |
| `gender` | `VARCHAR(10)` |  |  |
| `date_of_birth` | `DATE` | ⚠️ |  |
| `phone` | `VARCHAR(15)` | ⚠️ |  |
| `email` | `VARCHAR(200)` | ⚠️ |  |
| `address` | `VARCHAR(500)` | ⚠️ |  |
| `city` | `VARCHAR(100)` |  |  |
| `district` | `VARCHAR(100)` |  |  |
| `branch_code` | `VARCHAR(10)` |  |  |
| `customer_segment` | `VARCHAR(20)` |  |  |
| `kyc_status` | `VARCHAR(20)` |  |  |
| `register_date` | `DATE` |  |  |
| `is_active` | `INTEGER` |  |  |
| `__cdc_operation` | `VARCHAR(10)` |  |  |
| `__cdc_timestamp` | `TIMESTAMP` |  |  |
| `__cdc_timestamp_ms` | `BIGINT` |  |  |
| `__source_spark_batch_id` | `BIGINT` |  |  |
| `__consolidated_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `06_ddl_silver_cdc_current.sql`</sub>

### `lakehouse.silver.dim_deposit`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `deposit_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `product_code` | `STRING` |  |  |
| `principal_amount` | `DECIMAL(18,2)` |  |  |
| `interest_rate` | `DECIMAL(8,4)` |  |  |
| `term_months` | `INT` |  |  |
| `open_date` | `DATE` |  |  |
| `maturity_date` | `DATE` |  |  |
| `status` | `STRING` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.dim_device`

Cleansed device dimension tracking customer devices used for online and mobile banking. Captures device type, operating system, and browser information. Supports fraud detection and digital channel analytics.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 500 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `device_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `device_type` | `STRING` |  |  |
| `device_fingerprint` | `STRING` |  |  |
| `operating_system` | `STRING` |  |  |
| `ip_address` | `STRING` | ⚠️ |  |
| `is_trusted` | `INT` |  |  |
| `first_seen` | `TIMESTAMP` |  |  |
| `last_seen` | `TIMESTAMP` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.dim_employee`

Cleansed employee dimension containing bank staff information. Links employees to branches and roles. Used for operational reporting, CRM interaction attribution, and workforce analytics.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 100 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `employee_id` | `BIGINT` |  |  |
| `full_name` | `STRING` | ⚠️ |  |
| `branch_code` | `STRING` |  |  |
| `role` | `STRING` |  |  |
| `hire_date` | `DATE` |  |  |
| `salary` | `DECIMAL(12,2)` |  |  |
| `status` | `STRING` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.dim_loan`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `loan_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `product_code` | `STRING` |  |  |
| `branch_code` | `STRING` |  |  |
| `loan_amount` | `DECIMAL(18,2)` |  |  |
| `outstanding_balance` | `DECIMAL(18,2)` |  |  |
| `interest_rate` | `DECIMAL(8,4)` |  |  |
| `term_months` | `INT` |  |  |
| `disbursement_date` | `DATE` |  |  |
| `maturity_date` | `DATE` |  |  |
| `loan_status` | `STRING` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.dim_location`

Cleansed location and merchant dimension. Provides geographic and category information for merchants and point-of-sale locations. Used for geographic analytics, merchant spending analysis, and fraud detection based on transaction locations.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 200 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `location_id` | `BIGINT` |  |  |
| `merchant_name` | `STRING` |  |  |
| `merchant_category` | `STRING` |  |  |
| `city` | `STRING` |  |  |
| `state` | `STRING` |  |  |
| `latitude` | `DECIMAL(10,7)` |  |  |
| `longitude` | `DECIMAL(10,7)` |  |  |
| `is_high_risk_area` | `INT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.dim_product`

Reference dimension for banking products. Provides a standardized catalog of all products offered by the bank including savings, loans, and credit cards with their associated attributes such as interest rates and minimum balance requirements.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 10 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `product_sk` | `STRING` |  |  |
| `product_code` | `STRING` |  |  |
| `product_name` | `STRING` |  |  |
| `product_group` | `STRING` |  |  |
| `product_type` | `STRING` |  |  |
| `currency` | `STRING` |  |  |
| `is_active` | `INT` |  |  |
| `launch_date` | `DATE` |  |  |
| `effective_from` | `DATE` |  |  |
| `effective_to` | `DATE` |  |  |
| `is_current` | `INT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.fact_card_txn`

Cleansed fact table capturing all card-based transactions (credit and debit). Includes merchant details, MCC codes, and transaction status. Used for card spending analytics, merchant category analysis, and fraud detection on card transactions.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 10,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `txn_id` | `BIGINT` |  |  |
| `card_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `txn_date` | `TIMESTAMP` |  |  |
| `txn_amount` | `DECIMAL(18,2)` |  |  |
| `txn_type` | `STRING` |  |  |
| `currency` | `STRING` |  |  |
| `merchant_name` | `STRING` |  |  |
| `merchant_category` | `STRING` |  |  |
| `channel` | `STRING` |  |  |
| `status` | `STRING` |  |  |
| `entry_mode` | `STRING` |  |  |
| `decline_reason` | `STRING` |  |  |
| `created_ts` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.fact_crm_interaction`

Cleansed fact table capturing all customer relationship management interactions including calls, emails, and in-branch visits. Tracks interaction type, channel, responsible agent, and resolution status. Used for customer service analytics and agent performance reporting.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 5,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `interaction_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `interaction_date` | `TIMESTAMP` |  |  |
| `channel` | `STRING` |  |  |
| `direction` | `STRING` |  |  |
| `subject` | `STRING` |  |  |
| `category` | `STRING` |  |  |
| `status` | `STRING` |  |  |
| `assigned_to` | `STRING` |  |  |
| `satisfaction_score` | `INT` |  |  |
| `created_ts` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.fact_online_transaction`

Cleansed fact table capturing all transactions performed through online and digital banking channels. Links transactions to customer devices and merchants. Used for digital channel analytics, device-based fraud detection, and online payment pattern analysis.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 5,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `transaction_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `device_id` | `BIGINT` |  |  |
| `location_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `transaction_type` | `STRING` |  |  |
| `channel` | `STRING` |  |  |
| `amount` | `DECIMAL(18,2)` |  |  |
| `currency` | `STRING` |  |  |
| `is_fraud` | `INT` |  |  |
| `fraud_reason` | `STRING` |  |  |
| `status` | `STRING` |  |  |
| `transaction_date` | `TIMESTAMP` |  |  |
| `created_ts` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.fact_support_ticket`

Cleansed fact table capturing all customer support tickets. Tracks issue type, priority, status, and resolution timeline. Used for customer service performance analytics, SLA compliance monitoring, and issue trend analysis.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 2,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `ticket_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `issue_type` | `STRING` |  |  |
| `priority` | `STRING` |  |  |
| `status` | `STRING` |  |  |
| `date_opened` | `TIMESTAMP` |  |  |
| `date_resolved` | `TIMESTAMP` |  |  |
| `resolution_time_hrs` | `DECIMAL(8,2)` |  |  |
| `satisfaction_score` | `INT` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

### `lakehouse.silver.fact_txn_account`

Cleansed fact table capturing all account-level financial transactions including deposits, withdrawals, and transfers. Provides transaction details with balance-after snapshots for account activity analytics, cash flow analysis, and regulatory reporting.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `silver_all_dag`

**Tối thiểu**: 10,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.core_customer_bronze`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `txn_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `account_sk` | `STRING` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `txn_date` | `TIMESTAMP` |  |  |
| `txn_amount` | `DECIMAL(18,2)` |  |  |
| `txn_type` | `STRING` |  |  |
| `debit_credit` | `STRING` |  |  |
| `balance_after` | `DECIMAL(18,2)` |  |  |
| `channel` | `STRING` |  |  |
| `description` | `STRING` |  |  |
| `counter_account` | `STRING` |  |  |
| `created_ts` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `02_ddl_silver.sql`</sub>

---

## gold

### `lakehouse.gold.aml_monitoring`

Daily snapshot of AML typology flags scored at transaction grain. Five typologies: high value, structuring, velocity, multi-channel and geo-velocity. Grain is one row per account transaction per cob_dt. Used for suspicious activity review and regulatory reporting.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `gold_all_dag`

**Grain**: (txn_id, cob_dt) · **Tối thiểu**: 1,000 dòng · **Freshness**: 24h

**AI risk tier**: `high_risk` · **Cấm dùng cho**: `automated_account_freeze`, `automated_transaction_blocking`, `external_sharing`

**Upstream**: `banking.fact_txn_account_silver`, `banking.dim_customer_silver`, `banking.dim_account_silver`, `banking.fact_online_transaction_silver`, `banking.dim_location_silver`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `txn_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `customer_segment` | `STRING` |  |  |
| `branch_code` | `STRING` |  |  |
| `txn_amount` | `DECIMAL(18,2)` |  |  |
| `txn_type` | `STRING` |  |  |
| `debit_credit` | `STRING` |  |  |
| `channel` | `STRING` |  |  |
| `description` | `STRING` |  |  |
| `counter_account` | `STRING` |  |  |
| `txn_date` | `TIMESTAMP` |  |  |
| `distinct_states` | `INT` |  |  |
| `high_risk_locations` | `INT` |  |  |
| `high_value_flag` | `INT` |  |  |
| `structuring_flag` | `INT` |  |  |
| `velocity_flag` | `INT` |  |  |
| `multi_channel_flag` | `INT` |  |  |
| `geo_velocity_flag` | `INT` |  |  |
| `alert_score` | `INT` |  |  |
| `risk_level` | `INT` |  |  |
| `alert_generated` | `INT` |  |  |
| `is_fraud` | `INT` |  |  |
| `fraud_reason` | `STRING` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.campaign_target`

Daily snapshot history of campaign targeting outputs. Grain is one row per customer per cob_dt. Used for campaign auditability, replay, and historical analysis.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: important · **DAG**: `gold_all_dag`

**Grain**: (customer_id, cob_dt) · **Tối thiểu**: 500 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.rfm_segment_gold`, `banking.churn_prediction_gold`, `banking.cross_sell_segment_gold`, `banking.mart_customer_360_gold`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `rfm_segment` | `STRING` |  |  |
| `rfm_score` | `INT` |  |  |
| `recency_days` | `INT` |  |  |
| `frequency` | `BIGINT` |  |  |
| `monetary` | `DECIMAL(18,2)` |  |  |
| `churn_risk` | `STRING` |  |  |
| `is_churn_candidate` | `INT` |  |  |
| `days_since_last_txn` | `INT` |  |  |
| `customer_segment` | `STRING` |  |  |
| `aum_total` | `DECIMAL(18,2)` |  |  |
| `aum_bucket` | `STRING` |  |  |
| `primary_branch_code` | `STRING` |  |  |
| `primary_opportunity` | `STRING` |  |  |
| `no_credit_card` | `INT` |  |  |
| `campaign_type` | `STRING` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.churn_prediction`

Daily snapshot history of churn risk prediction. Grain is one row per customer per cob_dt. Used for retention analytics and backtesting.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: important · **DAG**: `gold_all_dag`

**Grain**: (customer_id, cob_dt) · **Tối thiểu**: 1,000 dòng · **Freshness**: 24h

**AI risk tier**: `high_risk` · **Cấm dùng cho**: `automated_account_closure`, `external_sharing`

**Upstream**: `banking.dim_customer_silver`, `banking.fact_txn_account_silver`, `banking.fact_card_txn_silver`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `txn_cnt_30d` | `BIGINT` |  |  |
| `txn_cnt_90d` | `BIGINT` |  |  |
| `txn_amt_30d` | `DECIMAL(18,2)` |  |  |
| `txn_amt_90d` | `DECIMAL(18,2)` |  |  |
| `days_since_last_txn` | `INT` |  |  |
| `churn_risk` | `STRING` |  |  |
| `is_churn_candidate` | `INT` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.cross_sell_segment`

Daily snapshot history of cross-sell opportunity analysis. Grain is one row per customer per cob_dt. Used for product recommendation analytics over time.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: important · **DAG**: `gold_all_dag`

**Grain**: (customer_id, cob_dt) · **Tối thiểu**: 1,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.dim_customer_silver`, `banking.dim_card_silver`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `customer_segment` | `STRING` |  |  |
| `no_credit_card` | `INT` |  |  |
| `no_debit_card` | `INT` |  |  |
| `primary_opportunity` | `STRING` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.customer_balance_summary`

Daily snapshot history of customer balance summary. Grain is one row per customer per cob_dt. Used for balance trend analysis and historical AUM tracking.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: important · **DAG**: `gold_all_dag`

**Grain**: (customer_id, cob_dt) · **Tối thiểu**: 5,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `external_sharing`

**Upstream**: `banking.mart_customer_360_gold`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `total_account_balance` | `DECIMAL(18,2)` |  |  |
| `avg_account_balance` | `DECIMAL(18,2)` |  |  |
| `aum_total` | `DECIMAL(18,2)` |  |  |
| `aum_bucket` | `STRING` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.customer_card_summary`

Daily snapshot history of customer card portfolio summary. Grain is one row per customer per cob_dt. Used for historical card usage and portfolio analysis.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: important · **DAG**: `gold_all_dag`

**Grain**: (customer_id, cob_dt) · **Tối thiểu**: 5,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `external_sharing`

**Upstream**: `banking.mart_customer_360_gold`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `total_cards` | `INT` |  |  |
| `cnt_credit_active` | `INT` |  |  |
| `cnt_debit_active` | `INT` |  |  |
| `max_credit_limit` | `DECIMAL(18,2)` |  |  |
| `total_card_txn_count_30d` | `INT` |  |  |
| `total_card_txn_amount_30d` | `DECIMAL(18,2)` |  |  |
| `avg_card_txn_amount_30d` | `DECIMAL(18,2)` |  |  |
| `distinct_merchant_categories` | `INT` |  |  |
| `last_card_txn_date` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.customer_loan_summary`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `total_loans` | `BIGINT` |  |  |
| `active_loans` | `BIGINT` |  |  |
| `overdue_loans` | `BIGINT` |  |  |
| `written_off_loans` | `BIGINT` |  |  |
| `total_loan_amount` | `DECIMAL(29,2)` |  |  |
| `total_loan_outstanding` | `DECIMAL(29,2)` |  |  |
| `avg_loan_interest_rate` | `DECIMAL(15,4)` |  |  |
| `max_loan_term_months` | `INT` |  |  |
| `payment_count` | `BIGINT` |  |  |
| `late_payment_count` | `BIGINT` |  |  |
| `missed_payment_count` | `BIGINT` |  |  |
| `total_amount_paid` | `DECIMAL(29,2)` |  |  |
| `total_penalty` | `DECIMAL(29,2)` |  |  |
| `late_payment_rate` | `DOUBLE` |  |  |
| `missed_payment_rate` | `DOUBLE` |  |  |
| `loan_to_deposit_ratio` | `DECIMAL(35,6)` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.customer_product_summary`

Daily snapshot history of customer product portfolio summary. Grain is one row per customer per cob_dt. Used for historical product holding analysis.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: important · **DAG**: `gold_all_dag`

**Grain**: (customer_id, cob_dt) · **Tối thiểu**: 5,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `external_sharing`

**Upstream**: `banking.mart_customer_360_gold`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `total_accounts` | `INT` |  |  |
| `cnt_casa_active` | `INT` |  |  |
| `cnt_td_active` | `INT` |  |  |
| `total_cards` | `INT` |  |  |
| `cnt_credit_cards` | `INT` |  |  |
| `cnt_debit_cards` | `INT` |  |  |
| `has_credit_card` | `INT` |  |  |
| `has_savings` | `INT` |  |  |
| `has_loan` | `INT` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.customer_transaction_summary`

Daily snapshot history of customer transaction activity. Grain is one row per customer per cob_dt. Used for transaction behavior trends and activity tracking.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: important · **DAG**: `gold_all_dag`

**Grain**: (customer_id, cob_dt) · **Tối thiểu**: 5,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `external_sharing`

**Upstream**: `banking.mart_customer_360_gold`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `acct_txn_count_30d` | `INT` |  |  |
| `acct_txn_amount_30d` | `DECIMAL(18,2)` |  |  |
| `acct_credit_count_30d` | `INT` |  |  |
| `acct_debit_count_30d` | `INT` |  |  |
| `card_txn_count_30d` | `INT` |  |  |
| `card_txn_amount_30d` | `DECIMAL(18,2)` |  |  |
| `total_txn_count_30d` | `INT` |  |  |
| `total_txn_amount_30d` | `DECIMAL(18,2)` |  |  |
| `last_txn_date` | `TIMESTAMP` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.fraud_risk_txn`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `txn_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `customer_segment` | `STRING` |  |  |
| `txn_amount` | `DECIMAL(18,2)` |  |  |
| `txn_type` | `STRING` |  |  |
| `debit_credit` | `STRING` |  |  |
| `balance_after` | `DECIMAL(18,2)` |  |  |
| `channel` | `STRING` |  |  |
| `description` | `STRING` |  |  |
| `counter_account` | `STRING` |  |  |
| `txn_date` | `TIMESTAMP` |  |  |
| `night_flag` | `INT` |  |  |
| `high_amount_flag` | `INT` |  |  |
| `neg_balance_flag` | `INT` |  |  |
| `anomaly_flag` | `INT` |  |  |
| `risk_score` | `INT` |  |  |
| `risk_level` | `INT` |  |  |
| `is_fraud` | `INT` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.loan_delinquency`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `loan_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `branch_code` | `STRING` |  |  |
| `product_code` | `STRING` |  |  |
| `loan_status` | `STRING` |  |  |
| `loan_amount` | `DECIMAL(18,2)` |  |  |
| `outstanding_balance` | `DECIMAL(18,2)` |  |  |
| `disbursement_date` | `DATE` |  |  |
| `vintage_month` | `STRING` |  |  |
| `months_on_book` | `INT` |  |  |
| `last_due_date` | `DATE` |  |  |
| `dpd` | `INT` |  |  |
| `dpd_bucket` | `STRING` |  |  |
| `prev_dpd` | `INT` |  |  |
| `prev_dpd_bucket` | `STRING` |  |  |
| `debt_group` | `INT` |  |  |
| `is_npl` | `INT` |  |  |
| `max_dpd_ever` | `INT` |  |  |
| `ever_30_plus` | `INT` |  |  |
| `ever_90_plus` | `INT` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.loan_portfolio_risk`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `branch_code` | `STRING` |  |  |
| `branch_name` | `STRING` |  |  |
| `product_code` | `STRING` |  |  |
| `total_loans` | `BIGINT` |  |  |
| `active_loans` | `BIGINT` |  |  |
| `overdue_loans` | `BIGINT` |  |  |
| `total_amount` | `DECIMAL(18,2)` |  |  |
| `total_outstanding` | `DECIMAL(18,2)` |  |  |
| `overdue_rate` | `DOUBLE` |  |  |
| `npl_proxy` | `DECIMAL(18,2)` |  |  |
| `payment_count` | `BIGINT` |  |  |
| `late_payment_count` | `BIGINT` |  |  |
| `late_payment_rate` | `DOUBLE` |  |  |
| `total_penalty` | `DECIMAL(18,2)` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.mart_branch_monthly_summary`

Monthly aggregated branch performance metrics including transaction volume, customer count, and revenue by branch.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: important · **DAG**: `gold_all_dag`

**Tối thiểu**: 100 dòng · **Freshness**: 24h

**AI risk tier**: `minimal_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.dim_branch_silver`, `banking.fact_txn_account_silver`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `branch_code` | `STRING` |  |  |
| `branch_name` | `STRING` |  |  |
| `region` | `STRING` |  |  |
| `city` | `STRING` |  |  |
| `txn_year` | `INT` |  |  |
| `txn_month` | `INT` |  |  |
| `txn_quarter` | `INT` |  |  |
| `active_customers` | `BIGINT` |  |  |
| `txn_count` | `BIGINT` |  |  |
| `total_txn_amount` | `DECIMAL(18,2)` |  |  |
| `avg_txn_amount` | `DECIMAL(18,2)` |  |  |
| `total_credit_amount` | `DECIMAL(18,2)` |  |  |
| `total_debit_amount` | `DECIMAL(18,2)` |  |  |
| `top_channel` | `STRING` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.mart_customer_360`

Daily snapshot history of the 360-degree customer view joining all dimensions and facts. Grain is one row per customer per cob_dt. Used for historical analytics, trend analysis, and backtesting.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: critical · **DAG**: `gold_all_dag`

**Grain**: (customer_id, cob_dt) · **Tối thiểu**: 5,000 dòng · **Freshness**: 24h

**AI risk tier**: `high_risk` · **Cấm dùng cho**: `automated_credit_decision`, `external_sharing`

**Upstream**: `banking.dim_customer_silver`, `banking.dim_account_silver`, `banking.dim_card_silver`, `banking.fact_txn_account_silver`, `banking.fact_card_txn_silver`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `full_name_masked` | `STRING` | ⚠️ |  |
| `age` | `INT` |  |  |
| `gender` | `STRING` |  |  |
| `primary_branch_code` | `STRING` |  |  |
| `customer_segment` | `STRING` |  |  |
| `kyc_status` | `STRING` |  |  |
| `register_date` | `DATE` |  |  |
| `total_accounts` | `INT` |  |  |
| `total_cards` | `INT` |  |  |
| `total_loans` | `INT` |  |  |
| `has_credit_card` | `INT` |  |  |
| `has_savings` | `INT` |  |  |
| `has_loan` | `INT` |  |  |
| `total_deposit_balance` | `DECIMAL(18,2)` |  |  |
| `total_loan_outstanding` | `DECIMAL(18,2)` |  |  |
| `aum_total` | `DECIMAL(18,2)` |  |  |
| `aum_bucket` | `STRING` |  |  |
| `txn_count_30d` | `INT` |  |  |
| `txn_amount_30d` | `DECIMAL(18,2)` |  |  |
| `last_txn_date` | `TIMESTAMP` |  |  |
| `days_since_last_txn` | `INT` |  |  |
| `primary_channel` | `STRING` |  |  |
| `interaction_count_90d` | `INT` |  |  |
| `last_interaction_date` | `TIMESTAMP` |  |  |
| `digital_txn_count_30d` | `INT` |  |  |
| `digital_txn_amount_30d` | `DECIMAL(18,2)` |  |  |
| `digital_channel_count_30d` | `INT` |  |  |
| `digital_device_count_30d` | `INT` |  |  |
| `last_digital_txn_date` | `TIMESTAMP` |  |  |
| `is_digital_active` | `INT` |  |  |
| `rfm_recency_score` | `INT` |  |  |
| `rfm_frequency_score` | `INT` |  |  |
| `rfm_monetary_score` | `INT` |  |  |
| `rfm_segment` | `STRING` |  |  |
| `churn_flag` | `INT` |  |  |
| `cross_sell_credit_card_flag` | `INT` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

### `lakehouse.gold.rfm_segment`

Daily snapshot history of RFM segmentation. Grain is one row per customer per cob_dt. Used for historical customer value classification and backtesting.

**Owner**: Data Engineering Team · **SLA**: daily · **Quality class**: important · **DAG**: `gold_all_dag`

**Grain**: (customer_id, cob_dt) · **Tối thiểu**: 1,000 dòng · **Freshness**: 24h

**AI risk tier**: `limited_risk` · **Cấm dùng cho**: `automated_decision_making`

**Upstream**: `banking.dim_customer_silver`, `banking.fact_txn_account_silver`, `banking.fact_card_txn_silver`

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `customer_sk` | `STRING` |  |  |
| `recency_days` | `INT` |  |  |
| `frequency` | `BIGINT` |  |  |
| `monetary` | `DECIMAL(18,2)` |  |  |
| `r_score` | `INT` |  |  |
| `f_score` | `INT` |  |  |
| `m_score` | `INT` |  |  |
| `rfm_score` | `INT` |  |  |
| `rfm_segment` | `STRING` |  |  |
| `cob_dt` | `DATE` |  |  |

<sub>Nguồn DDL: `03_ddl_gold.sql`</sub>

---

## meta

### `lakehouse.meta.cdc_watermark`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `table_name` | `VARCHAR(100)` |  |  |
| `last_snapshot_id` | `BIGINT` |  |  |
| `last_cdc_timestamp_ms` | `BIGINT` |  |  |
| `last_spark_batch_id` | `BIGINT` |  |  |
| `last_processed_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `06_ddl_silver_cdc_current.sql`</sub>

---

## card_crm

### `card_crm.card`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `card_id` | `BIGINT` |  |  |
| `card_no_masked` | `VARCHAR(19)` |  | format: 4111****1234 |
| `customer_id` | `BIGINT` |  | logical FK -> core_banking.customer |
| `account_id` | `BIGINT` |  | logical FK -> core_banking.account (for debit cards) |
| `product_code` | `VARCHAR(20)` |  |  |
| `card_type` | `VARCHAR(20)` |  | DEBIT / CREDIT / PREPAID |
| `card_brand` | `VARCHAR(20)` |  | VISA / MASTER / JCB / NAPAS |
| `credit_limit` | `NUMERIC(18,2)` |  | only for CREDIT cards |
| `issue_date` | `DATE` |  |  |
| `expiry_date` | `DATE` |  |  |
| `status` | `VARCHAR(20)` |  | ACTIVE / BLOCKED / EXPIRED / CLOSED |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_card_crm.sql`</sub>

### `card_crm.card_txn`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `txn_id` | `BIGINT` |  |  |
| `card_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  | denormalized |
| `txn_date` | `TIMESTAMP` |  |  |
| `txn_amount` | `NUMERIC(18,2)` |  |  |
| `txn_type` | `VARCHAR(20)` |  | PURCHASE / CASH_ADVANCE / REFUND / REVERSAL |
| `currency` | `CHAR(3)` |  |  |
| `merchant_name` | `VARCHAR(200)` |  |  |
| `merchant_category` | `VARCHAR(50)` |  | GROCERY / RESTAURANT / TRAVEL / ECOM / FUEL / EDUCATION |
| `mcc_code` | `VARCHAR(10)` |  | FK -> digital_banking.mcc_code (nullable) |
| `channel` | `VARCHAR(20)` |  | POS / ECOM / ATM |
| `status` | `VARCHAR(20)` |  | SUCCESS / FAILED / PENDING |
| `entry_mode` | `VARCHAR(10)` |  | CHIP / SWIPE / ONLINE |
| `decline_reason` | `VARCHAR(100)` |  | chỉ khi FAILED; có thể là tổ hợp "A |
| `B` | `PROCESSING_TIME_MS` |  | transaction processing time |
| `reference_number` | `VARCHAR(30)` |  | CDN + sequential number |
| `created_ts` | `TIMESTAMP` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_card_crm.sql`</sub>

### `card_crm.crm_interaction`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `interaction_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `interaction_date` | `TIMESTAMP` |  |  |
| `channel` | `VARCHAR(20)` |  | CALL / EMAIL / CHAT / BRANCH / SMS |
| `direction` | `VARCHAR(10)` |  | INBOUND / OUTBOUND |
| `subject` | `VARCHAR(500)` |  |  |
| `category` | `VARCHAR(30)` |  | COMPLAINT / INQUIRY / CAMPAIGN / CROSS_SELL / RETENTION |
| `status` | `VARCHAR(20)` |  | OPEN / RESOLVED / PENDING |
| `assigned_to` | `VARCHAR(100)` |  |  |
| `satisfaction_score` | `SMALLINT` |  | 1-5 rating (nullable) |
| `created_ts` | `TIMESTAMP` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `02_ddl_card_crm.sql`</sub>

---

## core_banking

### `core_banking.account`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `account_id` | `BIGINT` |  |  |
| `account_no` | `VARCHAR(20)` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `product_code` | `VARCHAR(20)` |  |  |
| `branch_code` | `VARCHAR(10)` |  |  |
| `account_type` | `VARCHAR(20)` |  | CASA / TIME_DEPOSIT |
| `currency` | `CHAR(3)` |  |  |
| `balance` | `NUMERIC(18,2)` |  |  |
| `open_date` | `DATE` |  |  |
| `close_date` | `DATE` |  |  |
| `status` | `VARCHAR(20)` |  | ACTIVE / CLOSED / FROZEN |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `01_ddl_core_banking.sql`</sub>

### `core_banking.aml_alert`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `alert_id` | `BIGINT` |  |  |
| `alert_number` | `VARCHAR(30)` |  | Auto-generated: AML-YYYYMMDD-NNNNN |
| `rule_id` | `BIGINT` |  |  |
| `transaction_id` | `BIGINT` |  | FK to txn_account (nullable for pattern-based alerts) |
| `card_txn_id` | `BIGINT` |  | FK to card_crm.card_txn (nullable) |
| `customer_id` | `BIGINT` |  | FK to customer |
| `account_id` | `BIGINT` |  | FK to account (nullable) |
| `alert_type` | `VARCHAR(50)` |  |  |
| `risk_score` | `NUMERIC(5,2)` |  | 0.00 - 100.00 |
| `risk_category` | `VARCHAR(20)` |  |  |
| `description` | `TEXT` |  |  |
| `evidence_json` | `JSONB` |  | Supporting evidence: transaction details |
| `patterns` | `TXN_AMOUNT` |  |  |
| `txn_date` | `TIMESTAMP` |  |  |
| `channel` | `VARCHAR(20)` |  |  |
| `status` | `VARCHAR(20)` |  |  |
| `pending` | `REVIEW` |  | FK to employee (compliance analyst) |
| `priority` | `VARCHAR(10)` |  |  |
| `due_date` | `DATE` |  |  |
| `notes` | `TEXT` |  |  |
| `ctr_required` | `SMALLINT` |  | Currency Transaction Report required |
| `sar_filed` | `SMALLINT` |  | Suspicious Activity Report filed |
| `sar_reference` | `VARCHAR(50)` |  |  |
| `created_at` | `TIMESTAMP` |  |  |
| `updated_at` | `TIMESTAMP` |  |  |
| `resolved_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `06_ddl_aml.sql`</sub>

### `core_banking.aml_alert_transaction`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `alert_id` | `BIGINT` |  |  |
| `transaction_id` | `BIGINT` |  | FK to txn_account |
| `created_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `06_ddl_aml.sql`</sub>

### `core_banking.aml_customer_risk`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  | FK to customer |
| `risk_level` | `VARCHAR(20)` |  |  |
| `requires` | `SENIOR` |  |  |
| `peps_flag` | `SMALLINT` |  | Politically Exposed Person |
| `sanctions_flag` | `SMALLINT` |  | On sanctions list |
| `adverse_media_flag` | `SMALLINT` |  | Adverse media hits |
| `total_alerts` | `INT` |  |  |
| `open_alerts` | `INT` |  |  |
| `last_alert_date` | `DATE` |  |  |
| `last_review_date` | `DATE` |  |  |
| `next_review_date` | `DATE` |  |  |
| `edd_required` | `SMALLINT` |  |  |
| `edd_reason` | `TEXT` |  |  |
| `source_of_wealth` | `VARCHAR(100)` |  |  |
| `expected_activity` | `TEXT` |  |  |
| `created_at` | `TIMESTAMP` |  |  |
| `updated_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `06_ddl_aml.sql`</sub>

### `core_banking.aml_rule`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `rule_id` | `BIGINT` |  |  |
| `rule_name` | `VARCHAR(200)` |  |  |
| `rule_code` | `VARCHAR(50)` |  |  |
| `rule_type` | `VARCHAR(50)` |  |  |
| `description` | `TEXT` |  |  |
| `threshold` | `NUMERIC(18,2)` |  |  |
| `threshold_currency` | `VARCHAR(3)` |  |  |
| `window_hours` | `INT` |  |  |
| `severity` | `VARCHAR(20)` |  |  |
| `is_active` | `SMALLINT` |  |  |
| `regulatory_ref` | `VARCHAR(100)` |  | Reference to regulation (e.g., 'TT 35/2019/TT-NHNN') |
| `created_by` | `VARCHAR(100)` |  |  |
| `created_at` | `TIMESTAMP` |  |  |
| `updated_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `06_ddl_aml.sql`</sub>

### `core_banking.branch`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `branch_code` | `VARCHAR(10)` |  |  |
| `branch_name` | `VARCHAR(200)` |  |  |
| `region` | `VARCHAR(20)` |  | NORTH / CENTRAL / SOUTH |
| `city` | `VARCHAR(100)` |  |  |
| `district` | `VARCHAR(100)` |  |  |
| `address` | `VARCHAR(500)` | ⚠️ |  |
| `manager_name` | `VARCHAR(200)` |  |  |
| `open_date` | `DATE` |  |  |
| `status` | `VARCHAR(20)` |  | ACTIVE / CLOSED |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `01_ddl_core_banking.sql`</sub>

### `core_banking.customer`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `customer_id` | `BIGINT` |  |  |
| `cccd` | `VARCHAR(12)` |  | National ID (12 digits) |
| `full_name` | `VARCHAR(200)` | ⚠️ |  |
| `gender` | `CHAR(1)` |  | M / F / O |
| `date_of_birth` | `DATE` | ⚠️ |  |
| `phone` | `VARCHAR(15)` | ⚠️ | 0xxxxxxxxx |
| `email` | `VARCHAR(200)` | ⚠️ |  |
| `address` | `VARCHAR(500)` | ⚠️ |  |
| `city` | `VARCHAR(100)` |  |  |
| `district` | `VARCHAR(100)` |  |  |
| `branch_code` | `VARCHAR(10)` |  | FK -> branch |
| `customer_segment` | `VARCHAR(20)` |  | RETAIL / PRIORITY / VIP |
| `kyc_status` | `VARCHAR(20)` |  | PENDING / VERIFIED / REJECTED |
| `register_date` | `DATE` |  |  |
| `is_active` | `SMALLINT` |  | 0 / 1 |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `01_ddl_core_banking.sql`</sub>

### `core_banking.deposit`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `deposit_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  | FK -> account (can be NULL for standalone savings) |
| `customer_id` | `BIGINT` |  |  |
| `product_code` | `VARCHAR(20)` |  |  |
| `principal_amount` | `NUMERIC(18,2)` |  |  |
| `interest_rate` | `NUMERIC(5,2)` |  | e.g. 5.50 = 5.5%/year |
| `term_months` | `SMALLINT` |  | 1/3/6/12/24/36 |
| `open_date` | `DATE` |  |  |
| `maturity_date` | `DATE` |  |  |
| `status` | `VARCHAR(20)` |  | ACTIVE / MATURED / EARLY_WITHDRAWN |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `01_ddl_core_banking.sql`</sub>

### `core_banking.employee`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `employee_id` | `BIGINT` |  |  |
| `full_name` | `VARCHAR(200)` | ⚠️ |  |
| `branch_code` | `VARCHAR(10)` |  |  |
| `role` | `VARCHAR(50)` |  | TELLER / MANAGER / ANALYST / DIRECTOR |
| `hire_date` | `DATE` |  |  |
| `salary` | `NUMERIC(12,2)` |  |  |
| `status` | `VARCHAR(20)` |  | ACTIVE / TERMINATED |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `01_ddl_core_banking.sql`</sub>

### `core_banking.loan`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `loan_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `product_code` | `VARCHAR(20)` |  |  |
| `branch_code` | `VARCHAR(10)` |  |  |
| `loan_amount` | `NUMERIC(18,2)` |  | original loan amount |
| `outstanding_balance` | `NUMERIC(18,2)` |  | current outstanding balance |
| `interest_rate` | `NUMERIC(5,2)` |  |  |
| `term_months` | `SMALLINT` |  |  |
| `disbursement_date` | `DATE` |  |  |
| `maturity_date` | `DATE` |  |  |
| `loan_status` | `VARCHAR(20)` |  | ACTIVE / CLOSED / OVERDUE / WRITTEN_OFF |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `01_ddl_core_banking.sql`</sub>

### `core_banking.loan_payment`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `payment_id` | `BIGINT` |  |  |
| `loan_id` | `BIGINT` |  | FK -> loan |
| `payment_date` | `DATE` |  |  |
| `scheduled_amount` | `NUMERIC(18,2)` |  | expected monthly payment |
| `amount_paid` | `NUMERIC(18,2)` |  | actual amount paid |
| `principal_component` | `NUMERIC(18,2)` |  |  |
| `interest_component` | `NUMERIC(18,2)` |  |  |
| `penalty` | `NUMERIC(18,2)` |  | late fee if any |
| `outstanding_after` | `NUMERIC(18,2)` |  |  |
| `days_late` | `SMALLINT` |  | 0 if on time |
| `payment_method` | `VARCHAR(30)` |  | BANK_TRANSFER / CASH / CHEQUE / DEBIT_CARD / MOBILE_APP |
| `payment_status` | `VARCHAR(20)` |  | PAID / LATE / MISSED / PENDING |
| `late_payment_flag` | `SMALLINT` |  | legacy 0/1 |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `01_ddl_core_banking.sql`</sub>

### `core_banking.product`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `product_code` | `VARCHAR(20)` |  |  |
| `product_name` | `VARCHAR(200)` |  |  |
| `product_group` | `VARCHAR(20)` |  | DEPOSIT / LOAN / CARD |
| `product_type` | `VARCHAR(30)` |  | CASA / SAVINGS / PERSONAL_LOAN / MORTGAGE / CREDIT_CARD / DEBIT_CARD |
| `currency` | `CHAR(3)` |  | VND / USD |
| `is_active` | `SMALLINT` |  | 0 / 1 |
| `launch_date` | `DATE` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `01_ddl_core_banking.sql`</sub>

### `core_banking.standing_order`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `order_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  | FK -> account |
| `customer_id` | `BIGINT` |  | FK -> customer |
| `order_type` | `VARCHAR(30)` |  | BILL_PAYMENT / TRANSFER / LOAN_PAYMENT |
| `beneficiary_name` | `VARCHAR(200)` |  |  |
| `beneficiary_account` | `VARCHAR(20)` |  |  |
| `amount` | `NUMERIC(18,2)` |  |  |
| `frequency` | `VARCHAR(20)` |  | MONTHLY / WEEKLY / QUARTERLY |
| `next_execute_date` | `DATE` |  |  |
| `status` | `VARCHAR(20)` |  | ACTIVE / PAUSED / CANCELLED |
| `created_date` | `DATE` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `01_ddl_core_banking.sql`</sub>

### `core_banking.txn_account`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `txn_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  | denormalized for query speed |
| `txn_date` | `TIMESTAMP` |  |  |
| `txn_amount` | `NUMERIC(18,2)` |  |  |
| `txn_type` | `VARCHAR(30)` |  | DEPOSIT / WITHDRAWAL / TRANSFER_IN / TRANSFER_OUT / FEE / INTEREST |
| `debit_credit` | `CHAR(1)` |  | D (Debit) / C (Credit) |
| `balance_after` | `NUMERIC(18,2)` |  |  |
| `channel` | `VARCHAR(20)` |  | BRANCH / ATM / INTERNET_BANKING / MOBILE_BANKING / POS |
| `description` | `VARCHAR(500)` |  |  |
| `counter_account` | `VARCHAR(20)` |  | counterparty account (for transfers) |
| `created_ts` | `TIMESTAMP` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `01_ddl_core_banking.sql`</sub>

---

## digital_banking

### `digital_banking.device`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `device_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  | logical FK -> core_banking.customer |
| `device_type` | `VARCHAR(30)` |  | MOBILE / TABLET / DESKTOP |
| `device_fingerprint` | `VARCHAR(200)` |  | unique device fingerprint |
| `operating_system` | `VARCHAR(50)` |  | iOS / Android / Windows / macOS |
| `ip_address` | `VARCHAR(45)` | ⚠️ | IPv4 or IPv6 |
| `is_trusted` | `SMALLINT` |  | 0/1 |
| `first_seen` | `TIMESTAMP` |  |  |
| `last_seen` | `TIMESTAMP` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `03_ddl_digital_banking.sql`</sub>

### `digital_banking.location`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `location_id` | `BIGINT` |  |  |
| `merchant_name` | `VARCHAR(200)` |  |  |
| `merchant_category` | `VARCHAR(100)` |  | grocery |
| `etc.` | `CITY` |  |  |
| `state` | `VARCHAR(100)` |  |  |
| `latitude` | `NUMERIC(10,7)` |  |  |
| `longitude` | `NUMERIC(10,7)` |  |  |
| `is_high_risk_area` | `SMALLINT` |  | 0/1 |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `03_ddl_digital_banking.sql`</sub>

### `digital_banking.mcc_code`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `mcc_code` | `VARCHAR(10)` |  |  |
| `description` | `VARCHAR(200)` |  |  |
| `category_group` | `VARCHAR(50)` |  | RETAIL / FOOD / TRAVEL / SERVICES / UTILITIES |
| `is_high_risk` | `SMALLINT` |  | 0/1 |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `03_ddl_digital_banking.sql`</sub>

### `digital_banking.merchant`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `merchant_id` | `BIGINT` |  |  |
| `merchant_name` | `VARCHAR(200)` |  |  |
| `merchant_category` | `VARCHAR(50)` |  |  |
| `mcc_code` | `VARCHAR(10)` |  | FK -> digital_banking.mcc_code |
| `city` | `VARCHAR(100)` |  |  |
| `state` | `VARCHAR(100)` |  |  |
| `risk_category` | `VARCHAR(20)` |  | LOW / MEDIUM / HIGH |
| `is_active` | `SMALLINT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `03_ddl_digital_banking.sql`</sub>

### `digital_banking.online_transaction`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `transaction_id` | `BIGINT` |  |  |
| `account_id` | `BIGINT` |  | FK -> core_banking.account (nullable for card-only txns) |
| `device_id` | `BIGINT` |  | FK -> digital_banking.device |
| `location_id` | `BIGINT` |  | FK -> digital_banking.location |
| `customer_id` | `BIGINT` |  | denormalized |
| `transaction_type` | `VARCHAR(30)` |  | PURCHASE / TRANSFER / PAYMENT / WITHDRAWAL / TOP_UP |
| `channel` | `VARCHAR(20)` |  | MOBILE_APP / WEB / API / POS |
| `amount` | `NUMERIC(18,2)` |  |  |
| `currency` | `CHAR(3)` |  |  |
| `is_fraud` | `SMALLINT` |  | 0/1 |
| `fraud_reason` | `VARCHAR(200)` |  |  |
| `status` | `VARCHAR(20)` |  | SUCCESS / FAILED / PENDING |
| `transaction_date` | `TIMESTAMP` |  |  |
| `created_ts` | `TIMESTAMP` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `03_ddl_digital_banking.sql`</sub>

### `digital_banking.support_ticket`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `ticket_id` | `BIGINT` |  |  |
| `customer_id` | `BIGINT` |  |  |
| `issue_type` | `VARCHAR(50)` |  | TRANSACTION_DISPUTE / ACCOUNT_ACCESS / CARD_BLOCK / GENERAL_INQUIRY / FEEDBACK |
| `priority` | `VARCHAR(10)` |  | LOW / MEDIUM / HIGH / URGENT |
| `status` | `VARCHAR(20)` |  | OPEN / IN_PROGRESS / RESOLVED / CLOSED |
| `date_opened` | `TIMESTAMP` |  |  |
| `date_resolved` | `TIMESTAMP` |  |  |
| `resolution_time_hrs` | `NUMERIC(8,2)` |  | auto-calculated |
| `satisfaction_score` | `SMALLINT` |  | 1-5 (nullable) |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `03_ddl_digital_banking.sql`</sub>

---

## opslakehouse

### `opslakehouse.contract_validation_log`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `id` | `SERIAL` |  |  |
| `dataset_id` | `VARCHAR(255)` |  | e.g. 'banking.dim_customer_silver' |
| `check_name` | `VARCHAR(100)` |  | e.g. 'required_columns' |
| `'unique_check'` | `CHECK_STATUS` |  | PASS / FAIL / WARN |
| `expected_value` | `TEXT` |  |  |
| `actual_value` | `TEXT` |  |  |
| `details` | `TEXT` |  |  |
| `cob_dt` | `DATE` |  |  |
| `checked_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `00_extensions.sql`</sub>

### `opslakehouse.data_lineage_audit`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `audit_id` | `BIGINT` |  |  |
| `source_table` | `VARCHAR(200)` |  |  |
| `source_column` | `VARCHAR(200)` |  |  |
| `target_table` | `VARCHAR(200)` |  |  |
| `target_column` | `VARCHAR(200)` |  |  |
| `transformation` | `TEXT` |  | SQL or description of transformation |
| `job_name` | `VARCHAR(100)` |  |  |
| `job_run_id` | `VARCHAR(100)` |  |  |
| `executed_at` | `TIMESTAMP` |  |  |
| `record_count` | `BIGINT` |  |  |
| `checksum` | `VARCHAR(100)` |  | Data checksum for integrity verification |

<sub>Nguồn DDL: `09_ddl_regulatory.sql`</sub>

### `opslakehouse.data_quality_log`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `id` | `SERIAL` |  |  |
| `check_name` | `VARCHAR(200)` |  | e.g. 'row_count' |
| `'fk_integrity'` | `TABLE_NAME` |  | e.g. 'silver.dim_customer' |
| `check_status` | `VARCHAR(20)` |  | PASS / FAIL / WARN |
| `expected_value` | `TEXT` |  | expected result |
| `actual_value` | `TEXT` |  | actual result |
| `details` | `TEXT` |  | error message or details |
| `cob_dt` | `DATE` |  |  |
| `checked_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `00_extensions.sql`</sub>

### `opslakehouse.dq_scorecard`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `scorecard_id` | `BIGINT` |  |  |
| `report_date` | `DATE` |  |  |
| `table_name` | `VARCHAR(200)` |  |  |
| `column_name` | `VARCHAR(200)` |  | NULL = table-level score |
| `dimension` | `VARCHAR(50)` |  |  |
| `score` | `NUMERIC(5,2)` |  |  |
| `threshold` | `NUMERIC(5,2)` |  | Minimum acceptable score |
| `is_pass` | `SMALLINT` |  |  |

<sub>Nguồn DDL: `09_ddl_regulatory.sql`</sub>

### `opslakehouse.flag_job_etl`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `id` | `SERIAL` |  |  |
| `job_name` | `VARCHAR(100)` |  | dag_id |
| `schema_name` | `VARCHAR(50)` |  | bronze / silver / gold / ops |
| `table_name` | `VARCHAR(100)` |  | dag_id (same as job_name) |
| `status` | `CHAR(1)` |  | R = Running |
| `S` | `=` |  | filled when status = R |
| `end_time` | `TIMESTAMP` |  | filled when status = S |
| `cob_dt` | `DATE` |  | business date (YYYY-MM-DD) |
| `created_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `00_extensions.sql`</sub>

### `opslakehouse.lineage_log`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `id` | `SERIAL` |  |  |
| `source_table` | `VARCHAR(255)` |  | e.g. 'lakehouse.silver.dim_customer' |
| `target_table` | `VARCHAR(255)` |  | e.g. 'lakehouse.gold.mart_customer_360' |
| `transform_type` | `VARCHAR(100)` |  | governance.lineage.TransformType |
| `dag_id` | `VARCHAR(255)` |  |  |
| `dag_run_id` | `VARCHAR(255)` |  |  |
| `snapshot_id` | `VARCHAR(255)` |  |  |
| `row_count` | `INTEGER` |  |  |
| `created_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `00_extensions.sql`</sub>

### `opslakehouse.pipeline_run_log`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `id` | `SERIAL` |  |  |
| `dag_id` | `VARCHAR(100)` |  |  |
| `task_id` | `VARCHAR(200)` |  |  |
| `status` | `VARCHAR(20)` |  | RUNNING / SUCCESS / FAILED / SKIPPED |
| `cob_dt` | `DATE` |  |  |
| `rows_processed` | `BIGINT` |  |  |
| `execution_time_s` | `NUMERIC(10,2)` |  |  |
| `error_message` | `TEXT` |  |  |
| `started_at` | `TIMESTAMP` |  |  |
| `completed_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `04_ddl_ops_metadata.sql`</sub>

### `opslakehouse.regulatory_report`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `report_id` | `BIGINT` |  |  |
| `report_type` | `VARCHAR(50)` |  |  |
| `report_name` | `VARCHAR(200)` |  |  |
| `report_period_start` | `DATE` |  |  |
| `report_period_end` | `DATE` |  |  |
| `generated_date` | `TIMESTAMP` |  |  |
| `data_json` | `JSONB` |  | Report data in JSON format |
| `summary_json` | `JSONB` |  | Summary statistics |
| `status` | `VARCHAR(20)` |  |  |
| `pending` | `REVIEW` |  |  |
| `needs` | `CORRECTION` |  |  |
| `submitted_at` | `TIMESTAMP` |  |  |
| `submission_reference` | `VARCHAR(100)` |  | Regulator reference number |
| `created_by` | `VARCHAR(100)` |  |  |
| `created_at` | `TIMESTAMP` |  |  |
| `updated_at` | `TIMESTAMP` |  |  |
| `notes` | `TEXT` |  |  |

<sub>Nguồn DDL: `09_ddl_regulatory.sql`</sub>

### `opslakehouse.regulatory_rule`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `rule_id` | `BIGINT` |  |  |
| `rule_code` | `VARCHAR(50)` |  |  |
| `regulation` | `VARCHAR(50)` |  | BCBS239 |
| `INTERNAL` | `RULE_NAME` |  |  |
| `description` | `TEXT` |  |  |
| `threshold_value` | `NUMERIC(18,2)` |  |  |
| `threshold_type` | `VARCHAR(20)` |  | MIN |
| `PERCENTAGE` | `IS_ACTIVE` |  |  |
| `created_at` | `TIMESTAMP` |  |  |
| `updated_at` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `09_ddl_regulatory.sql`</sub>

### `opslakehouse.source_table_registry`

_Chưa có data contract trong `governance/datasets/`._

| Cột | Kiểu | PII | Ghi chú |
|---|---|:-:|---|
| `id` | `SERIAL` |  |  |
| `schema_name` | `VARCHAR(50)` |  |  |
| `table_name` | `VARCHAR(100)` |  |  |
| `source_type` | `VARCHAR(20)` |  | postgresql / oracle / mysql |
| `jdbc_conn_id` | `VARCHAR(100)` |  | Airflow connection ID |
| `bronze_table` | `VARCHAR(200)` |  | Iceberg target: lakehouse.bronze.xxx |
| `silver_table` | `VARCHAR(200)` |  | Iceberg target: lakehouse.silver.xxx |
| `is_active` | `SMALLINT` |  |  |
| `last_updated` | `TIMESTAMP` |  |  |

<sub>Nguồn DDL: `04_ddl_ops_metadata.sql`</sub>

---

## Ghi chú

**Cột PII được đánh dấu bằng heuristic tên cột**, không phải bằng phân loại
thủ công. Hai giới hạn cần biết:

- **Sẽ bỏ sót** cột nhạy cảm đặt tên không theo mẫu thông dụng. Đã đo:
  mẫu không có `cccd` — số định danh cá nhân, và là trường nhạy cảm nhất
  của nền tảng — nên nó KHÔNG được đánh dấu ở cả 5 bảng chứa nó. Các cột
  khác cũng bị bỏ sót: `device_id`, `account_no`, `ip_address`,
  `latitude`/`longitude`, `manager_name`.
- **Đánh dấu cả cột đã masking** (ví dụ `full_name_masked`). Đây là chủ ý:
  cột đã che vẫn nằm trong lineage PII và vẫn cần kiểm soát truy cập.

Vì giới hạn thứ nhất, **đừng đọc con số trên là số cột PII**. Bản kiểm kê
đầy đủ, dựng bằng cách parse DDL với danh sách cột rộng hơn, là
[`PII_INVENTORY.md`](../06-security-compliance/PII_INVENTORY.md) §2.

**File DDL bị loại khỏi tài liệu này**, kèm lý do:

- `04_ddl_bronze_cdc_old.sql` — bản cũ đã thay bằng 04_ddl_bronze_cdc.sql
- `05_security.sql` — role và grant, không phải bảng dữ liệu
- `06_ddl_superset.sql` — schema nội bộ của Superset
- `07_ddl_mlflow.sql` — schema nội bộ của MLflow
- `08_ddl_data_vault_example.sql` — DDL minh hoạ cho DATA_VAULT_MAPPING, không triển khai

