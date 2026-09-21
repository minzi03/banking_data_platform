# Data Contracts

> ⚠️ **File này được SINH TỰ ĐỘNG. Đừng sửa tay.**
>
> Sinh bởi `scripts/generate_governance_docs.py` từ `governance/datasets/*.yaml`.
> Sinh lại: `py -3 scripts/generate_governance_docs.py`

Data contract khai báo ràng buộc của một dataset: grain, cột bắt buộc,
số dòng tối thiểu, SLA freshness, và phân loại rủi ro AI. Chúng được
`governance/enforcement.py` dùng để chặn dữ liệu không đạt.

**33 contract · 15 quality_class `critical` · 4 AI `high_risk`**

| Tầng | Số contract |
|---|---:|
| bronze | 1 |
| silver | 13 |
| gold | 19 |

---

## bronze

| Dataset | Bảng vật lý | Grain | Quality | AI risk | DAG |
|---|---|---|---|---|---|
| `banking.core_customer_bronze` | `bronze.core_customer` | — | important | `limited_risk` | `bronze_core_banking_dag` |

## silver

| Dataset | Bảng vật lý | Grain | Quality | AI risk | DAG |
|---|---|---|---|---|---|
| `banking.core_customer_silver` | `silver.dim_customer` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.dim_account_silver` | `silver.dim_account` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.dim_branch_silver` | `silver.dim_branch` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.dim_card_silver` | `silver.dim_card` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.dim_device_silver` | `silver.dim_device` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.dim_employee_silver` | `silver.dim_employee` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.dim_location_silver` | `silver.dim_location` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.dim_product_silver` | `silver.dim_product` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.fact_card_txn_silver` | `silver.fact_card_txn` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.fact_crm_interaction_silver` | `silver.fact_crm_interaction` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.fact_online_transaction_silver` | `silver.fact_online_transaction` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.fact_support_ticket_silver` | `silver.fact_support_ticket` | — | critical | `limited_risk` | `silver_all_dag` |
| `banking.fact_txn_account_silver` | `silver.fact_txn_account` | — | critical | `limited_risk` | `silver_all_dag` |

## gold

| Dataset | Bảng vật lý | Grain | Quality | AI risk | DAG |
|---|---|---|---|---|---|
| `banking.branch_monthly_summary_gold` | `gold.branch_monthly_summary` | — | important | `minimal_risk` | `gold_mart360_dag` |
| `banking.campaign_target_current_gold` | `gold.campaign_target_current` | — | important | `limited_risk` | `gold_all_dag` |
| `banking.campaign_target_gold` | `gold.campaign_target` | (customer_id, cob_dt) | important | `limited_risk` | `gold_mart360_dag` |
| `banking.churn_prediction_current_gold` | `gold.churn_prediction_current` | — | important | `high_risk` | `gold_all_dag` |
| `banking.churn_prediction_gold` | `gold.churn_prediction` | (customer_id, cob_dt) | important | `high_risk` | `gold_mart360_dag` |
| `banking.cross_sell_segment_current_gold` | `gold.cross_sell_segment_current` | — | important | `limited_risk` | `gold_all_dag` |
| `banking.cross_sell_segment_gold` | `gold.cross_sell_segment` | (customer_id, cob_dt) | important | `limited_risk` | `gold_mart360_dag` |
| `banking.customer_balance_summary_current_gold` | `gold.customer_balance_summary_current` | — | important | `limited_risk` | `gold_all_dag` |
| `banking.customer_balance_summary_gold` | `gold.customer_balance_summary` | (customer_id, cob_dt) | important | `limited_risk` | `gold_mart360_dag` |
| `banking.customer_card_summary_current_gold` | `gold.customer_card_summary_current` | — | important | `limited_risk` | `gold_all_dag` |
| `banking.customer_card_summary_gold` | `gold.customer_card_summary` | (customer_id, cob_dt) | important | `limited_risk` | `gold_mart360_dag` |
| `banking.customer_product_summary_current_gold` | `gold.customer_product_summary_current` | — | important | `limited_risk` | `gold_all_dag` |
| `banking.customer_product_summary_gold` | `gold.customer_product_summary` | (customer_id, cob_dt) | important | `limited_risk` | `gold_mart360_dag` |
| `banking.customer_transaction_summary_current_gold` | `gold.customer_transaction_summary_current` | — | important | `limited_risk` | `gold_all_dag` |
| `banking.customer_transaction_summary_gold` | `gold.customer_transaction_summary` | (customer_id, cob_dt) | important | `limited_risk` | `gold_mart360_dag` |
| `banking.mart_customer_360_current_gold` | `gold.mart_customer_360_current` | — | critical | `high_risk` | `gold_all_dag` |
| `banking.mart_customer_360_gold` | `gold.mart_customer_360` | (customer_id, cob_dt) | critical | `high_risk` | `gold_mart360_dag` |
| `banking.rfm_segment_current_gold` | `gold.rfm_segment_current` | — | important | `limited_risk` | `gold_all_dag` |
| `banking.rfm_segment_gold` | `gold.rfm_segment` | (customer_id, cob_dt) | important | `limited_risk` | `gold_mart360_dag` |

---

## Ràng buộc chi tiết

| Dataset | Cột bắt buộc | Không null | Tối thiểu | Freshness |
|---|---:|---:|---:|---:|
| `banking.core_customer_bronze` | 16 | 3 | 5,000 | 48h |
| `banking.branch_monthly_summary_gold` | 7 | 2 | 100 | 24h |
| `banking.campaign_target_current_gold` | 18 | 4 | 500 | 24h |
| `banking.campaign_target_gold` | 18 | 4 | 500 | 24h |
| `banking.churn_prediction_current_gold` | 10 | 4 | 1,000 | 24h |
| `banking.churn_prediction_gold` | 10 | 4 | 1,000 | 24h |
| `banking.cross_sell_segment_current_gold` | 7 | 4 | 1,000 | 24h |
| `banking.cross_sell_segment_gold` | 7 | 4 | 1,000 | 24h |
| `banking.customer_balance_summary_current_gold` | 7 | 3 | 5,000 | 24h |
| `banking.customer_balance_summary_gold` | 7 | 3 | 5,000 | 24h |
| `banking.customer_card_summary_current_gold` | 8 | 3 | 5,000 | 24h |
| `banking.customer_card_summary_gold` | 12 | 3 | 5,000 | 24h |
| `banking.customer_product_summary_current_gold` | 8 | 3 | 5,000 | 24h |
| `banking.customer_product_summary_gold` | 12 | 3 | 5,000 | 24h |
| `banking.customer_transaction_summary_current_gold` | 6 | 3 | 5,000 | 24h |
| `banking.customer_transaction_summary_gold` | 12 | 3 | 5,000 | 24h |
| `banking.mart_customer_360_current_gold` | 9 | 3 | 5,000 | 24h |
| `banking.mart_customer_360_gold` | 33 | 3 | 5,000 | 24h |
| `banking.rfm_segment_current_gold` | 11 | 4 | 1,000 | 24h |
| `banking.rfm_segment_gold` | 11 | 4 | 1,000 | 24h |
| `banking.core_customer_silver` | 18 | 6 | 5,000 | 24h |
| `banking.dim_account_silver` | 12 | 6 | 5,000 | 24h |
| `banking.dim_branch_silver` | 6 | 4 | 5 | 24h |
| `banking.dim_card_silver` | 8 | 6 | 1,000 | 24h |
| `banking.dim_device_silver` | 6 | 3 | 500 | 24h |
| `banking.dim_employee_silver` | 6 | 4 | 100 | 24h |
| `banking.dim_location_silver` | 6 | 4 | 200 | 24h |
| `banking.dim_product_silver` | 6 | 3 | 10 | 24h |
| `banking.fact_card_txn_silver` | 8 | 5 | 10,000 | 24h |
| `banking.fact_crm_interaction_silver` | 7 | 5 | 5,000 | 24h |
| `banking.fact_online_transaction_silver` | 8 | 6 | 5,000 | 24h |
| `banking.fact_support_ticket_silver` | 8 | 6 | 2,000 | 24h |
| `banking.fact_txn_account_silver` | 9 | 6 | 10,000 | 24h |

> `required_columns` là **tập con có chủ đích**, không phải bản kiểm kê cột.
> Danh sách cột đầy đủ nằm ở [`DATA_DICTIONARY.md`](DATA_DICTIONARY.md),
> sinh từ DDL chứ không từ contract.

