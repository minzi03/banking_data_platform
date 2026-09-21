# Lineage — Bản Đồ Phụ Thuộc

> ⚠️ **File này được SINH TỰ ĐỘNG. Đừng sửa tay.**
>
> Sinh bởi `scripts/generate_governance_docs.py` từ `upstream_dataset_ids`
> trong `governance/datasets/*.yaml`.

Đây là lineage **khai báo** — thứ contract nói. Lineage **quan sát được**
(thực sự chạy) nằm ở OpenMetadata. Hai cái lệch nhau là tín hiệu đáng điều tra.

**33 dataset · 45 cạnh phụ thuộc**

---

## Gốc — không có upstream

Dataset nhận dữ liệu từ ngoài đồ thị contract (source system, Kafka).

- `banking.core_customer_bronze` — bronze

---

## Lá — không có consumer

Dataset không dataset nào khác tiêu thụ. **Không phải lỗi mặc định**:
bảng serving là điểm cuối hợp lệ, consumer của chúng (Superset, API)
nằm ngoài đồ thị contract.

Đáng chú ý là các lá **không** phải serving — chúng được xây, được bảo trì,
và chưa có gì dùng tới.

| Dataset | Tầng | Ghi chú |
|---|---|---|
| `banking.branch_monthly_summary_gold` | gold | **đáng xem lại** |
| `banking.dim_device_silver` | silver | **đáng xem lại** |
| `banking.dim_employee_silver` | silver | **đáng xem lại** |
| `banking.dim_location_silver` | silver | **đáng xem lại** |
| `banking.dim_product_silver` | silver | **đáng xem lại** |
| `banking.fact_crm_interaction_silver` | silver | **đáng xem lại** |
| `banking.fact_online_transaction_silver` | silver | **đáng xem lại** |
| `banking.fact_support_ticket_silver` | silver | **đáng xem lại** |
| `banking.campaign_target_current_gold` | gold | serving — lá hợp lệ |
| `banking.churn_prediction_current_gold` | gold | serving — lá hợp lệ |
| `banking.cross_sell_segment_current_gold` | gold | serving — lá hợp lệ |
| `banking.customer_balance_summary_current_gold` | gold | serving — lá hợp lệ |
| `banking.customer_card_summary_current_gold` | gold | serving — lá hợp lệ |
| `banking.customer_product_summary_current_gold` | gold | serving — lá hợp lệ |
| `banking.customer_transaction_summary_current_gold` | gold | serving — lá hợp lệ |
| `banking.mart_customer_360_current_gold` | gold | serving — lá hợp lệ |
| `banking.rfm_segment_current_gold` | gold | serving — lá hợp lệ |

**8 lá đáng xem lại · 9 lá serving hợp lệ.**

---

## Đồ thị đầy đủ

Mỗi dataset kèm upstream (cái nó đọc) và downstream (cái đọc nó).

### bronze

**`banking.core_customer_bronze`** · `bronze.core_customer` · DAG `bronze_core_banking_dag`

- ↑ đọc từ: _(gốc)_
- ↓ được đọc bởi: `banking.dim_account_silver`, `banking.dim_branch_silver`, `banking.dim_card_silver`, `banking.dim_customer_silver`, `banking.dim_device_silver`, `banking.dim_employee_silver`, `banking.dim_location_silver`, `banking.dim_product_silver`, `banking.fact_card_txn_silver`, `banking.fact_crm_interaction_silver`, `banking.fact_online_transaction_silver`, `banking.fact_support_ticket_silver`, `banking.fact_txn_account_silver`

---

### silver

**`banking.dim_account_silver`** · `silver.dim_account` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: `banking.mart_customer_360_gold`

**`banking.dim_branch_silver`** · `silver.dim_branch` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: `banking.branch_monthly_summary_gold`

**`banking.dim_card_silver`** · `silver.dim_card` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: `banking.cross_sell_segment_gold`, `banking.mart_customer_360_gold`

**`banking.dim_customer_silver`** · `silver.dim_customer` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: `banking.churn_prediction_gold`, `banking.cross_sell_segment_gold`, `banking.mart_customer_360_gold`, `banking.rfm_segment_gold`

**`banking.dim_device_silver`** · `silver.dim_device` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: _(không có)_

**`banking.dim_employee_silver`** · `silver.dim_employee` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: _(không có)_

**`banking.dim_location_silver`** · `silver.dim_location` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: _(không có)_

**`banking.dim_product_silver`** · `silver.dim_product` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: _(không có)_

**`banking.fact_card_txn_silver`** · `silver.fact_card_txn` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: `banking.churn_prediction_gold`, `banking.mart_customer_360_gold`, `banking.rfm_segment_gold`

**`banking.fact_crm_interaction_silver`** · `silver.fact_crm_interaction` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: _(không có)_

**`banking.fact_online_transaction_silver`** · `silver.fact_online_transaction` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: _(không có)_

**`banking.fact_support_ticket_silver`** · `silver.fact_support_ticket` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: _(không có)_

**`banking.fact_txn_account_silver`** · `silver.fact_txn_account` · DAG `silver_all_dag`

- ↑ đọc từ: `banking.core_customer_bronze`
- ↓ được đọc bởi: `banking.branch_monthly_summary_gold`, `banking.churn_prediction_gold`, `banking.mart_customer_360_gold`, `banking.rfm_segment_gold`

---

### gold

**`banking.branch_monthly_summary_gold`** · `gold.branch_monthly_summary` · DAG `gold_mart360_dag`

- ↑ đọc từ: `banking.dim_branch_silver`, `banking.fact_txn_account_silver`
- ↓ được đọc bởi: _(không có)_

**`banking.campaign_target_current_gold`** · `gold.campaign_target_current` · DAG `gold_all_dag`

- ↑ đọc từ: `banking.campaign_target_gold`
- ↓ được đọc bởi: _(không có)_

**`banking.campaign_target_gold`** · `gold.campaign_target` · DAG `gold_mart360_dag`

- ↑ đọc từ: `banking.rfm_segment_gold`, `banking.churn_prediction_gold`, `banking.cross_sell_segment_gold`, `banking.mart_customer_360_gold`
- ↓ được đọc bởi: `banking.campaign_target_current_gold`

**`banking.churn_prediction_current_gold`** · `gold.churn_prediction_current` · DAG `gold_all_dag`

- ↑ đọc từ: `banking.churn_prediction_gold`
- ↓ được đọc bởi: _(không có)_

**`banking.churn_prediction_gold`** · `gold.churn_prediction` · DAG `gold_mart360_dag`

- ↑ đọc từ: `banking.dim_customer_silver`, `banking.fact_txn_account_silver`, `banking.fact_card_txn_silver`
- ↓ được đọc bởi: `banking.campaign_target_gold`, `banking.churn_prediction_current_gold`

**`banking.cross_sell_segment_current_gold`** · `gold.cross_sell_segment_current` · DAG `gold_all_dag`

- ↑ đọc từ: `banking.cross_sell_segment_gold`
- ↓ được đọc bởi: _(không có)_

**`banking.cross_sell_segment_gold`** · `gold.cross_sell_segment` · DAG `gold_mart360_dag`

- ↑ đọc từ: `banking.dim_customer_silver`, `banking.dim_card_silver`
- ↓ được đọc bởi: `banking.campaign_target_gold`, `banking.cross_sell_segment_current_gold`

**`banking.customer_balance_summary_current_gold`** · `gold.customer_balance_summary_current` · DAG `gold_all_dag`

- ↑ đọc từ: `banking.customer_balance_summary_gold`
- ↓ được đọc bởi: _(không có)_

**`banking.customer_balance_summary_gold`** · `gold.customer_balance_summary` · DAG `gold_mart360_dag`

- ↑ đọc từ: `banking.mart_customer_360_gold`
- ↓ được đọc bởi: `banking.customer_balance_summary_current_gold`

**`banking.customer_card_summary_current_gold`** · `gold.customer_card_summary_current` · DAG `gold_all_dag`

- ↑ đọc từ: `banking.customer_card_summary_gold`
- ↓ được đọc bởi: _(không có)_

**`banking.customer_card_summary_gold`** · `gold.customer_card_summary` · DAG `gold_mart360_dag`

- ↑ đọc từ: `banking.mart_customer_360_gold`
- ↓ được đọc bởi: `banking.customer_card_summary_current_gold`

**`banking.customer_product_summary_current_gold`** · `gold.customer_product_summary_current` · DAG `gold_all_dag`

- ↑ đọc từ: `banking.customer_product_summary_gold`
- ↓ được đọc bởi: _(không có)_

**`banking.customer_product_summary_gold`** · `gold.customer_product_summary` · DAG `gold_mart360_dag`

- ↑ đọc từ: `banking.mart_customer_360_gold`
- ↓ được đọc bởi: `banking.customer_product_summary_current_gold`

**`banking.customer_transaction_summary_current_gold`** · `gold.customer_transaction_summary_current` · DAG `gold_all_dag`

- ↑ đọc từ: `banking.customer_transaction_summary_gold`
- ↓ được đọc bởi: _(không có)_

**`banking.customer_transaction_summary_gold`** · `gold.customer_transaction_summary` · DAG `gold_mart360_dag`

- ↑ đọc từ: `banking.mart_customer_360_gold`
- ↓ được đọc bởi: `banking.customer_transaction_summary_current_gold`

**`banking.mart_customer_360_current_gold`** · `gold.mart_customer_360_current` · DAG `gold_all_dag`

- ↑ đọc từ: `banking.mart_customer_360_gold`
- ↓ được đọc bởi: _(không có)_

**`banking.mart_customer_360_gold`** · `gold.mart_customer_360` · DAG `gold_mart360_dag`

- ↑ đọc từ: `banking.dim_customer_silver`, `banking.dim_account_silver`, `banking.dim_card_silver`, `banking.fact_txn_account_silver`, `banking.fact_card_txn_silver`
- ↓ được đọc bởi: `banking.campaign_target_gold`, `banking.customer_balance_summary_gold`, `banking.customer_card_summary_gold`, `banking.customer_product_summary_gold`, `banking.customer_transaction_summary_gold`, `banking.mart_customer_360_current_gold`

**`banking.rfm_segment_current_gold`** · `gold.rfm_segment_current` · DAG `gold_all_dag`

- ↑ đọc từ: `banking.rfm_segment_gold`
- ↓ được đọc bởi: _(không có)_

**`banking.rfm_segment_gold`** · `gold.rfm_segment` · DAG `gold_mart360_dag`

- ↑ đọc từ: `banking.dim_customer_silver`, `banking.fact_txn_account_silver`, `banking.fact_card_txn_silver`
- ↓ được đọc bởi: `banking.campaign_target_gold`, `banking.rfm_segment_current_gold`

---

## Giới hạn

- Đây là lineage **cấp dataset**, không phải cấp cột. Không trả lời được
  *"cột này bắt nguồn từ đâu"*.
- Chỉ phản ánh **khai báo trong contract**. Một job đọc bảng mà không khai
  báo sẽ không xuất hiện ở đây. Ràng buộc khai báo ↔ SQL do
  `tests/governance/test_declared_sources_match_sql.py` giữ, nhưng nó kiểm
  YAML của job chứ không kiểm contract.
- Không có dataset nào của tầng CDC trong đồ thị này.

