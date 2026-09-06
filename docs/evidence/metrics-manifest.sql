-- =============================================================================
-- Metrics Manifest — Verification Queries (Trino)
-- =============================================================================
-- Mỗi query có một --@id khớp với đường dẫn trong metrics-manifest.yaml.
-- generate_metrics_manifest.py parse file này theo marker --@id, chạy từng
-- query với tham số :cob_dt, rồi ghi kết quả vào đúng node của manifest.
--
-- Quy ước:
--   :cob_dt   — snapshot đang đo, bắt buộc, không có default
--   :business_tz — business timezone (Asia/Ho_Chi_Minh). KHÔNG phải
--               workaround để bắt chước session timezone của Spark: sau
--               timezone migration, Spark và Trino ĐỘC LẬP implement cùng
--               một business-time contract. Cả hai chạy session UTC và cùng
--               derive ngày nghiệp vụ tường minh.
--   :catalog  — tên catalog của ENGINE ĐANG CHẠY. Spark gọi warehouse này là
--               `lakehouse`, Trino gọi là `iceberg` (tên file
--               init_trino/catalog/iceberg.properties quyết định). Bundle này
--               chạy trên Trino nên generator thay bằng environment.trino_catalog.
--               KHÔNG hard-code `lakehouse.` ở đây — sẽ fail
--               "Catalog 'lakehouse' not found".
--   Mọi query đọc fact PHẢI pin cob_dt (Rule A). Không có ngoại lệ ở đây vì
--   mọi metric trong manifest đều là metric của MỘT snapshot.
-- =============================================================================


-- =============================================================================
-- 0. SNAPSHOT ALIGNMENT
-- =============================================================================

--@id snapshot.bronze_max_cob_dt
SELECT CAST(MAX(cob_dt) AS VARCHAR) AS value
FROM :catalog.bronze.core_txn_account;

--@id snapshot.silver_max_cob_dt
SELECT CAST(MAX(cob_dt) AS VARCHAR) AS value
FROM :catalog.silver.fact_txn_account;

--@id snapshot.gold_max_cob_dt
SELECT CAST(MAX(cob_dt) AS VARCHAR) AS value
FROM :catalog.gold.mart_customer_360;

-- Guard: partition yêu cầu phải tồn tại ở CẢ BA layer, nếu không mọi metric
-- phía sau là metric của một ngày khác. Đo riêng từng layer để khi lệch còn
-- biết layer nào chưa rebuild.
--@id snapshot.bronze_partition_exists
SELECT COUNT(*) > 0 AS value
FROM :catalog.bronze.core_txn_account
WHERE cob_dt = DATE ':cob_dt';

--@id snapshot.silver_partition_exists
SELECT COUNT(*) > 0 AS value
FROM :catalog.silver.fact_txn_account
WHERE cob_dt = DATE ':cob_dt';

--@id snapshot.gold_partition_exists
SELECT COUNT(*) > 0 AS value
FROM :catalog.gold.mart_customer_360
WHERE cob_dt = DATE ':cob_dt';


-- =============================================================================
-- 1. BRONZE — rows + distinct business key LUÔN đi cặp
-- =============================================================================

--@id bronze.snapshot_rows.core_txn_account
SELECT
    COUNT(*)                  AS rows,
    COUNT(DISTINCT txn_id)    AS distinct_txn_id
FROM :catalog.bronze.core_txn_account
WHERE cob_dt = DATE ':cob_dt';

--@id bronze.snapshot_rows.core_card_txn
SELECT
    COUNT(*)                  AS rows,
    COUNT(DISTINCT txn_id)    AS distinct_txn_id
FROM :catalog.bronze.core_card_txn
WHERE cob_dt = DATE ':cob_dt';

--@id bronze.snapshot_rows.core_online_transaction
SELECT
    COUNT(*)                          AS rows,
    COUNT(DISTINCT transaction_id)    AS distinct_transaction_id
FROM :catalog.bronze.core_online_transaction
WHERE cob_dt = DATE ':cob_dt';

--@id bronze.snapshot_rows.core_customer
SELECT
    COUNT(*)                       AS rows,
    COUNT(DISTINCT customer_id)    AS distinct_customer_id
FROM :catalog.bronze.core_customer
WHERE cob_dt = DATE ':cob_dt';

--@id bronze.snapshot_rows.core_account
SELECT
    COUNT(*)                      AS rows,
    COUNT(DISTINCT account_id)    AS distinct_account_id
FROM :catalog.bronze.core_account
WHERE cob_dt = DATE ':cob_dt';

--@id bronze.partitions_present
-- Số partition đang tồn tại. Đây là con số giải thích vì sao COUNT(*) toàn bảng
-- từng ra 4.6M trong khi chỉ có ~2.3M giao dịch thật.
SELECT COUNT(DISTINCT cob_dt) AS value
FROM :catalog.bronze.core_txn_account;


-- =============================================================================
-- 2. SILVER — fact snapshot
-- =============================================================================

--@id silver.snapshot_rows.fact_txn_account
SELECT COUNT(*) AS rows, COUNT(DISTINCT txn_id) AS distinct_txn_id
FROM :catalog.silver.fact_txn_account
WHERE cob_dt = DATE ':cob_dt';

--@id silver.snapshot_rows.fact_card_txn
SELECT COUNT(*) AS rows, COUNT(DISTINCT txn_id) AS distinct_txn_id
FROM :catalog.silver.fact_card_txn
WHERE cob_dt = DATE ':cob_dt';

--@id silver.snapshot_rows.fact_online_transaction
SELECT COUNT(*) AS rows, COUNT(DISTINCT transaction_id) AS distinct_transaction_id
FROM :catalog.silver.fact_online_transaction
WHERE cob_dt = DATE ':cob_dt';

--@id silver.snapshot_rows.fact_crm_interaction
SELECT COUNT(*) AS rows, COUNT(DISTINCT interaction_id) AS distinct_interaction_id
FROM :catalog.silver.fact_crm_interaction
WHERE cob_dt = DATE ':cob_dt';

--@id silver.snapshot_rows.fact_support_ticket
SELECT COUNT(*) AS rows, COUNT(DISTINCT ticket_id) AS distinct_ticket_id
FROM :catalog.silver.fact_support_ticket
WHERE cob_dt = DATE ':cob_dt';


-- =============================================================================
-- 3. SCD2 SANITY — evidence cho claim "SCD Type 2 implementation"
-- =============================================================================

--@id silver.scd2.dim_customer
SELECT
    COUNT(*)                                                       AS total_rows,
    COUNT(DISTINCT customer_id)                                    AS distinct_business_keys,
    COUNT_IF(is_current = 1)                                       AS current_rows,
    COUNT(DISTINCT CASE WHEN is_current = 1 THEN customer_id END)  AS current_distinct_business_keys,
    COUNT_IF(is_current = 1)
      - COUNT(DISTINCT CASE WHEN is_current = 1 THEN customer_id END) AS duplicate_current_keys,
    COUNT_IF(effective_to = DATE '9999-12-31')                     AS open_ended_rows
FROM :catalog.silver.dim_customer;

--@id silver.scd2.dim_account
SELECT
    COUNT(*)                                                      AS total_rows,
    COUNT(DISTINCT account_id)                                    AS distinct_business_keys,
    COUNT_IF(is_current = 1)                                      AS current_rows,
    COUNT(DISTINCT CASE WHEN is_current = 1 THEN account_id END)  AS current_distinct_business_keys,
    COUNT_IF(is_current = 1)
      - COUNT(DISTINCT CASE WHEN is_current = 1 THEN account_id END) AS duplicate_current_keys,
    COUNT_IF(effective_to = DATE '9999-12-31')                    AS open_ended_rows
FROM :catalog.silver.dim_account;

--@id silver.scd2.dim_customer.overlapping_intervals
-- Hai version của cùng business key không được chồng khoảng thời gian.
-- Đây là invariant mà chỉ nhìn is_current sẽ không phát hiện được.
SELECT COUNT(*) AS value
FROM :catalog.silver.dim_customer a
JOIN :catalog.silver.dim_customer b
  ON a.customer_id = b.customer_id
 AND a.customer_sk <> b.customer_sk
 AND a.effective_from <= b.effective_to
 AND b.effective_from <= a.effective_to;

--@id silver.scd2.dim_account.overlapping_intervals
SELECT COUNT(*) AS value
FROM :catalog.silver.dim_account a
JOIN :catalog.silver.dim_account b
  ON a.account_id = b.account_id
 AND a.account_sk <> b.account_sk
 AND a.effective_from <= b.effective_to
 AND b.effective_from <= a.effective_to;


-- =============================================================================
-- 4. TRANSACTION SCALE — con số thay thế "4.6M+"
-- =============================================================================

--@id transaction_scale.curated_financial_transactions
-- Uniqueness là (domain, transaction_id). UNION ALL chứ KHÔNG phải UNION:
-- txn_id=123 ở account và txn_id=123 ở card là hai giao dịch khác nhau, UNION
-- trên id trần sẽ nuốt mất một cái.
SELECT
    COUNT(*)                              AS value,
    COUNT_IF(domain = 'account')          AS account_distinct,
    COUNT_IF(domain = 'card')             AS card_distinct,
    COUNT_IF(domain = 'online')           AS online_distinct
FROM (
    SELECT DISTINCT 'account' AS domain, CAST(txn_id AS VARCHAR) AS txn_key
    FROM :catalog.silver.fact_txn_account
    WHERE cob_dt = DATE ':cob_dt'
    UNION ALL
    SELECT DISTINCT 'card', CAST(txn_id AS VARCHAR)
    FROM :catalog.silver.fact_card_txn
    WHERE cob_dt = DATE ':cob_dt'
    UNION ALL
    SELECT DISTINCT 'online', CAST(transaction_id AS VARCHAR)
    FROM :catalog.silver.fact_online_transaction
    WHERE cob_dt = DATE ':cob_dt'
) t;


-- =============================================================================
-- 5. GOLD — grain checks
-- =============================================================================
-- Template áp cho từng bảng grain customer. Script sinh ra 9 biến thể từ
-- gold.grain_checks trong manifest; giữ một bản mẫu ở đây để review được.

--@id gold.grain_checks.TEMPLATE
SELECT
    COUNT(*)                                          AS rows,
    COUNT(DISTINCT customer_id)                       AS distinct_customer_id,
    COUNT(*) - COUNT(DISTINCT customer_id)            AS duplicate_customer_ids
FROM :catalog.gold.{table}
WHERE cob_dt = DATE ':cob_dt';

--@id gold.legacy_gold_current_objects
-- Đếm object legacy CÒN TỒN TẠI qua information_schema, KHÔNG phải
-- COUNT(*) trên từng bảng.
--
-- Bản trước liệt kê 8 bảng rồi COUNT(*) từng cái. Cách đó chỉ chạy được trong
-- giai đoạn chuyển tiếp: sau khi retire, chính các bảng đó biến mất nên query
-- ném TABLE_NOT_FOUND — metric "đã retire chưa" lại chết đúng lúc câu trả lời
-- là "rồi". information_schema trả 0 một cách tự nhiên.
SELECT COUNT(*) AS value
FROM :catalog.information_schema.tables
WHERE table_schema = 'gold' AND table_name LIKE '%current';


--@id serving.current_snapshot_alignment
-- Thay thế đúng bản chất cho stale check cũ: serving object phải TỒN TẠI,
-- có grain hợp lệ, không rỗng, và phục vụ ĐÚNG snapshot đang verify.
-- Trả về số object VI PHẠM (kỳ vọng 0).
SELECT COUNT(*) AS value
FROM (
    SELECT 'rfm_segment_current' AS t, COUNT(*) AS n, MAX(CAST(cob_dt AS VARCHAR)) AS cob, COUNT(*) - COUNT(DISTINCT customer_id) AS dup, COUNT(DISTINCT cob_dt) AS parts FROM :catalog.serving.rfm_segment_current
    UNION ALL SELECT 'churn_prediction_current' AS t, COUNT(*) AS n, MAX(CAST(cob_dt AS VARCHAR)) AS cob, COUNT(*) - COUNT(DISTINCT customer_id) AS dup, COUNT(DISTINCT cob_dt) AS parts FROM :catalog.serving.churn_prediction_current
    UNION ALL SELECT 'cross_sell_segment_current' AS t, COUNT(*) AS n, MAX(CAST(cob_dt AS VARCHAR)) AS cob, COUNT(*) - COUNT(DISTINCT customer_id) AS dup, COUNT(DISTINCT cob_dt) AS parts FROM :catalog.serving.cross_sell_segment_current
    UNION ALL SELECT 'campaign_target_current' AS t, COUNT(*) AS n, MAX(CAST(cob_dt AS VARCHAR)) AS cob, COUNT(*) - COUNT(DISTINCT customer_id) AS dup, COUNT(DISTINCT cob_dt) AS parts FROM :catalog.serving.campaign_target_current
    UNION ALL SELECT 'customer_balance_summary_current' AS t, COUNT(*) AS n, MAX(CAST(cob_dt AS VARCHAR)) AS cob, COUNT(*) - COUNT(DISTINCT customer_id) AS dup, COUNT(DISTINCT cob_dt) AS parts FROM :catalog.serving.customer_balance_summary_current
    UNION ALL SELECT 'customer_transaction_summary_current' AS t, COUNT(*) AS n, MAX(CAST(cob_dt AS VARCHAR)) AS cob, COUNT(*) - COUNT(DISTINCT customer_id) AS dup, COUNT(DISTINCT cob_dt) AS parts FROM :catalog.serving.customer_transaction_summary_current
    UNION ALL SELECT 'customer_product_summary_current' AS t, COUNT(*) AS n, MAX(CAST(cob_dt AS VARCHAR)) AS cob, COUNT(*) - COUNT(DISTINCT customer_id) AS dup, COUNT(DISTINCT cob_dt) AS parts FROM :catalog.serving.customer_product_summary_current
    UNION ALL SELECT 'customer_card_summary_current' AS t, COUNT(*) AS n, MAX(CAST(cob_dt AS VARCHAR)) AS cob, COUNT(*) - COUNT(DISTINCT customer_id) AS dup, COUNT(DISTINCT cob_dt) AS parts FROM :catalog.serving.customer_card_summary_current
    UNION ALL SELECT 'mart_customer_360_current' AS t, COUNT(*) AS n, MAX(CAST(cob_dt AS VARCHAR)) AS cob, COUNT(*) - COUNT(DISTINCT customer_id) AS dup, COUNT(DISTINCT cob_dt) AS parts FROM :catalog.serving.mart_customer_360_current
) x
WHERE n = 0 OR dup <> 0 OR parts <> 1 OR cob <> ':cob_dt';


--@id serving.objects_present
SELECT COUNT(*) AS value
FROM :catalog.information_schema.tables
WHERE table_schema = 'serving';


--@id serving.trino.visible_gold_objects
-- Trino là serving engine cho dbt/Superset/query ví dụ trong README.
-- Object nào Spark tạo được mà Trino không thấy thì KHÔNG phục vụ được.
-- So với serving.gold_objects_declared (đếm từ DDL) để lộ chênh lệch.
SELECT COUNT(*) AS value
FROM :catalog.information_schema.tables
WHERE table_schema = 'gold';


--@id serving.trino.mart_customer_360_current_visible
-- Object cụ thể đã biết là thiếu. Giữ query riêng để finding có tên, không chỉ
-- là một con số chênh lệch.
SELECT COUNT(*) AS value
FROM :catalog.information_schema.tables
WHERE table_schema = 'gold' AND table_name = 'mart_customer_360_current';


-- =============================================================================
-- 6. RECONCILIATION — invariant chống fan-out tái phát trên dữ liệu thật
-- =============================================================================

--@id gold.reconciliation.churn_vs_transaction_summary_30d
SELECT
    COUNT(*)                                                   AS compared_customers,
    COUNT_IF(c.txn_amt_30d <> s.total_txn_amount_30d)          AS mismatched_customers,
    COALESCE(MAX(ABS(c.txn_amt_30d - s.total_txn_amount_30d)), 0) AS max_amount_difference
FROM :catalog.gold.churn_prediction c
JOIN :catalog.gold.customer_transaction_summary s
  ON c.customer_id = s.customer_id
 AND c.cob_dt = s.cob_dt
WHERE c.cob_dt = DATE ':cob_dt';

--@id gold.reconciliation.churn_count_vs_transaction_summary_30d
SELECT
    COUNT(*)                                             AS compared_customers,
    COUNT_IF(c.txn_cnt_30d <> s.total_txn_count_30d)     AS mismatched_customers
FROM :catalog.gold.churn_prediction c
JOIN :catalog.gold.customer_transaction_summary s
  ON c.customer_id = s.customer_id
 AND c.cob_dt = s.cob_dt
WHERE c.cob_dt = DATE ':cob_dt';

--@id gold.reconciliation.rfm_monetary_recompute_90d
-- KHÔNG so RFM với customer_transaction_summary (30d vs 90d — semantics khác).
-- Thay vào đó tính lại monetary 90d thẳng từ Silver bằng đúng grain rule của
-- bản fix P0, rồi đối chiếu với giá trị Gold đã ghi.
WITH acct AS (
    SELECT customer_id, COALESCE(SUM(ABS(txn_amount)), 0) AS amt
    FROM :catalog.silver.fact_txn_account
    WHERE cob_dt = DATE ':cob_dt'
      AND CAST(txn_date AT TIME ZONE ':business_tz' AS DATE) >= DATE ':cob_dt' - INTERVAL '90' DAY
      AND CAST(txn_date AT TIME ZONE ':business_tz' AS DATE) <= DATE ':cob_dt'
    GROUP BY customer_id
),
card AS (
    SELECT customer_id,
           COALESCE(SUM(CASE WHEN txn_type NOT IN ('REFUND','REVERSAL') THEN txn_amount ELSE 0 END), 0) AS amt
    FROM :catalog.silver.fact_card_txn
    WHERE cob_dt = DATE ':cob_dt'
      AND status = 'SUCCESS'
      AND CAST(txn_date AT TIME ZONE ':business_tz' AS DATE) >= DATE ':cob_dt' - INTERVAL '90' DAY
      AND CAST(txn_date AT TIME ZONE ':business_tz' AS DATE) <= DATE ':cob_dt'
    GROUP BY customer_id
),
expected AS (
    SELECT c.customer_id,
           ROUND(COALESCE(a.amt, 0) + COALESCE(k.amt, 0), 2) AS expected_monetary
    FROM (SELECT customer_id FROM :catalog.silver.dim_customer WHERE is_current = 1) c
    LEFT JOIN acct a ON c.customer_id = a.customer_id
    LEFT JOIN card k ON c.customer_id = k.customer_id
)
SELECT
    COUNT(*)                                        AS compared_customers,
    COUNT_IF(r.monetary <> e.expected_monetary)     AS mismatched_customers
FROM :catalog.gold.rfm_segment r
JOIN expected e ON r.customer_id = e.customer_id
WHERE r.cob_dt = DATE ':cob_dt';


-- =============================================================================
-- 7. CDC CURRENT-STATE
-- =============================================================================

--@id cdc.current_state.dim_customer_current
SELECT
    COUNT(*)                                       AS rows,
    COUNT(DISTINCT customer_id)                    AS distinct_customer_id,
    COUNT(*) - COUNT(DISTINCT customer_id)         AS duplicate_keys
FROM :catalog.silver.dim_customer_current;

--@id cdc.current_state.dim_account_current
SELECT
    COUNT(*)                                      AS rows,
    COUNT(DISTINCT account_id)                    AS distinct_account_id,
    COUNT(*) - COUNT(DISTINCT account_id)         AS duplicate_keys
FROM :catalog.silver.dim_account_current;


-- =============================================================================
-- 8. ENVIRONMENT
-- =============================================================================

--@id environment.trino_version
SELECT version() AS value;


--@id platform.business_vs_utc_date_rows
-- OBSERVATION (không phải lỗi): số dòng mà ngày nghiệp vụ ICT KHÁC ngày UTC.
--
-- Trước migration đây là metric "hai engine bất đồng" và là WARN. Sau migration
-- nó đổi nghĩa hoàn toàn: cả hai engine chạy session UTC và cùng derive ngày
-- nghiệp vụ tường minh, nên con số này chỉ nói "bao nhiêu dòng thực sự rơi vào
-- ngày lịch khác giữa ICT và UTC" — hệ quả tất yếu của UTC+7, KHÔNG phải bug.
-- Kỳ vọng ~29% (7/24). Nếu nó bằng 0 thì mới đáng ngờ: nghĩa là conversion
-- không có tác dụng.
SELECT
    COUNT(*)                                                      AS total_rows,
    COUNT_IF(CAST(txn_date AS DATE)
             <> CAST(txn_date AT TIME ZONE ':business_tz' AS DATE)) AS divergent_date_rows
FROM :catalog.silver.fact_txn_account
WHERE cob_dt = DATE ':cob_dt';


--@id gold.reconciliation.branch_monthly_recompute
-- Cross-engine reconciliation cho CALENDAR bucket (không phải rolling window).
--
-- branch_monthly_summary là model đã chứng minh timezone bug không chỉ ảnh
-- hưởng cửa sổ 30/90 ngày mà cả bucket tháng dương lịch. Query này recompute
-- năm/tháng nghiệp vụ TRONG TRINO bằng đúng business-time contract, rồi đối
-- chiếu với giá trị Spark đã ghi vào Gold. Bằng 0 nghĩa là hai engine ĐỘC LẬP
-- implement cùng một contract và ra cùng kết quả.
WITH expected AS (
    SELECT
        da.branch_code,
        YEAR(CAST(t.txn_date AT TIME ZONE ':business_tz' AS DATE))  AS txn_year,
        MONTH(CAST(t.txn_date AT TIME ZONE ':business_tz' AS DATE)) AS txn_month,
        COUNT(t.txn_id)                                             AS txn_count,
        ROUND(COALESCE(SUM(ABS(t.txn_amount)), 0), 2)               AS total_txn_amount
    FROM :catalog.silver.fact_txn_account t
    JOIN :catalog.silver.dim_account da
      ON t.account_id = da.account_id AND da.is_current = 1
    WHERE t.cob_dt = DATE ':cob_dt'
    GROUP BY da.branch_code,
             YEAR(CAST(t.txn_date AT TIME ZONE ':business_tz' AS DATE)),
             MONTH(CAST(t.txn_date AT TIME ZONE ':business_tz' AS DATE))
)
SELECT
    COUNT(*)                                                       AS compared_buckets,
    COUNT_IF(g.txn_count <> e.txn_count
             OR g.total_txn_amount <> e.total_txn_amount)          AS mismatched_buckets
FROM :catalog.gold.mart_branch_monthly_summary g
JOIN expected e
  ON g.branch_code = e.branch_code
 AND g.txn_year = e.txn_year
 AND g.txn_month = e.txn_month
WHERE g.cob_dt = DATE ':cob_dt';


