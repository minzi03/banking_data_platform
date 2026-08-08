-- =============================================================================
-- DDL: Iceberg Table Optimization Properties
-- Run AFTER initial data load to set performance-tuning properties
-- Catalog: lakehouse
-- =============================================================================

-- =============================================================================
-- BRONZE LAYER - Set write optimization properties
-- =============================================================================

-- Small dimension tables (< 1K rows)
ALTER TABLE lakehouse.bronze.core_branch SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '67108864',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'none'
);

ALTER TABLE lakehouse.bronze.core_product SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '67108864',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'none'
);

ALTER TABLE lakehouse.bronze.core_mcc_code SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '67108864',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'none'
);

-- Medium dimension tables (1K - 100K rows)
ALTER TABLE lakehouse.bronze.core_customer SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.bronze.core_account SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.bronze.core_employee SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.bronze.core_card SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.bronze.core_deposit SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.bronze.core_loan SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.bronze.core_crm_interaction SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.bronze.core_device SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.bronze.core_location SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.bronze.core_support_ticket SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

-- Large fact tables (partitioned by cob_dt)
ALTER TABLE lakehouse.bronze.core_txn_account SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '268435456',
    'read.split.target-size' = '268435456',
    'write.distribution-mode' = 'hash',
    'write.parquet.page-size-bytes' = '2097152',
    'write.parquet.page-row-count-limit' = '30000'
);

ALTER TABLE lakehouse.bronze.core_card_txn SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '268435456',
    'read.split.target-size' = '268435456',
    'write.distribution-mode' = 'hash',
    'write.parquet.page-size-bytes' = '2097152',
    'write.parquet.page-row-count-limit' = '30000'
);

ALTER TABLE lakehouse.bronze.core_online_transaction SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '268435456',
    'read.split.target-size' = '268435456',
    'write.distribution-mode' = 'hash',
    'write.parquet.page-size-bytes' = '2097152',
    'write.parquet.page-row-count-limit' = '30000'
);

-- =============================================================================
-- SILVER LAYER - Set write optimization properties
-- =============================================================================

-- SCD Type 2 dimensions (partitioned by is_current)
ALTER TABLE lakehouse.silver.dim_customer SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.silver.dim_account SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

-- SCD Type 1 dimensions
ALTER TABLE lakehouse.silver.dim_product SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '67108864',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'none'
);

ALTER TABLE lakehouse.silver.dim_branch SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '67108864',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'none'
);

ALTER TABLE lakehouse.silver.dim_card SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.silver.dim_employee SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.silver.dim_device SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.silver.dim_location SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

-- Large fact tables (partitioned by cob_dt)
ALTER TABLE lakehouse.silver.fact_txn_account SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '268435456',
    'read.split.target-size' = '268435456',
    'write.distribution-mode' = 'hash',
    'write.parquet.page-size-bytes' = '2097152',
    'write.parquet.page-row-count-limit' = '30000'
);

ALTER TABLE lakehouse.silver.fact_card_txn SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '268435456',
    'read.split.target-size' = '268435456',
    'write.distribution-mode' = 'hash',
    'write.parquet.page-size-bytes' = '2097152',
    'write.parquet.page-row-count-limit' = '30000'
);

ALTER TABLE lakehouse.silver.fact_online_transaction SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '268435456',
    'read.split.target-size' = '268435456',
    'write.distribution-mode' = 'hash',
    'write.parquet.page-size-bytes' = '2097152',
    'write.parquet.page-row-count-limit' = '30000'
);

ALTER TABLE lakehouse.silver.fact_crm_interaction SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.silver.fact_support_ticket SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

-- =============================================================================
-- GOLD LAYER - Set write optimization properties
-- =============================================================================

-- Mart tables (heavily queried, optimized for reads)
ALTER TABLE lakehouse.gold.mart_customer_360 SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash',
    'write.parquet.page-size-bytes' = '1048576',
    'write.parquet.page-row-count-limit' = '20000'
);

ALTER TABLE lakehouse.gold.rfm_segment SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash',
    'write.parquet.page-size-bytes' = '1048576',
    'write.parquet.page-row-count-limit' = '20000'
);

ALTER TABLE lakehouse.gold.churn_prediction SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash',
    'write.parquet.page-size-bytes' = '1048576',
    'write.parquet.page-row-count-limit' = '20000'
);

ALTER TABLE lakehouse.gold.cross_sell_segment SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash',
    'write.parquet.page-size-bytes' = '1048576',
    'write.parquet.page-row-count-limit' = '20000'
);

ALTER TABLE lakehouse.gold.campaign_target SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash',
    'write.parquet.page-size-bytes' = '1048576',
    'write.parquet.page-row-count-limit' = '20000'
);

-- Summary tables
ALTER TABLE lakehouse.gold.customer_balance_summary SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.gold.customer_transaction_summary SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.gold.customer_product_summary SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.gold.customer_card_summary SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.gold.mart_branch_monthly_summary SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

-- Current/snapshot tables (for time-travel queries)
ALTER TABLE lakehouse.gold.rfm_segment_current SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.gold.churn_prediction_current SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.gold.cross_sell_segment_current SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.gold.campaign_target_current SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.gold.customer_balance_summary_current SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.gold.customer_transaction_summary_current SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.gold.customer_product_summary_current SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

ALTER TABLE lakehouse.gold.customer_card_summary_current SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'write.distribution-mode' = 'hash'
);

-- =============================================================================
-- Z-ORDERING for frequently filtered columns
-- Run OPTIMIZE with ZORDER after data load for better query performance
-- =============================================================================

-- Note: OPTIMIZE is a DML operation, run via Spark SQL
-- Example:
-- OPTIMIZE lakehouse.gold.mart_customer_360 ZORDER BY (customer_id);
-- OPTIMIZE lakehouse.gold.rfm_segment ZORDER BY (rfm_segment, customer_id);
-- OPTIMIZE lakehouse.gold.churn_prediction ZORDER BY (churn_risk, customer_id);

-- =============================================================================
-- END OF OPTIMIZATION SCRIPT
-- =============================================================================
