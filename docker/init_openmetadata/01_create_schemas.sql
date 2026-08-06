-- =============================================================================
-- OpenMetadata Init Script — Banking Data Platform
-- =============================================================================
-- This script creates additional schemas and initial configuration
-- for OpenMetadata integration.
-- =============================================================================

-- Create schema for OpenMetadata metadata
CREATE SCHEMA IF NOT EXISTS openmetadata;

-- Create schema for lineage tracking
CREATE SCHEMA IF NOT EXISTS opslakehouse;

-- Create lineage_log table (if not exists)
CREATE TABLE IF NOT EXISTS opslakehouse.lineage_log (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    transform_type VARCHAR(100) NOT NULL,
    dag_id VARCHAR(255) NOT NULL,
    dag_run_id VARCHAR(255) NOT NULL,
    snapshot_id VARCHAR(255),
    row_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create audit_log table (if not exists)
CREATE TABLE IF NOT EXISTS opslakehouse.audit_log (
    id SERIAL PRIMARY KEY,
    action VARCHAR(100) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    dag_id VARCHAR(255) NOT NULL,
    dag_run_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    details TEXT,
    row_count INTEGER DEFAULT 0,
    duration_seconds FLOAT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create contract_validation_log table (if not exists)
CREATE TABLE IF NOT EXISTS opslakehouse.contract_validation_log (
    id SERIAL PRIMARY KEY,
    dataset_id VARCHAR(255) NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    check_status VARCHAR(50) NOT NULL,
    expected_value TEXT,
    actual_value TEXT,
    details TEXT,
    cob_dt DATE NOT NULL,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_lineage_log_source ON opslakehouse.lineage_log(source_table);
CREATE INDEX IF NOT EXISTS idx_lineage_log_target ON opslakehouse.lineage_log(target_table);
CREATE INDEX IF NOT EXISTS idx_lineage_log_dag ON opslakehouse.lineage_log(dag_id);

CREATE INDEX IF NOT EXISTS idx_audit_log_table ON opslakehouse.audit_log(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON opslakehouse.audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_log_dag ON opslakehouse.audit_log(dag_id);

CREATE INDEX IF NOT EXISTS idx_contract_validation_dataset ON opslakehouse.contract_validation_log(dataset_id);
CREATE INDEX IF NOT EXISTS idx_contract_validation_cob ON opslakehouse.contract_validation_log(cob_dt);

-- =============================================================================
-- Sample data for testing (optional)
-- =============================================================================

-- Insert sample lineage record
INSERT INTO opslakehouse.lineage_log (source_table, target_table, transform_type, dag_id, dag_run_id, row_count)
VALUES ('lakehouse.bronze.core_customer', 'lakehouse.silver.dim_customer', 'scd2_merge', 'silver_all_dag', 'test_run', 10000)
ON CONFLICT DO NOTHING;

-- Insert sample audit record
INSERT INTO opslakehouse.audit_log (action, table_name, dag_id, dag_run_id, status, details, row_count)
VALUES ('ingest', 'lakehouse.bronze.core_customer', 'bronze_core_banking_dag', 'test_run', 'success', 'Ingested 10000 rows', 10000)
ON CONFLICT DO NOTHING;

-- Insert sample contract validation record
INSERT INTO opslakehouse.contract_validation_log (dataset_id, check_name, check_status, expected_value, actual_value, details, cob_dt)
VALUES ('banking.core_customer_silver', 'row_count', 'PASS', '5000', '10000', 'Row count OK: 10000', CURRENT_DATE)
ON CONFLICT DO NOTHING;
