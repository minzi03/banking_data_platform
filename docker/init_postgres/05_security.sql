-- =============================================================================
-- PostgreSQL Security Configuration — Banking Data Platform
-- =============================================================================
-- Creates dedicated roles for different access levels
-- Enables audit logging
-- =============================================================================

-- =============================================================================
-- 1. CREATE ROLES
-- =============================================================================

-- ETL User: Full read/write access for data pipeline
CREATE ROLE etl_user WITH LOGIN PASSWORD 'ETLPassword123';
GRANT ALL PRIVILEGES ON DATABASE banking_db TO etl_user;
GRANT ALL PRIVILEGES ON SCHEMA core_banking TO etl_user;
GRANT ALL PRIVILEGES ON SCHEMA card_crm TO etl_user;
GRANT ALL PRIVILEGES ON SCHEMA digital_banking TO etl_user;
GRANT ALL PRIVILEGES ON SCHEMA opslakehouse TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA core_banking GRANT ALL ON TABLES TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA card_crm GRANT ALL ON TABLES TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA digital_banking GRANT ALL ON TABLES TO etl_user;

-- Analytics User: Read-only access for reporting
CREATE ROLE analytics_user WITH LOGIN PASSWORD 'AnalyticsPassword123';
GRANT CONNECT ON DATABASE banking_db TO analytics_user;
GRANT USAGE ON SCHEMA core_banking TO analytics_user;
GRANT USAGE ON SCHEMA card_crm TO analytics_user;
GRANT USAGE ON SCHEMA digital_banking TO analytics_user;
GRANT USAGE ON SCHEMA opslakehouse TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA core_banking TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA card_crm TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA digital_banking TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA opslakehouse TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA core_banking GRANT SELECT ON TABLES TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA card_crm GRANT SELECT ON TABLES TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA digital_banking GRANT SELECT ON TABLES TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA opslakehouse GRANT SELECT ON TABLES TO analytics_user;

-- Read-Only User: Limited read access for external tools
CREATE ROLE readonly_user WITH LOGIN PASSWORD 'ReadOnlyPassword123';
GRANT CONNECT ON DATABASE banking_db TO readonly_user;
GRANT USAGE ON SCHEMA opslakehouse TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA opslakehouse TO readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA opslakehouse GRANT SELECT ON TABLES TO readonly_user;

-- CDC User: Replication privileges for Debezium
CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'CDCPassword123';

-- =============================================================================
-- 2. CREATE AUDIT LOG TABLE
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS opslakehouse;

CREATE TABLE IF NOT EXISTS opslakehouse.audit_log (
    id BIGSERIAL PRIMARY KEY,
    action VARCHAR(50) NOT NULL,
    table_name VARCHAR(200) NOT NULL,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    status VARCHAR(20) NOT NULL,
    details TEXT,
    row_count INTEGER DEFAULT 0,
    duration_seconds DECIMAL(10,2),
    error_message TEXT,
    metadata JSONB,
    created_by VARCHAR(100) DEFAULT current_user,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_audit_log_table_name ON opslakehouse.audit_log(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON opslakehouse.audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_log_status ON opslakehouse.audit_log(status);
CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON opslakehouse.audit_log(created_at);

-- Grant permissions on audit_log
GRANT ALL PRIVILEGES ON TABLE opslakehouse.audit_log TO etl_user;
GRANT SELECT ON TABLE opslakehouse.audit_log TO analytics_user;
GRANT SELECT ON TABLE opslakehouse.audit_log TO readonly_user;

-- =============================================================================
-- 3. CREATE DATA LINEAGE TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS opslakehouse.data_lineage (
    id BIGSERIAL PRIMARY KEY,
    source_table VARCHAR(200) NOT NULL,
    target_table VARCHAR(200) NOT NULL,
    transformation_type VARCHAR(50),
    dag_id VARCHAR(100),
    column_mappings JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lineage_source ON opslakehouse.data_lineage(source_table);
CREATE INDEX IF NOT EXISTS idx_lineage_target ON opslakehouse.data_lineage(target_table);

GRANT ALL PRIVILEGES ON TABLE opslakehouse.data_lineage TO etl_user;
GRANT SELECT ON TABLE opslakehouse.data_lineage TO analytics_user;

-- =============================================================================
-- 4. CREATE DATA QUALITY RESULTS TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS opslakehouse.data_quality_results (
    id BIGSERIAL PRIMARY KEY,
    dataset_id VARCHAR(200) NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    expected_value TEXT,
    actual_value TEXT,
    details TEXT,
    severity VARCHAR(20) DEFAULT 'FAIL',
    dag_id VARCHAR(100),
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_dataset ON opslakehouse.data_quality_results(dataset_id);
CREATE INDEX IF NOT EXISTS idx_dq_status ON opslakehouse.data_quality_results(status);

GRANT ALL PRIVILEGES ON TABLE opslakehouse.data_quality_results TO etl_user;
GRANT SELECT ON TABLE opslakehouse.data_quality_results TO analytics_user;

-- =============================================================================
-- 5. CREATE PII ACCESS LOG TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS opslakehouse.pii_access_log (
    id BIGSERIAL PRIMARY KEY,
    user_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(200) NOT NULL,
    column_name VARCHAR(100),
    access_type VARCHAR(20) NOT NULL,
    query_text TEXT,
    accessed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pii_user ON opslakehouse.pii_access_log(user_name);
CREATE INDEX IF NOT EXISTS idx_pii_table ON opslakehouse.pii_access_log(table_name);

GRANT ALL PRIVILEGES ON TABLE opslakehouse.pii_access_log TO etl_user;
GRANT SELECT ON TABLE opslakehouse.pii_access_log TO analytics_user;

-- =============================================================================
-- 6. ENABLE ROW LEVEL SECURITY (Optional)
-- =============================================================================

-- Example: Restrict access based on branch_code
-- ALTER TABLE core_banking.customer ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY customer_branch_policy ON core_banking.customer
--     USING (branch_code = current_setting('app.current_branch'));

-- =============================================================================
-- 7. CREATE VIEWS FOR MASKED DATA
-- =============================================================================

-- View for analytics with masked PII
CREATE OR REPLACE VIEW opslakehouse.v_customer_analytics AS
SELECT
    customer_id,
    CONCAT(LEFT(full_name, 1), '**') AS full_name_masked,
    gender,
    DATE_PART('year', AGE(date_of_birth)) AS age,
    city,
    district,
    branch_code,
    customer_segment,
    kyc_status,
    register_date,
    is_active
FROM core_banking.customer;

GRANT SELECT ON opslakehouse.v_customer_analytics TO analytics_user;
GRANT SELECT ON opslakehouse.v_customer_analytics TO readonly_user;

-- =============================================================================
-- DONE
-- =============================================================================
