-- =============================================================================
-- Extensions & Schemas — Banking Data Platform
-- Run first: creates all schemas needed by the platform
-- =============================================================================

-- Enable UUID extension (for future use)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- Source Schemas
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS core_banking;
CREATE SCHEMA IF NOT EXISTS card_crm;
CREATE SCHEMA IF NOT EXISTS digital_banking;

-- =============================================================================
-- Ops Metadata Schema (ETL flags, data quality logs)
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS opslakehouse;

-- =============================================================================
-- Airflow Backend (uses 'public' schema by default)
-- =============================================================================

-- =============================================================================
-- Table: flag_job_etl — ETL Pipeline Control Flags
-- INSERT-only pattern: 1 DAG = 1 pair of flags (R=Running, S=Success)
-- =============================================================================
CREATE TABLE IF NOT EXISTS opslakehouse.flag_job_etl (
    id              SERIAL PRIMARY KEY,
    job_name        VARCHAR(100)   NOT NULL,   -- dag_id
    schema_name     VARCHAR(50)    NOT NULL,   -- bronze / silver / gold / ops
    table_name      VARCHAR(100)   NOT NULL,   -- dag_id (same as job_name)
    status          CHAR(1)        NOT NULL,   -- R = Running, S = Success
    start_time      TIMESTAMP      NULL,       -- filled when status = R
    end_time        TIMESTAMP      NULL,       -- filled when status = S
    cob_dt          DATE           NOT NULL,   -- business date (YYYY-MM-DD)
    created_at      TIMESTAMP      NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT chk_flag_status CHECK (status IN ('R', 'S'))
);

CREATE INDEX IF NOT EXISTS idx_flag_job_cobdt
    ON opslakehouse.flag_job_etl (job_name, cob_dt);

CREATE INDEX IF NOT EXISTS idx_flag_status
    ON opslakehouse.flag_job_etl (status);

COMMENT ON TABLE  opslakehouse.flag_job_etl IS 'ETL pipeline control flags — INSERT-only, no UPDATE';
COMMENT ON COLUMN opslakehouse.flag_job_etl.job_name IS 'Dag_id that owns this flag';
COMMENT ON COLUMN opslakehouse.flag_job_etl.status IS 'R=Running, S=Success';
COMMENT ON COLUMN opslakehouse.flag_job_etl.cob_dt IS 'Business date for this ETL run';

-- =============================================================================
-- Table: data_quality_log — Data Quality Check Results
-- =============================================================================
CREATE TABLE IF NOT EXISTS opslakehouse.data_quality_log (
    id              SERIAL PRIMARY KEY,
    check_name      VARCHAR(200)   NOT NULL,   -- e.g. 'row_count', 'null_check', 'fk_integrity'
    table_name      VARCHAR(200)   NOT NULL,   -- e.g. 'silver.dim_customer'
    check_status    VARCHAR(20)    NOT NULL,   -- PASS / FAIL / WARN
    expected_value  TEXT,                       -- expected result
    actual_value    TEXT,                       -- actual result
    details         TEXT,                       -- error message or details
    cob_dt          DATE           NOT NULL,
    checked_at      TIMESTAMP      NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT chk_dq_status CHECK (check_status IN ('PASS', 'FAIL', 'WARN'))
);

CREATE INDEX IF NOT EXISTS idx_dq_table_cobdt
    ON opslakehouse.data_quality_log (table_name, cob_dt);

COMMENT ON TABLE opslakehouse.data_quality_log IS 'Data quality check results — audit trail for pipeline monitoring';
