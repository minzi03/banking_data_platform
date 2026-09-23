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

-- =============================================================================
-- Table: contract_validation_log — Data Contract Check Results
-- =============================================================================
-- Ghi bởi code_etl/shared/ops/contract_validation.py (ops_contract_validation_dag).
-- Bảng này trước đây chỉ được khai trong docker/init_openmetadata/, thư mục
-- không được mount vào đâu (đã xoá 2026-09-24) — nên nó chưa từng tồn tại.
CREATE TABLE IF NOT EXISTS opslakehouse.contract_validation_log (
    id              SERIAL PRIMARY KEY,
    dataset_id      VARCHAR(255)   NOT NULL,   -- e.g. 'banking.dim_customer_silver'
    check_name      VARCHAR(100)   NOT NULL,   -- e.g. 'required_columns', 'unique_check'
    check_status    VARCHAR(20)    NOT NULL,   -- PASS / FAIL / WARN
    expected_value  TEXT,
    actual_value    TEXT,
    details         TEXT,
    cob_dt          DATE           NOT NULL,
    checked_at      TIMESTAMP      NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT chk_contract_status CHECK (check_status IN ('PASS', 'FAIL', 'WARN'))
);

CREATE INDEX IF NOT EXISTS idx_contract_validation_dataset_cobdt
    ON opslakehouse.contract_validation_log (dataset_id, cob_dt);

COMMENT ON TABLE opslakehouse.contract_validation_log IS 'Data contract check results per dataset and cob_dt';

-- =============================================================================
-- Table: lineage_log — Table-level lineage edges
-- =============================================================================
-- Cột đúng với những gì governance/lineage.py (LineageTracker.write_to_postgres)
-- ghi. Giống contract_validation_log, bảng này trước đây chỉ được khai trong
-- docker/init_openmetadata/ — không ai chạy — nên chưa từng tồn tại (TD-13).
--
-- Writer: ops_lineage_dag ghi các cạnh job Silver/Gold khai trong YAML
-- (governance.lineage.declared_edges). DAG không có lịch — bảng rỗng cho tới khi
-- có người chạy nó. Xem TD-13.
CREATE TABLE IF NOT EXISTS opslakehouse.lineage_log (
    id              SERIAL PRIMARY KEY,
    source_table    VARCHAR(255)   NOT NULL,   -- e.g. 'lakehouse.silver.dim_customer'
    target_table    VARCHAR(255)   NOT NULL,   -- e.g. 'lakehouse.gold.mart_customer_360'
    transform_type  VARCHAR(100)   NOT NULL,   -- governance.lineage.TransformType
    dag_id          VARCHAR(255)   NOT NULL,
    dag_run_id      VARCHAR(255)   NOT NULL,
    snapshot_id     VARCHAR(255),
    row_count       INTEGER        DEFAULT 0,
    created_at      TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lineage_log_source ON opslakehouse.lineage_log (source_table);
CREATE INDEX IF NOT EXISTS idx_lineage_log_target ON opslakehouse.lineage_log (target_table);
CREATE INDEX IF NOT EXISTS idx_lineage_log_dag    ON opslakehouse.lineage_log (dag_id);

COMMENT ON TABLE opslakehouse.lineage_log IS 'Table-level lineage edges declared by Silver/Gold jobs, written by ops_lineage_dag';
