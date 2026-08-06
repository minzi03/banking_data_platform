-- =============================================================================
-- DDL: Ops Metadata (PostgreSQL 15)
-- Schema: opslakehouse
-- Purpose: ETL pipeline metadata — flags, data quality, lineage
-- Note: Schema and tables are created in 00_extensions.sql
--       This file adds any additional ops tables if needed
-- =============================================================================

-- =============================================================================
-- Table: pipeline_run_log — Detailed pipeline execution log
-- =============================================================================
CREATE TABLE IF NOT EXISTS opslakehouse.pipeline_run_log (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(100)    NOT NULL,
    task_id         VARCHAR(200)    NOT NULL,
    status          VARCHAR(20)     NOT NULL,       -- RUNNING / SUCCESS / FAILED / SKIPPED
    cob_dt          DATE            NOT NULL,
    rows_processed  BIGINT,
    execution_time_s NUMERIC(10,2),
    error_message   TEXT,
    started_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMP,
    --
    CONSTRAINT chk_pipeline_status CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED'))
);

CREATE INDEX idx_pipeline_dag_cobdt
    ON opslakehouse.pipeline_run_log (dag_id, cob_dt);

COMMENT ON TABLE opslakehouse.pipeline_run_log IS 'Detailed ETL pipeline execution log for monitoring and debugging';

-- =============================================================================
-- Table: source_table_registry — Registry of all source tables
-- =============================================================================
CREATE TABLE IF NOT EXISTS opslakehouse.source_table_registry (
    id              SERIAL PRIMARY KEY,
    schema_name     VARCHAR(50)     NOT NULL,
    table_name      VARCHAR(100)    NOT NULL,
    source_type     VARCHAR(20)     NOT NULL,       -- postgresql / oracle / mysql
    jdbc_conn_id    VARCHAR(100),                   -- Airflow connection ID
    bronze_table    VARCHAR(200),                   -- Iceberg target: lakehouse.bronze.xxx
    silver_table    VARCHAR(200),                   -- Iceberg target: lakehouse.silver.xxx
    is_active       SMALLINT        NOT NULL DEFAULT 1,
    last_updated    TIMESTAMP       NOT NULL DEFAULT NOW(),
    --
    CONSTRAINT uq_source_table UNIQUE (schema_name, table_name),
    CONSTRAINT chk_source_type CHECK (source_type IN ('postgresql', 'oracle', 'mysql')),
    CONSTRAINT chk_registry_active CHECK (is_active IN (0, 1))
);

COMMENT ON TABLE opslakehouse.source_table_registry IS 'Registry mapping source tables to Iceberg targets — single source of truth for pipeline config';
