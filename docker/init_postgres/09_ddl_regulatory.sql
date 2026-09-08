-- =============================================================================
-- Regulatory Compliance Tables
-- Banking Data Platform
-- =============================================================================
-- These tables support regulatory reporting requirements:
--   - BCBS 239 (Basel Committee on Banking Supervision)
--   - SBV/NHNN (State Bank of Vietnam) regulations
--   - Data Quality Scorecards
-- =============================================================================

-- ── Regulatory Report ───────────────────────────────────────────────────────
-- Stores generated regulatory reports
CREATE TABLE IF NOT EXISTS opslakehouse.regulatory_report (
    report_id           BIGINT PRIMARY KEY,
    report_type         VARCHAR(50) NOT NULL,
        -- BCBS239_RISK_DATA      : Risk data aggregation per BCBS 239
        -- BCBS239_RISK_REPORTING : Risk reporting per BCBS 239
        -- SBV_CARD_TRANSACTION   : Card transaction reporting (SBV)
        -- SBV_LOAN portfolio     : Loan portfolio reporting (SBV)
        -- SBV_DEPOSIT_REPORT     : Deposit reporting (SBV)
        -- SBV_FX_TRANSACTION     : Foreign exchange reporting (SBV)
        -- AML_CTR                : Currency Transaction Report
        -- AML_SAR                : Suspicious Activity Report
        -- INTERNAL_MIS           : Internal Management Information System

    report_name         VARCHAR(200) NOT NULL,
    report_period_start DATE NOT NULL,
    report_period_end   DATE NOT NULL,
    generated_date      TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Report content
    data_json           JSONB NOT NULL,  -- Report data in JSON format
    summary_json        JSONB,           -- Summary statistics

    -- Status workflow
    status              VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
        -- DRAFT: Generated, pending review
        -- REVIEWED: Reviewed by compliance officer
        -- APPROVED: Approved for submission
        -- SUBMITTED: Submitted to regulator
        -- REJECTED: Rejected, needs correction

    -- Submission details
    submitted_by        VARCHAR(100),
    submitted_at        TIMESTAMP,
    submission_reference VARCHAR(100),  -- Regulator reference number

    -- Metadata
    created_by          VARCHAR(100) NOT NULL DEFAULT 'SYSTEM',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    notes               TEXT
);

COMMENT ON TABLE opslakehouse.regulatory_report IS 'Regulatory reports for SBV, BCBS 239 compliance';

-- ── Data Quality Scorecard ──────────────────────────────────────────────────
-- Tracks data quality metrics per table/dimension
CREATE TABLE IF NOT EXISTS opslakehouse.dq_scorecard (
    scorecard_id        BIGINT PRIMARY KEY,
    report_date         DATE NOT NULL,

    -- Scope
    table_name          VARCHAR(200) NOT NULL,
    column_name         VARCHAR(200),  -- NULL = table-level score
    dimension           VARCHAR(50) NOT NULL,
        -- COMPLETENESS: % of non-null values
        -- ACCURACY: % of valid values (within expected range)
        -- CONSISTENCY: % of values consistent across related tables
        -- TIMELINESS: % of records within expected freshness
        -- UNIQUENESS: % of unique values where expected
        -- REFERENTIAL: % of FK references valid

    -- Scores (0.00 - 100.00)
    score               NUMERIC(5,2) NOT NULL,
    threshold           NUMERIC(5,2) NOT NULL DEFAULT 95.00,  -- Minimum acceptable score
    is_pass             SMALLINT GENERATED ALWAYS AS (
        CASE WHEN score >= threshold THEN 1 ELSE 0 END
    ) STORED,

    -- Detail metrics
    total_records       BIGINT,
    valid_records       BIGINT,
    invalid_records     BIGINT,
    null_records        BIGINT,
    duplicate_records   BIGINT,

    -- Threshold breach details
    breach_details      JSONB,  -- Specific records/values that failed

    -- Metadata
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    job_run_id          VARCHAR(100)  -- Airflow run ID
);

COMMENT ON TABLE opslakehouse.dq_scorecard IS 'Data quality scorecard for monitoring and compliance';

-- ── Regulatory Rule Configuration ───────────────────────────────────────────
-- Defines regulatory reporting rules and thresholds
CREATE TABLE IF NOT EXISTS opslakehouse.regulatory_rule (
    rule_id             BIGINT PRIMARY KEY,
    rule_code           VARCHAR(50) NOT NULL UNIQUE,
    regulation          VARCHAR(50) NOT NULL,  -- BCBS239, SBV, INTERNAL
    rule_name           VARCHAR(200) NOT NULL,
    description         TEXT,
    threshold_value     NUMERIC(18,2),
    threshold_type      VARCHAR(20),  -- MIN, MAX, PERCENTAGE
    is_active           SMALLINT DEFAULT 1,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE opslakehouse.regulatory_rule IS 'Regulatory reporting rule configuration';

-- ── Data Lineage Audit ──────────────────────────────────────────────────────
-- Tracks data lineage for regulatory audit trail
CREATE TABLE IF NOT EXISTS opslakehouse.data_lineage_audit (
    audit_id            BIGINT PRIMARY KEY,
    source_table        VARCHAR(200) NOT NULL,
    source_column       VARCHAR(200),
    target_table        VARCHAR(200) NOT NULL,
    target_column       VARCHAR(200),
    transformation      TEXT NOT NULL,  -- SQL or description of transformation
    job_name            VARCHAR(100),
    job_run_id          VARCHAR(100),
    executed_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    record_count        BIGINT,
    checksum            VARCHAR(100)  -- Data checksum for integrity verification
);

COMMENT ON TABLE opslakehouse.data_lineage_audit IS 'Data lineage audit trail for regulatory compliance';

-- ── Indexes ────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_regulatory_report_type ON opslakehouse.regulatory_report(report_type);
CREATE INDEX IF NOT EXISTS idx_regulatory_report_period ON opslakehouse.regulatory_report(report_period_end);
CREATE INDEX IF NOT EXISTS idx_regulatory_report_status ON opslakehouse.regulatory_report(status);
CREATE INDEX IF NOT EXISTS idx_dq_scorecard_date ON opslakehouse.dq_scorecard(report_date);
CREATE INDEX IF NOT EXISTS idx_dq_scorecard_table ON opslakehouse.dq_scorecard(table_name);
CREATE INDEX IF NOT EXISTS idx_dq_scorecard_pass ON opslakehouse.dq_scorecard(is_pass);
CREATE INDEX IF NOT EXISTS idx_lineage_audit_source ON opslakehouse.data_lineage_audit(source_table);
CREATE INDEX IF NOT EXISTS idx_lineage_audit_target ON opslakehouse.data_lineage_audit(target_table);

-- ── Sample Regulatory Rules ─────────────────────────────────────────────────
INSERT INTO opslakehouse.regulatory_rule (rule_id, rule_code, regulation, rule_name, description, threshold_value, threshold_type)
VALUES
    (1, 'BCBS239-COMPLETENESS', 'BCBS239', 'Risk Data Completeness', 'All risk data must be 100% complete', 100.00, 'PERCENTAGE'),
    (2, 'BCBS239-TIMELINESS', 'BCBS239', 'Risk Data Timeliness', 'Risk data must be available within T+1', 1.00, 'MAX'),
    (3, 'BCBS239-ACCURACY', 'BCBS239', 'Risk Data Accuracy', 'Risk data accuracy must be >= 99%', 99.00, 'PERCENTAGE'),
    (4, 'SBV-CTR-THRESHOLD', 'SBV', 'CTR Reporting Threshold', 'Cash transactions >= 2B VND require CTR', 2000000000.00, 'MIN'),
    (5, 'SBV-LOAN-NPL', 'SBV', 'NPL Reporting', 'Non-performing loan ratio must be reported quarterly', NULL, NULL),
    (6, 'INTERNAL-DQ-COMPLETENESS', 'INTERNAL', 'DQ Completeness Threshold', 'Data completeness must be >= 95%', 95.00, 'PERCENTAGE'),
    (7, 'INTERNAL-DQ-FRESHNESS', 'INTERNAL', 'DQ Freshness Threshold', 'Data must be refreshed within 24 hours', 24.00, 'MAX')
ON CONFLICT (rule_code) DO NOTHING;
