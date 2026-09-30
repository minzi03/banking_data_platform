-- =============================================================================
-- Iceberg Namespace Creation — Banking Data Platform
-- Chạy ĐẦU TIÊN bởi init_all.sh, trước mọi DDL bảng. Không dựa vào việc catalog
-- tự tạo namespace khi tạo bảng.
-- =============================================================================

-- Bronze layer — raw data from sources (batch snapshots + CDC change history)
CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;

-- Silver layer — cleaned, SCD-tracked dims, facts, CDC current state
CREATE NAMESPACE IF NOT EXISTS lakehouse.silver;

-- Gold layer — historical analytics marts (Spark)
CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;

-- Serving layer — current-serving tables (dbt via Trino)
CREATE NAMESPACE IF NOT EXISTS lakehouse.serving;

-- Sandbox layer — PII-masked data for non-production teams
CREATE NAMESPACE IF NOT EXISTS lakehouse.sandbox;

-- Quarantine — records flagged by ops/quarantine.py
CREATE NAMESPACE IF NOT EXISTS lakehouse.quarantine;

-- Meta — CDC consolidation watermark
CREATE NAMESPACE IF NOT EXISTS lakehouse.meta;
