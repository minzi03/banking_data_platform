-- =============================================================================
-- Iceberg Schema Creation — Banking Data Platform
-- Run via Spark SQL after Iceberg REST catalog is ready
-- This is a reference script; schemas are auto-created by the first write.
-- =============================================================================

-- Bronze layer — raw data from sources
CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;

-- Silver layer — cleaned, deduplicated, SCD-tracked
CREATE SCHEMA IF NOT EXISTS lakehouse.silver;

-- Gold layer — analytics-ready marts
CREATE SCHEMA IF NOT EXISTS lakehouse.gold;

-- Sandbox layer — PII-masked data for non-production teams
CREATE SCHEMA IF NOT EXISTS lakehouse.sandbox;

-- Staging layer — temporary data for CDC and intermediate processing
CREATE SCHEMA IF NOT EXISTS lakehouse.staging;
