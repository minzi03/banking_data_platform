-- =============================================================================
-- Superset Metadata Database
-- Creates the 'superset' database for Apache Superset metadata
-- =============================================================================

-- Create superset database (Superset manages its own schema)
SELECT 'CREATE DATABASE superset'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE superset TO banking_admin;
