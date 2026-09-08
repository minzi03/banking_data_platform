-- =============================================================================
-- MLflow Metadata Database
-- Creates the 'mlflow' database for MLflow tracking server
-- =============================================================================

-- Create mlflow database
SELECT 'CREATE DATABASE mlflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE mlflow TO banking_admin;
