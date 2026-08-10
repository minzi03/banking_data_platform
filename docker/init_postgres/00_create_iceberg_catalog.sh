#!/bin/bash
# =============================================================================
# Create Iceberg Catalog Database — Banking Data Platform
# The Iceberg REST catalog uses JDBC backend on this database
# =============================================================================
# Note: DO NOT use set -e; we handle errors explicitly
# POSTGRES_USER (banking_admin) is the superuser when POSTGRES_USER is set

echo "📦 Creating iceberg_catalog database..."

# Check if database already exists
DB_EXISTS=$(psql -v ON_ERROR_STOP=0 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='iceberg_catalog'" 2>/dev/null || echo "")

if [ "$DB_EXISTS" = "1" ]; then
    echo "  Database iceberg_catalog already exists"
else
    echo "  Creating database iceberg_catalog..."
    if ! psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "CREATE DATABASE iceberg_catalog;"; then
        echo "  ❌ FAILED to create iceberg_catalog database!"
        echo "  Continuing anyway (iceberg-rest may fail to start)"
    else
        echo "  ✅ Database iceberg_catalog created"
    fi
fi

# Grant all privileges
psql -v ON_ERROR_STOP=0 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c \
  "GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO $POSTGRES_USER;" || true

# Grant schema permissions in the iceberg_catalog database
psql -v ON_ERROR_STOP=0 --username "$POSTGRES_USER" --dbname "iceberg_catalog" -c \
  "GRANT ALL ON SCHEMA public TO $POSTGRES_USER;" || true

# Final verification
RESULT=$(psql -v ON_ERROR_STOP=0 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -tAc "SELECT datname FROM pg_database WHERE datname = 'iceberg_catalog'" 2>/dev/null || echo "")
if [ "$RESULT" = "iceberg_catalog" ]; then
    echo "✅ Iceberg catalog database ready"
else
    echo "❌ WARNING: iceberg_catalog database may not exist!"
fi
