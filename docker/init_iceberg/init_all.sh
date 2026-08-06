#!/bin/bash
# =============================================================================
# Iceberg DDL Init Script — Banking Data Platform
# Runs all DDL scripts to create Bronze/Silver/Gold tables in Iceberg catalog.
# Called by the banking-iceberg-init container on first startup.
# =============================================================================

set -e

CATALOG="lakehouse"
DDL_DIR="/opt/project/docker/init_iceberg"

echo "============================================="
echo "Iceberg DDL Init — Starting"
echo "============================================="

# Wait for Iceberg REST catalog to be ready
echo "Waiting for Iceberg REST catalog..."
for i in $(seq 1 30); do
    if curl -sf http://iceberg-rest:8181/v1/config > /dev/null 2>&1; then
        echo "Iceberg REST catalog is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: Iceberg REST catalog not ready after 30 attempts"
        exit 1
    fi
    echo "  Attempt $i/30 — waiting 2s..."
    sleep 2
done

# Run DDL scripts in order
echo ""
echo "--- Running Bronze DDL ---"
/opt/spark/bin/spark-sql -f "${DDL_DIR}/01_ddl_bronze.sql" 2>&1 | tail -5

echo ""
echo "--- Running Silver DDL ---"
/opt/spark/bin/spark-sql -f "${DDL_DIR}/02_ddl_silver.sql" 2>&1 | tail -5

echo ""
echo "--- Running Gold DDL ---"
/opt/spark/bin/spark-sql -f "${DDL_DIR}/03_ddl_gold.sql" 2>&1 | tail -5

echo ""
echo "--- Running Bronze CDC DDL ---"
/opt/spark/bin/spark-sql -f "${DDL_DIR}/04_ddl_bronze_cdc.sql" 2>&1 | tail -5

echo ""
echo "============================================="
echo "Iceberg DDL Init — Complete"
echo "Verifying tables..."

# Verify tables exist
/opt/spark/bin/spark-sql -e "SHOW TABLES IN ${CATALOG}.bronze" 2>/dev/null | head -20
/opt/spark/bin/spark-sql -e "SHOW TABLES IN ${CATALOG}.silver" 2>/dev/null | head -20
/opt/spark/bin/spark-sql -e "SHOW TABLES IN ${CATALOG}.gold" 2>/dev/null | head -20

echo "============================================="
echo "All DDL scripts executed successfully!"
