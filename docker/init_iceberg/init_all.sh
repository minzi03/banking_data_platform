#!/bin/bash
# =============================================================================
# Iceberg DDL Init Script — Banking Data Platform
# Runs all DDL scripts to create namespaces and Bronze/Silver/Gold/CDC tables.
# Called by the banking-iceberg-init container on first startup.
#
# Mọi bước phải fail thì container fail:
#   - pipefail: bản cũ chạy `spark-sql … | tail -5` dưới `set -e` trần, nên
#     exit code của spark-sql bị `tail` nuốt — DDL hỏng mà container vẫn thoát 0.
#   - 06_ddl_silver_cdc_current.sql từng không được gọi ở đâu: stack mới không có
#     silver.dim_*_current và CDC consolidation chết ngay ở lần chạy đầu.
# =============================================================================

set -euo pipefail

CATALOG="lakehouse"
DDL_DIR="/opt/project/docker/init_iceberg"

# Thứ tự có ý nghĩa: namespace trước, bảng sau.
DDL_FILES=(
    "create_schemas.sql"
    "01_ddl_bronze.sql"
    "02_ddl_silver.sql"
    "03_ddl_gold.sql"
    "04_ddl_bronze_cdc.sql"
    "06_ddl_silver_cdc_current.sql"
)

echo "============================================="
echo "Iceberg DDL Init — Starting"
echo "============================================="

echo "Waiting for Iceberg REST catalog..."
for i in $(seq 1 30); do
    if curl -sf http://iceberg-rest:8181/v1/config > /dev/null 2>&1; then
        echo "Iceberg REST catalog is ready!"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Iceberg REST catalog not ready after 30 attempts"
        exit 1
    fi
    echo "  Attempt $i/30 — waiting 2s..."
    sleep 2
done

for ddl in "${DDL_FILES[@]}"; do
    echo ""
    echo "--- Running ${ddl} ---"
    /opt/spark/bin/spark-sql -f "${DDL_DIR}/${ddl}" 2>&1 | tail -5
done

echo ""
echo "============================================="
echo "Iceberg DDL Init — Complete"
echo "Verifying tables..."
for ns in bronze silver gold meta; do
    echo "--- ${CATALOG}.${ns} ---"
    /opt/spark/bin/spark-sql -e "SHOW TABLES IN ${CATALOG}.${ns}" 2>/dev/null
done
echo "============================================="
echo "All DDL scripts executed successfully!"
