#!/bin/bash
# =============================================================================
# Apache Superset — Initialization Script
# Banking Data Platform
# =============================================================================
set -e

echo "============================================="
echo " Superset Init — Banking Data Platform"
echo "============================================="

# ── Step 1: Initialize Superset database ────────────────────────────────────
echo "[1/5] Initializing Superset database..."
superset db upgrade

# ── Step 2: Create admin user ──────────────────────────────────────────────
echo "[2/5] Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@banking.local \
    --password admin123 || echo "Admin user already exists, skipping."

# ── Step 3: Initialize Superset (roles, permissions) ────────────────────────
echo "[3/5] Initializing roles and permissions..."
superset init

# ── Step 4: Add Trino database connection ───────────────────────────────────
echo "[4/5] Adding Trino database connection..."
python /app/docker/superset/add_trino_connection.py || echo "Trino connection setup skipped (will configure via UI)."

# ── Step 5: Import dashboards ──────────────────────────────────────────────
echo "[5/5] Importing dashboards..."
python /app/docker/superset/import_dashboards.py || echo "Dashboard import skipped (import manually via UI)."

echo "============================================="
echo " Superset Init Complete!"
echo " UI: http://localhost:8088"
echo " Login: admin / admin123"
echo " Dashboards: 5 pre-built (Loan, Customer 360, Transaction, Fraud, Executive)"
echo "============================================="
