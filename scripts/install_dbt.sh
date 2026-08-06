#!/bin/bash
# =============================================================================
# Installation Script — dbt for Banking Data Platform
# =============================================================================
# Architecture: Lakehouse 2.0
# Usage: ./scripts/install_dbt.sh
# =============================================================================

set -e

echo "=========================================="
echo "Installing dbt for Banking Data Platform"
echo "=========================================="

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $python_version"

# Check pip
if ! command -v pip3 &> /dev/null; then
    echo "Error: pip3 not found. Please install pip."
    exit 1
fi

# Install dbt and dependencies
echo ""
echo "Installing dbt and dependencies..."
pip3 install --upgrade pip
pip3 install \
    dbt-core==1.12.0 \
    dbt-trino==1.8.0

echo ""
echo "dbt installed successfully!"

# Verify installation
echo ""
echo "Verifying installation..."
dbt --version

# Navigate to dbt project directory
cd "$(dirname "$0")/../dbt"

echo ""
echo "Installing dbt packages..."
dbt deps

echo ""
echo "=========================================="
echo "Installation complete!"
echo ""
echo "Next steps:"
echo "1. Configure profiles.yml with your connection details"
echo "2. Run: dbt deps"
echo "3. Run: dbt run --select semantic"
echo "4. Run: dbt test"
echo "5. Run: dbt docs generate && dbt docs serve"
echo "=========================================="
