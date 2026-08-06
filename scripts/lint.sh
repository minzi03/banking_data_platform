#!/bin/bash
# =============================================================================
# Lint Script — Banking Data Platform
# =============================================================================
# Usage: ./scripts/lint.sh
# =============================================================================

set -e

echo "=========================================="
echo " Banking Data Platform — Lint Check"
echo "=========================================="
echo ""

# Check if ruff is installed
if ! command -v ruff &> /dev/null; then
    echo "❌ ruff not found. Installing..."
    pip install ruff
fi

echo "=== Running ruff check ==="
ruff check governance/ code_etl/ tests/ --output-format=full
RUFF_CHECK_EXIT=$?

echo ""
echo "=== Running ruff format check ==="
ruff format --check governance/ code_etl/ tests/
RUFF_FORMAT_EXIT=$?

echo ""
echo "=========================================="
if [ $RUFF_CHECK_EXIT -eq 0 ] && [ $RUFF_FORMAT_EXIT -eq 0 ]; then
    echo "✅ All lint checks passed"
    exit 0
else
    echo "❌ Lint checks failed"
    exit 1
fi
