#!/bin/bash
# =============================================================================
# Performance Benchmark Script - Banking Data Platform
# =============================================================================
# Measures query performance before/after optimization
# Usage: ./benchmark_performance.sh [runs]
# =============================================================================

set -e

# Configuration
TRINO_CATALOG="lakehouse"
TRINO_HOST="localhost"
TRINO_PORT="8085"
NUM_RUNS=${1:-3}  # Default 3 runs for average

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# =============================================================================
# Helper Functions
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Run a Trino query and return execution time
run_query() {
    local query_name="$1"
    local query="$2"
    local total_time=0

    log_info "Running: $query_name"

    for i in $(seq 1 $NUM_RUNS); do
        start_time=$(date +%s%N)

        result=$(docker exec banking-trino trino \
            --catalog $TRINO_CATALOG \
            --schema gold \
            --execute "$query" 2>/dev/null | grep -v WARNING | grep -v "^Aug")

        end_time=$(date +%s%N)
        duration=$(( (end_time - start_time) / 1000000 ))  # Convert to ms

        total_time=$((total_time + duration))
        log_info "  Run $i: ${duration}ms"
    done

    avg_time=$((total_time / NUM_RUNS))
    echo "$query_name,$avg_time"
    return $avg_time
}

# =============================================================================
# Benchmark Queries
# =============================================================================

echo "=========================================="
echo "Performance Benchmark - Banking Data Platform"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  - Catalog: $TRINO_CATALOG"
echo "  - Runs per query: $NUM_RUNS"
echo "  - Output: benchmark_results.csv"
echo ""

# Create results file
RESULTS_FILE="benchmark_results.csv"
echo "query,avg_time_ms" > $RESULTS_FILE

# Benchmark 1: Simple count query
run_query "simple_count" "SELECT COUNT(*) FROM mart_customer_360"

# Benchmark 2: Filter by customer
run_query "customer_lookup" "SELECT * FROM mart_customer_360 WHERE customer_id = 1000"

# Benchmark 3: Aggregation by segment
run_query "rfm_segmentation" "SELECT rfm_segment, COUNT(*) as cnt FROM rfm_segment GROUP BY rfm_segment ORDER BY cnt DESC"

# Benchmark 4: Churn analysis
run_query "churn_analysis" "SELECT churn_risk, COUNT(*) as cnt FROM churn_prediction GROUP BY churn_risk ORDER BY cnt DESC"

# Benchmark 5: Join query (Customer 360 + RFM)
run_query "customer_rfm_join" "SELECT c.customer_id, c.full_name, r.rfm_segment FROM mart_customer_360 c JOIN rfm_segment r ON c.customer_id = r.customer_id LIMIT 1000"

# Benchmark 6: Time-based query (Gold summary)
run_query "balance_summary" "SELECT customer_id, total_balance FROM customer_balance_summary WHERE total_balance > 1000000 ORDER BY total_balance DESC LIMIT 100"

# Benchmark 7: Complex aggregation (Branch performance)
run_query "branch_performance" "SELECT branch_code, COUNT(*) as customer_count, AVG(total_balance) as avg_balance FROM mart_customer_360 GROUP BY branch_code ORDER BY customer_count DESC"

# =============================================================================
# Results Summary
# =============================================================================

echo ""
echo "=========================================="
echo "Benchmark Results Summary"
echo "=========================================="
echo ""

# Display results
column -t -s, $RESULTS_FILE

echo ""
echo "=========================================="
echo "Performance Tips"
echo "=========================================="
echo ""
echo "1. Check Spark UI: http://localhost:9090"
echo "   - Verify executor memory usage"
echo "   - Check shuffle partitions"
echo ""
echo "2. Check Iceberg table properties:"
echo "   docker exec banking-trino trino --catalog lakehouse --execute \"SHOW TBLPROPERTIES gold.mart_customer_360\""
echo ""
echo "3. Run OPTIMIZE for Z-Ordering:"
echo "   docker exec banking-spark-master /opt/spark/bin/spark-submit \\"
echo "     --master spark://spark-master:7077 \\"
echo "     /opt/project/code_etl/shared/ops/iceberg_maintenance.py --target mart --mode full"
echo ""
echo "4. Check file sizes in MinIO:"
echo "   http://localhost:9001 → lakehouse/gold/"
echo ""

log_success "Benchmark completed! Results saved to $RESULTS_FILE"
