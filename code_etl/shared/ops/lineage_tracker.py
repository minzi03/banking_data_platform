"""
Lineage Tracker Utility — Banking Data Platform

Wrapper for governance.lineage.LineageTracker that integrates with
Spark ETL jobs for automatic lineage recording.

Usage:
    from ops.lineage_tracker import get_lineage_tracker

    tracker = get_lineage_tracker()

    # Record lineage when writing data
    tracker.record_lineage(
        source_table="lakehouse.bronze.core_customer",
        target_table="lakehouse.silver.dim_customer",
        transform_type="scd2_merge",
        dag_id="silver_all_dag",
        dag_run_id="run_123",
        row_count=10000,
    )

    # At end of DAG, flush to PostgreSQL
    tracker.flush(spark)
"""

import os
import sys
from logging import getLogger

log = getLogger("lineage_tracker")

# Add governance to path
_GOVERNANCE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "governance"
)
if _GOVERNANCE_PATH not in sys.path:
    sys.path.insert(0, _GOVERNANCE_PATH)

from governance.lineage import LineageTracker

# ---------------------------------------------------------------------------
# Singleton instance
# ---------------------------------------------------------------------------

_tracker: LineageTracker | None = None


def get_lineage_tracker() -> LineageTracker:
    """
    Get the singleton LineageTracker instance.

    Returns:
        LineageTracker instance
    """
    global _tracker
    if _tracker is None:
        _tracker = LineageTracker()
    return _tracker


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------

def record_lineage(
    source_table: str,
    target_table: str,
    transform_type: str,
    dag_id: str,
    dag_run_id: str,
    snapshot_id: str | None = None,
    row_count: int = 0,
):
    """
    Record a lineage entry (convenience wrapper).

    Args:
        source_table: Source table
        target_table: Target table
        transform_type: Transform type (use TransformType constants)
        dag_id: Airflow DAG ID
        dag_run_id: Airflow DAG run ID
        snapshot_id: Iceberg snapshot ID (optional)
        row_count: Number of rows processed
    """
    tracker = get_lineage_tracker()
    return tracker.record_lineage(
        source_table=source_table,
        target_table=target_table,
        transform_type=transform_type,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        snapshot_id=snapshot_id,
        row_count=row_count,
    )


def flush_lineage(spark=None) -> None:
    """
    Flush all lineage records to PostgreSQL.

    Args:
        spark: SparkSession (optional)
    """
    tracker = get_lineage_tracker()
    tracker.write_to_pg(spark)
    tracker.clear_records()


def get_lineage_summary() -> str:
    """Get summary of recorded lineage."""
    tracker = get_lineage_tracker()
    return tracker.summary()
