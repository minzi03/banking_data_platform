"""
Lineage Tracking — Banking Data Platform

Tracks data lineage across the entire pipeline:
- Source → Bronze (JDBC ingestion)
- Bronze → Silver (SCD transforms)
- Silver → Gold (mart aggregations)
- Gold → dbt (semantic models)

Usage:
    from governance.lineage import LineageTracker

    tracker = LineageTracker()
    tracker.record_lineage(
        source_table="lakehouse.bronze.core_customer",
        target_table="lakehouse.silver.dim_customer",
        transform_type="scd2_merge",
        dag_id="silver_all_dag",
        dag_run_id="run_123",
        snapshot_id="iceberg-snapshot-456",
        row_count=10000,
    )
"""

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging import getLogger
from typing import Dict, List, Optional

log = getLogger("lineage")


# ---------------------------------------------------------------------------
# Lineage Record
# ---------------------------------------------------------------------------

@dataclass
class LineageRecord:
    """A single lineage record linking source to target."""
    source_table: str
    target_table: str
    transform_type: str
    dag_id: str
    dag_run_id: str
    snapshot_id: Optional[str] = None
    row_count: int = 0
    column_mappings: Dict[str, str] = field(default_factory=dict)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict:
        return {
            "source_table": self.source_table,
            "target_table": self.target_table,
            "transform_type": self.transform_type,
            "dag_id": self.dag_id,
            "dag_run_id": self.dag_run_id,
            "snapshot_id": self.snapshot_id,
            "row_count": self.row_count,
            "column_mappings": self.column_mappings,
            "timestamp": self.timestamp,
        }


# ---------------------------------------------------------------------------
# Transform Types
# ---------------------------------------------------------------------------

class TransformType:
    """Standard transform types for lineage tracking."""
    JDBC_INGEST = "jdbc_ingest"
    CDC_STREAMING = "cdc_streaming"
    SCD1_UPSERT = "scd1_upsert"
    SCD2_MERGE = "scd2_merge"
    FACT_LOAD = "fact_load"
    GOLD_MART = "gold_mart"
    DBT_MODEL = "dbt_model"
    PII_MASKING = "pii_masking"
    DATA_QUALITY = "data_quality"


# ---------------------------------------------------------------------------
# Lineage Tracker
# ---------------------------------------------------------------------------

class LineageTracker:
    """
    Tracks data lineage across the pipeline.

    Stores lineage records in PostgreSQL (opslakehouse.lineage_log)
    and optionally emits to OpenMetadata API.
    """

    def __init__(self):
        self._records: List[LineageRecord] = []
        self._pg_url = "jdbc:postgresql://postgres:5432/banking_db"
        self._pg_props = {
            "user": os.environ.get("POSTGRES_USER", "banking_admin"),
            "password": os.environ.get("POSTGRES_PASSWORD", "BankingAdmin123"),
            "driver": "org.postgresql.Driver",
        }

    def record_lineage(
        self,
        source_table: str,
        target_table: str,
        transform_type: str,
        dag_id: str,
        dag_run_id: str,
        snapshot_id: Optional[str] = None,
        row_count: int = 0,
        column_mappings: Optional[Dict[str, str]] = None,
    ) -> LineageRecord:
        """
        Record a lineage entry.

        Args:
            source_table: Source table (e.g., 'lakehouse.bronze.core_customer')
            target_table: Target table (e.g., 'lakehouse.silver.dim_customer')
            transform_type: Type of transform (use TransformType constants)
            dag_id: Airflow DAG ID
            dag_run_id: Airflow DAG run ID
            snapshot_id: Iceberg snapshot ID (if applicable)
            row_count: Number of rows processed
            column_mappings: Optional column name mappings (source → target)

        Returns:
            LineageRecord
        """
        record = LineageRecord(
            source_table=source_table,
            target_table=target_table,
            transform_type=transform_type,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            snapshot_id=snapshot_id,
            row_count=row_count,
            column_mappings=column_mappings or {},
        )

        self._records.append(record)
        log.info(
            f"Lineage recorded: {source_table} → {target_table} "
            f"({transform_type}, {row_count} rows)"
        )

        return record

    def get_records(self) -> List[LineageRecord]:
        """Get all recorded lineage entries."""
        return list(self._records)

    def get_upstream(self, table_name: str) -> List[LineageRecord]:
        """
        Get all upstream lineage for a table.

        Args:
            table_name: Target table to find upstream for

        Returns:
            List of LineageRecord where target_table matches
        """
        return [
            r for r in self._records
            if r.target_table == table_name
        ]

    def get_downstream(self, table_name: str) -> List[LineageRecord]:
        """
        Get all downstream lineage for a table.

        Args:
            table_name: Source table to find downstream for

        Returns:
            List of LineageRecord where source_table matches
        """
        return [
            r for r in self._records
            if r.source_table == table_name
        ]

    def get_full_lineage(self, table_name: str) -> List[LineageRecord]:
        """
        Get complete lineage chain (upstream + downstream) for a table.

        Args:
            table_name: Table to trace lineage for

        Returns:
            List of all related LineageRecord
        """
        upstream = self.get_upstream(table_name)
        downstream = self.get_downstream(table_name)
        return upstream + downstream

    def write_to_pg(self, spark=None) -> None:
        """
        Write lineage records to PostgreSQL.

        Args:
            spark: SparkSession (optional, for DataFrame creation)
        """
        if not self._records:
            log.info("No lineage records to write.")
            return

        if spark is None:
            log.warning("No SparkSession provided, skipping PG write.")
            return

        from pyspark.sql import Row
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, TimestampType
        )

        schema = StructType([
            StructField("source_table", StringType()),
            StructField("target_table", StringType()),
            StructField("transform_type", StringType()),
            StructField("dag_id", StringType()),
            StructField("dag_run_id", StringType()),
            StructField("snapshot_id", StringType()),
            StructField("row_count", IntegerType()),
            StructField("created_at", TimestampType()),
        ])

        rows = []
        for r in self._records:
            rows.append(Row(
                source_table=r.source_table,
                target_table=r.target_table,
                transform_type=r.transform_type,
                dag_id=r.dag_id,
                dag_run_id=r.dag_run_id,
                snapshot_id=r.snapshot_id,
                row_count=r.row_count,
                created_at=datetime.now(timezone.utc),
            ))

        df = spark.createDataFrame(rows, schema=schema)

        log.info(f"Writing {len(self._records)} lineage records to PostgreSQL...")
        df.write.jdbc(
            self._pg_url,
            "opslakehouse.lineage_log",
            mode="append",
            properties=self._pg_props,
        )
        log.info(f"Successfully wrote {len(self._records)} lineage records.")

    def clear_records(self) -> None:
        """Clear all in-memory lineage records."""
        self._records.clear()

    def summary(self) -> str:
        """Print summary of lineage records."""
        if not self._records:
            return "No lineage records."

        lines = [
            f"Lineage Summary: {len(self._records)} records",
            "",
        ]

        # Group by transform type
        by_type = {}
        for r in self._records:
            by_type.setdefault(r.transform_type, []).append(r)

        for transform_type, records in sorted(by_type.items()):
            lines.append(f"  [{transform_type}] {len(records)} records")
            for r in records:
                lines.append(f"    {r.source_table} → {r.target_table}")

        return "\n".join(lines)
