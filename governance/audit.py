"""
Audit Trail — Banking Data Platform

Logs all pipeline actions for compliance and debugging:
- Data ingestion (Bronze)
- Data transformation (Silver/Gold)
- Contract validation
- Lineage emission
- Data quality checks
- PII access tracking

Usage:
    from governance.audit import AuditLogger

    logger = AuditLogger()
    logger.log_action(
        action="ingest",
        table_name="lakehouse.bronze.core_customer",
        dag_id="bronze_core_banking_dag",
        dag_run_id="run_123",
        status="success",
        details="Ingested 10000 rows from PostgreSQL",
    )

    # Persist to PostgreSQL
    logger.write_to_pg(spark)
"""

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging import getLogger

log = getLogger("audit")


# ---------------------------------------------------------------------------
# Audit Record
# ---------------------------------------------------------------------------

@dataclass
class AuditRecord:
    """A single audit log entry."""
    action: str
    table_name: str
    dag_id: str
    dag_run_id: str
    status: str          # "success", "failed", "warning"
    details: str = ""
    row_count: int = 0
    duration_seconds: float | None = None
    error_message: str | None = None
    metadata: dict = field(default_factory=dict)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        return {
            "action": self.action,
            "table_name": self.table_name,
            "dag_id": self.dag_id,
            "dag_run_id": self.dag_run_id,
            "status": self.status,
            "details": self.details,
            "row_count": self.row_count,
            "duration_seconds": self.duration_seconds,
            "error_message": self.error_message,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
        }


# ---------------------------------------------------------------------------
# Action Types
# ---------------------------------------------------------------------------

class AuditAction:
    """Standard action types for audit logging."""
    INGEST = "ingest"
    TRANSFORM = "transform"
    VALIDATE = "validate"
    EMIT_LINEAGE = "emit_lineage"
    DATA_QUALITY = "data_quality"
    PII_MASKING = "pii_masking"
    PII_ACCESS = "pii_access"
    CONTRACT_VALIDATION = "contract_validation"
    MAINTENANCE = "maintenance"
    SECURITY = "security"


# ---------------------------------------------------------------------------
# Audit Logger
# ---------------------------------------------------------------------------

class AuditLogger:
    """
    Logs pipeline actions for compliance and debugging.

    Stores audit records in PostgreSQL (opslakehouse.audit_log)
    and optionally emits to structured logging.
    """

    def __init__(self):
        self._records: list[AuditRecord] = []
        self._pg_url = "jdbc:postgresql://postgres:5432/banking_db"
        self._pg_props = {
            "user": os.environ.get("POSTGRES_USER", "banking_admin"),
            "password": os.environ.get("POSTGRES_PASSWORD", "BankingAdmin123"),
            "driver": "org.postgresql.Driver",
        }

    def log_action(
        self,
        action: str,
        table_name: str,
        dag_id: str,
        dag_run_id: str,
        status: str,
        details: str = "",
        row_count: int = 0,
        duration_seconds: float | None = None,
        error_message: str | None = None,
        metadata: dict | None = None,
    ) -> AuditRecord:
        """
        Log a pipeline action.

        Args:
            action: Action type (use AuditAction constants)
            table_name: Table affected
            dag_id: Airflow DAG ID
            dag_run_id: Airflow DAG run ID
            status: "success", "failed", or "warning"
            details: Human-readable details
            row_count: Number of rows processed
            duration_seconds: How long the action took
            error_message: Error message if failed
            metadata: Additional metadata

        Returns:
            AuditRecord
        """
        record = AuditRecord(
            action=action,
            table_name=table_name,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            status=status,
            details=details,
            row_count=row_count,
            duration_seconds=duration_seconds,
            error_message=error_message,
            metadata=metadata or {},
        )

        self._records.append(record)

        # Log to console
        icon = "✅" if status == "success" else "❌" if status == "failed" else "⚠️"
        log.info(
            f"{icon} [{action}] {table_name} — {status} "
            f"({row_count} rows, {duration_seconds or 0:.1f}s)"
        )

        return record

    def log_ingest(
        self,
        table_name: str,
        dag_id: str,
        dag_run_id: str,
        row_count: int,
        duration_seconds: float | None = None,
        status: str = "success",
    ) -> AuditRecord:
        """Log a data ingestion action."""
        return self.log_action(
            action=AuditAction.INGEST,
            table_name=table_name,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            status=status,
            details=f"Ingested {row_count} rows",
            row_count=row_count,
            duration_seconds=duration_seconds,
        )

    def log_transform(
        self,
        table_name: str,
        dag_id: str,
        dag_run_id: str,
        row_count: int,
        transform_type: str = "",
        duration_seconds: float | None = None,
        status: str = "success",
    ) -> AuditRecord:
        """Log a data transformation action."""
        return self.log_action(
            action=AuditAction.TRANSFORM,
            table_name=table_name,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            status=status,
            details=f"Transformed {row_count} rows ({transform_type})",
            row_count=row_count,
            duration_seconds=duration_seconds,
            metadata={"transform_type": transform_type},
        )

    def log_validation(
        self,
        table_name: str,
        dag_id: str,
        dag_run_id: str,
        validation_type: str,
        passed: bool,
        details: str = "",
    ) -> AuditRecord:
        """Log a validation action (contract, DQ, etc.)."""
        return self.log_action(
            action=AuditAction.VALIDATE,
            table_name=table_name,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            status="success" if passed else "failed",
            details=f"{validation_type}: {details}",
            metadata={"validation_type": validation_type, "passed": passed},
        )

    def log_pii_access(
        self,
        table_name: str,
        column_name: str,
        user_name: str,
        access_type: str,
        query_text: str = "",
    ) -> None:
        """
        Log PII column access for compliance tracking.

        Args:
            table_name: Table containing PII
            column_name: PII column accessed
            user_name: User who accessed the data
            access_type: "read", "mask", "export"
            query_text: SQL query (optional)
        """
        self.log_action(
            action=AuditAction.PII_ACCESS,
            table_name=table_name,
            dag_id="system",
            dag_run_id="manual",
            status="success",
            details=f"PII access: {column_name} by {user_name}",
            metadata={
                "column_name": column_name,
                "user_name": user_name,
                "access_type": access_type,
                "query_text": query_text[:500] if query_text else "",
            },
        )

    def get_records(
        self,
        table_name: str | None = None,
        action: str | None = None,
        status: str | None = None,
    ) -> list[AuditRecord]:
        """
        Get audit records with optional filters.

        Args:
            table_name: Filter by table name
            action: Filter by action type
            status: Filter by status

        Returns:
            List of matching AuditRecord
        """
        records = self._records

        if table_name:
            records = [r for r in records if r.table_name == table_name]
        if action:
            records = [r for r in records if r.action == action]
        if status:
            records = [r for r in records if r.status == status]

        return records

    def get_audit_trail(
        self,
        table_name: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[AuditRecord]:
        """
        Get audit trail for a specific table.

        Args:
            table_name: Table to get audit trail for
            start_date: Start date filter (ISO format, e.g. "2025-01-01")
            end_date: End date filter (ISO format, e.g. "2025-12-31")

        Returns:
            List of AuditRecord
        """
        records = [r for r in self._records if r.table_name == table_name]

        if start_date:
            records = [
                r for r in records
                if r.timestamp[:len(start_date)] >= start_date
            ]
        if end_date:
            records = [
                r for r in records
                if r.timestamp[:len(end_date)] <= end_date
            ]

        return records

    def write_to_pg(self, spark=None) -> None:
        """
        Write audit records to PostgreSQL.

        Args:
            spark: SparkSession (optional, for DataFrame creation)
        """
        if not self._records:
            log.info("No audit records to write.")
            return

        if spark is None:
            log.warning("No SparkSession provided, skipping PG write.")
            return

        from pyspark.sql import Row
        from pyspark.sql.types import (
            FloatType,
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType([
            StructField("action", StringType()),
            StructField("table_name", StringType()),
            StructField("dag_id", StringType()),
            StructField("dag_run_id", StringType()),
            StructField("status", StringType()),
            StructField("details", StringType()),
            StructField("row_count", IntegerType()),
            StructField("duration_seconds", FloatType()),
            StructField("error_message", StringType()),
            StructField("created_at", TimestampType()),
        ])

        rows = []
        for r in self._records:
            rows.append(Row(
                action=r.action,
                table_name=r.table_name,
                dag_id=r.dag_id,
                dag_run_id=r.dag_run_id,
                status=r.status,
                details=r.details,
                row_count=r.row_count,
                duration_seconds=r.duration_seconds,
                error_message=r.error_message,
                created_at=datetime.now(timezone.utc),
            ))

        df = spark.createDataFrame(rows, schema=schema)

        log.info(f"Writing {len(self._records)} audit records to PostgreSQL...")
        df.write.jdbc(
            self._pg_url,
            "opslakehouse.audit_log",
            mode="append",
            properties=self._pg_props,
        )
        log.info(f"Successfully wrote {len(self._records)} audit records.")

    def clear_records(self) -> None:
        """Clear all in-memory audit records."""
        self._records.clear()

    def summary(self) -> str:
        """Print summary of audit records."""
        if not self._records:
            return "No audit records."

        lines = [
            f"Audit Summary: {len(self._records)} records",
            "",
        ]

        # Group by action
        by_action = {}
        for r in self._records:
            by_action.setdefault(r.action, []).append(r)

        for action, records in sorted(by_action.items()):
            success = sum(1 for r in records if r.status == "success")
            failed = sum(1 for r in records if r.status == "failed")
            lines.append(f"  [{action}] {len(records)} records ({success} success, {failed} failed)")

        return "\n".join(lines)
