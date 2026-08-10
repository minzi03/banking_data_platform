"""
Tests for governance.audit — Audit trail logging.
"""

from governance.audit import AuditAction, AuditLogger, AuditRecord

# ---------------------------------------------------------------------------
# Test AuditAction
# ---------------------------------------------------------------------------

class TestAuditAction:
    def test_constants(self):
        assert AuditAction.INGEST == "ingest"
        assert AuditAction.TRANSFORM == "transform"
        assert AuditAction.VALIDATE == "validate"
        assert AuditAction.EMIT_LINEAGE == "emit_lineage"
        assert AuditAction.DATA_QUALITY == "data_quality"
        assert AuditAction.PII_MASKING == "pii_masking"
        assert AuditAction.CONTRACT_VALIDATION == "contract_validation"
        assert AuditAction.MAINTENANCE == "maintenance"


# ---------------------------------------------------------------------------
# Test AuditRecord
# ---------------------------------------------------------------------------

class TestAuditRecord:
    def test_creation(self):
        record = AuditRecord(
            action="ingest",
            table_name="lakehouse.bronze.core_customer",
            dag_id="bronze_core_banking_dag",
            dag_run_id="run_123",
            status="success",
            details="Ingested 10000 rows",
            row_count=10000,
            duration_seconds=45.2,
        )
        assert record.action == "ingest"
        assert record.table_name == "lakehouse.bronze.core_customer"
        assert record.status == "success"
        assert record.row_count == 10000
        assert record.duration_seconds == 45.2
        assert record.timestamp is not None

    def test_to_dict(self):
        record = AuditRecord(
            action="ingest",
            table_name="table",
            dag_id="dag",
            dag_run_id="run",
            status="success",
        )
        d = record.to_dict()
        assert isinstance(d, dict)
        assert d["action"] == "ingest"
        assert d["status"] == "success"

    def test_optional_fields(self):
        record = AuditRecord(
            action="ingest",
            table_name="table",
            dag_id="dag",
            dag_run_id="run",
            status="success",
        )
        assert record.details == ""
        assert record.row_count == 0
        assert record.duration_seconds is None
        assert record.error_message is None
        assert record.metadata == {}


# ---------------------------------------------------------------------------
# Test AuditLogger
# ---------------------------------------------------------------------------

class TestAuditLogger:
    def test_log_action(self):
        logger = AuditLogger()
        record = logger.log_action(
            action="ingest",
            table_name="lakehouse.bronze.core_customer",
            dag_id="bronze_dag",
            dag_run_id="run_123",
            status="success",
            details="Ingested 10000 rows",
            row_count=10000,
        )
        assert record is not None
        assert record.action == "ingest"
        assert len(logger.get_records()) == 1

    def test_log_ingest(self):
        logger = AuditLogger()
        record = logger.log_ingest(
            table_name="lakehouse.bronze.core_customer",
            dag_id="bronze_dag",
            dag_run_id="run_123",
            row_count=10000,
            duration_seconds=45.2,
        )
        assert record.action == "ingest"
        assert record.row_count == 10000

    def test_log_transform(self):
        logger = AuditLogger()
        record = logger.log_transform(
            table_name="lakehouse.silver.dim_customer",
            dag_id="silver_dag",
            dag_run_id="run_456",
            row_count=8000,
            transform_type="scd2_merge",
            duration_seconds=30.5,
        )
        assert record.action == "transform"
        assert record.metadata["transform_type"] == "scd2_merge"

    def test_log_validation(self):
        logger = AuditLogger()
        record = logger.log_validation(
            table_name="lakehouse.silver.dim_customer",
            dag_id="ops_dq_dag",
            dag_run_id="run_789",
            validation_type="row_count",
            passed=True,
            details="Row count OK: 8000",
        )
        assert record.action == "validate"
        assert record.status == "success"
        assert record.metadata["passed"] is True

    def test_log_validation_failed(self):
        logger = AuditLogger()
        record = logger.log_validation(
            table_name="lakehouse.silver.dim_customer",
            dag_id="ops_dq_dag",
            dag_run_id="run_789",
            validation_type="row_count",
            passed=False,
            details="Row count too low",
        )
        assert record.status == "failed"

    def test_get_records_all(self):
        logger = AuditLogger()
        logger.log_action("ingest", "t1", "d1", "r1", "success")
        logger.log_action("transform", "t2", "d2", "r2", "success")
        logger.log_action("validate", "t3", "d3", "r3", "failed")

        all_records = logger.get_records()
        assert len(all_records) == 3

    def test_get_records_filter_by_table(self):
        logger = AuditLogger()
        logger.log_action("ingest", "table_a", "d1", "r1", "success")
        logger.log_action("ingest", "table_b", "d2", "r2", "success")
        logger.log_action("ingest", "table_a", "d3", "r3", "failed")

        filtered = logger.get_records(table_name="table_a")
        assert len(filtered) == 2

    def test_get_records_filter_by_action(self):
        logger = AuditLogger()
        logger.log_action("ingest", "t1", "d1", "r1", "success")
        logger.log_action("transform", "t2", "d2", "r2", "success")

        filtered = logger.get_records(action="ingest")
        assert len(filtered) == 1

    def test_get_records_filter_by_status(self):
        logger = AuditLogger()
        logger.log_action("ingest", "t1", "d1", "r1", "success")
        logger.log_action("ingest", "t2", "d2", "r2", "failed")

        filtered = logger.get_records(status="failed")
        assert len(filtered) == 1

    def test_get_audit_trail(self):
        logger = AuditLogger()
        logger.log_action("ingest", "table_a", "d1", "r1", "success")

        trail = logger.get_audit_trail("table_a")
        assert len(trail) == 1

    def test_clear_records(self):
        logger = AuditLogger()
        logger.log_action("ingest", "t", "d", "r", "success")
        assert len(logger.get_records()) == 1
        logger.clear_records()
        assert len(logger.get_records()) == 0

    def test_summary(self):
        logger = AuditLogger()
        logger.log_action("ingest", "t1", "d1", "r1", "success")
        logger.log_action("transform", "t2", "d2", "r2", "failed")

        summary = logger.summary()
        assert "2 records" in summary
        assert "ingest" in summary
        assert "transform" in summary

    def test_summary_empty(self):
        logger = AuditLogger()
        summary = logger.summary()
        assert "No audit records" in summary
