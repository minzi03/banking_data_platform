"""
Tests for governance.lineage — Lineage tracking across the pipeline.
"""

import pytest
from governance.lineage import LineageTracker, LineageRecord, TransformType


# ---------------------------------------------------------------------------
# Test TransformType
# ---------------------------------------------------------------------------

class TestTransformType:
    def test_constants(self):
        assert TransformType.JDBC_INGEST == "jdbc_ingest"
        assert TransformType.CDC_STREAMING == "cdc_streaming"
        assert TransformType.SCD1_UPSERT == "scd1_upsert"
        assert TransformType.SCD2_MERGE == "scd2_merge"
        assert TransformType.FACT_LOAD == "fact_load"
        assert TransformType.GOLD_MART == "gold_mart"
        assert TransformType.DBT_MODEL == "dbt_model"
        assert TransformType.PII_MASKING == "pii_masking"
        assert TransformType.DATA_QUALITY == "data_quality"


# ---------------------------------------------------------------------------
# Test LineageRecord
# ---------------------------------------------------------------------------

class TestLineageRecord:
    def test_creation(self):
        record = LineageRecord(
            source_table="lakehouse.bronze.core_customer",
            target_table="lakehouse.silver.dim_customer",
            transform_type=TransformType.SCD2_MERGE,
            dag_id="silver_all_dag",
            dag_run_id="run_123",
            row_count=10000,
        )
        assert record.source_table == "lakehouse.bronze.core_customer"
        assert record.target_table == "lakehouse.silver.dim_customer"
        assert record.transform_type == "scd2_merge"
        assert record.dag_id == "silver_all_dag"
        assert record.row_count == 10000
        assert record.timestamp is not None

    def test_to_dict(self):
        record = LineageRecord(
            source_table="source",
            target_table="target",
            transform_type="jdbc_ingest",
            dag_id="dag",
            dag_run_id="run",
        )
        d = record.to_dict()
        assert isinstance(d, dict)
        assert d["source_table"] == "source"
        assert d["target_table"] == "target"

    def test_optional_fields(self):
        record = LineageRecord(
            source_table="s",
            target_table="t",
            transform_type="jdbc_ingest",
            dag_id="d",
            dag_run_id="r",
        )
        assert record.snapshot_id is None
        assert record.row_count == 0
        assert record.column_mappings == {}


# ---------------------------------------------------------------------------
# Test LineageTracker
# ---------------------------------------------------------------------------

class TestLineageTracker:
    def test_record_lineage(self):
        tracker = LineageTracker()
        record = tracker.record_lineage(
            source_table="bronze.core_customer",
            target_table="silver.dim_customer",
            transform_type=TransformType.SCD2_MERGE,
            dag_id="silver_all_dag",
            dag_run_id="run_123",
            row_count=10000,
        )
        assert record is not None
        assert record.source_table == "bronze.core_customer"
        assert len(tracker.get_records()) == 1

    def test_get_records(self):
        tracker = LineageTracker()
        tracker.record_lineage("s1", "t1", "jdbc_ingest", "dag1", "run1")
        tracker.record_lineage("s2", "t2", "scd2_merge", "dag2", "run2")
        records = tracker.get_records()
        assert len(records) == 2

    def test_get_upstream(self):
        tracker = LineageTracker()
        tracker.record_lineage("bronze.core_customer", "silver.dim_customer", "scd2_merge", "dag1", "run1")
        tracker.record_lineage("bronze.core_account", "silver.dim_account", "scd2_merge", "dag1", "run1")
        tracker.record_lineage("silver.dim_customer", "gold.mart_360", "gold_mart", "dag2", "run2")

        upstream = tracker.get_upstream("silver.dim_customer")
        assert len(upstream) == 1
        assert upstream[0].source_table == "bronze.core_customer"

    def test_get_downstream(self):
        tracker = LineageTracker()
        tracker.record_lineage("bronze.core_customer", "silver.dim_customer", "scd2_merge", "dag1", "run1")
        tracker.record_lineage("silver.dim_customer", "gold.mart_360", "gold_mart", "dag2", "run2")
        tracker.record_lineage("silver.dim_customer", "gold.rfm_segment", "gold_mart", "dag2", "run2")

        downstream = tracker.get_downstream("silver.dim_customer")
        assert len(downstream) == 2

    def test_get_full_lineage(self):
        tracker = LineageTracker()
        tracker.record_lineage("bronze.core_customer", "silver.dim_customer", "scd2_merge", "dag1", "run1")
        tracker.record_lineage("silver.dim_customer", "gold.mart_360", "gold_mart", "dag2", "run2")

        full = tracker.get_full_lineage("silver.dim_customer")
        assert len(full) == 2

    def test_clear_records(self):
        tracker = LineageTracker()
        tracker.record_lineage("s", "t", "jdbc_ingest", "d", "r")
        assert len(tracker.get_records()) == 1
        tracker.clear_records()
        assert len(tracker.get_records()) == 0

    def test_summary(self):
        tracker = LineageTracker()
        tracker.record_lineage("s1", "t1", "jdbc_ingest", "d1", "r1")
        tracker.record_lineage("s2", "t2", "scd2_merge", "d2", "r2")
        summary = tracker.summary()
        assert "2 records" in summary
        assert "jdbc_ingest" in summary

    def test_summary_empty(self):
        tracker = LineageTracker()
        summary = tracker.summary()
        assert "No lineage records" in summary
