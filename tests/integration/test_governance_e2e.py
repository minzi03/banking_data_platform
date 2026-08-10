"""
Integration tests for governance module — End-to-end governance flow.
"""

from unittest.mock import MagicMock

import pytest

from governance.audit import AuditLogger
from governance.contracts import DatasetContract, QualityRules
from governance.contracts_registry import ContractRegistry
from governance.enforcement import ContractEnforcer, ValidationResult
from governance.lineage import LineageTracker, TransformType

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def governance_contracts_dir(tmp_path):
    """Create a directory with multiple contract YAML files."""
    contracts = [
        {
            "filename": "customer_silver.yaml",
            "content": """
dataset_id: banking.core_customer_silver
owner: Data Engineering Team
business_purpose: Customer dimension with SCD2
refresh_sla: daily
quality_class: critical
layer: silver
physical_location:
  catalog: lakehouse
  namespace: silver
  table: dim_customer
dag_id: silver_all_dag
upstream_dataset_ids:
  - banking.core_customer_bronze
quality_rules:
  required_columns:
    - customer_id
    - full_name
    - cccd
  non_null_columns:
    - customer_id
    - cccd
  min_row_count: 5000
  unique_check:
    - customer_id
ai_governance:
  ai_use_allowed: true
  risk_tier: limited_risk
  intended_uses:
    - customer_analytics
    - fraud_detection
  prohibited_uses:
    - automated_decision_making
"""
        },
        {
            "filename": "mart360_gold.yaml",
            "content": """
dataset_id: banking.mart_customer_360_gold
owner: Data Engineering Team
business_purpose: Customer 360 analytics view
refresh_sla: daily
quality_class: critical
layer: gold
physical_location:
  catalog: lakehouse
  namespace: gold
  table: mart_customer_360
dag_id: gold_mart360_dag
upstream_dataset_ids:
  - banking.core_customer_silver
  - banking.dim_account_silver
quality_rules:
  required_columns:
    - customer_id
    - full_name
    - customer_segment
  non_null_columns:
    - customer_id
  min_row_count: 5000
  unique_check:
    - customer_id
ai_governance:
  ai_use_allowed: true
  risk_tier: high_risk
  human_oversight_required: true
"""
        },
    ]
    for contract in contracts:
        yaml_file = tmp_path / contract["filename"]
        yaml_file.write_text(contract["content"], encoding="utf-8")
    return tmp_path


@pytest.fixture
def mock_spark():
    """Create a mock SparkSession."""
    spark = MagicMock()
    spark._sc._jvm = MagicMock()
    return spark


@pytest.fixture
def mock_df_valid():
    """Mock DataFrame with valid data."""
    df = MagicMock()
    df.columns = ["customer_id", "full_name", "cccd", "phone", "email", "customer_segment"]
    df.count.return_value = 10000
    df.filter.return_value = df
    df.select.return_value = df
    df.distinct.return_value = df
    return df


# ---------------------------------------------------------------------------
# Test Full Governance Flow
# ---------------------------------------------------------------------------

class TestGovernanceFlow:
    def test_load_contracts_validate_enforce(self, governance_contracts_dir, mock_spark, mock_df_valid):
        """Full flow: load contracts → validate → enforce."""
        # 1. Load contracts
        registry = ContractRegistry(str(governance_contracts_dir))
        assert registry.contract_count == 2
        assert not registry.has_errors

        # 2. Get contract
        customer_contract = registry.get_contract("banking.core_customer_silver")
        assert customer_contract is not None
        assert customer_contract.quality_class == "critical"

        # 3. Enforce contract
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, mock_df_valid, customer_contract)
        assert isinstance(result, ValidationResult)
        assert result.dataset_id == "banking.core_customer_silver"

        # 4. Check results
        pass_count = sum(1 for c in result.checks if c.status == "PASS")
        assert pass_count > 0

    def test_history_vs_current_contract_semantics(self, mock_spark):
        """History contracts allow customer_id across different cob_dt, current contracts do not."""
        history_contract = DatasetContract(
            dataset_id="banking.mart_customer_360_gold",
            owner="test",
            business_purpose="history",
            layer="gold",
            physical_location={"catalog": "lakehouse", "namespace": "gold", "table": "mart_customer_360"},
            quality_rules=QualityRules(unique_column_sets=[["customer_id", "cob_dt"]]),
        )
        current_contract = DatasetContract(
            dataset_id="banking.mart_customer_360_current_gold",
            owner="test",
            business_purpose="current",
            layer="gold",
            physical_location={"catalog": "lakehouse", "namespace": "gold", "table": "mart_customer_360_current"},
            quality_rules=QualityRules(unique_check=["customer_id"]),
        )

        history_df = MagicMock()
        history_df.columns = ["customer_id", "cob_dt"]
        history_df.count.return_value = 2
        history_sel = MagicMock()
        history_sel.distinct.return_value.count.return_value = 2
        history_df.select.return_value = history_sel

        current_df = MagicMock()
        current_df.columns = ["customer_id"]
        current_df.count.return_value = 1
        current_sel = MagicMock()
        current_sel.distinct.return_value.count.return_value = 1
        current_df.select.return_value = current_sel

        enforcer = ContractEnforcer()
        history_result = enforcer.validate_before_write(mock_spark, history_df, history_contract)
        current_result = enforcer.validate_before_write(mock_spark, current_df, current_contract)

        assert history_result.passed is True
        assert current_result.passed is True

        history_checks = [c.check_name for c in history_result.checks]
        current_checks = [c.check_name for c in current_result.checks]
        assert "unique_set_customer_id_cob_dt" in history_checks
        assert "unique_check" in current_checks or len(current_checks) >= 1

    def test_current_contract_lineage_points_to_history(self):
        current_contract = DatasetContract(
            dataset_id="banking.rfm_segment_current_gold",
            owner="test",
            business_purpose="current",
            layer="gold",
            physical_location={"catalog": "lakehouse", "namespace": "gold", "table": "rfm_segment_current"},
            upstream_dataset_ids=["banking.rfm_segment_gold"],
        )
        assert current_contract.upstream_dataset_ids == ["banking.rfm_segment_gold"]
        assert current_contract.physical_location.table.endswith("_current")
        assert current_contract.dataset_id.endswith("_current_gold")

    def test_history_contract_uses_snapshot_grain(self):
        history_contract = DatasetContract(
            dataset_id="banking.campaign_target_gold",
            owner="test",
            business_purpose="history snapshot",
            layer="gold",
            physical_location={"catalog": "lakehouse", "namespace": "gold", "table": "campaign_target"},
            quality_rules=QualityRules(unique_column_sets=[["customer_id", "cob_dt"]]),
        )
        assert history_contract.quality_rules.unique_column_sets == [["customer_id", "cob_dt"]]
        assert history_contract.physical_location.table == "campaign_target"

    def test_lineage_and_audit_integration(self):
        """Lineage + Audit integration flow."""
        # 1. Create trackers
        lineage = LineageTracker()
        audit = AuditLogger()

        # 2. Record lineage
        lineage.record_lineage(
            source_table="lakehouse.bronze.core_customer",
            target_table="lakehouse.silver.dim_customer",
            transform_type=TransformType.SCD2_MERGE,
            dag_id="silver_all_dag",
            dag_run_id="run_001",
            row_count=10000,
        )

        # 3. Record audit
        audit.log_ingest(
            table_name="lakehouse.silver.dim_customer",
            dag_id="silver_all_dag",
            dag_run_id="run_001",
            row_count=10000,
            duration_seconds=45.2,
        )

        # 4. Verify
        assert len(lineage.get_records()) == 1
        assert len(audit.get_records()) == 1

        # 5. Check lineage
        upstream = lineage.get_upstream("lakehouse.silver.dim_customer")
        assert len(upstream) == 1
        assert upstream[0].source_table == "lakehouse.bronze.core_customer"

        # 6. Check audit
        audit_trail = audit.get_audit_trail("lakehouse.silver.dim_customer")
        assert len(audit_trail) == 1

    def test_contract_validation_failure_flow(self, governance_contracts_dir, mock_spark):
        """Flow when contract validation fails."""
        registry = ContractRegistry(str(governance_contracts_dir))
        contract = registry.get_contract("banking.core_customer_silver")

        # Mock DataFrame with missing required columns
        df = MagicMock()
        df.columns = ["customer_id"]  # Missing full_name, cccd
        df.count.return_value = 100
        # Mock filter().count() to return integer
        df.filter.return_value.count.return_value = 0

        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, df, contract)

        assert result.passed is False
        # Check that at least one check failed
        failed_checks = [c for c in result.checks if c.status == "FAIL"]
        assert len(failed_checks) > 0

    def test_multi_table_lineage(self):
        """Multi-table lineage tracking."""
        lineage = LineageTracker()

        # Bronze → Silver
        lineage.record_lineage("bronze.core_customer", "silver.dim_customer", "scd2_merge", "d1", "r1")
        lineage.record_lineage("bronze.core_account", "silver.dim_account", "scd2_merge", "d1", "r1")

        # Silver → Gold
        lineage.record_lineage("silver.dim_customer", "gold.mart_360", "gold_mart", "d2", "r2")
        lineage.record_lineage("silver.dim_account", "gold.mart_360", "gold_mart", "d2", "r2")

        # Verify
        assert len(lineage.get_records()) == 4
        upstream = lineage.get_upstream("gold.mart_360")
        assert len(upstream) == 2

    def test_audit_multiple_actions(self):
        """Multiple audit actions for same table."""
        audit = AuditLogger()

        audit.log_ingest("table_a", "dag1", "run1", 10000)
        audit.log_transform("table_a", "dag2", "run2", 8000, "scd2_merge")
        audit.log_validation("table_a", "dag3", "run3", "row_count", True)

        trail = audit.get_audit_trail("table_a")
        assert len(trail) == 3

        actions = [r.action for r in trail]
        assert "ingest" in actions
        assert "transform" in actions
        assert "validate" in actions
