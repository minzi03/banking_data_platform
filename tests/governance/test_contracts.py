"""
Tests for governance.contracts — Pydantic models for dataset contracts.
"""

import pytest
from governance.contracts import (
    DatasetContract,
    QualityRules,
    AIGovernance,
    PhysicalLocation,
    QualityClass,
    RefreshSLA,
    Severity,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_contract_data():
    """Valid contract data for testing."""
    return {
        "dataset_id": "banking.test_customer_silver",
        "owner": "Data Engineering Team",
        "business_purpose": "Test customer dimension",
        "refresh_sla": "daily",
        "quality_class": "critical",
        "layer": "silver",
        "physical_location": {
            "catalog": "lakehouse",
            "namespace": "silver",
            "table": "dim_customer",
        },
    }


@pytest.fixture
def valid_quality_rules():
    """Valid quality rules for testing."""
    return {
        "required_columns": ["customer_id", "full_name"],
        "non_null_columns": ["customer_id"],
        "min_row_count": 100,
        "unique_check": ["customer_id"],
    }


@pytest.fixture
def valid_ai_governance():
    """Valid AI governance for testing."""
    return {
        "ai_use_allowed": True,
        "risk_tier": "limited_risk",
        "intended_uses": ["analytics", "reporting"],
        "prohibited_uses": ["automated_decision"],
    }


# ---------------------------------------------------------------------------
# Test PhysicalLocation
# ---------------------------------------------------------------------------

class TestPhysicalLocation:
    def test_creation(self):
        loc = PhysicalLocation(catalog="lakehouse", namespace="silver", table="dim_customer")
        assert loc.catalog == "lakehouse"
        assert loc.namespace == "silver"
        assert loc.table == "dim_customer"

    def test_full_table_name(self):
        loc = PhysicalLocation(catalog="lakehouse", namespace="silver", table="dim_customer")
        assert loc.full_table_name == "lakehouse.silver.dim_customer"

    def test_missing_catalog(self):
        with pytest.raises(Exception):
            PhysicalLocation(namespace="silver", table="dim_customer")


# ---------------------------------------------------------------------------
# Test Enums
# ---------------------------------------------------------------------------

class TestEnums:
    def test_quality_class_values(self):
        assert QualityClass.CRITICAL == "critical"
        assert QualityClass.IMPORTANT == "important"
        assert QualityClass.INFORMATIONAL == "informational"

    def test_refresh_sla_values(self):
        assert RefreshSLA.HOURLY == "hourly"
        assert RefreshSLA.DAILY == "daily"
        assert RefreshSLA.WEEKLY == "weekly"
        assert RefreshSLA.MONTHLY == "monthly"

    def test_severity_values(self):
        assert Severity.FAIL == "FAIL"
        assert Severity.WARN == "WARN"


# ---------------------------------------------------------------------------
# Test QualityRules
# ---------------------------------------------------------------------------

class TestQualityRules:
    def test_creation_with_defaults(self):
        rules = QualityRules()
        assert rules.required_columns == []
        assert rules.non_null_columns == []
        assert rules.min_row_count is None
        assert rules.unique_check == []

    def test_creation_with_values(self, valid_quality_rules):
        rules = QualityRules(**valid_quality_rules)
        assert rules.required_columns == ["customer_id", "full_name"]
        assert rules.non_null_columns == ["customer_id"]
        assert rules.min_row_count == 100
        assert rules.unique_check == ["customer_id"]

    def test_composite_unique_column_sets(self):
        rules = QualityRules(unique_column_sets=[["customer_id", "cob_dt"]])
        assert rules.unique_column_sets == [["customer_id", "cob_dt"]]

    def test_range_checks(self):
        rules = QualityRules(
            range_checks=[{"column": "balance", "min_value": 0, "max_value": 1000000}]
        )
        assert len(rules.range_checks) == 1
        assert rules.range_checks[0].column == "balance"


# ---------------------------------------------------------------------------
# Test AIGovernance
# ---------------------------------------------------------------------------

class TestAIGovernance:
    def test_creation_with_defaults(self):
        gov = AIGovernance()
        assert gov.ai_use_allowed is True
        assert gov.risk_tier == "limited_risk"
        assert gov.intended_uses == []
        assert gov.prohibited_uses == []

    def test_creation_with_values(self, valid_ai_governance):
        gov = AIGovernance(**valid_ai_governance)
        assert gov.ai_use_allowed is True
        assert gov.risk_tier == "limited_risk"
        assert "analytics" in gov.intended_uses
        assert "automated_decision" in gov.prohibited_uses

    def test_human_oversight(self):
        gov = AIGovernance(human_oversight_required=True)
        assert gov.human_oversight_required is True


# ---------------------------------------------------------------------------
# Test DatasetContract
# ---------------------------------------------------------------------------

class TestDatasetContract:
    def test_creation(self, valid_contract_data):
        contract = DatasetContract(**valid_contract_data)
        assert contract.dataset_id == "banking.test_customer_silver"
        assert contract.owner == "Data Engineering Team"
        assert contract.layer == "silver"

    def test_creation_with_all_fields(self, valid_contract_data):
        data = valid_contract_data.copy()
        data["quality_rules"] = {"required_columns": ["customer_id"]}
        data["ai_governance"] = {"ai_use_allowed": True}
        contract = DatasetContract(**data)
        assert contract.quality_rules.required_columns == ["customer_id"]
        assert contract.ai_governance.ai_use_allowed is True

    def test_missing_required_fields(self):
        with pytest.raises(Exception):
            DatasetContract()

    def test_missing_dataset_id(self):
        with pytest.raises(Exception):
            DatasetContract(owner="test", business_purpose="test", layer="silver",
                          physical_location={"catalog": "l", "namespace": "s", "table": "t"})

    def test_to_dict(self, valid_contract_data):
        contract = DatasetContract(**valid_contract_data)
        d = contract.to_dict()
        assert isinstance(d, dict)
        assert d["dataset_id"] == "banking.test_customer_silver"
        assert d["layer"] == "silver"

    def test_defaults(self, valid_contract_data):
        contract = DatasetContract(**valid_contract_data)
        assert contract.refresh_sla == "daily"
        assert contract.quality_class == "critical"
        assert contract.upstream_dataset_ids == []
        assert contract.dag_id is None

    def test_enum_values(self, valid_contract_data):
        valid_contract_data["refresh_sla"] = "weekly"
        valid_contract_data["quality_class"] = "important"
        contract = DatasetContract(**valid_contract_data)
        assert contract.refresh_sla == "weekly"
        assert contract.quality_class == "important"
