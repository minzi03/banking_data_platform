"""
Tests for governance.contracts_registry — Contract loading and validation.
"""

import pytest

from governance.contracts_registry import ContractRegistry

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_contract_yaml(tmp_path):
    """Create a valid contract YAML file."""
    content = """
dataset_id: banking.test_customer_silver
owner: Data Engineering Team
business_purpose: Test customer dimension
refresh_sla: daily
quality_class: critical
layer: silver
physical_location:
  catalog: lakehouse
  namespace: silver
  table: dim_customer
"""
    yaml_file = tmp_path / "test_customer_silver.yaml"
    yaml_file.write_text(content, encoding="utf-8")
    return tmp_path


@pytest.fixture
def invalid_contract_yaml(tmp_path):
    """Create an invalid contract YAML file (missing required fields)."""
    content = """
dataset_id: banking.test_invalid
owner: test
"""
    yaml_file = tmp_path / "test_invalid.yaml"
    yaml_file.write_text(content, encoding="utf-8")
    return tmp_path


@pytest.fixture
def empty_yaml(tmp_path):
    """Create an empty YAML file."""
    yaml_file = tmp_path / "empty.yaml"
    yaml_file.write_text("", encoding="utf-8")
    return tmp_path


@pytest.fixture
def syntax_error_yaml(tmp_path):
    """Create a YAML file with syntax error."""
    content = """
dataset_id: banking.test
owner: test
  invalid_indent: wrong
"""
    yaml_file = tmp_path / "syntax_error.yaml"
    yaml_file.write_text(content, encoding="utf-8")
    return tmp_path


@pytest.fixture
def multiple_contracts_yaml(tmp_path):
    """Create multiple valid contract YAML files."""
    contracts = [
        {
            "filename": "customer_silver.yaml",
            "content": """
dataset_id: banking.core_customer_silver
owner: Data Engineering Team
business_purpose: Customer dimension
layer: silver
physical_location:
  catalog: lakehouse
  namespace: silver
  table: dim_customer
"""
        },
        {
            "filename": "account_silver.yaml",
            "content": """
dataset_id: banking.core_account_silver
owner: Data Engineering Team
business_purpose: Account dimension
layer: silver
physical_location:
  catalog: lakehouse
  namespace: silver
  table: dim_account
"""
        },
        {
            "filename": "mart_gold.yaml",
            "content": """
dataset_id: banking.mart_customer_360_gold
owner: Analytics Team
business_purpose: Customer 360 view
layer: gold
physical_location:
  catalog: lakehouse
  namespace: gold
  table: mart_customer_360
"""
        },
    ]
    for contract in contracts:
        yaml_file = tmp_path / contract["filename"]
        yaml_file.write_text(contract["content"], encoding="utf-8")
    return tmp_path


# ---------------------------------------------------------------------------
# Test ContractRegistry
# ---------------------------------------------------------------------------

class TestContractRegistry:
    def test_load_valid_contracts(self, valid_contract_yaml):
        registry = ContractRegistry(str(valid_contract_yaml))
        assert registry.contract_count == 1
        assert not registry.has_errors

    def test_load_multiple_contracts(self, multiple_contracts_yaml):
        registry = ContractRegistry(str(multiple_contracts_yaml))
        assert registry.contract_count == 3
        assert not registry.has_errors

    def test_load_empty_directory(self, tmp_path):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        registry = ContractRegistry(str(empty_dir))
        assert registry.contract_count == 0

    def test_load_nonexistent_directory(self):
        registry = ContractRegistry("/nonexistent/path")
        assert registry.contract_count == 0

    def test_get_contract_valid(self, valid_contract_yaml):
        registry = ContractRegistry(str(valid_contract_yaml))
        contract = registry.get_contract("banking.test_customer_silver")
        assert contract is not None
        assert contract.dataset_id == "banking.test_customer_silver"

    def test_get_contract_invalid_id(self, valid_contract_yaml):
        registry = ContractRegistry(str(valid_contract_yaml))
        contract = registry.get_contract("banking.nonexistent")
        assert contract is None

    def test_get_contracts_by_layer(self, multiple_contracts_yaml):
        registry = ContractRegistry(str(multiple_contracts_yaml))
        silver_contracts = registry.get_contracts_by_layer("silver")
        assert len(silver_contracts) == 2
        gold_contracts = registry.get_contracts_by_layer("gold")
        assert len(gold_contracts) == 1

    def test_get_contracts_by_owner(self, multiple_contracts_yaml):
        registry = ContractRegistry(str(multiple_contracts_yaml))
        de_contracts = registry.get_contracts_by_owner("Data Engineering Team")
        assert len(de_contracts) == 2
        analytics_contracts = registry.get_contracts_by_owner("Analytics Team")
        assert len(analytics_contracts) == 1

    def test_get_all_contracts(self, multiple_contracts_yaml):
        registry = ContractRegistry(str(multiple_contracts_yaml))
        all_contracts = registry.get_all_contracts()
        assert len(all_contracts) == 3
        assert isinstance(all_contracts, dict)

    def test_validate_all(self, valid_contract_yaml):
        registry = ContractRegistry(str(valid_contract_yaml))
        issues = registry.validate_all()
        assert isinstance(issues, list)
        assert len(issues) == 0

    def test_summary(self, valid_contract_yaml):
        registry = ContractRegistry(str(valid_contract_yaml))
        summary = registry.summary()
        assert "Contract Registry Summary" in summary
        assert "1 contracts" in summary

    def test_current_contract_filename_loads(self, tmp_path):
        content = """
dataset_id: banking.mart_customer_360_current_gold
owner: Data Engineering Team
business_purpose: Current serving table
layer: gold
physical_location:
  catalog: lakehouse
  namespace: gold
  table: mart_customer_360_current
"""
        yaml_file = tmp_path / "banking.mart_customer_360_current_gold.yaml"
        yaml_file.write_text(content, encoding="utf-8")
        registry = ContractRegistry(str(tmp_path))
        contract = registry.get_contract("banking.mart_customer_360_current_gold")
        assert contract is not None
        assert contract.physical_location.table == "mart_customer_360_current"

    def test_has_errors_false(self, valid_contract_yaml):
        registry = ContractRegistry(str(valid_contract_yaml))
        assert registry.has_errors is False

    def test_errors_empty(self, valid_contract_yaml):
        registry = ContractRegistry(str(valid_contract_yaml))
        assert registry.errors == []
