"""
Tests for governance.enforcement — Contract validation before write.
"""

import pytest
from unittest.mock import MagicMock, patch
from governance.contracts import DatasetContract, QualityRules
from governance.enforcement import (
    ContractEnforcer,
    ValidationResult,
    CheckResult,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_spark():
    """Create a mock SparkSession."""
    spark = MagicMock()
    spark._sc._jvm = MagicMock()
    return spark


@pytest.fixture
def mock_df_pass():
    """Mock DataFrame that passes all checks."""
    df = MagicMock()
    df.columns = ["customer_id", "full_name", "phone", "email", "cccd"]
    df.count.return_value = 10000
    df.filter.return_value = df
    df.select.return_value = df
    df.distinct.return_value = df
    return df


@pytest.fixture
def mock_df_fail_null():
    """Mock DataFrame with null values."""
    df = MagicMock()
    df.columns = ["customer_id", "full_name", "phone"]
    df.count.return_value = 100

    # Simulate null check: filter(isNull()).count() returns non-zero
    null_df = MagicMock()
    null_df.count.return_value = 5
    df.filter.return_value = null_df

    return df


@pytest.fixture
def mock_df_fail_row_count():
    """Mock DataFrame with too few rows."""
    df = MagicMock()
    df.columns = ["customer_id", "full_name"]
    df.count.return_value = 0  # Empty table
    df.filter.return_value = df
    df.select.return_value = df
    df.distinct.return_value = df
    return df


@pytest.fixture
def contract_pass():
    """Contract that should pass validation."""
    return DatasetContract(
        dataset_id="banking.test_silver",
        owner="test",
        business_purpose="test",
        layer="silver",
        physical_location={"catalog": "lakehouse", "namespace": "silver", "table": "dim_test"},
        quality_rules=QualityRules(
            required_columns=["customer_id", "full_name"],
            non_null_columns=["customer_id"],
            min_row_count=1,
            unique_check=["customer_id"],
        ),
    )


@pytest.fixture
def contract_fail():
    """Contract that should fail validation."""
    return DatasetContract(
        dataset_id="banking.test_silver",
        owner="test",
        business_purpose="test",
        layer="silver",
        physical_location={"catalog": "lakehouse", "namespace": "silver", "table": "dim_test"},
        quality_rules=QualityRules(
            required_columns=["customer_id", "full_name", "missing_col"],
            non_null_columns=["customer_id"],
            min_row_count=5000,
            unique_check=["customer_id"],
        ),
    )


# ---------------------------------------------------------------------------
# Test CheckResult
# ---------------------------------------------------------------------------

class TestCheckResult:
    def test_creation(self):
        result = CheckResult(
            check_name="row_count",
            status="PASS",
            expected="1",
            actual="1000",
            details="Row count OK",
        )
        assert result.check_name == "row_count"
        assert result.status == "PASS"
        assert result.expected == "1"
        assert result.actual == "1000"
        assert result.details == "Row count OK"


# ---------------------------------------------------------------------------
# Test ValidationResult
# ---------------------------------------------------------------------------

class TestValidationResult:
    def test_creation(self):
        result = ValidationResult(dataset_id="banking.test")
        assert result.dataset_id == "banking.test"
        assert result.passed is True
        assert result.checks == []

    def test_add_pass_check(self):
        result = ValidationResult(dataset_id="banking.test")
        check = CheckResult(check_name="test", status="PASS", expected="1", actual="1")
        result.add_check(check)
        assert result.passed is True
        assert result.pass_count == 1

    def test_add_fail_check(self):
        result = ValidationResult(dataset_id="banking.test")
        check = CheckResult(check_name="test", status="FAIL", expected="1", actual="0")
        result.add_check(check)
        assert result.passed is False
        assert result.fail_count == 1

    def test_add_warn_check(self):
        result = ValidationResult(dataset_id="banking.test")
        check = CheckResult(check_name="test", status="WARN", expected="1", actual="0")
        result.add_check(check)
        assert result.passed is True  # WARN doesn't fail
        assert result.warn_count == 1

    def test_summary(self):
        result = ValidationResult(dataset_id="banking.test")
        result.add_check(CheckResult(check_name="c1", status="PASS", expected="1", actual="1"))
        result.add_check(CheckResult(check_name="c2", status="FAIL", expected="1", actual="0"))
        summary = result.summary()
        assert "banking.test" in summary
        assert "FAIL" in summary

    def test_to_dict(self):
        result = ValidationResult(dataset_id="banking.test")
        result.add_check(CheckResult(check_name="c1", status="PASS", expected="1", actual="1"))
        d = result.to_dict()
        assert isinstance(d, dict)
        assert d["dataset_id"] == "banking.test"
        assert len(d["checks"]) == 1


# ---------------------------------------------------------------------------
# Test ContractEnforcer
# ---------------------------------------------------------------------------

class TestContractEnforcer:
    def test_validate_pass_required_columns(self, mock_spark, mock_df_pass, contract_pass):
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, mock_df_pass, contract_pass)
        # Check that required_columns check was run
        check_names = [c.check_name for c in result.checks]
        assert "required_columns" in check_names

    def test_validate_fail_required_columns(self, mock_spark, mock_df_pass, contract_fail):
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, mock_df_pass, contract_fail)
        # Should fail because missing_col doesn't exist
        required_check = [c for c in result.checks if c.check_name == "required_columns"][0]
        assert required_check.status == "FAIL"

    def test_validate_non_null_check(self, mock_spark, mock_df_pass, contract_pass):
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, mock_df_pass, contract_pass)
        null_check = [c for c in result.checks if c.check_name == "non_null_columns"]
        assert len(null_check) == 1

    def test_validate_row_count(self, mock_spark, mock_df_pass, contract_pass):
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, mock_df_pass, contract_pass)
        row_count_check = [c for c in result.checks if c.check_name == "row_count"]
        assert len(row_count_check) == 1
        assert row_count_check[0].status == "PASS"

    def test_validate_row_count_fail(self, mock_spark, mock_df_fail_row_count, contract_fail):
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, mock_df_fail_row_count, contract_fail)
        row_count_check = [c for c in result.checks if c.check_name == "row_count"]
        assert len(row_count_check) == 1
        assert row_count_check[0].status == "FAIL"

    def test_validate_unique_check(self, mock_spark, mock_df_pass, contract_pass):
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, mock_df_pass, contract_pass)
        unique_check = [c for c in result.checks if c.check_name == "unique_check"]
        assert len(unique_check) == 1

    def test_validate_no_rules(self, mock_spark, mock_df_pass):
        """Contract with no quality rules should pass all checks."""
        contract = DatasetContract(
            dataset_id="banking.test",
            owner="test",
            business_purpose="test",
            layer="silver",
            physical_location={"catalog": "lakehouse", "namespace": "silver", "table": "dim_test"},
        )
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, mock_df_pass, contract)
        assert result.passed is True
        assert len(result.checks) == 0

    def test_validate_with_range_check(self, mock_spark, mock_df_pass):
        """Contract with range check should run range validation."""
        from governance.contracts import RangeCheck
        contract = DatasetContract(
            dataset_id="banking.test",
            owner="test",
            business_purpose="test",
            layer="silver",
            physical_location={"catalog": "lakehouse", "namespace": "silver", "table": "dim_test"},
            quality_rules=QualityRules(
                range_checks=[RangeCheck(column="balance", min_value=0, max_value=1000000)],
            ),
        )
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, mock_df_pass, contract)
        range_check = [c for c in result.checks if c.check_name == "range_balance"]
        assert len(range_check) == 1

    def test_validate_column_not_found(self, mock_spark):
        """Column not found in DataFrame should return FAIL."""
        df = MagicMock()
        df.columns = ["col_a", "col_b"]
        df.count.return_value = 100

        contract = DatasetContract(
            dataset_id="banking.test",
            owner="test",
            business_purpose="test",
            layer="silver",
            physical_location={"catalog": "lakehouse", "namespace": "silver", "table": "dim_test"},
            quality_rules=QualityRules(
                required_columns=["nonexistent_col"],
            ),
        )
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(spark=None, df=df, contract=contract)
        assert result.passed is False

    def test_validate_composite_unique_column_set_pass(self, mock_spark):
        df = MagicMock()
        df.columns = ["customer_id", "cob_dt"]
        df.count.return_value = 100
        selected_df = MagicMock()
        selected_df.distinct.return_value.count.return_value = 100
        df.select.return_value = selected_df

        contract = DatasetContract(
            dataset_id="banking.test_gold",
            owner="test",
            business_purpose="test",
            layer="gold",
            physical_location={"catalog": "lakehouse", "namespace": "gold", "table": "test"},
            quality_rules=QualityRules(unique_column_sets=[["customer_id", "cob_dt"]]),
        )
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, df, contract)
        composite_checks = [c for c in result.checks if c.check_name == "unique_set_customer_id_cob_dt"]
        assert len(composite_checks) == 1
        assert composite_checks[0].status == "PASS"

    def test_validate_composite_unique_column_set_fail(self, mock_spark):
        df = MagicMock()
        df.columns = ["customer_id", "cob_dt"]
        df.count.return_value = 100
        selected_df = MagicMock()
        selected_df.distinct.return_value.count.return_value = 90
        df.select.return_value = selected_df

        contract = DatasetContract(
            dataset_id="banking.test_gold",
            owner="test",
            business_purpose="test",
            layer="gold",
            physical_location={"catalog": "lakehouse", "namespace": "gold", "table": "test"},
            quality_rules=QualityRules(unique_column_sets=[["customer_id", "cob_dt"]]),
        )
        enforcer = ContractEnforcer()
        result = enforcer.validate_before_write(mock_spark, df, contract)
        composite_checks = [c for c in result.checks if c.check_name == "unique_set_customer_id_cob_dt"]
        assert len(composite_checks) == 1
        assert composite_checks[0].status == "FAIL"
