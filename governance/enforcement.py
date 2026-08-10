"""
Contract Enforcement — Banking Data Platform

Validates DataFrames against dataset contracts before writing to the lakehouse.
Blocks pipeline on FAIL, logs warning on WARN.

Usage:
    from governance.contracts_registry import ContractRegistry
    from governance.enforcement import ContractEnforcer

    registry = ContractRegistry()
    contract = registry.get_contract("banking.core_customer_silver")

    enforcer = ContractEnforcer()
    result = enforcer.validate_before_write(spark, df, contract)

    if not result.passed:
        raise ValueError(f"Contract validation failed: {result.summary()}")
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging import getLogger
from typing import Any

from governance.contracts import DatasetContract

log = getLogger("enforcement")


# ---------------------------------------------------------------------------
# Validation Result
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    """Result of a single check."""
    check_name: str
    status: str          # "PASS", "WARN", "FAIL"
    expected: str
    actual: str
    details: str = ""


@dataclass
class ValidationResult:
    """Aggregated result of all contract checks."""
    dataset_id: str
    passed: bool = True
    checks: list[CheckResult] = field(default_factory=list)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def add_check(self, check: CheckResult) -> None:
        self.checks.append(check)
        if check.status == "FAIL":
            self.passed = False

    @property
    def pass_count(self) -> int:
        return sum(1 for c in self.checks if c.status == "PASS")

    @property
    def warn_count(self) -> int:
        return sum(1 for c in self.checks if c.status == "WARN")

    @property
    def fail_count(self) -> int:
        return sum(1 for c in self.checks if c.status == "FAIL")

    def summary(self) -> str:
        lines = [
            f"Contract Validation: {self.dataset_id}",
            f"  Result: {'PASS' if self.passed else 'FAIL'}",
            f"  Checks: {self.pass_count} PASS, {self.warn_count} WARN, {self.fail_count} FAIL",
        ]
        for check in self.checks:
            icon = "✅" if check.status == "PASS" else "⚠️" if check.status == "WARN" else "❌"
            lines.append(f"    {icon} {check.check_name}: {check.details}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "passed": self.passed,
            "timestamp": self.timestamp,
            "checks": [
                {
                    "check_name": c.check_name,
                    "status": c.status,
                    "expected": c.expected,
                    "actual": c.actual,
                    "details": c.details,
                }
                for c in self.checks
            ],
        }


# ---------------------------------------------------------------------------
# Contract Enforcer
# ---------------------------------------------------------------------------

class ContractEnforcer:
    """
    Validates DataFrames against dataset contracts.

    Checks:
    1. Schema validation (required_columns, non_null_columns)
    2. Row count validation (min_row_count, max_row_count)
    3. Date validation (forbid_future_dates)
    4. Uniqueness validation (unique_check)
    5. Range validation (range_checks)
    6. Referential integrity (referential_integrity)
    """

    def validate_before_write(
        self,
        spark,
        df,
        contract: DatasetContract,
    ) -> ValidationResult:
        """
        Validate a DataFrame against a dataset contract.

        Args:
            spark: SparkSession
            df: DataFrame to validate
            contract: DatasetContract to validate against

        Returns:
            ValidationResult with all check results
        """
        result = ValidationResult(dataset_id=contract.dataset_id)
        rules = contract.quality_rules

        # 1. Schema validation — required columns
        if rules.required_columns:
            self._check_required_columns(df, rules.required_columns, result)

        # 2. Null check
        if rules.non_null_columns:
            self._check_non_null(df, rules.non_null_columns, result)

        # 3. Row count validation
        if rules.min_row_count is not None or rules.max_row_count is not None:
            self._check_row_count(df, rules.min_row_count, rules.max_row_count, result)

        # 4. Uniqueness validation (single columns)
        if rules.unique_check:
            self._check_unique(df, rules.unique_check, result)

        # 5. Composite uniqueness validation
        if rules.unique_column_sets:
            for column_set in rules.unique_column_sets:
                self._check_unique_column_set(df, column_set, result)

        # 6. Date validation
        if rules.date_column and rules.forbid_future_dates:
            self._check_future_dates(df, rules.date_column, result)

        # 7. Range checks
        for range_check in rules.range_checks:
            self._check_range(df, range_check.column, range_check.min_value, range_check.max_value, result)

        # 8. Referential integrity
        for ref in rules.referential_integrity:
            self._check_referential_integrity(
                spark, df, ref.column, ref.ref_table, ref.ref_column, result
            )

        # 9. Freshness check (via SLA from contract)
        if rules.freshness_sla_hours and rules.date_column:
            self._check_freshness(
                spark, contract.physical_location.full_table_name,
                rules.freshness_sla_hours, rules.date_column, result,
            )

        return result

    # -----------------------------------------------------------------------
    # Individual Check Methods
    # -----------------------------------------------------------------------

    def _check_required_columns(
        self, df, required_columns: list[str], result: ValidationResult
    ) -> None:
        """Check that required columns exist in the DataFrame."""
        actual_columns = set(df.columns)
        missing = [col for col in required_columns if col not in actual_columns]

        if missing:
            result.add_check(CheckResult(
                check_name="required_columns",
                status="FAIL",
                expected=str(required_columns),
                actual=str(list(actual_columns)),
                details=f"Missing columns: {missing}",
            ))
        else:
            result.add_check(CheckResult(
                check_name="required_columns",
                status="PASS",
                expected=str(required_columns),
                actual=str(required_columns),
                details=f"All {len(required_columns)} required columns present",
            ))

    def _check_non_null(
        self, df, non_null_columns: list[str], result: ValidationResult
    ) -> None:
        """Check that specified columns have no NULL values."""
        issues = []
        for col in non_null_columns:
            if col not in df.columns:
                issues.append(f"{col}: column not found")
                continue
            null_count = df.filter(df[col].isNull()).count()
            if null_count > 0:
                issues.append(f"{col}: {null_count} nulls")

        if issues:
            result.add_check(CheckResult(
                check_name="non_null_columns",
                status="FAIL",
                expected="0 nulls",
                actual="; ".join(issues),
                details=f"Null violations: {len(issues)} columns",
            ))
        else:
            result.add_check(CheckResult(
                check_name="non_null_columns",
                status="PASS",
                expected="0 nulls",
                actual="0 nulls",
                details=f"All {len(non_null_columns)} columns non-null",
            ))

    def _check_row_count(
        self,
        df,
        min_rows: int | None,
        max_rows: int | None,
        result: ValidationResult,
    ) -> None:
        """Check row count is within expected bounds."""
        count = df.count()
        issues = []

        if min_rows is not None and count < min_rows:
            issues.append(f"Below minimum: {count} < {min_rows}")
        if max_rows is not None and count > max_rows:
            issues.append(f"Above maximum: {count} > {max_rows}")

        expected = f"[{min_rows or '∞'}, {max_rows or '∞'}]"

        if issues:
            result.add_check(CheckResult(
                check_name="row_count",
                status="FAIL",
                expected=expected,
                actual=str(count),
                details="; ".join(issues),
            ))
        else:
            result.add_check(CheckResult(
                check_name="row_count",
                status="PASS",
                expected=expected,
                actual=str(count),
                details=f"Row count OK: {count}",
            ))

    def _check_unique(
        self, df, unique_columns: list[str], result: ValidationResult
    ) -> None:
        """Check that specified columns have unique values."""
        total = df.count()
        issues = []

        for col in unique_columns:
            if col not in df.columns:
                issues.append(f"{col}: column not found")
                continue
            distinct = df.select(col).distinct().count()
            if total != distinct:
                dup_count = total - distinct
                issues.append(f"{col}: {dup_count} duplicates")

        if issues:
            result.add_check(CheckResult(
                check_name="unique_check",
                status="FAIL",
                expected="0 duplicates",
                actual="; ".join(issues),
                details=f"Uniqueness violations: {len(issues)} columns",
            ))
        else:
            result.add_check(CheckResult(
                check_name="unique_check",
                status="PASS",
                expected="0 duplicates",
                actual="0 duplicates",
                details=f"All {len(unique_columns)} columns unique",
            ))

    def _check_unique_column_set(
        self, df, columns: list[str], result: ValidationResult
    ) -> None:
        """Check that the combination of columns is unique."""
        missing = [col for col in columns if col not in df.columns]
        check_name = f"unique_set_{'_'.join(columns)}"

        if missing:
            result.add_check(CheckResult(
                check_name=check_name,
                status="FAIL",
                expected=f"Unique combination: {columns}",
                actual=f"Missing columns: {missing}",
                details=f"Columns not found in DataFrame: {missing}",
            ))
            return

        total = df.count()
        distinct = df.select(*columns).distinct().count()

        if total != distinct:
            dup_count = total - distinct
            result.add_check(CheckResult(
                check_name=check_name,
                status="FAIL",
                expected="0 duplicate key combinations",
                actual=f"{dup_count} duplicates",
                details=f"Duplicate combinations found for columns {columns}: {dup_count}",
            ))
        else:
            result.add_check(CheckResult(
                check_name=check_name,
                status="PASS",
                expected="0 duplicate key combinations",
                actual="0 duplicate key combinations",
                details=f"All combinations unique for columns {columns}",
            ))

    def _check_future_dates(
        self, df, date_column: str, result: ValidationResult
    ) -> None:
        """Check that date column has no future dates."""
        if date_column not in df.columns:
            result.add_check(CheckResult(
                check_name="future_dates",
                status="FAIL",
                expected="no future dates",
                actual=f"column '{date_column}' not found",
                details=f"Column '{date_column}' not found in DataFrame",
            ))
            return

        from pyspark.sql import functions as F

        today = F.current_date()
        future_count = df.filter(F.col(date_column) > today).count()

        if future_count > 0:
            result.add_check(CheckResult(
                check_name="future_dates",
                status="FAIL",
                expected="0 future dates",
                actual=f"{future_count} future dates",
                details=f"Found {future_count} rows with dates after today",
            ))
        else:
            result.add_check(CheckResult(
                check_name="future_dates",
                status="PASS",
                expected="0 future dates",
                actual="0 future dates",
                details="No future dates found",
            ))

    def _check_range(
        self,
        df,
        column: str,
        min_value: float | None,
        max_value: float | None,
        result: ValidationResult,
    ) -> None:
        """Check that column values are within min/max bounds."""
        if column not in df.columns:
            result.add_check(CheckResult(
                check_name=f"range_{column}",
                status="FAIL",
                expected=f"[{min_value}, {max_value}]",
                actual=f"column '{column}' not found",
                details=f"Column '{column}' not found in DataFrame",
            ))
            return

        from pyspark.sql import functions as F

        conditions = []
        if min_value is not None:
            conditions.append(F.col(column) < min_value)
        if max_value is not None:
            conditions.append(F.col(column) > max_value)

        if conditions:
            out_of_range = df.filter(F.or_(*conditions)).count()
        else:
            out_of_range = 0

        expected = f"[{min_value or '∞'}, {max_value or '∞'}]"

        if out_of_range > 0:
            result.add_check(CheckResult(
                check_name=f"range_{column}",
                status="FAIL",
                expected=expected,
                actual=f"{out_of_range} out of range",
                details=f"{out_of_range} values outside range {expected}",
            ))
        else:
            result.add_check(CheckResult(
                check_name=f"range_{column}",
                status="PASS",
                expected=expected,
                actual="all in range",
                details=f"All values within range {expected}",
            ))

    def _check_referential_integrity(
        self,
        spark,
        df,
        column: str,
        ref_table: str,
        ref_column: str,
        result: ValidationResult,
    ) -> None:
        """Check that FK column values exist in referenced table."""
        if column not in df.columns:
            result.add_check(CheckResult(
                check_name=f"fk_{column}",
                status="FAIL",
                expected=f"FK to {ref_table}.{ref_column}",
                actual=f"column '{column}' not found",
                details=f"Column '{column}' not found in DataFrame",
            ))
            return

        try:
            ref_df = spark.table(ref_table)

            if ref_column not in ref_df.columns:
                result.add_check(CheckResult(
                    check_name=f"fk_{column}",
                    status="FAIL",
                    expected=f"FK to {ref_table}.{ref_column}",
                    actual=f"ref column '{ref_column}' not found in {ref_table}",
                    details=f"Reference column '{ref_column}' not found in {ref_table}",
                ))
                return

            source_vals = df.select(column).distinct()
            ref_vals = ref_df.select(ref_column).distinct()
            orphans = source_vals.join(
                ref_vals, source_vals[column] == ref_vals[ref_column], "left_anti"
            ).count()

            if orphans > 0:
                result.add_check(CheckResult(
                    check_name=f"fk_{column}",
                    status="FAIL",
                    expected="0 orphans",
                    actual=f"{orphans} orphans",
                    details=f"{orphans} records: {column} not in {ref_table}.{ref_column}",
                ))
            else:
                result.add_check(CheckResult(
                    check_name=f"fk_{column}",
                    status="PASS",
                    expected="0 orphans",
                    actual="0 orphans",
                    details=f"All FK values exist in {ref_table}.{ref_column}",
                ))
        except Exception as e:
            result.add_check(CheckResult(
                check_name=f"fk_{column}",
                status="WARN",
                expected=f"FK to {ref_table}.{ref_column}",
                actual="error",
                details=f"Could not validate FK: {e}",
            ))

    def _check_freshness(
        self,
        spark,
        table_name: str,
        sla_hours: int,
        date_column: str,
        result: ValidationResult,
    ) -> None:
        """Check data freshness using FreshnessChecker."""
        try:
            from governance.freshness_checks import FreshnessChecker
            checker = FreshnessChecker()
            fres = checker.check_freshness(spark, table_name, sla_hours, date_column)

            if fres.status == "FAIL":
                result.add_check(CheckResult(
                    check_name="freshness",
                    status="FAIL",
                    expected=f"Data within {sla_hours}h",
                    actual=fres.details,
                    details=fres.details,
                ))
            elif fres.status == "WARN":
                result.add_check(CheckResult(
                    check_name="freshness",
                    status="WARN",
                    expected=f"Data within {sla_hours}h",
                    actual=fres.details,
                    details=fres.details,
                ))
            else:
                result.add_check(CheckResult(
                    check_name="freshness",
                    status="PASS",
                    expected=f"Data within {sla_hours}h",
                    actual=fres.details,
                    details=fres.details,
                ))
        except Exception as e:
            result.add_check(CheckResult(
                check_name="freshness",
                status="WARN",
                expected=f"Freshness within {sla_hours}h",
                actual="error",
                details=f"Could not check freshness: {e}",
            ))
