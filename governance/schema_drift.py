"""
Schema Drift Detection — Banking Data Platform

Detects schema changes by comparing current table schema against
expected schema defined in contracts.

Usage:
    from governance.schema_drift import SchemaDriftDetector

    detector = SchemaDriftDetector()
    result = detector.detect_drift(spark, "lakehouse.silver.dim_customer",
                                   expected_columns=["customer_id", "full_name", ...])
"""

from dataclasses import dataclass, field
from logging import getLogger
from typing import Dict, List, Optional, Set, Tuple

log = getLogger("schema_drift")


@dataclass
class SchemaDriftResult:
    """Result of schema drift detection."""
    check_name: str
    status: str          # "PASS", "WARN", "FAIL"
    details: str
    added_columns: List[str] = field(default_factory=list)
    removed_columns: List[str] = field(default_factory=list)
    type_changes: List[Dict] = field(default_factory=list)
    current_columns: List[str] = field(default_factory=list)
    expected_columns: List[str] = field(default_factory=list)


class SchemaDriftDetector:
    """
    Schema drift detection by comparing current schema against expected.

    Detects:
    - Added columns (new columns not in expected schema)
    - Removed columns (expected columns missing from current schema)
    - Type changes (columns with different data types)
    """

    def detect_drift(
        self,
        spark,
        table: str,
        expected_columns: List[str],
        expected_types: Optional[Dict[str, str]] = None,
        ignore_case: bool = True,
    ) -> SchemaDriftResult:
        """
        Detect schema drift by comparing current vs expected schema.

        Args:
            spark: SparkSession
            table: Full table name
            expected_columns: List of expected column names
            expected_types: Optional dict of expected column types
                           (e.g., {"customer_id": "string", "balance": "double"})
            ignore_case: Whether to ignore case when comparing column names

        Returns:
            SchemaDriftResult with drift details
        """
        try:
            df = spark.table(table)
        except Exception as e:
            return SchemaDriftResult(
                check_name="schema_drift",
                status="FAIL",
                details=f"Could not read table: {e}",
            )

        current_columns = list(df.columns)
        current_types = dict(df.dtypes)

        # Normalize for comparison
        if ignore_case:
            current_set = {c.lower(): c for c in current_columns}
            expected_set = {c.lower(): c for c in expected_columns}
        else:
            current_set = {c: c for c in current_columns}
            expected_set = {c: c for c in expected_columns}

        current_keys = set(current_set.keys())
        expected_keys = set(expected_set.keys())

        # Find drift
        added = list(current_keys - expected_keys)
        removed = list(expected_keys - current_keys)
        common = current_keys & expected_keys

        # Check type changes
        type_changes = []
        if expected_types:
            for col_key in common:
                current_col = current_set[col_key]
                expected_col = expected_set[col_key]

                # Look up expected type
                expected_type = None
                for exp_col, exp_type in expected_types.items():
                    if exp_col.lower() == expected_col.lower():
                        expected_type = exp_type
                        break

                if expected_type:
                    actual_type = current_types.get(current_col, "unknown")
                    if expected_type.lower() not in actual_type.lower():
                        type_changes.append({
                            "column": current_col,
                            "expected_type": expected_type,
                            "actual_type": actual_type,
                        })

        # Determine status
        if removed or type_changes:
            status = "FAIL"
        elif added:
            status = "WARN"
        else:
            status = "PASS"

        # Build details
        issues = []
        if added:
            actual_names = [current_set[a] for a in added]
            issues.append(f"Added columns: {actual_names}")
        if removed:
            issues.append(f"Removed columns: {removed}")
        if type_changes:
            for tc in type_changes:
                issues.append(
                    f"Type change: {tc['column']} "
                    f"({tc['expected_type']} → {tc['actual_type']})"
                )

        details = "; ".join(issues) if issues else "Schema matches expected"

        return SchemaDriftResult(
            check_name="schema_drift",
            status=status,
            details=details,
            added_columns=[current_set[a] for a in added],
            removed_columns=removed,
            type_changes=type_changes,
            current_columns=current_columns,
            expected_columns=expected_columns,
        )

    def detect_drift_from_contract(
        self,
        spark,
        table: str,
        required_columns: List[str],
        non_null_columns: Optional[List[str]] = None,
    ) -> SchemaDriftResult:
        """
        Detect drift using contract quality rules.

        Args:
            spark: SparkSession
            table: Full table name
            required_columns: Columns that must exist
            non_null_columns: Columns that must not be null (for type inference)

        Returns:
            SchemaDriftResult
        """
        return self.detect_drift(
            spark=spark,
            table=table,
            expected_columns=required_columns,
        )

    def get_schema_diff(
        self,
        spark,
        table: str,
        expected_columns: List[str],
    ) -> Dict[str, Set[str]]:
        """
        Get a diff of current vs expected schema.

        Returns:
            Dict with 'added', 'removed', 'common' sets
        """
        try:
            df = spark.table(table)
            current_set = set(df.columns)
            expected_set = set(expected_columns)

            return {
                "added": current_set - expected_set,
                "removed": expected_set - current_set,
                "common": current_set & expected_set,
            }
        except Exception as e:
            log.error(f"Error getting schema diff: {e}")
            return {"added": set(), "removed": set(), "common": set()}

    def compare_two_tables(
        self,
        spark,
        source_table: str,
        target_table: str,
    ) -> SchemaDriftResult:
        """
        Compare schemas of two tables (e.g., source vs target).

        Useful for verifying ETL transformations maintain expected schema.
        """
        try:
            source_df = spark.table(source_table)
            target_df = spark.table(target_table)
        except Exception as e:
            return SchemaDriftResult(
                check_name="schema_comparison",
                status="FAIL",
                details=f"Could not read tables: {e}",
            )

        source_columns = list(source_df.columns)
        target_columns = list(target_df.columns)

        return self.detect_drift(
            spark=spark,
            table=source_table,
            expected_columns=target_columns,
        )
