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

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path

# ops_schema_drift_dag chạy file này bằng `spark-submit governance/schema_drift.py`,
# tức là như script: sys.path[0] là governance/, không phải gốc repo.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from governance.ddl_schema import DEFAULT_DDL_DIR, declared_schemas, normalize_type  # noqa: E402

log = getLogger("schema_drift")

# ── Mức độ thay đổi schema ──────────────────────────────────────────────────
# ADDITIVE: consumer cũ vẫn đọc được — cột mới (Iceberg thêm cột dạng optional),
#   hoặc nâng kiểu theo luật type promotion của Iceberg spec v2.
# BREAKING: consumer cũ vỡ hoặc âm thầm đọc sai — mất cột, đổi kiểu ngoài luật
#   promotion, hay bảng khai trong DDL mà không tồn tại.
NONE, ADDITIVE, BREAKING = "NONE", "ADDITIVE", "BREAKING"
_PROMOTIONS = {("INT", "BIGINT"), ("FLOAT", "DOUBLE")}
_DECIMAL = re.compile(r"DECIMAL\((\d+),(\d+)\)")


def is_safe_promotion(old: str, new: str) -> bool:
    """Luật promotion của Iceberg: int→long, float→double, decimal(P,S)→decimal(P',S) với P' >= P."""
    old, new = normalize_type(old), normalize_type(new)
    if old == new or (old, new) in _PROMOTIONS:
        return True
    a, b = _DECIMAL.fullmatch(old), _DECIMAL.fullmatch(new)
    return bool(a and b and a.group(2) == b.group(2) and int(b.group(1)) >= int(a.group(1)))


@dataclass
class DriftReport:
    """So schema thực tế của một bảng với schema khai trong DDL."""

    table: str
    missing_table: bool = False
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    promoted: list[tuple[str, str, str]] = field(default_factory=list)
    incompatible: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def severity(self) -> str:
        if self.missing_table or self.removed or self.incompatible:
            return BREAKING
        if self.added or self.promoted:
            return ADDITIVE
        return NONE

    def describe(self) -> str:
        if self.missing_table:
            return "bảng khai trong DDL nhưng không tồn tại"
        parts = []
        if self.removed:
            parts.append(f"mất cột {self.removed}")
        parts += [f"{c}: {o} → {n} (không phải promotion)" for c, o, n in self.incompatible]
        if self.added:
            parts.append(f"thêm cột {self.added}")
        parts += [f"{c}: {o} → {n} (promotion)" for c, o, n in self.promoted]
        return "; ".join(parts) or "khớp DDL"


def classify(table: str, declared: dict[str, str], actual: dict[str, str] | None) -> DriftReport:
    """Hàm thuần: declared/actual là {cột: kiểu}; actual None nghĩa là bảng không tồn tại."""
    report = DriftReport(table=table)
    if actual is None:
        report.missing_table = True
        return report
    declared = {c.lower(): normalize_type(t) for c, t in declared.items()}
    actual = {c.lower(): normalize_type(t) for c, t in actual.items()}
    report.added = sorted(set(actual) - set(declared))
    report.removed = sorted(set(declared) - set(actual))
    for column in sorted(set(declared) & set(actual)):
        old, new = declared[column], actual[column]
        if old != new:
            bucket = report.promoted if is_safe_promotion(old, new) else report.incompatible
            bucket.append((column, old, new))
    return report


def check_tables(
    read_schema: Callable[[str], dict[str, str] | None],
    declared: dict[str, dict[str, str]],
    tables: list[str],
) -> list[DriftReport]:
    """read_schema(fqn) → {cột: kiểu}, hoặc None nếu bảng không tồn tại."""
    return [classify(t, declared[t], read_schema(t)) for t in tables]


@dataclass
class SchemaDriftResult:
    """Result of schema drift detection."""

    check_name: str
    status: str  # "PASS", "WARN", "FAIL"
    details: str
    added_columns: list[str] = field(default_factory=list)
    removed_columns: list[str] = field(default_factory=list)
    type_changes: list[dict] = field(default_factory=list)
    current_columns: list[str] = field(default_factory=list)
    expected_columns: list[str] = field(default_factory=list)
    severity: str = NONE  # NONE / ADDITIVE / BREAKING


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
        expected_columns: list[str],
        expected_types: dict[str, str] | None = None,
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
                        type_changes.append(
                            {
                                "column": current_col,
                                "expected_type": expected_type,
                                "actual_type": actual_type,
                            }
                        )

        # Mức độ: nâng kiểu theo luật promotion (int→bigint…) không làm vỡ consumer.
        breaking_types = [tc for tc in type_changes if not is_safe_promotion(tc["expected_type"], tc["actual_type"])]
        if removed or breaking_types:
            status, severity = "FAIL", BREAKING
        elif added or type_changes:
            status, severity = "WARN", ADDITIVE
        else:
            status, severity = "PASS", NONE

        # Build details
        issues = []
        if added:
            actual_names = [current_set[a] for a in added]
            issues.append(f"Added columns: {actual_names}")
        if removed:
            issues.append(f"Removed columns: {removed}")
        if type_changes:
            for tc in type_changes:
                issues.append(f"Type change: {tc['column']} ({tc['expected_type']} → {tc['actual_type']})")

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
            severity=severity,
        )

    def detect_drift_from_contract(
        self,
        spark,
        table: str,
        required_columns: list[str],
        non_null_columns: list[str] | None = None,
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
        expected_columns: list[str],
    ) -> dict[str, set[str]]:
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

        source_columns = list(source_df.columns)  # noqa: F841
        target_columns = list(target_df.columns)

        return self.detect_drift(
            spark=spark,
            table=source_table,
            expected_columns=target_columns,
        )


# ── CLI: ops_schema_drift_dag ────────────────────────────────────────────────
# Bản trước DAG gọi `spark-submit governance/schema_drift.py --table … --columns …`
# nhưng file KHÔNG có entrypoint: Spark nạp các class rồi thoát 0, nên DAG luôn
# báo xanh mà không kiểm gì. Giờ: so mọi bảng khai trong DDL của các tầng được
# chọn với schema thật, exit 1 nếu có thay đổi BREAKING.


def select_tables(declared: dict[str, dict[str, str]], layers: list[str], tables: list[str]) -> list[str]:
    if tables:
        unknown = sorted(set(t.lower() for t in tables) - set(declared))
        if unknown:
            raise SystemExit(f"bảng không có trong DDL: {unknown}")
        return sorted(t.lower() for t in tables)
    return sorted(t for t in declared if t.split(".")[1] in layers)


def _spark_reader(spark) -> Callable[[str], dict[str, str] | None]:
    def read(table: str) -> dict[str, str] | None:
        if not spark.catalog.tableExists(table):
            return None
        return dict(spark.table(table).dtypes)

    return read


def main(argv: list[str] | None = None, read_schema: Callable[[str], dict[str, str] | None] | None = None) -> int:
    parser = argparse.ArgumentParser(description="So schema lakehouse với DDL, exit 1 nếu có thay đổi BREAKING")
    parser.add_argument(
        "--layer",
        action="append",
        choices=["bronze", "silver", "gold"],
        help="tầng cần kiểm (lặp được); mặc định silver + gold",
    )
    parser.add_argument("--table", action="append", default=[], help="kiểm riêng bảng này (tên đầy đủ)")
    parser.add_argument("--ddl-dir", type=Path, default=DEFAULT_DDL_DIR)
    args = parser.parse_args(argv)

    declared = declared_schemas(args.ddl_dir)
    tables = select_tables(declared, args.layer or ["silver", "gold"], args.table)
    if not tables:
        print("không có bảng nào để kiểm", file=sys.stderr)
        return 2

    if read_schema is None:
        from pyspark.sql import SparkSession

        read_schema = _spark_reader(SparkSession.builder.appName("schema_drift").getOrCreate())

    reports = check_tables(read_schema, declared, tables)
    for r in reports:
        print(f"{r.severity:<8} {r.table}: {r.describe()}")
    breaking = [r for r in reports if r.severity == BREAKING]
    print(f"\n{len(reports)} bảng · {len(breaking)} BREAKING · {sum(r.severity == ADDITIVE for r in reports)} ADDITIVE")
    return 1 if breaking else 0


if __name__ == "__main__":
    sys.exit(main())
