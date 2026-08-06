#!/usr/bin/env python3
"""
Data Quality Validation — Banking Data Platform

Reads DQ rules from dq_rules.yml, executes checks against Iceberg tables,
and writes results to opslakehouse.data_quality_log.

Usage:
    spark-submit --master spark://spark-master:7077 \
        data_quality.py --cob_dt 2025-01-01 --layer silver

    spark-submit --master spark://spark-master:7077 \
        data_quality.py --cob_dt 2025-01-01 --layer gold

    spark-submit --master spark://spark-master:7077 \
        data_quality.py --cob_dt 2025-01-01 --layer all
"""

import argparse
import os
import sys
from datetime import datetime
from logging import basicConfig, getLogger, INFO
from typing import Any, Dict, List, Optional, Tuple

import yaml

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, ".."))

from spark.spark_session import get_spark_session

basicConfig(
    level=INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = getLogger("data_quality")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DQ_LOG_TABLE = "opslakehouse.data_quality_log"
RULES_FILE = os.path.join(_HERE, "dq_rules.yml")

# Map layer prefix to YAML filter
LAYER_PREFIXES = {
    "silver": "lakehouse.silver.",
    "gold": "lakehouse.gold.",
    "bronze": "lakehouse.bronze.",
}


# ---------------------------------------------------------------------------
# YAML Loader
# ---------------------------------------------------------------------------
def load_rules(path: str) -> Dict[str, Any]:
    """Load DQ rules from YAML file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Individual Check Executors
# ---------------------------------------------------------------------------

def check_row_count(spark, table: str, rule: Dict) -> Tuple[str, str, str]:
    """Check row count >= min_rows."""
    try:
        df = spark.table(table)
        count = df.count()
        min_rows = rule.get("min_rows", 1)
        max_rows = rule.get("max_rows")

        if count < min_rows:
            return "FAIL", str(min_rows), f"Actual: {count} (min: {min_rows})"
        if max_rows and count > max_rows:
            return "FAIL", str(max_rows), f"Actual: {count} (max: {max_rows})"
        return "PASS", str(count), f"Row count OK: {count}"
    except Exception as e:
        return "FAIL", "N/A", f"Error: {e}"


def check_null(spark, table: str, rule: Dict) -> Tuple[str, str, str]:
    """Check columns are not NULL."""
    try:
        df = spark.table(table)
        columns = rule.get("columns", [])
        issues = []
        for col in columns:
            if col not in df.columns:
                issues.append(f"Column '{col}' not found")
                continue
            null_count = df.filter(df[col].isNull()).count()
            if null_count > 0:
                issues.append(f"{col}: {null_count} nulls")

        if issues:
            detail = "; ".join(issues)
            return "FAIL", "0", detail
        return "PASS", "0", f"All {len(columns)} columns non-null"
    except Exception as e:
        return "FAIL", "N/A", f"Error: {e}"


def check_unique(spark, table: str, rule: Dict) -> Tuple[str, str, str]:
    """Check columns are unique."""
    try:
        df = spark.table(table)
        columns = rule.get("columns", [])
        issues = []
        for col in columns:
            if col not in df.columns:
                issues.append(f"Column '{col}' not found")
                continue
            total = df.count()
            distinct = df.select(col).distinct().count()
            if total != distinct:
                dup_count = total - distinct
                issues.append(f"{col}: {dup_count} duplicates")

        if issues:
            detail = "; ".join(issues)
            return "FAIL", "0", detail
        return "PASS", "0", f"All {len(columns)} columns unique"
    except Exception as e:
        return "FAIL", "N/A", f"Error: {e}"


def check_range(spark, table: str, rule: Dict) -> Tuple[str, str, str]:
    """Check column values are within min/max bounds."""
    try:
        df = spark.table(table)
        col_name = rule.get("column")
        min_val = rule.get("min_value")
        max_val = rule.get("max_value")

        if col_name not in df.columns:
            return "FAIL", "N/A", f"Column '{col_name}' not found"

        total = df.count()
        out_of_range = df.filter(
            (df[col_name] < min_val) | (df[col_name] > max_val)
        ).count() if min_val is not None and max_val is not None else 0

        if min_val is not None and max_val is None:
            out_of_range = df.filter(df[col_name] < min_val).count()
        elif min_val is None and max_val is not None:
            out_of_range = df.filter(df[col_name] > max_val).count()

        if out_of_range > 0:
            bounds = f"[{min_val}, {max_val}]" if min_val and max_val else f">={min_val}" if min_val else f"<={max_val}"
            return "FAIL", bounds, f"{out_of_range} values out of range {bounds}"
        return "PASS", str(min_val), f"All values within bounds"
    except Exception as e:
        return "FAIL", "N/A", f"Error: {e}"


def check_referential_integrity(spark, table: str, rule: Dict) -> Tuple[str, str, str]:
    """Check FK column values exist in referenced table."""
    try:
        col_name = rule.get("column")
        ref_table = rule.get("ref_table")
        ref_column = rule.get("ref_column")

        if not all([col_name, ref_table, ref_column]):
            return "FAIL", "N/A", "Missing column/ref_table/ref_column in rule"

        df = spark.table(table)
        ref_df = spark.table(ref_table)

        if col_name not in df.columns:
            return "FAIL", "N/A", f"Column '{col_name}' not found in {table}"
        if ref_column not in ref_df.columns:
            return "FAIL", "N/A", f"Column '{ref_column}' not found in {ref_table}"

        # Find orphan records
        source_vals = df.select(col_name).distinct()
        ref_vals = ref_df.select(ref_column).distinct()
        orphans = source_vals.join(
            ref_vals, source_vals[col_name] == ref_vals[ref_column], "left_anti"
        ).count()

        if orphans > 0:
            return "FAIL", "0", f"{orphans} orphan records: {col_name} not in {ref_table}.{ref_column}"
        return "PASS", "0", f"All FK values exist in {ref_table}.{ref_column}"
    except Exception as e:
        return "FAIL", "N/A", f"Error: {e}"


# ---------------------------------------------------------------------------
# New Check Types — Phase 1: Governance & Data Quality
# ---------------------------------------------------------------------------

def check_anomaly_detection(spark, table: str, rule: Dict) -> Tuple[str, str, str]:
    """Check for statistical anomalies (volume deviation, outliers)."""
    try:
        from governance.anomaly_detection import AnomalyDetector

        detector = AnomalyDetector()
        expected_min = rule.get("min_row_count")
        expected_max = rule.get("max_row_count")
        threshold_pct = rule.get("threshold_pct", 20.0)
        historical_avg = rule.get("historical_avg")

        result = detector.detect_volume_anomaly(
            spark=spark,
            table=table,
            expected_min=expected_min,
            expected_max=expected_max,
            threshold_pct=threshold_pct,
            historical_avg=historical_avg,
        )

        return result.status, str(result.total_count), result.details
    except Exception as e:
        return "FAIL", "N/A", f"Error: {e}"


def check_freshness(spark, table: str, rule: Dict) -> Tuple[str, str, str]:
    """Check data freshness against SLA."""
    try:
        from governance.freshness_checks import FreshnessChecker

        checker = FreshnessChecker()
        sla_hours = rule.get("sla_hours", 24)
        date_column = rule.get("date_column", "_ingested_at")

        result = checker.check_freshness(
            spark=spark,
            table=table,
            sla_hours=sla_hours,
            date_column=date_column,
        )

        return result.status, str(result.sla_hours or "N/A"), result.details
    except Exception as e:
        return "FAIL", "N/A", f"Error: {e}"


def check_schema_drift(spark, table: str, rule: Dict) -> Tuple[str, str, str]:
    """Check for schema drift against expected columns."""
    try:
        from governance.schema_drift import SchemaDriftDetector

        detector = SchemaDriftDetector()
        expected_columns = rule.get("expected_columns", [])

        if not expected_columns:
            return "WARN", "N/A", "No expected_columns defined in rule"

        result = detector.detect_drift(
            spark=spark,
            table=table,
            expected_columns=expected_columns,
        )

        return result.status, str(len(result.expected_columns)), result.details
    except Exception as e:
        return "FAIL", "N/A", f"Error: {e}"


# Dispatcher
CHECK_DISPATCH = {
    "row_count": check_row_count,
    "null_check": check_null,
    "unique_check": check_unique,
    "range_check": check_range,
    "referential_integrity": check_referential_integrity,
    # New check types — Phase 1
    "anomaly_detection": check_anomaly_detection,
    "freshness_check": check_freshness,
    "schema_drift": check_schema_drift,
}


# ---------------------------------------------------------------------------
# Run All Checks for One Table
# ---------------------------------------------------------------------------

def run_checks_for_table(spark, table: str, checks: List[Dict], cob_dt: str) -> List[Dict[str, Any]]:
    """Run all DQ checks for a single table, return result records."""
    results = []
    for check in checks:
        check_name = check.get("name")
        severity = check.get("severity", "FAIL")

        executor = CHECK_DISPATCH.get(check_name)
        if not executor:
            log.warning(f"Unknown check type: {check_name}, skipping")
            continue

        log.info(f"  Running {check_name} on {table} ...")
        status, expected, details = executor(spark, table, check)

        # If severity is WARN, downgrade FAIL to WARN
        if severity == "WARN" and status == "FAIL":
            status = "WARN"

        results.append({
            "check_name": check_name,
            "table_name": table,
            "check_status": status,
            "expected_value": expected,
            "actual_value": details,
            "details": details,
            "cob_dt": cob_dt,
            "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

        icon = "✅" if status == "PASS" else "⚠️" if status == "WARN" else "❌"
        log.info(f"    {icon} {check_name}: {status} — {details}")

    return results


# ---------------------------------------------------------------------------
# Write Results to PostgreSQL
# ---------------------------------------------------------------------------

def write_results_to_pg(results: List[Dict[str, Any]]) -> None:
    """Write DQ results to opslakehouse.data_quality_log via JDBC."""
    if not results:
        log.info("No results to write.")
        return

    jdbc_url = "jdbc:postgresql://postgres:5432/banking_db"
    props = {
        "user": os.environ.get("POSTGRES_USER", "banking_admin"),
        "password": os.environ.get("POSTGRES_PASSWORD", "BankingAdmin123"),
        "driver": "org.postgresql.Driver",
    }

    from pyspark.sql import Row
    from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
    import datetime

    spark = results[0]["_spark"]
    cob_dt_str = results[0]["cob_dt"]

    # Build rows with proper types
    rows = []
    for r in results:
        rows.append(Row(
            check_name=r["check_name"],
            table_name=r["table_name"],
            check_status=r["check_status"],
            expected_value=r["expected_value"],
            actual_value=r["actual_value"],
            details=r["details"],
            cob_dt=datetime.datetime.strptime(cob_dt_str, "%Y-%m-%d").date(),
            checked_at=datetime.datetime.strptime(r["checked_at"], "%Y-%m-%d %H:%M:%S"),
        ))

    schema = StructType([
        StructField("check_name", StringType()),
        StructField("table_name", StringType()),
        StructField("check_status", StringType()),
        StructField("expected_value", StringType()),
        StructField("actual_value", StringType()),
        StructField("details", StringType()),
        StructField("cob_dt", DateType()),
        StructField("checked_at", TimestampType()),
    ])
    df = spark.createDataFrame(rows, schema=schema)

    # Delete existing results for same cob_dt + same tables (idempotent re-runs)
    try:
        tables_in_batch = list(set(r["table_name"] for r in results))
        tables_str = ",".join(f"'{t}'" for t in tables_in_batch)
        log.info(f"Clearing existing DQ results for {len(tables_in_batch)} tables (cob_dt={cob_dt_str})...")
        conn = spark._sc._jvm.java.sql.DriverManager.getConnection(
            jdbc_url,
            props["user"],
            props["password"],
        )
        stmt = conn.createStatement()
        stmt.executeUpdate(
            f"DELETE FROM {DQ_LOG_TABLE} WHERE cob_dt = '{cob_dt_str}' AND table_name IN ({tables_str})"
        )
        stmt.close()
        conn.close()
    except Exception as e:
        log.warning(f"Could not clear old results (first run?): {e}")

    # Write new results
    log.info(f"Writing {len(results)} DQ results to {DQ_LOG_TABLE}...")
    df.write.jdbc(jdbc_url, DQ_LOG_TABLE, mode="append", properties=props)
    log.info(f"Successfully wrote {len(results)} DQ results.")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(results: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    """Print summary and return (pass, warn, fail) counts."""
    pass_count = sum(1 for r in results if r["check_status"] == "PASS")
    warn_count = sum(1 for r in results if r["check_status"] == "WARN")
    fail_count = sum(1 for r in results if r["check_status"] == "FAIL")
    total = pass_count + warn_count + fail_count

    log.info("=" * 60)
    log.info(f"DQ SUMMARY: {total} checks executed")
    log.info(f"  ✅ PASS: {pass_count}")
    log.info(f"  ⚠️  WARN: {warn_count}")
    log.info(f"  ❌ FAIL: {fail_count}")
    log.info("=" * 60)

    if fail_count > 0:
        log.error("FAILED CHECKS:")
        for r in results:
            if r["check_status"] == "FAIL":
                log.error(f"  ❌ {r['table_name']} / {r['check_name']}: {r['details']}")

    return pass_count, warn_count, fail_count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Data Quality Validation")
    parser.add_argument("--cob_dt", required=True, help="Business date (YYYY-MM-DD)")
    parser.add_argument(
        "--layer",
        required=True,
        choices=["silver", "gold", "bronze", "all"],
        help="Layer to validate",
    )
    parser.add_argument(
        "--rules_file",
        default=RULES_FILE,
        help="Path to DQ rules YAML (default: dq_rules.yml)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    cob_dt = args.cob_dt
    layer = args.layer
    rules_file = args.rules_file

    log.info(f"Data Quality Validation — cob_dt={cob_dt}, layer={layer}")

    # Load rules
    rules = load_rules(rules_file)
    all_tables = rules.get("tables", {})

    # Filter by layer
    if layer == "all":
        target_tables = all_tables
    else:
        prefix = LAYER_PREFIXES[layer]
        target_tables = {
            k: v for k, v in all_tables.items() if k.startswith(prefix)
        }

    log.info(f"Checking {len(target_tables)} tables ...")

    # Create Spark session
    spark = get_spark_session("DataQualityValidation")

    # Run checks
    all_results = []
    for table, table_rules in sorted(target_tables.items()):
        log.info(f"\n--- {table} ---")
        checks = table_rules.get("checks", [])
        results = run_checks_for_table(spark, table, checks, cob_dt)
        all_results.extend(results)

    # Summary
    pass_count, warn_count, fail_count = print_summary(all_results)

    # Write to PostgreSQL
    # Attach spark reference for DataFrame creation
    for r in all_results:
        r["_spark"] = spark
    write_results_to_pg(all_results)

    # Exit code
    if fail_count > 0:
        log.error(f"DQ FAILED with {fail_count} failures")
        sys.exit(1)
    else:
        log.info("DQ PASSED — all checks OK")
        sys.exit(0)


if __name__ == "__main__":
    main()
