"""
Quarantine Checks — Business Rule Violations

Reads quarantine rules from quarantine_rules.yml, executes checks against
Iceberg tables, and writes violating records to quarantine tables.

Usage:
    spark-submit --master spark://spark-master:7077 \
        quarantine.py --cob_dt 2025-01-01 --layer silver

    spark-submit --master spark://spark-master:7077 \
        quarantine.py --cob_dt 2025-01-01 --layer all
"""

import argparse
import os
import sys
from logging import INFO, basicConfig, getLogger
from typing import Any

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
log = getLogger("quarantine")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
QUARANTINE_LOG_TABLE = "opslakehouse.quarantine_log"
RULES_FILE = os.path.join(_HERE, "quarantine_rules.yml")


# ---------------------------------------------------------------------------
# YAML Loader
# ---------------------------------------------------------------------------
def load_rules(path: str) -> dict[str, Any]:
    """Load quarantine rules from YAML file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Violation Check Executor
# ---------------------------------------------------------------------------
def check_violation(spark, source_table: str, condition: str) -> list[dict]:
    """
    Execute a violation check and return violating records.

    Args:
        spark: SparkSession
        source_table: Source table to check
        condition: SQL condition to filter violating records

    Returns:
        List of violating records as dicts
    """
    try:
        df = spark.table(source_table)

        # Apply condition filter
        violating_df = df.filter(condition)

        # Collect violating records
        records = violating_df.collect()

        return [row.asDict() for row in records]
    except Exception as e:
        log.error(f"Error checking violation on {source_table}: {e}")
        return []


# ---------------------------------------------------------------------------
# Write to Quarantine Table
# ---------------------------------------------------------------------------
def write_to_quarantine(spark, records: list[dict], target_table: str,
                       violation_type: str, source_table: str) -> int:
    """
    Write violating records to quarantine table.

    Args:
        spark: SparkSession
        records: List of violating records
        target_table: Target quarantine table
        violation_type: Type of violation
        source_table: Source table name

    Returns:
        Number of records written
    """
    if not records:
        return 0

    try:
        from datetime import datetime

        from pyspark.sql import Row

        # Get target table schema
        target_df = spark.table(target_table)
        target_columns = set(target_df.columns)

        # Filter records to only include columns that exist in target
        filtered_records = []
        for record in records:
            filtered_row = {}
            for col in target_columns:
                if col in record:
                    filtered_row[col] = record[col]
                elif col == "violation_type":
                    filtered_row[col] = violation_type
                elif col == "source_table":
                    filtered_row[col] = source_table
                elif col == "detected_at":
                    filtered_row[col] = datetime.now()
                elif col == "violation_detail":
                    filtered_row[col] = f"Violation in {source_table}"
                else:
                    filtered_row[col] = None
            filtered_records.append(filtered_row)

        # Create DataFrame
        df = spark.createDataFrame([Row(**r) for r in filtered_records])

        # Write to quarantine table (append mode)
        df.write.format("iceberg").mode("append").saveAsTable(target_table)

        return len(records)
    except Exception as e:
        log.error(f"Error writing to quarantine table {target_table}: {e}")
        return 0


# ---------------------------------------------------------------------------
# Run Quarantine Checks for One Rule Set
# ---------------------------------------------------------------------------
def run_quarantine_checks(spark, rule_name: str, rule_config: dict,
                         cob_dt: str) -> list[dict]:
    """
    Run all quarantine checks for a rule set.

    Args:
        spark: SparkSession
        rule_name: Name of the rule set
        rule_config: Rule configuration
        cob_dt: Business date

    Returns:
        List of check results
    """
    results = []
    source_table = rule_config["source_table"]
    target_table = rule_config["target_table"]
    violations = rule_config.get("violations", [])

    log.info(f"\n--- Quarantine checks for {rule_name} ---")
    log.info(f"  Source: {source_table}")
    log.info(f"  Target: {target_table}")

    for violation in violations:
        violation_name = violation["name"]
        severity = violation.get("severity", "FAIL")
        condition = violation["condition"]

        log.info(f"  Checking: {violation_name} (severity: {severity})")

        # Execute check
        violating_records = check_violation(spark, source_table, condition)

        if violating_records:
            # Write to quarantine table
            rows_written = write_to_quarantine(
                spark, violating_records, target_table,
                violation_name, source_table
            )

            result = {
                "rule_name": rule_name,
                "violation_name": violation_name,
                "severity": severity,
                "status": "QUARANTINED" if severity == "FAIL" else "WARNED",
                "violations_found": len(violating_records),
                "rows_written": rows_written,
                "source_table": source_table,
                "target_table": target_table,
            }
            results.append(result)

            icon = "⚠️" if severity == "WARN" else "❌"
            log.info(f"    {icon} {violation_name}: {len(violating_records)} records quarantined")
        else:
            result = {
                "rule_name": rule_name,
                "violation_name": violation_name,
                "severity": severity,
                "status": "PASS",
                "violations_found": 0,
                "rows_written": 0,
                "source_table": source_table,
                "target_table": target_table,
            }
            results.append(result)
            log.info(f"    ✅ {violation_name}: No violations found")

    return results


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
def print_summary(results: list[dict]) -> tuple[int, int, int]:
    """Print summary and return (pass, warn, fail) counts."""
    pass_count = sum(1 for r in results if r["status"] == "PASS")
    warn_count = sum(1 for r in results if r["status"] == "WARNED")
    fail_count = sum(1 for r in results if r["status"] == "QUARANTINED")
    total_violations = sum(r["violations_found"] for r in results)

    log.info("=" * 60)
    log.info("QUARANTINE SUMMARY")
    log.info("=" * 60)
    log.info(f"  ✅ PASS: {pass_count}")
    log.info(f"  ⚠️  WARN: {warn_count}")
    log.info(f"  ❌ FAIL: {fail_count}")
    log.info(f"  📊 Total violations: {total_violations}")
    log.info("=" * 60)

    if fail_count > 0:
        log.error("FAILED CHECKS:")
        for r in results:
            if r["status"] == "QUARANTINED":
                log.error(f"  ❌ {r['rule_name']}/{r['violation_name']}: {r['violations_found']} records")

    return pass_count, warn_count, fail_count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Quarantine Checks")
    parser.add_argument("--cob_dt", required=True, help="Business date (YYYY-MM-DD)")
    parser.add_argument(
        "--layer",
        required=True,
        choices=["silver", "gold", "all"],
        help="Layer to check",
    )
    parser.add_argument(
        "--rules_file",
        default=RULES_FILE,
        help="Path to quarantine rules YAML",
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

    log.info(f"Quarantine Checks — cob_dt={cob_dt}, layer={layer}")

    # Load rules
    rules = load_rules(rules_file)
    quarantine_rules = rules.get("quarantine_rules", {})

    # Filter by layer if needed
    if layer != "all":
        quarantine_rules = {
            k: v for k, v in quarantine_rules.items()
            if v.get("source_table", "").startswith(f"lakehouse.{layer}.")
        }

    log.info(f"Checking {len(quarantine_rules)} rule sets ...")

    # Create Spark session
    spark = get_spark_session("QuarantineChecks")

    # Run checks
    all_results = []
    for rule_name, rule_config in quarantine_rules.items():
        results = run_quarantine_checks(spark, rule_name, rule_config, cob_dt)
        all_results.extend(results)

    # Summary
    pass_count, warn_count, fail_count = print_summary(all_results)

    # Exit code
    if fail_count > 0:
        log.error(f"Quarantine checks found {fail_count} violations")
        sys.exit(1)
    else:
        log.info("Quarantine checks passed")
        sys.exit(0)


if __name__ == "__main__":
    main()
