"""
Freshness Checks — Banking Data Platform

SLA-based data freshness monitoring.
Detects stale data that hasn't been refreshed within expected timeframes.

Usage:
    from governance.freshness_checks import FreshnessChecker

    checker = FreshnessChecker()
    result = checker.check_freshness(spark, "lakehouse.silver.dim_customer",
                                     sla_hours=24, date_column="_ingested_at")
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from logging import getLogger

# Lazy import: pyspark may not be available in CI governance checks
try:
    from pyspark.sql import functions as F
except ImportError:
    F = None  # type: ignore[assignment]

log = getLogger("freshness_checks")


@dataclass
class FreshnessResult:
    """Result of freshness check."""
    check_name: str
    status: str          # "PASS", "WARN", "FAIL"
    details: str
    last_updated: str | None = None
    age_hours: float | None = None
    sla_hours: int | None = None


class FreshnessChecker:
    """
    SLA-based data freshness monitoring.

    Checks whether data has been refreshed within the expected timeframe.
    """

    def check_freshness(
        self,
        spark,
        table: str,
        sla_hours: int = 24,
        date_column: str = "_ingested_at",
    ) -> FreshnessResult:
        """
        Check if data is fresh within SLA.

        Args:
            spark: SparkSession
            table: Full table name
            sla_hours: Maximum allowed data age in hours
            date_column: Column containing the last update timestamp

        Returns:
            FreshnessResult with freshness status
        """
        try:
            df = spark.table(table)
        except Exception as e:
            return FreshnessResult(
                check_name="freshness",
                status="FAIL",
                details=f"Could not read table: {e}",
            )

        # Check if date column exists
        if date_column not in df.columns:
            return FreshnessResult(
                check_name="freshness",
                status="WARN",
                details=f"Date column '{date_column}' not found, skipping freshness check",
            )

        try:
            # Get the latest timestamp
            latest_row = df.select(
                F.max(date_column).alias("last_updated")
            ).collect()[0]

            last_updated = latest_row["last_updated"]

            if last_updated is None:
                return FreshnessResult(
                    check_name="freshness",
                    status="FAIL",
                    details=f"No data found in '{date_column}' column",
                    sla_hours=sla_hours,
                )

            # Calculate age
            if isinstance(last_updated, datetime):
                now = datetime.now(timezone.utc)
                if last_updated.tzinfo is None:
                    last_updated = last_updated.replace(tzinfo=timezone.utc)
                age = now - last_updated
                age_hours = age.total_seconds() / 3600
                last_updated_str = last_updated.isoformat()
            else:
                # If it's a string or other type, try to parse
                age_hours = None
                last_updated_str = str(last_updated)

            if age_hours is not None:
                if age_hours > sla_hours:
                    return FreshnessResult(
                        check_name="freshness",
                        status="FAIL",
                        details=f"Data is {age_hours:.1f} hours old (SLA: {sla_hours}h)",
                        last_updated=last_updated_str,
                        age_hours=age_hours,
                        sla_hours=sla_hours,
                    )
                else:
                    return FreshnessResult(
                        check_name="freshness",
                        status="PASS",
                        details=f"Data is {age_hours:.1f} hours old (SLA: {sla_hours}h)",
                        last_updated=last_updated_str,
                        age_hours=age_hours,
                        sla_hours=sla_hours,
                    )
            else:
                return FreshnessResult(
                    check_name="freshness",
                    status="PASS",
                    details=f"Latest record: {last_updated_str}",
                    last_updated=last_updated_str,
                    sla_hours=sla_hours,
                )

        except Exception as e:
            return FreshnessResult(
                check_name="freshness",
                status="FAIL",
                details=f"Error checking freshness: {e}",
            )

    def check_partition_freshness(
        self,
        spark,
        table: str,
        partition_column: str = "cob_dt",
        sla_days: int = 1,
    ) -> FreshnessResult:
        """
        Check freshness based on partition dates.

        Args:
            spark: SparkSession
            table: Full table name
            partition_column: Partition date column
            sla_days: Maximum allowed partition age in days

        Returns:
            FreshnessResult
        """
        try:
            df = spark.table(table)
        except Exception as e:
            return FreshnessResult(
                check_name="partition_freshness",
                status="FAIL",
                details=f"Could not read table: {e}",
            )

        if partition_column not in df.columns:
            return FreshnessResult(
                check_name="partition_freshness",
                status="WARN",
                details=f"Partition column '{partition_column}' not found",
            )

        try:
            latest_partition = df.select(
                F.max(partition_column).alias("latest")
            ).collect()[0]["latest"]

            if latest_partition is None:
                return FreshnessResult(
                    check_name="partition_freshness",
                    status="FAIL",
                    details="No partitions found",
                    sla_hours=sla_days * 24,
                )

            # Calculate age
            if isinstance(latest_partition, datetime):
                now = datetime.now(timezone.utc)
                age_days = (now - latest_partition.replace(tzinfo=timezone.utc)).days
            else:
                # Try to parse as date string
                from datetime import date
                if isinstance(latest_partition, date):
                    age_days = (datetime.now().date() - latest_partition).days
                else:
                    age_days = 0  # Can't calculate, assume fresh

            if age_days > sla_days:
                return FreshnessResult(
                    check_name="partition_freshness",
                    status="FAIL",
                    details=f"Latest partition is {age_days} days old (SLA: {sla_days}d)",
                    last_updated=str(latest_partition),
                    age_hours=age_days * 24,
                    sla_hours=sla_days * 24,
                )

            return FreshnessResult(
                check_name="partition_freshness",
                status="PASS",
                details=f"Latest partition: {latest_partition} ({age_days}d old)",
                last_updated=str(latest_partition),
                age_hours=age_days * 24,
                sla_hours=sla_days * 24,
            )

        except Exception as e:
            return FreshnessResult(
                check_name="partition_freshness",
                status="FAIL",
                details=f"Error checking partition freshness: {e}",
            )
