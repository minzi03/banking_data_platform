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

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from logging import getLogger

# Lazy import: pyspark may not be available in CI governance checks
try:
    from pyspark.sql import functions as F
except ImportError:
    F = None  # type: ignore[assignment]

log = getLogger("freshness_checks")

# Ngày nghiệp vụ theo giờ Việt Nam: UTC+7 cố định, không có giờ mùa hè — cùng
# lựa chọn với iceberg_maintenance.py (TD-10).
BUSINESS_TZ = timezone(timedelta(hours=7))


def _reference_instant(value) -> tuple[datetime, str] | None:
    """
    Mốc để tính tuổi dữ liệu, hoặc None nếu không có mốc hợp lý.

    datetime  → chính nó (không có tz thì coi là UTC, như trước)
    date      → lúc ngày nghiệp vụ đó KẾT THÚC theo giờ Việt Nam

    Với cột ngày như `cob_dt`, câu hỏi freshness là "dữ liệu của ngày D có sẵn
    trong bao lâu sau khi D khép lại". Lượt chạy 09:00 hôm sau thấy snapshot D
    mới 9 giờ tuổi — trong SLA 24h. Pipeline trễ quá SLA thì FAIL.

    `datetime` là lớp con của `date`, nên phải kiểm trước.
    """
    if isinstance(value, datetime):
        instant = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return instant, instant.isoformat()
    if isinstance(value, date):
        day_end = datetime.combine(value + timedelta(days=1), time.min, tzinfo=BUSINESS_TZ)
        return day_end, f"{value.isoformat()} (ngày nghiệp vụ kết thúc {day_end.isoformat()})"
    return None


@dataclass
class FreshnessResult:
    """Result of freshness check."""

    check_name: str
    status: str  # "PASS", "WARN", "FAIL"
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
            latest_row = df.select(F.max(date_column).alias("last_updated")).collect()[0]

            last_updated = latest_row["last_updated"]

            if last_updated is None:
                return FreshnessResult(
                    check_name="freshness",
                    status="FAIL",
                    details=f"No data found in '{date_column}' column",
                    sla_hours=sla_hours,
                )

            reference = _reference_instant(last_updated)
            if reference is None:
                # Trước đây nhánh này trả PASS ("Latest record: ...") cho mọi kiểu
                # không phải datetime — gồm cả DATE. Nên 19 contract Gold khai
                # `date_column: cob_dt` chưa từng được đánh giá freshness, và luôn
                # báo xanh. Không tính được tuổi thì nói là không tính được.
                return FreshnessResult(
                    check_name="freshness",
                    status="WARN",
                    details=(
                        f"Không đánh giá được tuổi dữ liệu: '{date_column}' có kiểu "
                        f"{type(last_updated).__name__}, cần date hoặc timestamp"
                    ),
                    last_updated=str(last_updated),
                    sla_hours=sla_hours,
                )

            instant, last_updated_str = reference
            age_hours = (datetime.now(timezone.utc) - instant).total_seconds() / 3600
            return FreshnessResult(
                check_name="freshness",
                status="FAIL" if age_hours > sla_hours else "PASS",
                details=f"Data is {age_hours:.1f} hours old (SLA: {sla_hours}h) · latest {last_updated_str}",
                last_updated=last_updated_str,
                age_hours=age_hours,
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
            latest_partition = df.select(F.max(partition_column).alias("latest")).collect()[0]["latest"]

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

                age_days = (
                    (datetime.now().date() - latest_partition).days
                    if isinstance(latest_partition, date)
                    else 0  # Can't calculate, assume fresh
                )

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
