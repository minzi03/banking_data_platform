"""
Anomaly Detection — Banking Data Platform

Statistical outlier detection for data quality monitoring.
Detects volume anomalies (row count deviations) and statistical outliers
in numeric columns.

Usage:
    from governance.anomaly_detection import AnomalyDetector

    detector = AnomalyDetector()

    # Volume anomaly
    result = detector.detect_volume_anomaly(spark, "lakehouse.silver.dim_customer",
                                            expected_min=5000, expected_max=20000)

    # Statistical outlier
    result = detector.detect_statistical_outlier(df, "txn_amount", method="zscore")
"""

from dataclasses import dataclass, field
from logging import getLogger
from typing import Dict, List, Optional, Tuple

log = getLogger("anomaly_detection")


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""
    check_name: str
    status: str          # "PASS", "WARN", "FAIL"
    details: str
    anomaly_count: int = 0
    total_count: int = 0
    metadata: Dict = field(default_factory=dict)


class AnomalyDetector:
    """
    Statistical anomaly detection for data quality.

    Methods:
    - detect_volume_anomaly: Row count deviation detection
    - detect_statistical_outlier: Z-score/IQR outlier detection
    - detect_column_anomaly: Per-column statistical checks
    """

    def detect_volume_anomaly(
        self,
        spark,
        table: str,
        expected_min: Optional[int] = None,
        expected_max: Optional[int] = None,
        threshold_pct: float = 20.0,
        historical_avg: Optional[int] = None,
    ) -> AnomalyResult:
        """
        Detect row count anomalies.

        Args:
            spark: SparkSession
            table: Full table name (e.g., 'lakehouse.silver.dim_customer')
            expected_min: Minimum expected row count
            expected_max: Maximum expected row count
            threshold_pct: Percentage deviation threshold for alert
            historical_avg: Historical average row count (if available)

        Returns:
            AnomalyResult with anomaly status
        """
        try:
            df = spark.table(table)
            actual_count = df.count()
        except Exception as e:
            return AnomalyResult(
                check_name="volume_anomaly",
                status="FAIL",
                details=f"Could not read table: {e}",
            )

        issues = []

        # Check against explicit bounds
        if expected_min is not None and actual_count < expected_min:
            issues.append(f"Below minimum: {actual_count} < {expected_min}")
        if expected_max is not None and actual_count > expected_max:
            issues.append(f"Above maximum: {actual_count} > {expected_max}")

        # Check against historical average
        if historical_avg is not None and historical_avg > 0:
            deviation_pct = abs(actual_count - historical_avg) / historical_avg * 100
            if deviation_pct > threshold_pct:
                direction = "above" if actual_count > historical_avg else "below"
                issues.append(
                    f"Deviation {deviation_pct:.1f}% {direction} historical avg "
                    f"({actual_count} vs {historical_avg})"
                )

        if issues:
            return AnomalyResult(
                check_name="volume_anomaly",
                status="WARN",
                details="; ".join(issues),
                total_count=actual_count,
                metadata={
                    "expected_min": expected_min,
                    "expected_max": expected_max,
                    "historical_avg": historical_avg,
                },
            )

        return AnomalyResult(
            check_name="volume_anomaly",
            status="PASS",
            details=f"Row count OK: {actual_count}",
            total_count=actual_count,
        )

    def detect_statistical_outlier(
        self,
        df,
        column: str,
        method: str = "zscore",
        threshold: float = 3.0,
    ) -> AnomalyResult:
        """
        Detect statistical outliers in a numeric column.

        Args:
            df: Spark DataFrame
            column: Column name to check
            method: Detection method ('zscore' or 'iqr')
            threshold: Threshold for outlier detection
                      - zscore: number of std devs (default 3.0)
                      - iqr: IQR multiplier (default 1.5)

        Returns:
            AnomalyResult with outlier count
        """
        if column not in df.columns:
            return AnomalyResult(
                check_name=f"outlier_{column}",
                status="FAIL",
                details=f"Column '{column}' not found",
            )

        from pyspark.sql import functions as F

        try:
            total_count = df.count()
            if total_count == 0:
                return AnomalyResult(
                    check_name=f"outlier_{column}",
                    status="PASS",
                    details="Empty DataFrame, no outliers",
                    total_count=0,
                )

            # Compute statistics
            stats = df.select(
                F.mean(column).alias("mean"),
                F.stddev(column).alias("stddev"),
                F.expr(f"percentile_approx({column}, 0.25)").alias("q1"),
                F.expr(f"percentile_approx({column}, 0.75)").alias("q3"),
            ).collect()[0]

            mean_val = stats["mean"] or 0
            stddev_val = stats["stddev"] or 0
            q1 = stats["q1"] or 0
            q3 = stats["q3"] or 0

            if method == "zscore":
                if stddev_val == 0:
                    outlier_count = 0
                else:
                    outlier_count = df.filter(
                        F.abs(F.col(column) - mean_val) > threshold * stddev_val
                    ).count()
            elif method == "iqr":
                iqr = q3 - q1
                lower_bound = q1 - threshold * iqr
                upper_bound = q3 + threshold * iqr
                outlier_count = df.filter(
                    (F.col(column) < lower_bound) | (F.col(column) > upper_bound)
                ).count()
            else:
                return AnomalyResult(
                    check_name=f"outlier_{column}",
                    status="FAIL",
                    details=f"Unknown method: {method}",
                )

            outlier_pct = (outlier_count / total_count * 100) if total_count > 0 else 0

            if outlier_count > 0:
                return AnomalyResult(
                    check_name=f"outlier_{column}",
                    status="WARN",
                    details=f"{outlier_count} outliers ({outlier_pct:.1f}%) using {method}",
                    anomaly_count=outlier_count,
                    total_count=total_count,
                    metadata={
                        "method": method,
                        "threshold": threshold,
                        "mean": mean_val,
                        "stddev": stddev_val,
                        "q1": q1,
                        "q3": q3,
                    },
                )

            return AnomalyResult(
                check_name=f"outlier_{column}",
                status="PASS",
                details=f"No outliers detected using {method}",
                total_count=total_count,
            )

        except Exception as e:
            return AnomalyResult(
                check_name=f"outlier_{column}",
                status="FAIL",
                details=f"Error detecting outliers: {e}",
            )

    def detect_column_anomaly(
        self,
        df,
        column: str,
        expected_type: Optional[str] = None,
        expected_min: Optional[float] = None,
        expected_max: Optional[float] = None,
    ) -> AnomalyResult:
        """
        Detect column-level anomalies (type, range, etc.).

        Args:
            df: Spark DataFrame
            column: Column name to check
            expected_type: Expected data type (e.g., 'int', 'string', 'date')
            expected_min: Minimum expected value
            expected_max: Maximum expected value

        Returns:
            AnomalyResult
        """
        if column not in df.columns:
            return AnomalyResult(
                check_name=f"column_anomaly_{column}",
                status="FAIL",
                details=f"Column '{column}' not found",
            )

        from pyspark.sql import functions as F

        issues = []

        # Check data type
        if expected_type:
            actual_type = dict(df.dtypes).get(column, "unknown")
            if expected_type not in actual_type:
                issues.append(f"Type mismatch: expected {expected_type}, got {actual_type}")

        # Check range
        if expected_min is not None or expected_max is not None:
            stats = df.select(
                F.min(column).alias("min_val"),
                F.max(column).alias("max_val"),
            ).collect()[0]

            actual_min = stats["min_val"]
            actual_max = stats["max_val"]

            if expected_min is not None and actual_min is not None and actual_min < expected_min:
                issues.append(f"Min value {actual_min} below expected {expected_min}")
            if expected_max is not None and actual_max is not None and actual_max > expected_max:
                issues.append(f"Max value {actual_max} above expected {expected_max}")

        if issues:
            return AnomalyResult(
                check_name=f"column_anomaly_{column}",
                status="WARN",
                details="; ".join(issues),
            )

        return AnomalyResult(
            check_name=f"column_anomaly_{column}",
            status="PASS",
            details=f"Column '{column}' anomalies OK",
        )
