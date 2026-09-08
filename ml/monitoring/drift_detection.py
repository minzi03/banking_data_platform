"""
Model Monitoring & Drift Detection — Banking Data Platform
==========================================================
Monitors model performance and data drift using MLflow.

Features:
    - Data drift detection (feature distribution changes)
    - Model performance tracking
    - Alert on degradation
    - Comparison between training and serving data

Usage:
    python drift_detection.py --cob_dt 2025-06-30
    python drift_detection.py --baseline cob_dt=2025-01-01 --current cob_dt=2025-06-30
"""

import os
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Tuple

import mlflow
import numpy as np
import pandas as pd
from scipy import stats
from trino.dbapi import connect

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_NAME = "credit_scoring"

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "serving")

# Drift thresholds
PSI_THRESHOLD = 0.2  # Population Stability Index
KS_THRESHOLD = 0.1   # Kolmogorov-Smirnov statistic
MEAN_SHIFT_THRESHOLD = 0.1  # 10% mean shift


def get_trino_connection():
    """Create Trino connection."""
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


def load_serving_data(cob_dt: str) -> pd.DataFrame:
    """Load serving data for monitoring."""
    sql = f"""
    SELECT
        customer_id,
        age,
        total_accounts,
        total_cards,
        total_loans,
        aum_total,
        txn_count_30d,
        txn_amount_30d,
        days_since_last_txn,
        rfm_recency_score,
        rfm_frequency_score,
        rfm_monetary_score
    FROM mart_customer_360_current
    WHERE cob_dt = date '{cob_dt}'
    """

    conn = get_trino_connection()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def calculate_psi(baseline: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
    """
    Calculate Population Stability Index (PSI).
    PSI < 0.1: No significant change
    PSI 0.1-0.2: Moderate change
    PSI > 0.2: Significant change (drift detected)
    """
    # Create bins from baseline
    breakpoints = np.percentile(baseline, np.linspace(0, 100, bins + 1))
    breakpoints = np.unique(breakpoints)

    baseline_counts = np.histogram(baseline, bins=breakpoints)[0] / len(baseline)
    current_counts = np.histogram(current, bins=breakpoints)[0] / len(current)

    # Avoid division by zero
    baseline_counts = np.where(baseline_counts == 0, 0.0001, baseline_counts)
    current_counts = np.where(current_counts == 0, 0.0001, current_counts)

    psi = np.sum((current_counts - baseline_counts) * np.log(current_counts / baseline_counts))
    return float(psi)


def calculate_ks_statistic(baseline: np.ndarray, current: np.ndarray) -> float:
    """Calculate Kolmogorov-Smirnov statistic."""
    statistic, _ = stats.ks_2samp(baseline, current)
    return float(statistic)


def detect_drift(baseline_df: pd.DataFrame, current_df: pd.DataFrame) -> Dict:
    """Detect drift between baseline and current data."""
    results = {
        "timestamp": datetime.now().isoformat(),
        "baseline_rows": len(baseline_df),
        "current_rows": len(current_df),
        "features": {},
        "drift_detected": False,
        "drifted_features": [],
    }

    numeric_columns = baseline_df.select_dtypes(include=[np.number]).columns

    for col in numeric_columns:
        if col in ["customer_id"]:
            continue

        baseline_vals = baseline_df[col].dropna().values
        current_vals = current_df[col].dropna().values

        if len(baseline_vals) == 0 or len(current_vals) == 0:
            continue

        psi = calculate_psi(baseline_vals, current_vals)
        ks = calculate_ks_statistic(baseline_vals, current_vals)

        mean_shift = abs(np.mean(current_vals) - np.mean(baseline_vals)) / (np.mean(baseline_vals) + 1e-6)

        feature_result = {
            "psi": round(psi, 4),
            "ks_statistic": round(ks, 4),
            "mean_shift": round(mean_shift, 4),
            "baseline_mean": round(float(np.mean(baseline_vals)), 2),
            "current_mean": round(float(np.mean(current_vals)), 2),
            "baseline_std": round(float(np.std(baseline_vals)), 2),
            "current_std": round(float(np.std(current_vals)), 2),
        }

        # Check for drift
        drift_detected = (
            psi > PSI_THRESHOLD or
            ks > KS_THRESHOLD or
            mean_shift > MEAN_SHIFT_THRESHOLD
        )

        feature_result["drift_detected"] = drift_detected
        results["features"][col] = feature_result

        if drift_detected:
            results["drift_detected"] = True
            results["drifted_features"].append(col)

    return results


def log_drift_results(drift_results: Dict, cob_dt: str):
    """Log drift detection results to MLflow."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("model_monitoring")

    with mlflow.start_run(run_name=f"drift_detection_{cob_dt}"):
        # Log summary metrics
        mlflow.log_param("cob_dt", cob_dt)
        mlflow.log_param("baseline_rows", drift_results["baseline_rows"])
        mlflow.log_param("current_rows", drift_results["current_rows"])
        mlflow.log_param("drift_detected", drift_results["drift_detected"])
        mlflow.log_param("n_drifted_features", len(drift_results["drifted_features"]))

        # Log feature-level metrics
        for feature, metrics in drift_results["features"].items():
            mlflow.log_metric(f"psi_{feature}", metrics["psi"])
            mlflow.log_metric(f"ks_{feature}", metrics["ks_statistic"])
            mlflow.log_metric(f"mean_shift_{feature}", metrics["mean_shift"])

        # Log full results as artifact
        mlflow.log_dict(drift_results, "drift_detection_results.json")

        run_id = mlflow.active_run().info.run_id
        logger.info("Drift detection logged to MLflow run: %s", run_id)


def print_drift_report(drift_results: Dict):
    """Print human-readable drift report."""
    logger.info("=" * 70)
    logger.info(" DRIFT DETECTION REPORT")
    logger.info("=" * 70)
    logger.info(" Timestamp:      %s", drift_results["timestamp"])
    logger.info(" Baseline rows:  %d", drift_results["baseline_rows"])
    logger.info(" Current rows:   %d", drift_results["current_rows"])
    logger.info("")

    if drift_results["drift_detected"]:
        logger.warning(" ⚠️  DRIFT DETECTED in %d features:", len(drift_results["drifted_features"]))
        for f in drift_results["drifted_features"]:
            logger.warning("    - %s", f)
    else:
        logger.info(" ✅ No significant drift detected")

    logger.info("")
    logger.info(" Feature Details:")
    logger.info(" %-30s %8s %8s %8s %10s", "Feature", "PSI", "KS", "MeanShift", "Status")
    logger.info(" " + "-" * 66)

    for feature, metrics in sorted(drift_results["features"].items()):
        status = "⚠️ DRIFT" if metrics["drift_detected"] else "✅ OK"
        logger.info(
            " %-30s %8.4f %8.4f %8.4f %10s",
            feature, metrics["psi"], metrics["ks_statistic"],
            metrics["mean_shift"], status,
        )

    logger.info("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Model Drift Detection")
    parser.add_argument("--baseline", default="2025-01-01", help="Baseline cob_dt")
    parser.add_argument("--current", default="2025-06-30", help="Current cob_dt")
    parser.add_argument("--log-to-mlflow", action="store_true", help="Log results to MLflow")
    args = parser.parse_args()

    logger.info("Loading baseline data (cob_dt=%s)...", args.baseline)
    baseline_df = load_serving_data(args.baseline)

    logger.info("Loading current data (cob_dt=%s)...", args.current)
    current_df = load_serving_data(args.current)

    if len(baseline_df) == 0 or len(current_df) == 0:
        logger.error("Insufficient data for drift detection")
        return

    # Detect drift
    drift_results = detect_drift(baseline_df, current_df)

    # Print report
    print_drift_report(drift_results)

    # Log to MLflow
    if args.log_to_mlflow:
        log_drift_results(drift_results, args.current)


if __name__ == "__main__":
    main()
