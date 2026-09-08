"""
Credit Scoring ML Pipeline — Banking Data Platform
=====================================================
Example ML pipeline using MLflow for model tracking and registry.

Pipeline:
    1. Load customer data from Trino (via serving layer)
    2. Feature engineering (RFM, transaction patterns, product holdings)
    3. Train XGBoost model for credit scoring
    4. Log model, metrics, artifacts to MLflow
    5. Register model in MLflow Model Registry

Usage:
    python credit_scoring.py
    python credit_scoring.py --cob_dt 2025-06-30
"""

import os
import logging
import argparse
from datetime import datetime

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from trino.dbapi import connect
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report
)
from sklearn.preprocessing import LabelEncoder, StandardScaler
import xgboost as xgb

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_NAME = "credit_scoring"
MODEL_NAME = "credit_scoring_model"

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "serving")


def get_trino_connection():
    """Create Trino connection."""
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


def load_data(cob_dt: str) -> pd.DataFrame:
    """Load Customer 360 data from serving layer."""
    logger.info("Loading data from Trino for cob_dt=%s", cob_dt)

    sql = f"""
    SELECT
        customer_id,
        age,
        gender,
        customer_segment,
        total_accounts,
        total_cards,
        total_loans,
        has_credit_card,
        has_savings,
        has_loan,
        total_deposit_balance,
        total_loan_outstanding,
        aum_total,
        txn_count_30d,
        txn_amount_30d,
        days_since_last_txn,
        interaction_count_90d,
        rfm_recency_score,
        rfm_frequency_score,
        rfm_monetary_score,
        churn_flag
    FROM mart_customer_360_current
    WHERE cob_dt = date '{cob_dt}'
    """

    conn = get_trino_connection()
    df = pd.read_sql(sql, conn)
    conn.close()

    logger.info("Loaded %d rows", len(df))
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create features for credit scoring model."""
    logger.info("Engineering features...")

    # Encode categorical variables
    le_segment = LabelEncoder()
    df["segment_encoded"] = le_segment.fit_transform(df["customer_segment"])

    le_gender = LabelEncoder()
    df["gender_encoded"] = le_gender.fit_transform(df["gender"].fillna("Unknown"))

    # Feature engineering
    df["debt_to_asset_ratio"] = np.where(
        df["aum_total"] > 0,
        df["total_loan_outstanding"] / df["aum_total"],
        0
    )

    df["txn_velocity"] = np.where(
        df["days_since_last_txn"] > 0,
        df["txn_count_30d"] / df["days_since_last_txn"],
        0
    )

    df["avg_txn_amount"] = np.where(
        df["txn_count_30d"] > 0,
        df["txn_amount_30d"] / df["txn_count_30d"],
        0
    )

    df["product_diversity"] = (
        df["has_credit_card"] + df["has_savings"] + df["has_loan"]
    )

    # Target variable: credit risk (simplified - based on loan default patterns)
    # In production, this would come from actual loan performance data
    df["credit_risk"] = np.where(
        (df["total_loan_outstanding"] > 0) &
        (df["days_since_last_txn"] > 60) &
        (df["debt_to_asset_ratio"] > 0.8),
        1,  # High risk
        0   # Low risk
    )

    return df


def train_model(df: pd.DataFrame, cob_dt: str):
    """Train XGBoost model and log to MLflow."""
    # Set MLflow tracking URI
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    # Select features
    feature_columns = [
        "age", "total_accounts", "total_cards", "total_loans",
        "total_deposit_balance", "total_loan_outstanding", "aum_total",
        "txn_count_30d", "txn_amount_30d", "days_since_last_txn",
        "interaction_count_90d", "rfm_recency_score", "rfm_frequency_score",
        "rfm_monetary_score", "segment_encoded", "gender_encoded",
        "debt_to_asset_ratio", "txn_velocity", "avg_txn_amount",
        "product_diversity",
    ]

    X = df[feature_columns].fillna(0)
    y = df["credit_risk"]

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train XGBoost
    logger.info("Training XGBoost model...")
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        random_state=42,
        use_label_encoder=False,
        eval_metric="logloss",
    )

    model.fit(
        X_train_scaled, y_train,
        eval_set=[(X_test_scaled, y_test)],
        verbose=False,
    )

    # Predictions
    y_pred = model.predict(X_test_scaled)
    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]

    # Metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    auc = roc_auc_score(y_test, y_pred_proba)

    logger.info("Model Metrics:")
    logger.info("  Accuracy:  %.4f", accuracy)
    logger.info("  Precision: %.4f", precision)
    logger.info("  Recall:    %.4f", recall)
    logger.info("  F1 Score:  %.4f", f1)
    logger.info("  AUC-ROC:   %.4f", auc)

    # Log to MLflow
    with mlflow.start_run(run_name=f"credit_scoring_{cob_dt}"):
        # Log parameters
        mlflow.log_param("model_type", "XGBoost")
        mlflow.log_param("cob_dt", cob_dt)
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("max_depth", 6)
        mlflow.log_param("learning_rate", 0.1)
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        mlflow.log_param("n_features", len(feature_columns))

        # Log metrics
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("auc_roc", auc)

        # Log feature importance
        importance = dict(zip(feature_columns, model.feature_importances_))
        mlflow.log_dict(importance, "feature_importance.json")

        # Log model
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
        )

        # Log scaler
        mlflow.sklearn.log_model(scaler, artifact_path="scaler")

        # Log classification report
        report = classification_report(y_test, y_pred, output_dict=True)
        mlflow.log_dict(report, "classification_report.json")

        # Log confusion matrix
        cm = confusion_matrix(y_test, y_pred)
        mlflow.log_dict({"matrix": cm.tolist()}, "confusion_matrix.json")

        run_id = mlflow.active_run().info.run_id
        logger.info("MLflow Run ID: %s", run_id)
        logger.info("Model registered as: %s", MODEL_NAME)

    return model, scaler, {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "auc_roc": auc,
    }


def main():
    parser = argparse.ArgumentParser(description="Credit Scoring ML Pipeline")
    parser.add_argument("--cob_dt", default="2025-06-30", help="Business date")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info(" Credit Scoring ML Pipeline")
    logger.info("=" * 60)
    logger.info(" Cob_dt: %s", args.cob_dt)

    # Load data
    df = load_data(args.cob_dt)

    if len(df) == 0:
        logger.error("No data found for cob_dt=%s", args.cob_dt)
        return

    # Feature engineering
    df = engineer_features(df)

    # Train model
    model, scaler, metrics = train_model(df, args.cob_dt)

    logger.info("=" * 60)
    logger.info(" Pipeline Complete!")
    logger.info("=" * 60)
    logger.info(" Model: %s", MODEL_NAME)
    logger.info(" Metrics: %s", metrics)


if __name__ == "__main__":
    main()
