"""
Banking Data Platform — Customer 360 API
=========================================
REST API serving Customer 360 data from the serving layer via Trino.

Endpoints:
    GET /customer/{customer_id}/overview      — Customer overview
    GET /customer/{customer_id}/transactions  — Recent transactions
    GET /customer/{customer_id}/risk-score    — Risk assessment
    GET /customer/{customer_id}/recommendations — Product recommendations
    GET /health                               — Health check
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from trino.dbapi import connect

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── App ─────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Customer 360 API",
    description="REST API for Customer 360 data — Banking Data Platform",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── CORS ────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ──────────────────────────────────────────────────────────────────
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "admin")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "serving")


# ── Database Connection ─────────────────────────────────────────────────────
def get_trino_connection():
    """Create a Trino connection."""
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


def execute_query(sql: str, params: tuple = None) -> list[dict]:
    """Execute a Trino query and return results as list of dicts."""
    conn = get_trino_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()
        conn.close()


# ── Response Models ─────────────────────────────────────────────────────────
class CustomerOverview(BaseModel):
    """Customer overview response."""
    customer_id: int
    full_name_masked: str
    age: Optional[int]
    gender: Optional[str]
    customer_segment: str
    kyc_status: str
    register_date: Optional[str]
    total_accounts: int
    total_cards: int
    total_loans: int
    has_credit_card: bool
    has_savings: bool
    has_loan: bool
    aum_total: Optional[float]
    aum_bucket: Optional[str]
    primary_branch_code: Optional[str]
    cob_dt: str


class TransactionSummary(BaseModel):
    """Transaction summary response."""
    customer_id: int
    txn_count_30d: int
    txn_amount_30d: Optional[float]
    last_txn_date: Optional[str]
    days_since_last_txn: Optional[int]
    primary_channel: Optional[str]
    cob_dt: str


class RiskScore(BaseModel):
    """Risk assessment response."""
    customer_id: int
    churn_risk: str
    is_churn_candidate: bool
    rfm_segment: str
    rfm_score: Optional[float]
    days_since_last_txn: Optional[int]
    risk_factors: list[str]
    cob_dt: str


class Recommendation(BaseModel):
    """Product recommendation response."""
    customer_id: int
    cross_sell_credit_card_flag: bool
    primary_opportunity: Optional[str]
    rfm_segment: str
    aum_bucket: Optional[str]
    recommended_products: list[str]
    campaign_type: str
    cob_dt: str


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    timestamp: str
    trino_host: str
    trino_port: int


# ── Endpoints ───────────────────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Health check endpoint."""
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        trino_status = "connected"
    except Exception as e:
        logger.error(f"Trino health check failed: {e}")
        trino_status = "disconnected"

    return HealthResponse(
        status="healthy" if trino_status == "connected" else "degraded",
        timestamp=datetime.utcnow().isoformat(),
        trino_host=TRINO_HOST,
        trino_port=TRINO_PORT,
    )


@app.get("/customer/{customer_id}/overview", response_model=CustomerOverview, tags=["Customer"])
async def get_customer_overview(
    customer_id: int,
    cob_dt: Optional[str] = Query(None, description="Business date (YYYY-MM-DD). Defaults to latest.")
):
    """
    Get customer overview: segment, accounts, cards, loans, AUM.

    Returns a comprehensive view of the customer including:
    - Personal info (masked)
    - Product holdings
    - Asset under management
    - KYC status
    """
    cob_dt_filter = f"AND cob_dt = date '{cob_dt}'" if cob_dt else ""

    sql = f"""
    SELECT
        customer_id,
        full_name_masked,
        age,
        gender,
        customer_segment,
        kyc_status,
        CAST(register_date AS VARCHAR) AS register_date,
        total_accounts,
        total_cards,
        total_loans,
        CASE WHEN has_credit_card = 1 THEN true ELSE false END AS has_credit_card,
        CASE WHEN has_savings = 1 THEN true ELSE false END AS has_savings,
        CASE WHEN has_loan = 1 THEN true ELSE false END AS has_loan,
        aum_total,
        aum_bucket,
        primary_branch_code,
        CAST(cob_dt AS VARCHAR) AS cob_dt
    FROM mart_customer_360_current
    WHERE customer_id = ?
    {cob_dt_filter}
    LIMIT 1
    """

    results = execute_query(sql, (customer_id,))
    if not results:
        raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")

    return CustomerOverview(**results[0])


@app.get("/customer/{customer_id}/transactions", response_model=TransactionSummary, tags=["Customer"])
async def get_customer_transactions(
    customer_id: int,
    cob_dt: Optional[str] = Query(None, description="Business date (YYYY-MM-DD). Defaults to latest.")
):
    """
    Get customer transaction summary for the last 30 days.

    Returns:
    - Transaction count and amount
    - Last transaction date
    - Primary channel used
    """
    cob_dt_filter = f"AND cob_dt = date '{cob_dt}'" if cob_dt else ""

    sql = f"""
    SELECT
        customer_id,
        txn_count_30d,
        txn_amount_30d,
        CAST(last_txn_date AS VARCHAR) AS last_txn_date,
        days_since_last_txn,
        primary_channel,
        CAST(cob_dt AS VARCHAR) AS cob_dt
    FROM mart_customer_360_current
    WHERE customer_id = ?
    {cob_dt_filter}
    LIMIT 1
    """

    results = execute_query(sql, (customer_id,))
    if not results:
        raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")

    return TransactionSummary(**results[0])


@app.get("/customer/{customer_id}/risk-score", response_model=RiskScore, tags=["Risk"])
async def get_risk_score(
    customer_id: int,
    cob_dt: Optional[str] = Query(None, description="Business date (YYYY-MM-DD). Defaults to latest.")
):
    """
    Get customer risk assessment based on transaction patterns and RFM analysis.

    Returns:
    - Churn risk level (HIGH/MEDIUM/LOW)
    - RFM segment and score
    - Risk factors identified
    """
    cob_dt_filter = f"AND cob_dt = date '{cob_dt}'" if cob_dt else ""

    sql = f"""
    SELECT
        c.customer_id,
        c.churn_risk,
        c.is_churn_candidate,
        m.rfm_segment,
        m.rfm_recency_score + m.rfm_frequency_score + m.rfm_monetary_score AS rfm_score,
        c.days_since_last_txn,
        CAST(c.cob_dt AS VARCHAR) AS cob_dt
    FROM churn_prediction_current c
    JOIN mart_customer_360_current m ON c.customer_id = m.customer_id
    WHERE c.customer_id = ?
    {cob_dt_filter}
    LIMIT 1
    """

    results = execute_query(sql, (customer_id,))
    if not results:
        raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")

    row = results[0]

    # Build risk factors
    risk_factors = []
    if row.get("is_churn_candidate"):
        risk_factors.append("Churn candidate — high risk of attrition")
    if row.get("days_since_last_txn", 0) and row["days_since_last_txn"] > 60:
        risk_factors.append(f"Inactive for {row['days_since_last_txn']} days")
    if row.get("rfm_segment") in ["At Risk", "About to Sleep", "Hibernating", "Lost"]:
        risk_factors.append(f"RFM segment: {row['rfm_segment']}")
    if row.get("churn_risk") == "HIGH":
        risk_factors.append("High churn risk score")

    return RiskScore(
        customer_id=row["customer_id"],
        churn_risk=row["churn_risk"],
        is_churn_candidate=row["is_churn_candidate"],
        rfm_segment=row["rfm_segment"],
        rfm_score=row.get("rfm_score"),
        days_since_last_txn=row.get("days_since_last_txn"),
        risk_factors=risk_factors,
        cob_dt=row["cob_dt"],
    )


@app.get("/customer/{customer_id}/recommendations", response_model=Recommendation, tags=["Recommendations"])
async def get_recommendations(
    customer_id: int,
    cob_dt: Optional[str] = Query(None, description="Business date (YYYY-MM-DD). Defaults to latest.")
):
    """
    Get product recommendations based on RFM analysis and product gap.

    Returns:
    - Cross-sell opportunities
    - Recommended products
    - Campaign type for engagement
    """
    cob_dt_filter = f"AND cob_dt = date '{cob_dt}'" if cob_dt else ""

    sql = f"""
    SELECT
        m.customer_id,
        CASE WHEN m.cross_sell_credit_card_flag = 1 THEN true ELSE false END AS cross_sell_credit_card_flag,
        ct.primary_opportunity,
        m.rfm_segment,
        m.aum_bucket,
        ct.campaign_type,
        CAST(m.cob_dt AS VARCHAR) AS cob_dt
    FROM mart_customer_360_current m
    LEFT JOIN campaign_target_current ct ON m.customer_id = ct.customer_id
    WHERE m.customer_id = ?
    {cob_dt_filter}
    LIMIT 1
    """

    results = execute_query(sql, (customer_id,))
    if not results:
        raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")

    row = results[0]

    # Build recommended products
    recommended_products = []
    if row.get("cross_sell_credit_card_flag"):
        recommended_products.append("Credit Card")
    if row.get("primary_opportunity"):
        recommended_products.append(row["primary_opportunity"])
    if row.get("aum_bucket") in ["500M-1B", "1B+"]:
        recommended_products.append("Priority Banking Package")
    if row.get("rfm_segment") in ["Loyal Customers", "Potential Loyalists"]:
        recommended_products.append("Savings Account Premium")

    return Recommendation(
        customer_id=row["customer_id"],
        cross_sell_credit_card_flag=row["cross_sell_credit_card_flag"],
        primary_opportunity=row.get("primary_opportunity"),
        rfm_segment=row["rfm_segment"],
        aum_bucket=row.get("aum_bucket"),
        recommended_products=recommended_products,
        campaign_type=row.get("campaign_type", "GENERAL"),
        cob_dt=row["cob_dt"],
    )


# ── Root ────────────────────────────────────────────────────────────────────
@app.get("/", tags=["System"])
async def root():
    """API root — returns available endpoints."""
    return {
        "service": "Customer 360 API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "overview": "/customer/{customer_id}/overview",
            "transactions": "/customer/{customer_id}/transactions",
            "risk_score": "/customer/{customer_id}/risk-score",
            "recommendations": "/customer/{customer_id}/recommendations",
        },
        "docs": "/docs",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
