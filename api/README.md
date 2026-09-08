# Customer 360 API

REST API serving Customer 360 data from the Banking Data Platform serving layer.

## Architecture

```
Client → FastAPI (port 8000) → Trino (port 8080) → Iceberg Serving Layer
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/customer/{id}/overview` | Customer overview (segment, accounts, AUM) |
| GET | `/customer/{id}/transactions` | Transaction summary (30d) |
| GET | `/customer/{id}/risk-score` | Risk assessment (churn, RFM) |
| GET | `/customer/{id}/recommendations` | Product recommendations |

## Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cob_dt` | string | latest | Business date (YYYY-MM-DD) |

## Example Requests

### Customer Overview
```bash
curl http://localhost:8000/customer/1/overview
curl http://localhost:8000/customer/1/overview?cob_dt=2025-06-30
```

### Transaction Summary
```bash
curl http://localhost:8000/customer/1/transactions
```

### Risk Score
```bash
curl http://localhost:8000/customer/1/risk-score
```

### Recommendations
```bash
curl http://localhost:8000/customer/1/recommendations
```

## Response Examples

### Overview
```json
{
  "customer_id": 1,
  "full_name_masked": "Nguyen V***",
  "age": 35,
  "gender": "M",
  "customer_segment": "RETAIL",
  "kyc_status": "VERIFIED",
  "total_accounts": 2,
  "total_cards": 3,
  "total_loans": 1,
  "has_credit_card": true,
  "has_savings": true,
  "has_loan": true,
  "aum_total": 150000000,
  "aum_bucket": "100M-500M",
  "cob_dt": "2025-06-30"
}
```

### Risk Score
```json
{
  "customer_id": 1,
  "churn_risk": "LOW",
  "is_churn_candidate": false,
  "rfm_segment": "Loyal Customers",
  "rfm_score": 75.5,
  "days_since_last_txn": 5,
  "risk_factors": [],
  "cob_dt": "2025-06-30"
}
```

## Docker

```bash
# Build
docker build -t banking-api:latest .

# Run
docker run -p 8000:8000 \
  -e TRINO_HOST=trino \
  -e TRINO_PORT=8080 \
  banking-api:latest
```

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# API docs
http://localhost:8000/docs
```
