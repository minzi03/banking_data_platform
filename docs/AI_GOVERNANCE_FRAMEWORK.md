# AI Governance Framework — Banking Data Platform

> **Date**: 2026-09-07
> **Reference**: GPBank JD requires AI Governance (hallucination, bias, drift detection)
> **Scope**: Framework for responsible AI/ML in banking data platform

---

## 1. Executive Summary

This framework establishes governance controls for AI/ML models deployed within the Banking Data Platform. It addresses key requirements from banking regulators and industry best practices, including:

- Model validation and risk management
- Data quality and fairness
- Transparency and explainability
- Monitoring and alerting
- Ethical considerations

---

## 2. AI Governance Principles

| Principle | Description | Implementation |
|-----------|-------------|----------------|
| **Fairness** | Models must not discriminate against protected groups | Bias detection, fairness metrics |
| **Transparency** | Model decisions must be explainable | SHAP, LIME, feature importance |
| **Accountability** | Clear ownership for every model | Model Registry, approval workflows |
| **Privacy** | Customer data must be protected | PII masking, differential privacy |
| **Robustness** | Models must perform reliably | Drift detection, A/B testing |
| **Compliance** | Adhere to banking regulations | Audit trails, regulatory reporting |

---

## 3. Model Risk Management

### 3.1 Model Classification

| Risk Level | Description | Governance Requirements |
|------------|-------------|------------------------|
| **Low** | Internal analytics, non-decision-making | Basic validation, documentation |
| **Medium** | Customer-facing recommendations | Full validation, bias testing, monitoring |
| **High** | Credit decisions, fraud detection | Independent validation, regulatory approval |
| **Critical** | AML/KYC decisions | Board approval, external audit |

### 3.2 Model Lifecycle

```
┌─────────────┐
│  Development │ ← Feature engineering, training
└──────┬──────┘
       ↓
┌─────────────┐
│  Validation  │ ← Backtesting, bias testing, stress testing
└──────┬──────┘
       ↓
┌─────────────┐
│  Approval    │ ← Model Risk Committee review
└──────┬──────┘
       ↓
┌─────────────┐
│  Deployment  │ ← Champion/Challenger, A/B testing
└──────┬──────┘
       ↓
┌─────────────┐
│  Monitoring  │ ← Performance, drift, fairness metrics
└──────┬──────┘
       ↓
┌─────────────┐
│  Retirement  │ ← Model decommissioning
└─────────────┘
```

### 3.3 Model Registry (MLflow)

Every model must be registered with:

| Field | Description |
|-------|-------------|
| Model Name | Unique identifier |
| Version | Semantic versioning |
| Owner | Responsible team/individual |
| Risk Level | Low/Medium/High/Critical |
| Use Case | Business purpose |
| Training Data | Dataset reference |
| Performance Metrics | Accuracy, precision, recall, AUC |
| Fairness Metrics | Demographic parity, equal opportunity |
| Approval Status | Pending/Approved/Rejected |
| Last Review Date | Most recent validation date |
| Next Review Date | Scheduled next validation |

---

## 4. Bias Detection & Fairness

### 4.1 Protected Attributes

In banking, models must be tested for bias against:

| Attribute | Values to Test |
|-----------|---------------|
| Gender | M, F, O |
| Age | 18-25, 26-35, 36-50, 50+ |
| Location | Urban, Rural |
| Ethnicity | (where legally permitted) |

### 4.2 Fairness Metrics

| Metric | Definition | Threshold |
|--------|-----------|-----------|
| **Demographic Parity** | P(ŷ=1\|A=a) = P(ŷ=1\|A=b) | Difference < 0.1 |
| **Equal Opportunity** | TPR across groups equal | Difference < 0.1 |
| **Equalized Odds** | TPR and FPR across groups equal | Difference < 0.1 |
| **Disparate Impact** | Selection rate ratio | > 0.8 |

### 4.3 Implementation

```python
# Example: Bias detection in credit scoring
from fairlearn.metrics import MetricFrame, demographic_parity_difference

# Load model predictions
y_pred = model.predict(X_test)

# Create metric frame
metric_frame = MetricFrame(
    metrics={"accuracy": accuracy_score},
    y_true=y_test,
    y_pred=y_pred,
    sensitive_features=X_test["gender"]
)

# Check demographic parity
dp_diff = demographic_parity_difference(
    y_test, y_pred, sensitive_features=X_test["gender"]
)

# Alert if bias detected
if abs(dp_diff) > 0.1:
    logger.warning(f"Bias detected: demographic parity difference = {dp_diff}")
    # Trigger review workflow
```

---

## 5. Model Drift Detection

### 5.1 Types of Drift

| Drift Type | Description | Detection Method |
|------------|-------------|-----------------|
| **Data Drift** | Input feature distribution changes | PSI, KS test |
| **Concept Drift** | Relationship between features and target changes | Performance monitoring |
| **Prediction Drift** | Output distribution changes | KL divergence |

### 5.2 Monitoring Schedule

| Metric | Frequency | Alert Threshold |
|--------|-----------|----------------|
| PSI (all features) | Daily | > 0.2 |
| KS statistic | Daily | > 0.1 |
| Model accuracy | Weekly | Drop > 5% |
| AUC-ROC | Weekly | Drop > 3% |
| Feature importance | Monthly | Top feature change > 20% |

### 5.3 Alert Workflow

```
Drift Detected → Alert Data Science Team → Root Cause Analysis
                                            ↓
                                    ┌───────┴───────┐
                                    ↓               ↓
                              Data Issue      Model Degradation
                                    ↓               ↓
                              Fix Data        Retrain Model
                                    ↓               ↓
                              Revalidate      Revalidate
                                    ↓               ↓
                                    └───────┬───────┘
                                            ↓
                                      Update Production
```

---

## 6. Hallucination Detection (for LLM/GenAI)

### 6.1 What is Hallucination in Banking?

- Model generates plausible but incorrect information
- Recommendations based on non-existent patterns
- False positives in fraud detection presented as facts

### 6.2 Detection Methods

| Method | Description | Implementation |
|--------|-------------|---------------|
| **Fact Verification** | Cross-reference outputs with source data | Database lookups |
| **Confidence Scoring** | Low confidence = potential hallucination | Model probability thresholds |
| **Human-in-the-Loop** | Expert review for high-stakes decisions | Approval workflows |
| **Consistency Checks** | Multiple model agreement | Ensemble methods |

### 6.3 Controls

```python
# Example: Confidence-based hallucination detection
def validate_prediction(prediction, confidence, threshold=0.7):
    """Validate model prediction for potential hallucination."""
    if confidence < threshold:
        return {
            "status": "REVIEW_REQUIRED",
            "reason": f"Low confidence ({confidence:.2%})",
            "recommendation": "Route to human reviewer"
        }
    return {"status": "APPROVED", "confidence": confidence}
```

---

## 7. Data Leakage Prevention

### 7.1 What is Data Leakage?

- Target variable information accidentally included in features
- Future information used to predict past outcomes
- Training/test data contamination

### 7.2 Prevention Controls

| Control | Description |
|---------|-------------|
| **Temporal Split** | Train on past, test on future |
| **Feature Audit** | Review all features for leakage risk |
| **Data Versioning** | Track exact datasets used for training |
| **Pipeline Isolation** | Separate training and serving pipelines |

### 7.3 Validation Checklist

- [ ] No future data in training set
- [ ] No target-derived features in input
- [ ] Train/test split respects time ordering
- [ ] Feature engineering uses only historical data
- [ ] No data leakage from test set to training set

---

## 8. Prompt Injection Controls (for GenAI)

### 8.1 Threats

- Users manipulating prompts to extract sensitive data
- Adversarial inputs causing incorrect outputs
- Unauthorized access through prompt manipulation

### 8.2 Controls

| Control | Description |
|---------|-------------|
| **Input Sanitization** | Filter special characters, SQL injection patterns |
| **Output Validation** | Verify responses against business rules |
| **Access Controls** | Role-based access to GenAI features |
| **Rate Limiting** | Prevent abuse through rate limiting |
| **Audit Logging** | Log all prompts and responses |

---

## 9. Model Explainability

### 9.1 Techniques

| Technique | Use Case | Tool |
|-----------|----------|------|
| **SHAP** | Feature importance | shap library |
| **LIME** | Local explanations | lime library |
| **Feature Importance** | Global importance | XGBoost, sklearn |
| **Partial Dependence** | Feature effects | pdpbox library |

### 9.2 Documentation Requirements

Every model must have:

1. **Model Card**: Purpose, training data, performance metrics
2. **Feature Documentation**: Description, source, importance
3. **Limitations**: Known biases, edge cases
4. **Fairness Report**: Bias testing results
5. **Monitoring Plan**: Metrics, thresholds, alerting

---

## 10. Audit Trail

### 10.1 Required Logging

| Event | Data to Log |
|-------|-------------|
| **Model Training** | Dataset, hyperparameters, metrics, timestamp |
| **Model Deployment** | Version, timestamp, deployed_by |
| **Prediction** | Input, output, confidence, timestamp |
| **Drift Alert** | Feature, metric, threshold, actual value |
| **Bias Detection** | Attribute, metric, threshold, actual value |
| **Model Update** | Old version, new version, reason, approved_by |

### 10.2 Storage

- MLflow tracking server (model metadata)
- PostgreSQL audit_log (prediction logging)
- MinIO (model artifacts, training data)

---

## 11. Regulatory Compliance

### 11.1 Applicable Regulations

| Regulation | Jurisdiction | Key Requirements |
|------------|-------------|------------------|
| **BCBS 239** | Global (Basel) | Risk data aggregation, accuracy |
| **EU AI Act** | European Union | High-risk AI requirements |
| **SBV Circular 35** | Vietnam | Card transaction monitoring |
| **GDPR** | Global | Data privacy, right to explanation |

### 11.2 Compliance Checklist

- [ ] Model inventory maintained in MLflow Registry
- [ ] Risk classification completed for all models
- [ ] Bias testing documented and approved
- [ ] Monitoring dashboards operational
- [ ] Audit trail logging enabled
- [ ] Model cards documented
- [ ] Regulatory reports generated

---

## 12. Roles & Responsibilities

| Role | Responsibilities |
|------|-----------------|
| **Model Owner** | Business accountability, use case definition |
| **Data Scientist** | Model development, validation, documentation |
| **ML Engineer** | Deployment, monitoring, infrastructure |
| **Risk Manager** | Risk assessment, approval, compliance |
| **Compliance Officer** | Regulatory reporting, audit support |
| **Data Steward** | Data quality, lineage, privacy |

---

## 13. Tools & Infrastructure

| Tool | Purpose | Location |
|------|---------|----------|
| **MLflow** | Model tracking, registry | http://localhost:5000 |
| **SHAP** | Model explainability | Python library |
| **Fairlearn** | Bias detection | Python library |
| **Great Expectations** | Data quality | Future enhancement |
| **Prometheus** | Metrics collection | http://localhost:9095 |
| **Grafana** | Monitoring dashboards | http://localhost:3000 |

---

## 14. References

- **GPBank JD**: "AI Governance — hallucination, bias, drift, data leakage, prompt injection"
- **SR 11-7 (OCC)**: Guidance on Model Risk Management
- **BCBS 239**: Principles for effective risk data aggregation
- **EU AI Act**: Regulation on artificial intelligence
- **Fairlearn**: Microsoft's fairness assessment toolkit
- **MLflow**: Open-source ML platform
