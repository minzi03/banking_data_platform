"""
Governance Module — Banking Data Platform

Provides data contracts, enforcement, anomaly detection, freshness checks,
schema drift detection, lineage tracking, and audit trail for the
Medallion architecture.
"""

from governance.anomaly_detection import AnomalyDetector, AnomalyResult
from governance.audit import AuditAction, AuditLogger, AuditRecord
from governance.contracts import AIGovernance, DatasetContract, QualityRules
from governance.contracts_registry import ContractRegistry
from governance.enforcement import ContractEnforcer, ValidationResult
from governance.freshness_checks import FreshnessChecker, FreshnessResult
from governance.lineage import LineageRecord, LineageTracker, TransformType
from governance.schema_drift import SchemaDriftDetector, SchemaDriftResult

__all__ = [
    # Contracts
    "DatasetContract",
    "QualityRules",
    "AIGovernance",
    "ContractRegistry",
    "ContractEnforcer",
    "ValidationResult",
    # Anomaly Detection
    "AnomalyDetector",
    "AnomalyResult",
    # Freshness Checks
    "FreshnessChecker",
    "FreshnessResult",
    # Schema Drift
    "SchemaDriftDetector",
    "SchemaDriftResult",
    # Lineage
    "LineageTracker",
    "LineageRecord",
    "TransformType",
    # Audit
    "AuditLogger",
    "AuditRecord",
    "AuditAction",
]
