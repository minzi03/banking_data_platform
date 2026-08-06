"""
Governance Module — Banking Data Platform

Provides data contracts, enforcement, anomaly detection, freshness checks,
schema drift detection, lineage tracking, and audit trail for the
Medallion architecture.
"""

from governance.contracts import DatasetContract, QualityRules, AIGovernance
from governance.contracts_registry import ContractRegistry
from governance.enforcement import ContractEnforcer, ValidationResult
from governance.anomaly_detection import AnomalyDetector, AnomalyResult
from governance.freshness_checks import FreshnessChecker, FreshnessResult
from governance.schema_drift import SchemaDriftDetector, SchemaDriftResult
from governance.lineage import LineageTracker, LineageRecord, TransformType
from governance.audit import AuditLogger, AuditRecord, AuditAction

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
