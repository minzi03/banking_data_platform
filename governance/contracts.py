"""
Data Contract Models — Banking Data Platform

Pydantic models for YAML-based dataset contracts.
Each governed dataset has a contract defining:
- Metadata (owner, purpose, SLA)
- Quality rules (schema, nullability, uniqueness, ranges)
- AI governance (usage policies, risk tiers)
"""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class QualityClass(str, Enum):
    """Quality classification for governed datasets."""
    CRITICAL = "critical"          # Blocks pipeline on failure
    IMPORTANT = "important"        # Logs warning, continues
    INFORMATIONAL = "informational"  # Logs only


class RefreshSLA(str, Enum):
    """Expected data refresh frequency."""
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class Severity(str, Enum):
    """Check severity level."""
    FAIL = "FAIL"    # Blocks pipeline
    WARN = "WARN"    # Logs but passes


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------

class PhysicalLocation(BaseModel):
    """Physical location of the dataset in the lakehouse."""
    catalog: str = Field(..., description="Iceberg catalog name (e.g., 'lakehouse')")
    namespace: str = Field(..., description="Schema/namespace (e.g., 'silver', 'gold')")
    table: str = Field(..., description="Table name (e.g., 'dim_customer')")

    @property
    def full_table_name(self) -> str:
        return f"{self.catalog}.{self.namespace}.{self.table}"


class RangeCheck(BaseModel):
    """Range validation for a numeric column."""
    column: str
    min_value: float | None = None
    max_value: float | None = None


class ReferentialIntegrity(BaseModel):
    """Foreign key integrity check."""
    column: str
    ref_table: str
    ref_column: str


class QualityRules(BaseModel):
    """Quality rules for dataset validation."""
    required_columns: list[str] = Field(
        default_factory=list,
        description="Columns that must exist in the schema"
    )
    non_null_columns: list[str] = Field(
        default_factory=list,
        description="Columns that must not contain NULL values"
    )
    min_row_count: int | None = Field(
        default=None,
        description="Minimum expected row count"
    )
    max_row_count: int | None = Field(
        default=None,
        description="Maximum expected row count"
    )
    date_column: str | None = Field(
        default=None,
        description="Primary date column for freshness checks"
    )
    forbid_future_dates: bool = Field(
        default=False,
        description="If True, dates must not be in the future"
    )
    unique_check: list[str] = Field(
        default_factory=list,
        description="Columns that must be individually unique"
    )
    unique_column_sets: list[list[str]] = Field(
        default_factory=list,
        description="Column sets that must be unique together (composite uniqueness)"
    )
    range_checks: list[RangeCheck] = Field(
        default_factory=list,
        description="Range validations for numeric columns"
    )
    referential_integrity: list[ReferentialIntegrity] = Field(
        default_factory=list,
        description="Foreign key integrity checks"
    )
    freshness_sla_hours: int | None = Field(
        default=None,
        description="Maximum allowed data age in hours"
    )


class AIGovernance(BaseModel):
    """AI governance policies for the dataset."""
    ai_use_allowed: bool = Field(
        default=True,
        description="Whether AI/ML use is permitted"
    )
    risk_tier: str = Field(
        default="limited_risk",
        description="Risk classification (e.g., 'minimal_risk', 'limited_risk', 'high_risk')"
    )
    intended_uses: list[str] = Field(
        default_factory=list,
        description="Approved use cases"
    )
    prohibited_uses: list[str] = Field(
        default_factory=list,
        description="Prohibited use cases"
    )
    human_oversight_required: bool = Field(
        default=False,
        description="Whether human oversight is required for AI decisions"
    )
    model_lineage_required: bool = Field(
        default=False,
        description="Whether ML lineage tracking is required"
    )


# ---------------------------------------------------------------------------
# Main Contract Model
# ---------------------------------------------------------------------------

class DatasetContract(BaseModel):
    """
    Dataset Contract — defines metadata, quality rules, and governance
    for a governed dataset in the lakehouse.
    """
    dataset_id: str = Field(
        ...,
        description="Unique identifier (e.g., 'banking.core_customer_silver')"
    )
    owner: str = Field(
        ...,
        description="Team or person responsible for this dataset"
    )
    business_purpose: str = Field(
        ...,
        description="Business description of the dataset"
    )
    refresh_sla: RefreshSLA = Field(
        default=RefreshSLA.DAILY,
        description="Expected refresh frequency"
    )
    quality_class: QualityClass = Field(
        default=QualityClass.IMPORTANT,
        description="Quality classification"
    )
    layer: str = Field(
        ...,
        description="Medallion layer (bronze, silver, gold)"
    )
    physical_location: PhysicalLocation = Field(
        ...,
        description="Physical location in the lakehouse"
    )
    dag_id: str | None = Field(
        default=None,
        description="Airflow DAG ID that produces this dataset"
    )
    upstream_dataset_ids: list[str] = Field(
        default_factory=list,
        description="List of upstream dataset IDs"
    )
    quality_rules: QualityRules = Field(
        default_factory=QualityRules,
        description="Quality validation rules"
    )
    ai_governance: AIGovernance = Field(
        default_factory=AIGovernance,
        description="AI governance policies"
    )

    class Config:
        use_enum_values = True
        validate_assignment = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return self.model_dump()
