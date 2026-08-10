"""
Contract Registry — Banking Data Platform

Loads and validates all YAML dataset contracts from the datasets/ directory.
Provides lookup by dataset_id and validation of contract schema.
"""

import os
from logging import getLogger
from pathlib import Path

import yaml

from governance.contracts import DatasetContract

log = getLogger("contracts_registry")

# Default datasets directory (relative to this file)
_DEFAULT_DATASETS_DIR = os.path.join(os.path.dirname(__file__), "datasets")


class ContractRegistry:
    """
    Registry of all dataset contracts.

    Usage:
        registry = ContractRegistry()  # loads from default datasets/ dir
        contract = registry.get_contract("banking.core_customer_silver")
        all_contracts = registry.get_all_contracts()
    """

    def __init__(self, datasets_dir: str | None = None):
        """
        Initialize registry by loading all YAML contracts.

        Args:
            datasets_dir: Path to directory containing YAML contract files.
                         Defaults to governance/datasets/
        """
        self._datasets_dir = datasets_dir or _DEFAULT_DATASETS_DIR
        self._contracts: dict[str, DatasetContract] = {}
        self._errors: list[dict] = []
        self._load_all()

    def _load_all(self) -> None:
        """Load and validate all YAML files in datasets directory."""
        datasets_path = Path(self._datasets_dir)

        if not datasets_path.exists():
            log.warning(f"Datasets directory not found: {self._datasets_dir}")
            return

        yaml_files = sorted(datasets_path.glob("*.yaml"))
        yaml_files.extend(sorted(datasets_path.glob("*.yml")))

        log.info(f"Loading contracts from {self._datasets_dir} ...")

        for yaml_file in yaml_files:
            try:
                self._load_contract(yaml_file)
            except Exception as e:
                error_msg = f"Failed to load {yaml_file.name}: {e}"
                log.error(error_msg)
                self._errors.append({
                    "file": yaml_file.name,
                    "error": str(e),
                })

        log.info(
            f"Loaded {len(self._contracts)} contracts, "
            f"{len(self._errors)} errors"
        )

    def _load_contract(self, yaml_path: Path) -> None:
        """Load a single YAML contract file."""
        with open(yaml_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        if raw is None:
            raise ValueError(f"Empty YAML file: {yaml_path}")

        # Parse into Pydantic model (validates schema)
        contract = DatasetContract(**raw)

        # Register by dataset_id
        if contract.dataset_id in self._contracts:
            log.warning(
                f"Duplicate dataset_id '{contract.dataset_id}' "
                f"in {yaml_path.name} — overwriting"
            )

        self._contracts[contract.dataset_id] = contract
        log.debug(f"  Loaded: {contract.dataset_id}")

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def get_contract(self, dataset_id: str) -> DatasetContract | None:
        """
        Get contract by dataset_id.

        Args:
            dataset_id: Unique identifier (e.g., 'banking.core_customer_silver')

        Returns:
            DatasetContract or None if not found
        """
        return self._contracts.get(dataset_id)

    def get_all_contracts(self) -> dict[str, DatasetContract]:
        """Get all loaded contracts."""
        return dict(self._contracts)

    def get_contracts_by_layer(self, layer: str) -> dict[str, DatasetContract]:
        """Get all contracts for a specific layer (bronze, silver, gold)."""
        return {
            k: v for k, v in self._contracts.items()
            if v.layer == layer
        }

    def get_contracts_by_owner(self, owner: str) -> dict[str, DatasetContract]:
        """Get all contracts for a specific owner."""
        return {
            k: v for k, v in self._contracts.items()
            if v.owner == owner
        }

    @property
    def contract_count(self) -> int:
        """Number of loaded contracts."""
        return len(self._contracts)

    @property
    def errors(self) -> list[dict]:
        """List of loading errors."""
        return list(self._errors)

    @property
    def has_errors(self) -> bool:
        """Whether there were any loading errors."""
        return len(self._errors) > 0

    def validate_all(self) -> list[dict]:
        """
        Validate all loaded contracts and return issues.

        Returns:
            List of validation issues (empty if all valid)
        """
        issues = []
        for dataset_id, contract in self._contracts.items():
            # Check required fields
            if not contract.physical_location.table:
                issues.append({
                    "dataset_id": dataset_id,
                    "issue": "Missing physical_location.table",
                })
            if not contract.owner:
                issues.append({
                    "dataset_id": dataset_id,
                    "issue": "Missing owner",
                })
            if not contract.business_purpose:
                issues.append({
                    "dataset_id": dataset_id,
                    "issue": "Missing business_purpose",
                })
        return issues

    def summary(self) -> str:
        """Print summary of loaded contracts."""
        lines = [
            "Contract Registry Summary",
            f"  Total contracts: {self.contract_count}",
            f"  Loading errors: {len(self._errors)}",
            "",
        ]

        # Group by layer
        layers = {}
        for contract in self._contracts.values():
            layers.setdefault(contract.layer, []).append(contract)

        for layer, contracts in sorted(layers.items()):
            lines.append(f"  [{layer.upper()}] {len(contracts)} contracts")
            for c in contracts:
                lines.append(f"    - {c.dataset_id} ({c.quality_class})")

        if self._errors:
            lines.append("")
            lines.append("  ERRORS:")
            for err in self._errors:
                lines.append(f"    - {err['file']}: {err['error']}")

        return "\n".join(lines)
