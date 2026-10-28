"""Tests for topic registry and schema reference validation."""

import shutil
from pathlib import Path

import yaml

from schemas.validate_topic_registry import validate_registry

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "schemas" / "topic_registry.yml"


def registry_data() -> dict:
    return yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))


def write_registry(tmp_path, registry: dict) -> Path:
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    shutil.copy2(ROOT / "schemas" / "online_transaction_event_v1.json", schema_dir)
    path = schema_dir / "topic_registry.yml"
    path.write_text(yaml.safe_dump(registry), encoding="utf-8")
    return path


def test_current_topic_registry_is_valid():
    assert validate_registry(REGISTRY) == []


def test_registry_requires_idempotency_key(tmp_path):
    registry = registry_data()
    registry["topics"][0].pop("idempotency_key")

    errors = validate_registry(write_registry(tmp_path, registry))

    assert any("idempotency_key" in error for error in errors)


def test_unknown_owner_is_rejected(tmp_path):
    registry = registry_data()
    registry["topics"][0]["owner"] = "unknown-team"

    errors = validate_registry(write_registry(tmp_path, registry))

    assert any("owner 'unknown-team' is not declared" in error for error in errors)


def test_schema_event_identity_must_match_registry(tmp_path):
    registry = registry_data()
    registry["topics"][0]["event_type"] = "WRONG_EVENT"

    errors = validate_registry(write_registry(tmp_path, registry))

    assert any("event_type does not match schema" in error for error in errors)


def test_duplicate_topics_are_rejected(tmp_path):
    registry = registry_data()
    registry["topics"].append(registry["topics"][0].copy())

    errors = validate_registry(write_registry(tmp_path, registry))

    assert any("duplicate topic name" in error for error in errors)
