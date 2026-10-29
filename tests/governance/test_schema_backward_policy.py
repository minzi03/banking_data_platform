"""Tests for the repository-level backward compatibility policy."""

import json
from pathlib import Path

import pytest

from schemas.check_backward_compatibility import CompatibilityError, validate_backward

ROOT = Path(__file__).resolve().parents[2]
V1 = ROOT / "schemas" / "online_transaction_event_v1.json"


def write_candidate(tmp_path, mutate):
    candidate = json.loads(V1.read_text(encoding="utf-8"))
    mutate(candidate)
    path = tmp_path / "candidate.json"
    path.write_text(json.dumps(candidate), encoding="utf-8")
    return path


def test_optional_field_addition_is_backward_compatible(tmp_path):
    path = write_candidate(
        tmp_path,
        lambda schema: schema["properties"]["payload"]["properties"].update(
            {"risk_band": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH"]}}
        ),
    )

    validate_backward(V1, path)


def test_new_required_field_is_rejected(tmp_path):
    def add_required(schema):
        schema["properties"]["payload"]["properties"]["risk_band"] = {"type": "string"}
        schema["properties"]["payload"]["required"].append("risk_band")

    with pytest.raises(CompatibilityError, match="required payload field added"):
        validate_backward(V1, write_candidate(tmp_path, add_required))


def test_removed_required_field_is_rejected(tmp_path):
    def remove_required(schema):
        schema["properties"]["payload"]["required"].remove("currency")

    with pytest.raises(CompatibilityError, match="required payload field removed"):
        validate_backward(V1, write_candidate(tmp_path, remove_required))
