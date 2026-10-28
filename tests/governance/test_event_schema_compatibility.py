"""Compatibility checks for the next online transaction event schema version."""

import json
from pathlib import Path

import jsonschema

from tests.governance.test_event_schema_contract import valid_event

ROOT = Path(__file__).resolve().parents[2]
V1 = ROOT / "schemas" / "online_transaction_event_v1.json"
V2 = ROOT / "schemas" / "online_transaction_event_v2.json"


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_v2_is_a_valid_schema():
    jsonschema.Draft202012Validator.check_schema(load(V2))


def test_v2_preserves_existing_v1_payload_fields():
    event = valid_event()
    event["event_version"] = 2

    jsonschema.validate(event, load(V2))


def test_v2_event_can_be_downgraded_to_v1_when_no_new_field_is_used():
    event = valid_event()
    event["event_version"] = 2
    jsonschema.validate(event, load(V2))
    event["event_version"] = 1

    jsonschema.validate(event, load(V1))


def test_v2_new_field_is_not_v1_compatible():
    event = valid_event()
    event["event_version"] = 2
    event["payload"]["risk_band"] = "MEDIUM"

    with __import__("pytest").raises(jsonschema.ValidationError):
        jsonschema.validate(event, load(V1))


def test_v2_optional_risk_band_is_valid():
    event = valid_event()
    event["event_version"] = 2
    event["payload"]["risk_band"] = "MEDIUM"

    jsonschema.validate(event, load(V2))


def test_v2_does_not_make_existing_required_fields_optional():
    event = valid_event()
    event["event_version"] = 2
    event["payload"].pop("transaction_id")

    with __import__("pytest").raises(jsonschema.ValidationError):
        jsonschema.validate(event, load(V2))


def test_v1_remains_strict_about_unknown_fields():
    event = valid_event()
    event["payload"]["risk_band"] = "MEDIUM"

    with __import__("pytest").raises(jsonschema.ValidationError):
        jsonschema.validate(event, load(V1))
