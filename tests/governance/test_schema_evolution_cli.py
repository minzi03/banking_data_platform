"""Tests for the candidate schema evolution CLI."""

import json
from pathlib import Path

from scripts.validate_schema_evolution import main

ROOT = Path(__file__).resolve().parents[2]
V1 = ROOT / "schemas" / "online_transaction_event_v1.json"
TOPIC = "postgresql.banking.digital_banking.online_transaction"


def write_candidate(tmp_path, required=False):
    schema = json.loads(V1.read_text(encoding="utf-8"))
    schema["properties"]["payload"]["properties"]["risk_band"] = {"type": "string"}
    if required:
        schema["properties"]["payload"]["required"].append("risk_band")
    path = tmp_path / "candidate.json"
    path.write_text(json.dumps(schema), encoding="utf-8")
    return path


def test_cli_accepts_optional_field(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        ["validate_schema_evolution", "--topic", TOPIC, "--candidate", str(write_candidate(tmp_path))],
    )

    assert main() == 0
    assert "backward-compatible" in capsys.readouterr().out


def test_cli_rejects_new_required_field(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        [
            "validate_schema_evolution",
            "--topic",
            TOPIC,
            "--candidate",
            str(write_candidate(tmp_path, required=True)),
        ],
    )

    assert main() == 1
    assert "incompatible schema" in capsys.readouterr().out
