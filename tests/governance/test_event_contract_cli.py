"""Tests for the public event contract validation CLI wrapper."""

import json
import subprocess
import sys
from pathlib import Path

from tests.governance.test_event_schema_contract import valid_event

ROOT = Path(__file__).resolve().parents[2]
TOPIC = "postgresql.banking.digital_banking.online_transaction"


def test_cli_validates_event_from_stdin():
    result = subprocess.run(
        [sys.executable, "scripts/validate_event_contract.py", "--topic", TOPIC],
        input=json.dumps(valid_event()),
        text=True,
        capture_output=True,
        cwd=ROOT,
        check=False,
    )

    assert result.returncode == 0
    assert "Event contract valid" in result.stdout


def test_cli_rejects_unknown_topic():
    result = subprocess.run(
        [sys.executable, "scripts/validate_event_contract.py", "--topic", "unknown.topic"],
        input=json.dumps(valid_event()),
        text=True,
        capture_output=True,
        cwd=ROOT,
        check=False,
    )

    assert result.returncode == 1
    assert "Unknown topic" in result.stdout
