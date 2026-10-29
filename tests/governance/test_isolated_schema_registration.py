"""Tests for the isolated Schema Registry registration workflow."""

import subprocess
import sys
from pathlib import Path

import scripts.register_isolated_event_schema as workflow

ROOT = Path(__file__).resolve().parents[2]


def test_workflow_defaults_to_dry_run(monkeypatch):
    calls = []

    class Result:
        returncode = 0

    def fake_run(command, **kwargs):
        calls.append(command)
        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        ["register_isolated_event_schema", "--registry-url", "http://registry:8081"],
    )

    assert workflow.main() == 0
    assert len(calls) == 2
    assert "--apply" not in calls[1]
    assert str(ROOT / "scripts" / "wait_for_schema_registry.py") in calls[0]


def test_workflow_requires_explicit_apply(monkeypatch):
    calls = []

    class Result:
        returncode = 0

    def fake_run(command, **kwargs):
        calls.append(command)
        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        ["register_isolated_event_schema", "--registry-url", "http://registry:8081", "--apply"],
    )

    assert workflow.main() == 0
    assert "--apply" in calls[1]
