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
    assert len(calls) == 2
    assert "--apply" in calls[1]


def test_verify_after_apply_runs_read_only_check(monkeypatch):
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
        [
            "register_isolated_event_schema",
            "--registry-url",
            "http://registry:8081",
            "--apply",
            "--verify",
            "--version",
            "1",
        ],
    )

    assert workflow.main() == 0
    assert "--apply" in calls[1]
    assert str(ROOT / "scripts" / "verify_schema_registry.py") in calls[2]


def test_verify_local_runs_registry_and_drift_checks(monkeypatch):
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
        [
            "register_isolated_event_schema",
            "--registry-url",
            "http://registry:8081",
            "--topic",
            "topic.name",
            "--apply",
            "--verify-local",
            "--version",
            "3",
        ],
    )

    assert workflow.main() == 0
    assert len(calls) == 4
    assert str(ROOT / "scripts" / "verify_schema_registry.py") in calls[2]
    assert str(ROOT / "scripts" / "verify_registered_event_schema.py") in calls[3]
    assert "--version" in calls[2]
    assert calls[2][calls[2].index("--version") + 1] == "3"
    assert calls[3][calls[3].index("--topic") + 1] == "topic.name"


def test_verify_without_apply_is_rejected(monkeypatch):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        ["register_isolated_event_schema", "--registry-url", "http://registry:8081", "--verify"],
    )

    assert workflow.main() == 2
    assert calls == []


def test_verify_local_without_apply_is_rejected(monkeypatch):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "register_isolated_event_schema",
            "--registry-url",
            "http://registry:8081",
            "--verify-local",
        ],
    )

    assert workflow.main() == 2
    assert calls == []


def test_readiness_failure_stops_registration(monkeypatch):
    calls = []

    class Result:
        returncode = 7

    def fake_run(command, **kwargs):
        calls.append(command)
        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "register_isolated_event_schema",
            "--registry-url",
            "http://registry:8081",
            "--apply",
            "--verify-local",
        ],
    )

    assert workflow.main() == 7
    assert len(calls) == 1
    assert str(ROOT / "scripts" / "wait_for_schema_registry.py") in calls[0]


def test_registration_failure_stops_verification(monkeypatch):
    calls = []

    class Result:
        def __init__(self, returncode):
            self.returncode = returncode

    def fake_run(command, **kwargs):
        calls.append(command)
        return Result(0 if len(calls) == 1 else 9)

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "register_isolated_event_schema",
            "--registry-url",
            "http://registry:8081",
            "--apply",
            "--verify-local",
        ],
    )

    assert workflow.main() == 9
    assert len(calls) == 2


def test_registry_verification_failure_stops_local_drift_check(monkeypatch):
    calls = []

    class Result:
        def __init__(self, returncode):
            self.returncode = returncode

    def fake_run(command, **kwargs):
        calls.append(command)
        return Result(0 if len(calls) < 3 else 11)

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "register_isolated_event_schema",
            "--registry-url",
            "http://registry:8081",
            "--apply",
            "--verify-local",
        ],
    )

    assert workflow.main() == 11
    assert len(calls) == 3


def test_local_drift_failure_is_returned(monkeypatch):
    calls = []

    class Result:
        def __init__(self, returncode):
            self.returncode = returncode

    def fake_run(command, **kwargs):
        calls.append(command)
        return Result(0 if len(calls) < 4 else 13)

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "register_isolated_event_schema",
            "--registry-url",
            "http://registry:8081",
            "--apply",
            "--verify-local",
        ],
    )

    assert workflow.main() == 13
    assert len(calls) == 4
    assert str(ROOT / "scripts" / "verify_registered_event_schema.py") in calls[3]


def test_timeout_is_forwarded_to_readiness_check(monkeypatch):
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
        [
            "register_isolated_event_schema",
            "--registry-url",
            "http://registry:8081",
            "--timeout-seconds",
            "17",
        ],
    )

    assert workflow.main() == 0
    assert calls[0][calls[0].index("--timeout-seconds") + 1] == "17"
    assert len(calls) == 2
