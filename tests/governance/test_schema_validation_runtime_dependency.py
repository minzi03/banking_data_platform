"""Governance checks for event validation runtime dependencies."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_spark_worker_declares_jsonschema_runtime_dependency():
    requirements = (ROOT / "docker" / "requirements-spark.txt").read_text(encoding="utf-8").lower()

    assert "jsonschema" in requirements
