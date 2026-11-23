"""Static contracts for reproducible local and CI lint tooling."""

from pathlib import Path

import tomllib
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPECTED_RUFF = "0.17.1"


def test_pyproject_pins_ci_ruff_version():
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["project"]["optional-dependencies"]["dev"]
    assert f"ruff=={EXPECTED_RUFF}" in project["project"]["optional-dependencies"]["dev"]


def test_pre_commit_uses_same_ruff_version():
    config = yaml.safe_load((REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    ruff_repos = [repo for repo in config["repos"] if repo["repo"] == "https://github.com/astral-sh/ruff-pre-commit"]
    assert len(ruff_repos) == 1
    assert ruff_repos[0]["rev"] == f"v{EXPECTED_RUFF}"


def test_ci_uses_same_ruff_version():
    ci = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert f"pip install ruff=={EXPECTED_RUFF}" in ci
