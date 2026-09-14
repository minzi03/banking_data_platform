"""
TD-5 Contract Test — Shell Failure Propagation
================================================

Nghiêm cấm `|| true` trên các bước là gate (lint, format, security scan, test).
Ngoại trừ: cleanup, log retrieval, idempotent setup — hợp lý dùng `|| true`.

Phát hiện qua pattern: bước trong workflow YAML có tên gợi ý gate
(check|lint|format|test|scan|validate|build) + `|| true` trên dòng lệnh chạy.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

# Gate steps: những step có name chứa từ khóa này mà dùng `|| true` → fail
GATE_KEYWORDS = re.compile(
    r"\b(lint|format|ruff|bandit|test|scan|validate|check|build|security)\b",
    re.IGNORECASE,
)


def _workflow_files() -> list[Path]:
    if not WORKFLOW_DIR.exists():
        return []
    return sorted(WORKFLOW_DIR.glob("*.yml"))


def _find_masked_gate_steps(path: Path) -> list[str]:
    """Tìm step name là gate mà run có `|| true`."""
    text = path.read_text(encoding="utf-8", errors="replace")
    try:
        wf = yaml.safe_load(text)
    except Exception:
        return []

    violations = []
    jobs = wf.get("jobs", {}) or {}
    for job_name, job in jobs.items():
        if not isinstance(job, dict):
            continue
        for step in job.get("steps", []):
            if not isinstance(step, dict):
                continue
            name = step.get("name", "")
            if not name:
                continue
            # Chỉ check steps có tên là gate keyword
            if not GATE_KEYWORDS.search(name):
                continue
            run_cmd = step.get("run", "")
            if not run_cmd:
                continue
            # Loại trừ cleanup/teardown contexts
            if any(kw in name.lower() for kw in ["cleanup", "teardown", "log"]):
                continue
            # Tìm `|| true` trên dòng không phải comment
            for line in run_cmd.splitlines():
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                if "|| true" in stripped and "--output-format" not in stripped:
                    violations.append(
                        f"  job={job_name} step=\"{name}\"\n    {stripped}"
                    )
    return violations


@pytest.mark.parametrize("wf_path", _workflow_files(), ids=lambda p: p.name)
def test_no_masked_gate_steps(wf_path):
    """Gate steps (lint, format, security) không được dùng `|| true`."""
    violations = _find_masked_gate_steps(wf_path)
    assert not violations, (
        f"{wf_path.name}: gate step dùng `|| true` — nuốt lỗi:\n"
        + "\n".join(violations)
    )
