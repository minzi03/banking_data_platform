#!/usr/bin/env python3
"""
Summarise a pytest JUnit XML into a pass/fail matrix.

Exists because deriving results from pytest's human-readable output keeps
going wrong. The TD-1 baseline harness first tried to read the collected count
out of the terminal banner with `grep -oE '^[0-9]+ tests collected'`; with
`-v` in addopts pytest prints `===== 34 tests collected =====` instead, the
anchor never matched, the harness read 0 and aborted before running a single
test — a whole 16-minute CI round for nothing.

JUnit XML is the machine-readable channel pytest already offers. Use it.

Usage:
    python scripts/summarize_junit.py /tmp/junit.xml
    python scripts/summarize_junit.py /tmp/junit.xml --title "Baseline"

Exit code is 0 when the file parses, regardless of test results: this
summarises, it does not gate. The caller keeps pytest's own exit code.
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def _force_utf8_output() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, OSError):
                pass


def load_cases(path: Path) -> list[tuple[str, str, str]]:
    """Trả về [(status, node_id, first_line_of_message)] theo thứ tự trong file."""
    root = ET.parse(path).getroot()
    # pytest ghi <testsuites><testsuite>… ở bản mới, <testsuite> trần ở bản cũ.
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))

    cases: list[tuple[str, str, str]] = []
    for suite in suites:
        for case in suite.iter("testcase"):
            node_id = f"{case.get('classname', '')}::{case.get('name', '')}"
            problem = case.find("failure")
            if problem is None:
                problem = case.find("error")
            if problem is None:
                status = "SKIP" if case.find("skipped") is not None else "PASS"
                cases.append((status, node_id, ""))
                continue
            raw = (problem.get("message") or problem.text or "").strip()
            first_line = raw.splitlines()[0] if raw else ""
            cases.append(("FAIL", node_id, first_line[:200]))
    return cases


def render(cases: list[tuple[str, str, str]], title: str) -> str:
    counts = {status: sum(1 for c in cases if c[0] == status) for status in ("PASS", "FAIL", "SKIP")}
    lines = [
        f"### {title}",
        "",
        f"collected {len(cases)} · passed {counts['PASS']} · "
        f"failed {counts['FAIL']} · skipped {counts['SKIP']}",
        "",
    ]
    # Fail trước: đó là thứ người đọc cần, và nó phải ở đầu chứ không nằm lẫn.
    for status in ("FAIL", "SKIP", "PASS"):
        for case_status, node_id, message in cases:
            if case_status != status:
                continue
            lines.append(f"- `{case_status}` {node_id}" + (f" — {message}" if message else ""))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    _force_utf8_output()
    parser = argparse.ArgumentParser(description="Summarise pytest JUnit XML")
    parser.add_argument("junit_xml", type=Path)
    parser.add_argument("--title", default="Test results")
    args = parser.parse_args(argv)

    if not args.junit_xml.exists():
        print(f"không tìm thấy junit xml: {args.junit_xml}", file=sys.stderr)
        return 1

    print(render(load_cases(args.junit_xml), args.title))
    return 0


if __name__ == "__main__":
    sys.exit(main())
