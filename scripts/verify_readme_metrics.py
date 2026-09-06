#!/usr/bin/env python3
"""
Verify README metrics against the verified evidence manifest.

README is a PROJECTION of the manifest, not a place where metrics are defined.
This script fails when the two disagree, so a number can never be edited by hand
in README and quietly diverge from what was actually measured.

    docs/evidence/metrics-manifest.yaml   ← source of truth (generated + verified)
                  ↓  readme_bindings
              README.md                   ← projection

Each binding names the exact README claim and the manifest path holding its
verified value. The claim text is matched literally in README, so renaming a
claim without updating the binding is also caught.

Usage:
    python scripts/verify_readme_metrics.py
    python scripts/verify_readme_metrics.py --manifest <path> --readme <path>
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "docs" / "evidence" / "metrics-manifest.yaml"
README_PATH = REPO_ROOT / "README.md"

MISSING = object()


def _force_utf8_output() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, OSError):
                pass


def get_path(root: Any, dotted: str) -> Any:
    node = root
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return MISSING
        node = node[part]
    return node


def normalise_number(value: Any) -> set[str]:
    """
    Cách một con số có thể xuất hiện hợp lệ trong README.

    2300000 → {"2300000", "2,300,000", "2.3M"} — README được phép trình bày
    thân thiện, miễn là nó chiếu đúng giá trị đã verify.
    """
    if not isinstance(value, int):
        return {str(value)}
    forms = {str(value), f"{value:,}"}
    if value >= 1_000_000 and value % 100_000 == 0:
        forms.add(f"{value / 1_000_000:.1f}M".replace(".0M", "M"))
    return forms


def verify(manifest: dict, readme: str) -> tuple[list[str], list[str]]:
    """Trả về (errors, checked) — errors rỗng nghĩa là README khớp manifest."""
    errors: list[str] = []
    checked: list[str] = []

    status = manifest["manifest"]["verification"]["status"]
    if status == "pending":
        errors.append(
            "manifest chưa được sinh (status=pending) — không thể verify README "
            "với evidence chưa đo. Chạy generate_metrics_manifest.py trước."
        )
        return errors, checked

    for binding in manifest.get("readme_bindings", []):
        claim = binding["readme_claim"]
        path = binding["manifest_path"]
        note = binding.get("status")

        if claim not in readme:
            errors.append(f"claim không có trong README: {claim!r} (binding {path})")
            continue

        value = get_path(manifest, path)
        if value is MISSING:
            errors.append(f"manifest_path không tồn tại: {path}")
            continue

        # Binding trỏ vào một nhánh (nhiều số) → chỉ kiểm claim có mặt.
        if isinstance(value, dict):
            checked.append(f"{claim}  →  {path} (nhóm metric, kiểm sự hiện diện)")
            continue

        if value is None:
            errors.append(f"{path} chưa có giá trị nhưng README đang claim: {claim!r}")
            continue

        forms = normalise_number(value)
        if not any(f in claim for f in forms):
            errors.append(
                f"README drift: claim {claim!r} không chứa giá trị đã verify "
                f"{value!r} (chấp nhận: {sorted(forms)}) từ {path}"
            )
            continue

        suffix = f"  [{note}]" if note else ""
        checked.append(f"{claim}  →  {path} = {value}{suffix}")

    return errors, checked


def main(argv: list[str] | None = None) -> int:
    _force_utf8_output()
    parser = argparse.ArgumentParser(description="Verify README ↔ metrics manifest")
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--readme", type=Path, default=README_PATH)
    args = parser.parse_args(argv)

    manifest = yaml.safe_load(args.manifest.read_text(encoding="utf-8"))
    readme = args.readme.read_text(encoding="utf-8")

    errors, checked = verify(manifest, readme)

    for line in checked:
        print(f"  ok    {line}")
    for line in errors:
        print(f"  DRIFT {line}", file=sys.stderr)

    print()
    if errors:
        print(
            f"README drift: {len(errors)} claim không khớp manifest. "
            "Sửa README (hoặc binding), KHÔNG sửa giá trị trong manifest bằng tay.",
            file=sys.stderr,
        )
        return 1
    print(f"README khớp manifest: {len(checked)}/{len(checked)} binding.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
