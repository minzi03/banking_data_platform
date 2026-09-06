"""
Contract cho verifier README ↔ manifest.

Bảng metric không còn là nơi duy nhất publish một con số: executive summary
cũng nêu chúng. Một con số nằm trong văn xuôi mà không được đăng ký sẽ trôi
khỏi bảng ngay lần manifest đổi tiếp theo, và CI không thấy — đúng cách các
node mermaid từng giữ kiến trúc cũ trong khi prose quanh nó đã được sửa.

Luật: mọi numeric claim publish NGOÀI bảng evidence phải có projection riêng
được drift-check.

Chạy tĩnh, không cần stack.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_verifier():
    path = REPO_ROOT / "scripts" / "verify_readme_metrics.py"
    spec = importlib.util.spec_from_file_location("verify_readme_metrics", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


verifier = _load_verifier()


@pytest.fixture(scope="module")
def manifest() -> dict:
    return yaml.safe_load(
        (REPO_ROOT / "docs" / "evidence" / "metrics-manifest.yaml").read_text(
            encoding="utf-8"
        )
    )


@pytest.fixture(scope="module")
def readme() -> str:
    return (REPO_ROOT / "README.md").read_text(encoding="utf-8")


class TestProjectionParsing:
    def test_legacy_single_claim_still_reads(self):
        """Dạng cũ vẫn hợp lệ — contract không phải viết lại một lượt."""
        got = verifier.iter_projections({"readme_claim": "X", "manifest_path": "a.b"})
        assert got == [("metrics_table", "X")]

    def test_multi_projection_reads_location_and_claim(self):
        got = verifier.iter_projections({
            "manifest_path": "a.b",
            "readme_claims": [
                {"location": "metrics_table", "claim": "| X | 10 |"},
                {"location": "executive_summary", "claim": "10 X models"},
            ],
        })
        assert got == [
            ("metrics_table", "| X | 10 |"),
            ("executive_summary", "10 X models"),
        ]


class TestDriftIsCaughtInEveryProjection:
    """
    Điểm mấu chốt: đổi manifest phải làm ĐỎ mọi nơi con số đó được publish,
    không chỉ ô trong bảng.
    """

    def test_shipped_readme_matches_shipped_manifest(self, manifest, readme):
        errors, checked = verifier.verify(manifest, readme)
        assert not errors, errors
        assert checked, "không projection nào được kiểm — verifier hỏng?"

    def test_executive_summary_is_actually_registered(self, manifest):
        locations = {
            location
            for binding in manifest["readme_bindings"]
            for location, _claim in verifier.iter_projections(binding)
        }
        assert "executive_summary" in locations, (
            "executive summary đang nêu số mà không có projection nào — "
            "văn xuôi sẽ trôi khỏi bảng mà CI không thấy"
        )

    def test_changing_a_metric_reddens_both_table_and_summary(self, manifest, readme):
        drifted = yaml.safe_load(yaml.safe_dump(manifest))
        drifted["metrics"]["gold"]["tables"]["value"] = 11

        errors, _checked = verifier.verify(drifted, readme)
        locations = [e for e in errors if "README drift" in e]
        assert any("metrics_table" in e for e in locations), locations
        assert any("executive_summary" in e for e in locations), (
            f"executive summary không bị bắt khi manifest đổi: {locations}"
        )


class TestNumberFormatting:
    @pytest.mark.parametrize(
        "form", ["2300000", "2,300,000", "2.3M", "2.3 million"]
    )
    def test_millions_may_be_written_readably(self, form):
        """
        README được phép trình bày thân thiện, miễn là chiếu đúng giá trị đã
        verify. "2.3M" hợp cho ô bảng; câu văn thì đọc là "2.3 million".
        """
        assert form in verifier.normalise_number(2_300_000)

    def test_a_different_number_is_not_accepted(self):
        assert "2.4 million" not in verifier.normalise_number(2_300_000)
