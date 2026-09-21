"""
Contract Test — Link nội bộ trong tài liệu phải trỏ tới file có thật
=====================================================================

Link gãy là lỗi tài liệu duy nhất mà máy kiểm được một cách chắc chắn. Mọi
lỗi tài liệu khác cần người đọc; cái này thì không.

Test này được viết TRƯỚC khi tái cấu trúc `docs/` (Giai đoạn 4 trong
`docs/DOCUMENTATION_PLAN.md`), và phải xanh trên cấu trúc cũ. Lý do: nếu nó
đỏ sau khi move file, nguyên nhân chắc chắn là move — không phải nội dung mới
thêm vào cùng lúc. Một lưới an toàn dựng sau khi ngã thì không chứng minh
được gì.

Phạm vi: chỉ file markdown **được git track**. File trong `.gitignore`
(`docs/interview/`) và package vendor (`dbt/dbt_packages/`) nằm ngoài — chúng
không phải tài liệu của dự án.

Chạy: pytest tests/governance/test_docs_links_resolve.py -v
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# [text](target) — bỏ phần #anchor, bỏ link có khoảng trắng (không phải đường dẫn)
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s#]+)(?:#[^)]*)?\)")

EXTERNAL_PREFIXES = ("http://", "https://", "mailto:", "tel:")


def _tracked_markdown() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "*.md"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=True,
    )
    return [REPO_ROOT / line for line in result.stdout.split() if line]


def _internal_links(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    return [target for target in LINK_RE.findall(text) if not target.startswith(EXTERNAL_PREFIXES)]


MARKDOWN_FILES = _tracked_markdown()


def test_markdown_files_are_found():
    """
    Guard chống pass rỗng: nếu `git ls-files` hỏng, mọi test dưới sẽ xanh
    vì không có gì để kiểm.
    """
    assert len(MARKDOWN_FILES) >= 30, (
        f"Chỉ tìm thấy {len(MARKDOWN_FILES)} file markdown được track. "
        "Nghi ngờ git ls-files hỏng — test link sẽ xanh một cách vô nghĩa."
    )


def test_internal_links_are_found():
    """Guard thứ hai: phải thực sự có link để kiểm, không chỉ có file."""
    total = sum(len(_internal_links(p)) for p in MARKDOWN_FILES if p.exists())
    assert total >= 100, f"Chỉ trích được {total} link nội bộ. Nghi ngờ regex hỏng — test sẽ xanh mà không kiểm gì."


@pytest.mark.parametrize(
    "md_path",
    MARKDOWN_FILES,
    ids=lambda p: p.relative_to(REPO_ROOT).as_posix() if isinstance(p, Path) else "",
)
def test_links_resolve(md_path: Path):
    """Mọi link nội bộ phải trỏ tới một file hoặc thư mục có thật."""
    if not md_path.exists():
        pytest.skip(f"{md_path.name} được track nhưng không có trên đĩa")

    base = md_path.parent
    broken: list[str] = []

    for target in _internal_links(md_path):
        resolved = (base / target).resolve()
        if not resolved.exists():
            broken.append(target)

    rel = md_path.relative_to(REPO_ROOT).as_posix()
    assert not broken, (
        f"{rel} có link trỏ tới file không tồn tại:\n"
        + "\n".join(f"  - {b}" for b in broken)
        + "\n\nSửa đường dẫn, hoặc tạo file đích. "
        "Nếu vừa di chuyển file, cập nhật mọi link trỏ tới nó."
    )
