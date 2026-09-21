"""
Contract Test — DATA_CONTRACTS.md và LINEAGE.md phải khớp với contract
======================================================================

Hai tài liệu này là phép chiếu của `governance/datasets/*.yaml`, sinh bởi
`scripts/generate_governance_docs.py`. Cùng lý do với data dictionary: tài
liệu tra cứu viết tay lệch khỏi nguồn một cách im lặng.

Chạy: pytest tests/governance/test_governance_docs_current.py -v
Sinh lại: py -3 scripts/generate_governance_docs.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATOR = REPO_ROOT / "scripts" / "generate_governance_docs.py"
CONTRACTS_DOC = REPO_ROOT / "docs" / "03-data" / "DATA_CONTRACTS.md"
LINEAGE_DOC = REPO_ROOT / "docs" / "03-data" / "LINEAGE.md"

sys.path.insert(0, str(REPO_ROOT / "scripts"))


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    # check=False là chủ ý: returncode chính là thứ được assert.
    return subprocess.run(
        [sys.executable, str(GENERATOR), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=REPO_ROOT,
        check=False,
    )


@pytest.mark.parametrize("doc", [CONTRACTS_DOC, LINEAGE_DOC])
def test_doc_is_committed(doc: Path):
    assert doc.exists(), f"Thiếu {doc.relative_to(REPO_ROOT)}. Chạy: py -3 scripts/generate_governance_docs.py"


def test_docs_match_contracts():
    """Bản đã commit phải khớp bản sinh lại từ governance/datasets/."""
    result = _run("--check")
    assert result.returncode == 0, (
        "DATA_CONTRACTS.md hoặc LINEAGE.md đã lệch khỏi governance/datasets/.\n"
        "Sinh lại rồi commit: py -3 scripts/generate_governance_docs.py\n\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_generator_is_deterministic():
    """Hai lần chạy phải cho cùng kết quả — gate đỏ ngẫu nhiên sẽ bị bỏ qua."""
    first = _run()
    before = (
        CONTRACTS_DOC.read_text(encoding="utf-8"),
        LINEAGE_DOC.read_text(encoding="utf-8"),
    )
    second = _run()
    after = (
        CONTRACTS_DOC.read_text(encoding="utf-8"),
        LINEAGE_DOC.read_text(encoding="utf-8"),
    )
    assert first.returncode == 0 and second.returncode == 0
    assert before == after, "Generator không tất định — nghi ngờ thiếu sort ở đâu đó."


def test_dangling_reference_detection_works():
    """
    Cơ chế phát hiện tham chiếu treo phải THỰC SỰ hoạt động.

    Không assert "hiện có 0 tham chiếu treo" — hiện tại repo ĐANG CÓ 4 cái
    (bốn Gold contract trỏ tới `banking.dim_customer_silver`, trong khi
    contract của `silver.dim_customer` lại mang dataset_id
    `banking.core_customer_silver`). Sửa việc đó phải đụng 8 chỗ trong code
    và test, nên nằm ngoài phạm vi PR tài liệu.

    Cái test này bảo đảm: nếu có tham chiếu treo, nó PHẢI xuất hiện trong
    LINEAGE.md. Một cơ chế phát hiện âm thầm hỏng còn tệ hơn không có.

    Khi 4 tham chiếu kia được sửa, đổi test này thành assert cứng
    `analyse_lineage(...)["dangling"] == {}` để chặn tái diễn.
    """
    from generate_governance_docs import analyse_lineage, load_contracts

    graph = analyse_lineage(load_contracts())
    dangling = graph["dangling"]

    content = LINEAGE_DOC.read_text(encoding="utf-8")

    if dangling:
        assert "Tham chiếu treo" in content, (
            f"Có {len(dangling)} contract với upstream không tồn tại, "
            "nhưng LINEAGE.md không hề nêu — cơ chế phát hiện đã hỏng."
        )
        for cid in dangling:
            assert cid in content, f"Tham chiếu treo của `{cid}` không xuất hiện trong LINEAGE.md"
    else:
        assert "Tham chiếu treo" not in content, (
            "Không còn tham chiếu treo nhưng LINEAGE.md vẫn còn mục cảnh báo — sinh lại tài liệu."
        )


def test_lineage_graph_is_not_empty():
    """
    Guard chống pass rỗng: nếu load_contracts() hỏng, generator vẫn ghi ra
    file hợp lệ nhưng rỗng, và --check sẽ xanh vô nghĩa.
    """
    from generate_governance_docs import analyse_lineage, load_contracts

    contracts = load_contracts()
    assert len(contracts) >= 30, f"Chỉ đọc được {len(contracts)} contract — nghi ngờ glob hỏng"

    graph = analyse_lineage(contracts)
    edges = sum(len(v) for v in graph["downstream"].values())
    assert edges >= 20, f"Đồ thị chỉ có {edges} cạnh — nghi ngờ upstream_dataset_ids không được đọc"
