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


def test_no_dangling_upstream_references():
    """
    KHÔNG contract nào được trỏ upstream tới một dataset_id không tồn tại.

    Đây là assert CỨNG. Bản đầu của test này chỉ kiểm "cơ chế phát hiện có
    chạy không", vì lúc viết repo đang có 4 tham chiếu treo: bốn Gold contract
    trỏ tới `banking.dim_customer_silver`, trong khi contract của
    `silver.dim_customer` lại mang `dataset_id` là `banking.core_customer_silver`
    — đặt theo tổ tiên Bronze thay vì theo bảng nó mô tả.

    Bốn tham chiếu kia đúng: 12/13 contract silver theo quy ước
    `banking.<physical_table>_silver`. `dim_customer` là ngoại lệ duy nhất, nên
    contract đã được đổi tên cho khớp. Giờ siết lại thành assert cứng để lineage
    không thể đứt lần nữa mà không ai biết.

    Vì sao đáng siết: khi tham chiếu treo, dataset nguồn trông như không có ai
    dùng còn dataset đích trông như không có nguồn — cả hai đều SAI, và cả hai
    đều trông bình thường nếu chỉ đọc từng file contract riêng lẻ.
    """
    from generate_governance_docs import analyse_lineage, load_contracts

    graph = analyse_lineage(load_contracts())
    dangling = graph["dangling"]

    assert dangling == {}, (
        "Có contract khai báo upstream trỏ tới dataset_id không tồn tại:\n"
        + "\n".join(f"  - {cid} → {missing}" for cid, missing in sorted(dangling.items()))
        + "\n\nLineage đứt tại đây. Sửa `upstream_dataset_ids`, hoặc đổi "
        "`dataset_id` của contract đích cho khớp quy ước "
        "`banking.<physical_table>_<layer>`."
    )


def test_lineage_doc_has_no_stale_dangling_section():
    """LINEAGE.md không được còn mục cảnh báo khi đã hết tham chiếu treo."""
    content = LINEAGE_DOC.read_text(encoding="utf-8")
    assert "Tham chiếu treo" not in content, (
        "Không còn tham chiếu treo nhưng LINEAGE.md vẫn còn mục cảnh báo — "
        "chạy: py -3 scripts/generate_governance_docs.py"
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
