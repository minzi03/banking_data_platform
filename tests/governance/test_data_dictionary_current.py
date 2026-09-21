"""
Contract Test — Data Dictionary phải khớp với DDL và contract
==============================================================

`docs/DATA_DICTIONARY.md` được sinh bởi `scripts/generate_data_dictionary.py`
từ hai nguồn: DDL trong `docker/init_*/` và data contract trong
`governance/datasets/`.

Tài liệu tra cứu viết tay hỏng theo một kiểu đặc trưng: **im lặng**. Thêm một
cột vào DDL, không ai nhớ sửa tài liệu, và từ đó người đọc tra cứu một schema
không còn tồn tại. Không có gì đỏ, vì tài liệu không chạy.

Test này làm tài liệu chạy: nếu bản đã commit lệch khỏi bản sinh lại từ nguồn,
CI đỏ.

Chạy: pytest tests/governance/test_data_dictionary_current.py -v
Sinh lại: py -3 scripts/generate_data_dictionary.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATOR = REPO_ROOT / "scripts" / "generate_data_dictionary.py"
DICTIONARY = REPO_ROOT / "docs" / "DATA_DICTIONARY.md"


def _run_generator(*args: str) -> subprocess.CompletedProcess[str]:
    # check=False là CHỦ Ý: chính returncode là thứ các test dưới assert.
    # Để subprocess tự raise sẽ biến một kết quả cần kiểm tra thành exception,
    # và thông điệp lỗi sẽ mất phần stdout/stderr giải thích cái gì lệch.
    return subprocess.run(
        [sys.executable, str(GENERATOR), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=REPO_ROOT,
        check=False,
    )


def test_generator_exists():
    assert GENERATOR.exists(), f"Thiếu generator: {GENERATOR.relative_to(REPO_ROOT)}"


def test_dictionary_is_committed():
    assert DICTIONARY.exists(), (
        f"Thiếu {DICTIONARY.relative_to(REPO_ROOT)}. Chạy: py -3 scripts/generate_data_dictionary.py"
    )


def test_dictionary_matches_sources():
    """Bản đã commit phải khớp bản sinh lại từ DDL + contract."""
    result = _run_generator("--check")
    assert result.returncode == 0, (
        "docs/DATA_DICTIONARY.md đã lệch khỏi DDL hoặc data contract.\n"
        "Sinh lại rồi commit: py -3 scripts/generate_data_dictionary.py\n\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_generator_output_is_deterministic():
    """
    Sinh hai lần phải cho cùng kết quả.

    Nếu không, --check sẽ đỏ ngẫu nhiên và người ta sẽ học cách bỏ qua nó —
    một gate nhiễu còn tệ hơn không có gate.
    """
    first = _run_generator("--stdout")
    second = _run_generator("--stdout")
    assert first.returncode == 0, f"Lần chạy 1 thất bại:\n{first.stderr}"
    assert second.returncode == 0, f"Lần chạy 2 thất bại:\n{second.stderr}"
    assert first.stdout == second.stdout, (
        "Generator không tất định — hai lần chạy cho kết quả khác nhau. Nghi ngờ thiếu sort ở đâu đó."
    )


def test_dictionary_covers_all_three_lakehouse_layers():
    """
    Guard chống pass rỗng: nếu regex parse DDL hỏng, generator vẫn chạy được
    và sinh ra một file rỗng hợp lệ — --check sẽ xanh một cách vô nghĩa.
    """
    content = DICTIONARY.read_text(encoding="utf-8")
    for layer in ("bronze", "silver", "gold"):
        assert f"## {layer}" in content, (
            f"Data dictionary thiếu hẳn tầng '{layer}'. "
            "Nghi ngờ regex parse DDL hỏng — test này tồn tại để chặn "
            "trường hợp generator chạy thành công nhưng không parse được gì."
        )


@pytest.mark.parametrize("table", ["mart_customer_360", "dim_customer", "fact_txn_account"])
def test_dictionary_contains_core_tables(table: str):
    """Vài bảng lõi phải luôn có mặt — chặn trường hợp parse sót diện rộng."""
    content = DICTIONARY.read_text(encoding="utf-8")
    assert table in content, f"Data dictionary không có bảng lõi '{table}'"
