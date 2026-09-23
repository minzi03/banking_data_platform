"""
Contract Test — manifest phải đo cob_dt của MỌI bảng Bronze (TD-14)
===================================================================

`snapshot.bronze_max_cob_dt` và `snapshot.bronze_partition_exists` chỉ đo
`bronze.core_txn_account`. Dimension Bronze nạp full-snapshot — mỗi lần nạp
thay cả bảng — nên ngày 2026-09-23 một lượt nạp lại cho `2026-09-21` để cả 13
dimension lệch ngày trong khi `snapshot_layers_aligned` vẫn xanh. Chỉ một
metric không thuộc invariant nào (`core_customer.rows` 10000 → 0) làm lộ ra.

Bản sửa là query `bronze.tables_at_cob_dt` cộng invariant
`bronze_every_table_at_cob_dt`, so nó với `bronze.batch_tables` (đếm từ config).
Test này giữ hai mắt xích tĩnh của cơ chế đó:

1. Danh sách bảng trong query bằng đúng tập bảng đích của `code_etl/bronze/*/*.yml`.
   Thêm workload mà quên query thì đỏ ngay lúc commit, không đợi tới lúc sinh
   manifest.
2. Invariant tồn tại, là `error`, và so đúng hai metric đó.

Chạy: pytest tests/governance/test_bronze_alignment_query.py -v
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SQL = REPO_ROOT / "docs" / "evidence" / "metrics-manifest.sql"
MANIFEST = REPO_ROOT / "docs" / "evidence" / "metrics-manifest.yaml"
QUERY_ID = "bronze.tables_at_cob_dt"


def _query_block() -> str:
    text = SQL.read_text(encoding="utf-8")
    marker = f"--@id {QUERY_ID}\n"
    assert marker in text, f"thiếu query {QUERY_ID} trong {SQL.name}"
    return text.split(marker, 1)[1].split("--@id ", 1)[0]


def _config_targets() -> set[str]:
    targets = set()
    for path in (REPO_ROOT / "code_etl" / "bronze").glob("*/*.yml"):
        config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if isinstance(config, dict) and "target" in config:
            targets.add(config["target"]["table"])
    return targets


def test_inputs_are_found():
    """Guard chống pass rỗng: glob hoặc regex hỏng không được làm test xanh."""
    assert len(_config_targets()) >= 15
    assert len(re.findall(r":catalog\.bronze\.(\w+)", _query_block())) >= 15


def test_query_covers_exactly_the_bronze_workloads():
    queried = re.findall(r":catalog\.bronze\.(\w+)", _query_block())
    assert len(queried) == len(set(queried)), f"bảng lặp trong query: {sorted(queried)}"
    targets = _config_targets()
    assert set(queried) == targets, (
        f"query {QUERY_ID} lệch với config Bronze.\n"
        f"  thiếu trong query: {sorted(targets - set(queried))}\n"
        f"  thừa trong query:  {sorted(set(queried) - targets)}"
    )


def test_every_table_is_checked_at_the_requested_cob_dt():
    """Mỗi nhánh phải lọc theo cob_dt được yêu cầu, không phải MAX hay cả bảng."""
    branches = [b for b in _query_block().split("UNION ALL") if ":catalog.bronze." in b]
    unfiltered = [b.strip()[:80] for b in branches if "WHERE cob_dt = DATE ':cob_dt'" not in b]
    assert not unfiltered, f"nhánh không lọc theo :cob_dt: {unfiltered}"


def test_invariant_blocks_promotion_on_any_drift():
    invariants = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))["invariants"]
    inv = invariants.get("bronze_every_table_at_cob_dt")
    assert inv is not None, "thiếu invariant bronze_every_table_at_cob_dt"
    assert inv["metric"] == f"metrics.{QUERY_ID}.value"
    assert inv["compare_to"] == "metrics.bronze.batch_tables.value"
    assert inv["operator"] == "eq"
    assert inv["severity"] == "error", "warn vẫn promote — đúng thứ TD-14 phải chặn"


def test_no_sql_block_is_cut_short_by_a_semicolon():
    """
    Generator lấy câu lệnh là mọi thứ TRƯỚC dấu `;` đầu tiên của khối
    (`body.split(";")[0]` trong render_query_bundle). Bản đầu của query TD-14 có
    một `;` trong comment — câu lệnh bị cắt giữa chừng và Trino từ chối ở lượt
    chạy thật. `--render-sql` không bắt được vì nó chỉ in, không chạy.

    Nên sau dấu `;` đầu tiên chỉ được còn comment hoặc dòng trống. Áp cho mọi
    khối, tách đúng cách generator tách.
    """
    text = SQL.read_text(encoding="utf-8")
    blocks = re.split(r"^--@id\s+(\S+)\s*$", text, flags=re.MULTILINE)[1:]
    pairs = list(zip(blocks[::2], blocks[1::2], strict=True))
    assert len(pairs) >= 20, f"chỉ tách được {len(pairs)} khối — nghi ngờ định dạng file đổi"
    bad = []
    for qid, body in pairs:
        if ";" not in body:
            bad.append(f"{qid}: không có ';'")
            continue
        leftover = [
            ln.strip() for ln in body.split(";", 1)[1].splitlines() if ln.strip() and not ln.strip().startswith("--")
        ]
        if leftover:
            bad.append(f"{qid}: còn SQL sau ';' đầu tiên → {leftover[0][:60]!r}")
    assert not bad, "câu lệnh bị cắt sớm hoặc thiếu ';':\n  " + "\n  ".join(bad)
