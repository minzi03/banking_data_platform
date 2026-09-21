"""
Contract Test — Khai báo nguồn phải khớp với SQL
=================================================

Các job metadata-driven (bronze/silver/gold) khai báo nguồn ở ba chỗ:

    source.tables                  — dùng cho lineage và tài liệu
    upstream_flags                 — dùng cho SqlSensor chờ upstream
    validation.require_snapshots   — dùng cho assert_source_snapshots()

`require_snapshots` là ràng buộc CHẶN: `assert_source_snapshots()` trong
`code_etl/gold/base_job/gold_job.py` raise RuntimeError khi bảng được liệt kê
không có partition cho cob_dt đang xử lý.

Nếu một bảng được khai báo nhưng SQL không hề đọc nó, guard biến thành nguồn
LỖI GIẢ: job chết vì thiếu partition của một bảng mà output không phụ thuộc.

Đã xảy ra thật:

  - `gold/mart360/customer_360.yml` khai báo `silver.fact_online_transaction`
    ở cả ba chỗ trong khi SQL không chứa chuỗi "online". DDL Gold thì đã có sẵn
    6 cột `digital_*` chưa bao giờ được điền — tức là một feature dừng giữa
    chừng: DDL + khai báo đã vào, SQL thì chưa.

  - `silver/facts/fact_online_transaction.yml` khai báo `silver.dim_device` và
    `silver.dim_location` ở `source.tables`, kèm comment header ghi "Joins:
    dim_customer + dim_device + dim_location", nhưng SQL chỉ JOIN dim_customer.

Đây là mặt trái của một guard đúng: `assert_source_snapshots()` là tính năng
tốt, nhưng chỉ đúng khi khai báo phản ánh thực tế. Test này giữ hai thứ đó
đồng bộ.

Chạy: pytest tests/governance/test_declared_sources_match_sql.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
ETL_ROOT = REPO_ROOT / "code_etl"

# Khai báo được miễn trừ, kèm lý do. Trống là chủ ý: hiện không có ca hợp lệ
# nào mà một bảng được khai báo lại không xuất hiện trong SQL.
#
# Nếu phải thêm vào đây, ghi rõ lý do — một miễn trừ không có lý do sẽ tái tạo
# đúng lỗi mà test này tồn tại để chặn.
ALLOWED_UNUSED: dict[str, set[str]] = {}


def _metadata_driven_configs() -> list[tuple[Path, dict]]:
    """Mọi YAML job có khối `sql` — tức là job metadata-driven."""
    results: list[tuple[Path, dict]] = []
    for path in sorted(ETL_ROOT.rglob("*.yml")):
        try:
            config = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError:
            continue
        if isinstance(config, dict) and isinstance(config.get("sql"), str):
            results.append((path, config))
    return results


def _declared_tables(config: dict) -> dict[str, list[str]]:
    """Gom khai báo nguồn theo từng khoá, giữ nguyên tên khoá để báo lỗi rõ."""
    source = config.get("source") or {}
    validation = config.get("validation") or {}
    return {
        "source.tables": list(source.get("tables") or []),
        "upstream_flags": list(config.get("upstream_flags") or []),
        "validation.require_snapshots": list(validation.get("require_snapshots") or []),
    }


def _appears_in_sql(table_ref: str, sql: str) -> bool:
    """
    So khớp bằng tên bảng (phần cuối của `schema.table`).

    Cố ý KHÔNG so khớp chuỗi đầy đủ `silver.fact_txn_account`, vì SQL viết
    fully-qualified theo catalog (`lakehouse.silver.fact_txn_account`) trong
    khi khai báo thì không có catalog. So theo tên bảng bắt được cả hai dạng.
    """
    return table_ref.split(".")[-1] in sql


def test_metadata_driven_configs_exist():
    """Nếu glob hỏng, các test dưới sẽ pass rỗng — chặn trường hợp đó."""
    configs = _metadata_driven_configs()
    assert len(configs) >= 40, (
        f"Chỉ tìm thấy {len(configs)} job config có khối `sql` dưới {ETL_ROOT}. "
        "Nghi ngờ glob hỏng — test này sẽ pass một cách vô nghĩa nếu không thấy file nào."
    )


@pytest.mark.parametrize(
    "config_path,config",
    _metadata_driven_configs(),
    ids=lambda v: str(v.relative_to(REPO_ROOT)) if isinstance(v, Path) else "",
)
def test_declared_sources_appear_in_sql(config_path: Path, config: dict):
    """Mọi bảng được khai báo phải thực sự xuất hiện trong SQL của job."""
    sql = config["sql"]
    rel = config_path.relative_to(REPO_ROOT).as_posix()
    allowed = ALLOWED_UNUSED.get(rel, set())

    violations: list[str] = []
    for key, tables in _declared_tables(config).items():
        for table_ref in tables:
            if table_ref in allowed:
                continue
            if not _appears_in_sql(table_ref, sql):
                violations.append(f"{key}: {table_ref}")

    assert not violations, (
        f"{rel} khai báo bảng mà SQL không đọc:\n" + "\n".join(f"  - {v}" for v in violations) + "\n\n"
        "Khai báo thừa ở `validation.require_snapshots` khiến "
        "assert_source_snapshots() chặn job khi bảng đó thiếu partition, "
        "dù output không phụ thuộc vào nó — một lỗi giả.\n"
        "Sửa bằng một trong hai cách: gỡ khai báo, HOẶC dùng bảng đó trong SQL."
    )


def test_require_snapshots_subset_of_source_tables():
    """
    `require_snapshots` phải nằm trong `source.tables`.

    Chặn chiều ngược lại: một bảng bị chặn bởi guard nhưng không được khai báo
    là nguồn sẽ không xuất hiện trong lineage, khiến sự cố khó truy vết.
    """
    violations: list[str] = []
    for config_path, config in _metadata_driven_configs():
        declared = _declared_tables(config)
        source_tables = set(declared["source.tables"])
        for table_ref in declared["validation.require_snapshots"]:
            if table_ref not in source_tables:
                rel = config_path.relative_to(REPO_ROOT).as_posix()
                violations.append(f"{rel}: require_snapshots có {table_ref}, source.tables thì không")

    assert not violations, "require_snapshots không phải tập con của source.tables:\n" + "\n".join(
        f"  - {v}" for v in violations
    )
