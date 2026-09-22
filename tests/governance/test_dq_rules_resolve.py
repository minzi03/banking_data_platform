"""
Contract Test — Rule file THẬT của DQ và quarantine phải phân giải được
=======================================================================

`code_etl/shared/ops/dq_rules.yml` là cấu hình production: `data_quality.py`
đọc đúng file này trong Airflow. Trước test này, **không test nào nạp nó**.
`tests/ops/test_data_quality.py` dùng fixture `sample_dq_rules` — một YAML
tổng hợp dựng trong `tmp_path`. Nên toàn bộ bộ test có thể xanh trong khi file
thật hỏng, và đó chính là chuyện đã xảy ra: một rule trỏ vào
`lakehouse.gold.branch_monthly_summary` (bảng thật có tiền tố `mart_`) làm task
`dq_gold_checks` đỏ mỗi ngày mà không ai biết — xem
[`technical-debt.md` TD-9](../../docs/05-quality/technical-debt.md).

Test này bịt hai lỗ, theo hai kiểu hỏng khác nhau:

**1. Tên bảng sai → fail LOUD nhưng vô hình.** Mọi hàm check trong
`data_quality.py` bọc `try/except` và trả `("FAIL", "N/A", f"Error: {e}")`. Bảng
không tồn tại nên sinh FAIL thật, `sys.exit(1)` thật — nhưng DAG đặt
`email_on_failure: False` và không ai đọc `data_quality_log`, nên một job đỏ
hằng ngày vẫn im lặng.

**2. Tên check sai → fail OPEN.** `run_checks_for_table` bỏ qua check không có
trong `CHECK_DISPATCH` bằng một `log.warning` rồi `continue`. Gõ `nul_check`
thay `null_check` thì check **không chạy**, không có dòng nào trong log, và job
vẫn `exit 0`. Bảng đó trông như đã được kiểm.

Nguồn sự thật cho "bảng có tồn tại" là DDL (`docker/init_iceberg/*.sql`), không
phải một danh sách viết trong test này — danh sách cứng chỉ chuyển chỗ của
drift. Hàm parse DDL dùng lại `scripts/generate_data_dictionary.py`, kể cả
`SKIP_DDL`, để hai bên không thể hiểu khác nhau về "DDL đang hiệu lực".

Phạm vi: test này kiểm **tên** phân giải được, không kiểm cột có tồn tại hay
ngưỡng có hợp lý — những thứ đó cần Spark và dữ liệu thật.

Chạy: pytest tests/governance/test_dq_rules_resolve.py -v
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]

DQ_RULES = REPO_ROOT / "code_etl" / "shared" / "ops" / "dq_rules.yml"
QUARANTINE_RULES = REPO_ROOT / "code_etl" / "shared" / "ops" / "quarantine_rules.yml"
DATA_QUALITY_PY = REPO_ROOT / "code_etl" / "shared" / "ops" / "data_quality.py"
DICTIONARY_GENERATOR = REPO_ROOT / "scripts" / "generate_data_dictionary.py"
ICEBERG_DDL_DIR = REPO_ROOT / "docker" / "init_iceberg"


# ---------------------------------------------------------------------------
# Nguồn sự thật: bảng khai trong DDL
# ---------------------------------------------------------------------------
def _load_dictionary_generator():
    """
    Nạp `scripts/generate_data_dictionary.py` để dùng lại `parse_ddl` và
    `SKIP_DDL`.

    Đăng ký vào `sys.modules` là BẮT BUỘC, không phải cho tiện: `@dataclass`
    trong module đó tra `sys.modules[cls.__module__]` khi kiểm `KW_ONLY`, nên
    exec một module chưa đăng ký sẽ chết với AttributeError.
    """
    spec = importlib.util.spec_from_file_location("generate_data_dictionary", DICTIONARY_GENERATOR)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _ddl_tables() -> dict[str, str]:
    """Ánh xạ tên bảng đầy đủ → file DDL khai nó."""
    gen = _load_dictionary_generator()
    tables: dict[str, str] = {}
    for path in sorted(ICEBERG_DDL_DIR.glob("*.sql")):
        if path.name in gen.SKIP_DDL:
            continue
        for table in gen.parse_ddl(path):
            tables.setdefault(table.fqn, path.name)
    return tables


DDL_TABLES = _ddl_tables()


# ---------------------------------------------------------------------------
# Nguồn sự thật: CHECK_DISPATCH trong data_quality.py
# ---------------------------------------------------------------------------
def _load_data_quality_module():
    """
    Nạp `data_quality.py` để đọc `CHECK_DISPATCH` thật, không phải bản chép lại.

    pyspark bị stub CHỈ trong lúc exec rồi khôi phục ngay — cùng lý do và cùng
    cách làm với `tests/ops/test_data_quality.py`: job `test` trong ci.yml
    không cài pyspark, còn để MagicMock lại trong `sys.modules` sẽ giết mọi
    test cần pyspark thật chạy sau (`pyspark.__spec__ is not set`).
    """
    spec = importlib.util.spec_from_file_location("data_quality_rules_contract", DATA_QUALITY_PY)
    module = importlib.util.module_from_spec(spec)
    stubbed = {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.types": MagicMock(),
    }
    saved = {name: sys.modules.get(name) for name in stubbed}
    sys.modules.update(stubbed)
    try:
        spec.loader.exec_module(module)
    finally:
        for name, previous in saved.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
    return module


CHECK_DISPATCH = _load_data_quality_module().CHECK_DISPATCH


# ---------------------------------------------------------------------------
# Rule file thật
# ---------------------------------------------------------------------------
DQ_TABLES: dict[str, dict] = yaml.safe_load(DQ_RULES.read_text(encoding="utf-8"))["tables"]
QUARANTINE_GROUPS: dict[str, dict] = yaml.safe_load(QUARANTINE_RULES.read_text(encoding="utf-8"))["quarantine_rules"]


def _dq_checks() -> list[tuple[str, int, dict]]:
    """(tên bảng, thứ tự check, rule) cho mọi check khai trong dq_rules.yml."""
    return [(table, i, check) for table, cfg in DQ_TABLES.items() for i, check in enumerate(cfg.get("checks") or [])]


DQ_CHECKS = _dq_checks()

# Tên bảng mà một rule trỏ tới NGOÀI khoá của chính nó. Cả hai đều được
# `spark.table(...)` gọi trực tiếp, nên gõ sai ở đây hỏng đúng như gõ sai khoá.
CROSS_TABLE_KEYS = ("ref_table", "source_table")


def _cross_table_refs() -> list[tuple[str, str, str, str]]:
    """(bảng, tên check, khoá, bảng được trỏ tới)."""
    return [
        (table, check.get("name", f"#{i}"), key, check[key])
        for table, i, check in DQ_CHECKS
        for key in CROSS_TABLE_KEYS
        if check.get(key)
    ]


CROSS_TABLE_REFS = _cross_table_refs()


def _fix_hint(table: str) -> str:
    """Gợi ý bảng gần nhất trong DDL — lỗi thường là thiếu/thừa tiền tố."""
    leaf = table.rsplit(".", 1)[-1]
    near = sorted(t for t in DDL_TABLES if t.rsplit(".", 1)[-1].endswith(leaf) or leaf.endswith(t.rsplit(".", 1)[-1]))
    return f"\nỨng viên trong DDL: {', '.join(near)}" if near else ""


# ---------------------------------------------------------------------------
# Guard chống pass rỗng
# ---------------------------------------------------------------------------
# Đếm tại 2026-09-22: 54 bảng DDL · 29 mục dq_rules · 88 check · 18 refs chéo ·
# 5 nhóm quarantine · 18 violation. Ngưỡng đặt dưới số thật để rule mới không
# làm đỏ, nhưng đủ cao để một lần parse hỏng không thể xanh.
MIN_DDL_TABLES = 45
MIN_DQ_TABLES = 25
MIN_DQ_CHECKS = 80
MIN_CROSS_TABLE_REFS = 15
MIN_QUARANTINE_GROUPS = 5
MIN_QUARANTINE_VIOLATIONS = 15


def test_ddl_tables_are_parsed():
    """
    Nếu regex parse DDL hỏng, `DDL_TABLES` rỗng → mọi test dưới đỏ, không xanh.
    Guard này tồn tại để thông điệp lỗi nói đúng nguyên nhân (parse hỏng) thay
    vì báo 29 bảng đều không tồn tại.
    """
    assert len(DDL_TABLES) >= MIN_DDL_TABLES, (
        f"Chỉ parse được {len(DDL_TABLES)} bảng từ {ICEBERG_DDL_DIR.name}/. "
        "Nghi ngờ regex CREATE TABLE trong generate_data_dictionary.py hỏng."
    )


def test_dq_rules_are_parsed():
    """Guard chống pass rỗng cho chính file rule — parse hỏng thì không có gì để kiểm."""
    assert len(DQ_TABLES) >= MIN_DQ_TABLES, (
        f"Chỉ đọc được {len(DQ_TABLES)} mục bảng trong dq_rules.yml. "
        "Nghi ngờ YAML hỏng hoặc khoá `tables` đã đổi tên — test sẽ xanh mà không kiểm gì."
    )
    assert len(DQ_CHECKS) >= MIN_DQ_CHECKS, (
        f"Chỉ đọc được {len(DQ_CHECKS)} check trong dq_rules.yml (kỳ vọng >= {MIN_DQ_CHECKS})."
    )
    assert len(CROSS_TABLE_REFS) >= MIN_CROSS_TABLE_REFS, (
        f"Chỉ tìm thấy {len(CROSS_TABLE_REFS)} tham chiếu chéo "
        f"({'/'.join(CROSS_TABLE_KEYS)}). Nghi ngờ cấu trúc rule đã đổi."
    )


def test_check_dispatch_is_populated():
    """CHECK_DISPATCH rỗng sẽ làm mọi assertion về tên check trở thành vô nghĩa."""
    assert len(CHECK_DISPATCH) >= 5, (
        f"CHECK_DISPATCH chỉ có {len(CHECK_DISPATCH)} khoá: {sorted(CHECK_DISPATCH)}. "
        "Nghi ngờ nạp sai module data_quality.py."
    )


# ---------------------------------------------------------------------------
# dq_rules.yml — tên bảng
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("table", sorted(DQ_TABLES))
def test_dq_rule_table_exists_in_ddl(table: str):
    """
    Mọi khoá trong `dq_rules.yml` phải là bảng có khai trong DDL.

    Không tồn tại nghĩa là `spark.table()` ném exception, exception được tính
    FAIL, và task DQ của tầng đó đỏ mỗi lần chạy.
    """
    assert table in DDL_TABLES, (
        f"dq_rules.yml khai `{table}` nhưng không DDL nào trong "
        f"docker/init_iceberg/ tạo bảng này.\n"
        f"Mỗi check trên bảng này sẽ FAIL với 'Error: ...' và làm task DQ đỏ."
        f"{_fix_hint(table)}"
    )


@pytest.mark.parametrize(
    ("table", "check_name", "key", "referenced"),
    CROSS_TABLE_REFS,
    ids=[f"{t}-{c}-{k}" for t, c, k, _ in CROSS_TABLE_REFS],
)
def test_dq_referenced_table_exists_in_ddl(table: str, check_name: str, key: str, referenced: str):
    """
    `ref_table` và `source_table` cũng được `spark.table()` gọi trực tiếp, nên
    chúng hỏng theo đúng cùng một cách với khoá bảng.
    """
    assert referenced in DDL_TABLES, (
        f"dq_rules.yml: check `{check_name}` trên `{table}` trỏ {key}=`{referenced}`, "
        f"bảng này không có trong DDL.{_fix_hint(referenced)}"
    )


# ---------------------------------------------------------------------------
# dq_rules.yml — tên check
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("table", "index", "check"),
    DQ_CHECKS,
    ids=[f"{t}-{i}-{c.get('name', 'unnamed')}" for t, i, c in DQ_CHECKS],
)
def test_dq_check_name_is_dispatchable(table: str, index: int, check: dict):
    """
    Tên check phải là khoá của `CHECK_DISPATCH`.

    Đây là lỗ fail-OPEN: tên lạ chỉ sinh một `log.warning` rồi `continue`, nên
    check không chạy, log không có dòng nào, và job vẫn `exit 0`. Bảng trông
    như đã được kiểm. Không có gì đỏ ở runtime — chỉ test này bắt được.
    """
    name = check.get("name")
    assert name, f"dq_rules.yml: check thứ {index} của `{table}` không có khoá `name`"
    assert name in CHECK_DISPATCH, (
        f"dq_rules.yml: check `{name}` trên `{table}` không có trong CHECK_DISPATCH "
        f"(data_quality.py). Nó sẽ bị bỏ qua IM LẶNG và job vẫn exit 0.\n"
        f"Khoá hợp lệ: {', '.join(sorted(CHECK_DISPATCH))}"
    )


# ---------------------------------------------------------------------------
# quarantine_rules.yml
# ---------------------------------------------------------------------------
def test_quarantine_rules_are_parsed():
    """Guard chống pass rỗng cho file quarantine."""
    violations = sum(len(cfg.get("violations") or []) for cfg in QUARANTINE_GROUPS.values())
    assert len(QUARANTINE_GROUPS) >= MIN_QUARANTINE_GROUPS, (
        f"Chỉ đọc được {len(QUARANTINE_GROUPS)} nhóm rule trong quarantine_rules.yml."
    )
    assert violations >= MIN_QUARANTINE_VIOLATIONS, (
        f"Chỉ đọc được {violations} violation trong quarantine_rules.yml (kỳ vọng >= {MIN_QUARANTINE_VIOLATIONS})."
    )


@pytest.mark.parametrize("group", sorted(QUARANTINE_GROUPS))
def test_quarantine_source_table_exists_in_ddl(group: str):
    """
    `source_table` được `spark.table()` đọc trong `check_violation`. Sai tên ở
    đây tệ hơn ở DQ: lỗi bị bắt, log ở mức ERROR, và hàm trả về `[]` — tức
    "không có vi phạm". Job `exit 0` và bảng đó coi như sạch.
    """
    source = QUARANTINE_GROUPS[group].get("source_table")
    assert source, f"quarantine_rules.yml: nhóm `{group}` không khai source_table"
    assert source in DDL_TABLES, (
        f"quarantine_rules.yml: nhóm `{group}` đọc source_table=`{source}`, "
        f"bảng này không có trong DDL. `check_violation` sẽ trả [] — "
        f"không phân biệt được với 'không có vi phạm'.{_fix_hint(source)}"
    )


@pytest.mark.parametrize("group", sorted(QUARANTINE_GROUPS))
def test_quarantine_violations_are_well_formed(group: str):
    """Mỗi violation cần `name` và `condition` — thiếu `condition` là KeyError lúc chạy."""
    for i, violation in enumerate(QUARANTINE_GROUPS[group].get("violations") or []):
        assert violation.get("name"), f"quarantine_rules.yml: violation thứ {i} của `{group}` thiếu `name`"
        assert violation.get("condition"), (
            f"quarantine_rules.yml: violation `{violation.get('name', i)}` của `{group}` thiếu `condition` — "
            "`run_quarantine_checks` truy cập violation['condition'] không phòng bị, nên job chết ngay."
        )


def test_quarantine_target_tables_have_no_ddl():
    """
    Ghi lại một khoảng hở đã biết, KHÔNG phải kiểm một bất biến.

    Không DDL nào tạo `lakehouse.quarantine.*`, kể cả schema (`create_schemas.sql`
    tạo bronze/silver/gold/sandbox/staging). Nên `write_to_quarantine` luôn ném ở
    `spark.table(target_table)`, log ERROR, và trả 0 — hàng vi phạm không được
    ghi đi đâu cả.

    Test này FAIL khi ai đó thêm DDL cho quarantine: lúc đó hãy xoá nó và đổi
    thành assertion ngược (target_table phải tồn tại), rồi cập nhật
    `docs/05-quality/DATA_QUALITY.md` §5.
    """
    targets = sorted({cfg["target_table"] for cfg in QUARANTINE_GROUPS.values() if cfg.get("target_table")})
    with_ddl = [t for t in targets if t in DDL_TABLES]
    assert not with_ddl, (
        f"Bảng quarantine đã có DDL: {with_ddl}. Khoảng hở mà test này ghi lại đã được "
        "lấp — hãy thay test này bằng assertion 'mọi target_table phải tồn tại trong DDL'."
    )
