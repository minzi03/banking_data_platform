"""
Replication slot không được giữ WAL vô hạn
==========================================

Postgres mặc định `max_slot_wal_keep_size = -1`: một replication slot không ai đọc
giữ WAL mãi mãi. Đo trên stack 2026-09-27: ba slot `debezium_slot_*` — không file
nào trong repo, kể cả lịch sử git, dùng tên đó — giữ 3.6 GB WAL (`wal_status =
extended`) và tăng theo mỗi lần seed. Slot đúng tên cũng giữ WAL mỗi khi connector
dừng hoặc task lỗi, và offset Debezium mất khi `docker compose down` (anonymous volume).

`05-cdc-setup.sh` đặt trần; vượt trần thì slot bị vô hiệu thay vì làm đầy đĩa.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CDC_SETUP = (REPO_ROOT / "docker" / "init_postgres" / "05-cdc-setup.sh").read_text(encoding="utf-8")
SLOT_NAME = re.compile(r'"slot\.name":\s*"(\w+)"')


def _size_in_mb(value: str) -> int:
    m = re.fullmatch(r"(\d+)\s*(MB|GB)", value)
    assert m, f"không đọc được kích thước {value!r}"
    return int(m.group(1)) * (1024 if m.group(2) == "GB" else 1)


def test_slot_wal_retention_is_capped():
    m = re.search(r"ALTER SYSTEM SET max_slot_wal_keep_size = '([^']+)';", CDC_SETUP)
    assert m, "05-cdc-setup.sh không đặt max_slot_wal_keep_size — slot mồ côi giữ WAL vô hạn"
    assert 0 < _size_in_mb(m.group(1)) <= 16 * 1024, f"trần {m.group(1)} phải dương và hữu hạn"


def test_connector_definitions_agree_on_slot_names():
    """Script và DAG đăng ký connector phải dùng cùng tên slot — lệch tên là sinh slot mồ côi."""
    script = SLOT_NAME.findall((REPO_ROOT / "code_etl" / "cdc" / "register_connectors.py").read_text(encoding="utf-8"))
    dag = SLOT_NAME.findall(
        (REPO_ROOT / "airflow" / "dags" / "cdc" / "cdc_register_connectors_dag.py").read_text(encoding="utf-8")
    )
    assert script, "không tìm thấy slot.name trong register_connectors.py — regex hỏng?"
    assert sorted(script) == sorted(dag), f"script {sorted(script)} ≠ DAG {sorted(dag)}"


DECIMAL_MODE = re.compile(r'"decimal\.handling\.mode":\s*"(\w+)"')


@pytest.mark.parametrize(
    "path",
    ["code_etl/cdc/register_connectors.py", "airflow/dags/cdc/cdc_register_connectors_dag.py"],
)
def test_every_connector_sends_decimals_as_strings(path):
    """
    Mặc định `precise` gửi NUMERIC dạng bytes base64 ("BxL/1Fg="); cdc_dlq cast sang
    decimal(18,2) ra NULL, nên balance / txn_amount / amount NULL ở MỌI dòng Bronze CDC
    (đo trên stack 2026-09-30: core_account_cdc 90.000 dòng, 0 balance khác NULL).
    """
    text = (REPO_ROOT / path).read_text(encoding="utf-8")
    connectors = SLOT_NAME.findall(text)
    modes = DECIMAL_MODE.findall(text)
    assert connectors, f"{path}: không tìm thấy connector nào — regex hỏng?"
    assert modes == ["string"] * len(connectors), (
        f"{path}: {len(connectors)} connector, decimal.handling.mode = {modes}"
    )
