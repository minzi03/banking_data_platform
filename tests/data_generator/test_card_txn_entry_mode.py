"""
card_txn.entry_mode + decline_reason — theo phân phối thật của bộ Xóm Bank.

Bất biến lấy từ chính dữ liệu tham khảo (157.224 giao dịch thẻ):
- lỗi PIN chỉ xảy ra khi có mặt thẻ (chip/quẹt), không bao giờ online;
- lỗi CVV / ngày hết hạn / số thẻ chỉ xảy ra online;
- lỗi tổ hợp tồn tại ("Bad PIN,Insufficient Balance") và được giữ;
- chỉ giao dịch lỗi mới có lý do.

Thêm hai mắt xích dễ hỏng âm thầm:
- danh sách cột generate_all.py ghi card_txn phải cùng độ dài và thứ tự với
  tuple generator trả về — lệch là COPY đổ giá trị sang nhầm cột;
- cột và constraint mới phải có ở CẢ DDL gốc (volume mới) lẫn migration (volume cũ).
"""

from __future__ import annotations

import importlib.util
import random
import re
import sys
import types
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "data_generator"))

from generators.card_crm import (  # noqa: E402
    DECLINE_REASON_WEIGHTS,
    card_entry_mode,
    decline_reason,
    generate_card_txn,
)

SEED_CONFIG = REPO_ROOT / "data_generator" / "config" / "seed_config.yaml"
GENERATE_ALL = REPO_ROOT / "data_generator" / "generate_all.py"
DDL = REPO_ROOT / "docker" / "init_postgres" / "02_ddl_card_crm.sql"
MIGRATIONS = REPO_ROOT / "data_generator" / "migrations"

PIN = "WRONG_PIN"
CARD_NOT_PRESENT = {"WRONG_CVV", "INVALID_EXPIRY", "INVALID_CARD_NUMBER"}


def _labels(reason: str) -> set[str]:
    return set(reason.split(","))


@pytest.fixture(scope="module")
def rows() -> list[tuple]:
    random.seed(11)
    config = yaml.safe_load(SEED_CONFIG.read_text(encoding="utf-8"))["card_crm"]["card_txn"]
    cards = [(i, i, "DEBIT", "ACTIVE") for i in range(1, 51)]
    return generate_card_txn(20_000, config, cards, ["5411", "5812"])


def _columns_written_by_generate_all() -> list[str]:
    text = GENERATE_ALL.read_text(encoding="utf-8")
    # write_rows(...) hoặc write_chunked(writer, csv_writer, ...) — cả hai nêu cột ngay sau tên bảng.
    match = re.search(r'"card_crm", "card_txn", \[(.*?)\]', text, re.S)
    assert match, "không tìm thấy danh sách cột card_txn trong generate_all.py"
    return re.findall(r'"(\w+)"', match.group(1))


def _as_dicts(rows: list[tuple]) -> list[dict]:
    columns = _columns_written_by_generate_all()
    return [dict(zip(columns, r, strict=True)) for r in rows]


def test_column_list_matches_generated_tuple(rows):
    assert len(_columns_written_by_generate_all()) == len(rows[0])
    row = _as_dicts(rows[:1])[0]
    assert row["entry_mode"] in {"CHIP", "SWIPE", "ONLINE"}
    assert row["channel"] in {"POS", "ECOM", "ATM"}


def test_entry_mode_follows_channel(rows):
    for row in _as_dicts(rows):
        if row["channel"] == "ECOM":
            assert row["entry_mode"] == "ONLINE"
        else:
            assert row["entry_mode"] in {"CHIP", "SWIPE"}


def test_only_failed_transactions_have_a_reason(rows):
    for row in _as_dicts(rows):
        assert (row["status"] == "FAILED") == (row["decline_reason"] is not None), row


def test_reasons_are_possible_for_how_the_card_was_used(rows):
    failed = [row for row in _as_dicts(rows) if row["status"] == "FAILED"]
    assert len(failed) > 500, "quá ít giao dịch lỗi để kiểm"
    for row in failed:
        labels = _labels(row["decline_reason"])
        if row["entry_mode"] == "ONLINE":
            assert PIN not in labels, row
        else:
            assert not labels & CARD_NOT_PRESENT, row


def test_weights_keep_reference_shape():
    """Tổ hợp được giữ, nhãn trong tổ hợp sắp theo tên, mọi nhãn vừa cột."""
    combos = [r for weights in DECLINE_REASON_WEIGHTS.values() for r in weights if "," in r]
    assert combos, "bỏ mất lỗi tổ hợp của dữ liệu tham khảo"
    for weights in DECLINE_REASON_WEIGHTS.values():
        for reason in weights:
            parts = reason.split(",")
            assert parts == sorted(parts), reason
            assert len(reason) <= 100
    # Nhãn phổ biến nhất ở mọi kiểu dùng thẻ là thiếu số dư — như dữ liệu thật.
    for weights in DECLINE_REASON_WEIGHTS.values():
        assert max(weights, key=weights.get) == "INSUFFICIENT_FUNDS"


def test_helpers_directly():
    random.seed(2)
    assert card_entry_mode("ECOM") == "ONLINE"
    assert decline_reason("SUCCESS", "CHIP") is None
    assert decline_reason("PENDING", "ONLINE") is None
    assert decline_reason("FAILED", "ONLINE") in DECLINE_REASON_WEIGHTS["ONLINE"]


def test_ddl_and_migration_declare_the_same_columns_and_constraints():
    ddl = DDL.read_text(encoding="utf-8")
    migration = "\n".join(p.read_text(encoding="utf-8") for p in sorted(MIGRATIONS.glob("*.sql")))
    for column in ("entry_mode", "decline_reason"):
        assert re.search(rf"^\s+{column}\s+VARCHAR", ddl, re.M), f"DDL thiếu {column}"
        assert f"ADD COLUMN IF NOT EXISTS {column}" in migration, f"migration thiếu {column}"
    for name in ("chk_ct_entry_mode", "chk_ct_decline_reason"):
        assert f"CONSTRAINT {name}" in ddl
        assert f"conname = '{name}'" in migration, f"migration không kiểm {name} trước khi thêm"


def test_migrations_are_idempotent_by_construction():
    """Mọi ADD COLUMN có IF NOT EXISTS; mọi ADD CONSTRAINT nằm sau kiểm pg_constraint."""
    for path in sorted(MIGRATIONS.glob("*.sql")):
        sql = path.read_text(encoding="utf-8")
        adds = re.findall(r"ADD COLUMN(?! IF NOT EXISTS)", sql)
        assert not adds, f"{path.name}: ADD COLUMN không idempotent"
        for name in re.findall(r"ADD CONSTRAINT (\w+)", sql):
            assert f"conname = '{name}'" in sql, f"{path.name}: {name} không được kiểm trước"


class _FakeCursor:
    def __init__(self, log, fail_on):
        self.log, self.fail_on = log, fail_on

    def execute(self, sql):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("boom")
        self.log.append(sql)

    def close(self):
        pass


class _FakeConn:
    def __init__(self, fail_on=None):
        self.executed, self.commits, self.rollbacks, self.fail_on = [], 0, 0, fail_on

    def cursor(self):
        return _FakeCursor(self.executed, self.fail_on)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture
def writer_cls(monkeypatch):
    """
    PostgresWriter import psycopg2 ở đầu module, còn job unit test của CI không cài
    psycopg2 (chỉ cần cho seed). Các test dưới dùng connection giả, nên thay
    psycopg2 bằng module rỗng — CHỈ khi thiếu thật, và monkeypatch gỡ sau test.
    """
    if importlib.util.find_spec("psycopg2") is None:
        stub = types.ModuleType("psycopg2")
        stub.extras = types.ModuleType("psycopg2.extras")
        monkeypatch.setitem(sys.modules, "psycopg2", stub)
        monkeypatch.setitem(sys.modules, "psycopg2.extras", stub.extras)
    path = REPO_ROOT / "data_generator" / "connectors" / "postgres_writer.py"
    spec = importlib.util.spec_from_file_location("_postgres_writer_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.PostgresWriter


def _writer(cls, conn):
    writer = cls("h", 1, "db", "u", "p")
    writer.conn = conn
    return writer


def test_apply_migrations_runs_files_in_order(tmp_path, writer_cls):
    (tmp_path / "002_b.sql").write_text("SELECT 2;", encoding="utf-8")
    (tmp_path / "001_a.sql").write_text("SELECT 1;", encoding="utf-8")
    conn = _FakeConn()
    assert _writer(writer_cls, conn).apply_migrations(tmp_path) == ["001_a.sql", "002_b.sql"]
    assert conn.executed == ["SELECT 1;", "SELECT 2;"]
    assert conn.commits == 2


def test_apply_migrations_stops_and_rolls_back_on_failure(tmp_path, writer_cls):
    (tmp_path / "001_a.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "002_b.sql").write_text("BROKEN;", encoding="utf-8")
    (tmp_path / "003_c.sql").write_text("SELECT 3;", encoding="utf-8")
    conn = _FakeConn(fail_on="BROKEN")
    with pytest.raises(RuntimeError):
        _writer(writer_cls, conn).apply_migrations(tmp_path)
    assert conn.executed == ["SELECT 1;"]
    assert conn.rollbacks == 1
