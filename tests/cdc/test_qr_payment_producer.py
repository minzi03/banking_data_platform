"""
Contract Test — QR payment producer + topic tiêu thụ (khoá new2 §3.1).
=====================================================================

Đề bài: *"QR Payment Producer: Push events directly to Kafka"*, *"Producer Python
giả lập sự kiện giao dịch"*.

Điểm dễ sai nhất: producer phát JSON phẳng, trong khi consumer hiện tại
(`cdc_dlq.validate_and_split`) chỉ hiểu envelope Debezium
(`payload` + `__op` + `__ts_ms`). Lệch shape → mọi event rơi vào DLQ với
PARSE_ERROR, và người viết tưởng producer hỏng.

Test này chốt:
  1. Event producer sinh ra có đúng envelope mà consumer đọc.
  2. Mọi cột khai trong `cdc_qr_payment.yml` đều có trong event.
  3. Config QR bật dedup và trỏ topic riêng (không trộn với topic Debezium).
  4. DDL `qr_payment_cdc` có đủ cột merchant.
  5. Replay (`--txn-ids`, `--event-ts-ms`) phát đúng ID cũ, đúng event-time,
     không lặp vô hạn — dùng bởi scripts/verify_checkpoint_restart.py.

Chạy: pytest tests/cdc/test_qr_payment_producer.py -v
"""

from __future__ import annotations

import importlib.util
import json
import random
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PRODUCER = PROJECT_ROOT / "scripts" / "qr_payment_producer.py"
QR_CONFIG = PROJECT_ROOT / "code_etl" / "cdc" / "config" / "cdc_qr_payment.yml"
CDC_DDL = PROJECT_ROOT / "docker" / "init_iceberg" / "04_ddl_bronze_cdc.sql"


def _load_producer():
    spec = importlib.util.spec_from_file_location("qr_producer", str(PRODUCER))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _qr_config() -> dict:
    return yaml.safe_load(QR_CONFIG.read_text(encoding="utf-8"))


def test_producer_file_exists():
    assert PRODUCER.exists(), "thiếu scripts/qr_payment_producer.py"


def test_event_has_debezium_envelope():
    """
    Consumer đọc `payload` + `__op` + `__ts_ms`. Thiếu một cái là event rơi DLQ.
    """
    mod = _load_producer()
    evt = mod.build_qr_event(1001, 5, rng=random.Random(1))

    assert set(evt) == {"payload"}, "event phải bọc trong khoá `payload` như Debezium"
    payload = evt["payload"]
    assert payload["__op"] == "c"
    assert str(payload["__ts_ms"]).isdigit(), "__ts_ms phải là chuỗi số (ms)"


def test_event_accepts_fixed_event_time_for_replay():
    """Replay phải giữ nguyên event-time để dedup state nhận diện đúng event cũ."""
    mod = _load_producer()
    event = mod.build_qr_event(1003, 5, rng=random.Random(1), event_ts_ms=1_700_000_000_123)

    assert event["payload"]["__ts_ms"] == "1700000000123"
    assert event["payload"]["transaction_date"] == "1700000000123"


def test_fixed_transaction_ids_are_emitted_once_in_dry_run(capsys):
    """The replay CLI must not loop forever or generate replacement IDs."""
    mod = _load_producer()
    assert mod.main(["--dry-run", "--txn-ids", "7001,7002", "--rate", "0"]) == 0

    lines = capsys.readouterr().out.splitlines()
    payloads = [json.loads(line)["payload"] for line in lines if line.startswith("{")]
    assert [payload["transaction_id"] for payload in payloads] == [7001, 7002]


def test_event_carries_every_configured_column():
    """Mỗi cột YAML khai phải có trong payload — thiếu là NULL lặng lẽ ở Bronze."""
    mod = _load_producer()
    evt = mod.build_qr_event(1002, 6, rng=random.Random(2))["payload"]
    declared = [c["name"] for c in _qr_config()["target"]["columns"]]

    missing = [c for c in declared if c not in evt]
    assert not missing, f"payload thiếu cột YAML khai: {missing}"


def test_event_is_qr_payment_for_a_merchant():
    """Đây là producer QR: payment_channel phải là QR, và có merchant_id."""
    mod = _load_producer()
    for seed in range(5):
        p = mod.build_qr_event(2000 + seed, 7, rng=random.Random(seed))["payload"]
        assert p["payment_channel"] == "QR"
        assert p["merchant_id"] == 7
        assert p["status"] in {"SUCCESS", "FAILED", "PENDING"}


def test_decimal_amount_is_a_string():
    """Debezium connector cấu hình decimal.handling.mode=string — producer phải khớp."""
    mod = _load_producer()
    p = mod.build_qr_event(3001, 8, rng=random.Random(3))["payload"]
    assert isinstance(p["amount"], str), "amount phải là chuỗi decimal"
    float(p["amount"])


def test_qr_config_targets_its_own_topic_and_table():
    """Không trộn QR event vào topic/bảng của Debezium."""
    cfg = _qr_config()
    assert cfg["kafka"]["topic"].startswith("qr."), cfg["kafka"]["topic"]
    assert "postgresql" not in cfg["kafka"]["topic"]
    assert cfg["target"]["table"] == "qr_payment_cdc"


def test_qr_config_enables_dedup():
    """Producer retry (acks=all) → dedup theo transaction_id là bắt buộc."""
    cfg = _qr_config()
    assert cfg.get("dedup"), "qr_payment thiếu khối dedup"
    assert cfg["dedup"]["keys"] == ["transaction_id"]


def test_qr_target_table_exists_in_cdc_ddl():
    ddl = CDC_DDL.read_text(encoding="utf-8")
    assert "lakehouse.bronze.qr_payment_cdc" in ddl, "DDL thiếu bảng qr_payment_cdc"
    block = ddl.split("lakehouse.bronze.qr_payment_cdc", 1)[1].split("USING iceberg", 1)[0]
    for col in ("transaction_id", "merchant_id", "payment_channel", "refund_flag", "__cdc_timestamp"):
        assert col in block, f"qr_payment_cdc thiếu cột {col}"


def test_producer_handles_missing_kafka_library(monkeypatch, capsys):
    """
    Máy chưa cài kafka-python phải báo rõ cách cài, không crash bằng traceback.
    """
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "kafka":
            raise ImportError("no kafka")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    mod = _load_producer()
    rc = mod.main(["--count", "1"])
    assert rc == 2
    assert "kafka-python" in capsys.readouterr().err
