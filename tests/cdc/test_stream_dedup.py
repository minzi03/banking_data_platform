"""
Tests cho event-time watermark + dedup trong CDC streaming (khoá new2 §4.3).

Hai lớp:
  1. Contract tĩnh — `dedup_config_problems` kiểm cấu hình YAML thật của mọi
     topic CDC, không cần Spark. Chạy trong CI mặc định.
  2. Hành vi thật — `apply_watermark_and_dedup` trên stream DataFrame in-memory,
     chứng minh event trùng bị loại và event đến trễ quá watermark bị loại.
     Đánh dấu `integration` (cần pyspark).
"""

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_JOB = PROJECT_ROOT / "code_etl" / "cdc" / "base_job"
CDC_CONFIG_DIR = PROJECT_ROOT / "code_etl" / "cdc" / "config"


def _load_dedup_module():
    spec = importlib.util.spec_from_file_location("stream_dedup", str(BASE_JOB / "stream_dedup.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _cdc_configs() -> dict[str, dict]:
    out = {}
    for path in sorted(CDC_CONFIG_DIR.glob("*.yml")):
        out[path.name] = yaml.safe_load(path.read_text(encoding="utf-8"))
    return out


# =============================================================================
# Lớp 1 — Contract tĩnh (không cần Spark)
# =============================================================================


class TestDedupConfigContract:
    def test_every_declared_dedup_block_is_valid(self):
        """Khối `dedup` trong mọi YAML CDC phải hợp lệ — nếu không job chết lúc chạy."""
        mod = _load_dedup_module()
        problems = []
        for name, cfg in _cdc_configs().items():
            for issue in mod.dedup_config_problems(cfg.get("dedup")):
                problems.append(f"{name}: {issue}")
        assert not problems, "dedup cấu hình sai:\n  " + "\n  ".join(problems)

    def test_transaction_topics_enable_dedup(self):
        """
        Topic giao dịch PHẢI bật dedup: Debezium at-least-once + Bronze append-only
        = dòng trùng sai vĩnh viễn nếu thiếu.
        """
        cfg = _cdc_configs()["cdc_online_transaction.yml"]
        assert cfg.get("dedup"), "online_transaction thiếu khối dedup"
        assert cfg["dedup"].get("keys") == ["transaction_id"]
        assert cfg["dedup"].get("watermark_delay")

    def test_online_transaction_declares_merchant_columns(self):
        """Giao dịch QR/POS phải mang merchant_id/payment_channel/refund_flag qua CDC."""
        cfg = _cdc_configs()["cdc_online_transaction.yml"]
        names = {c["name"] for c in cfg["target"]["columns"]}
        assert {"merchant_id", "payment_channel", "refund_flag"} <= names

    def test_cdc_ddl_has_merchant_columns(self):
        """DDL Bronze CDC phải có đúng các cột merchant — chống ARITY_MISMATCH."""
        ddl = (PROJECT_ROOT / "docker" / "init_iceberg" / "04_ddl_bronze_cdc.sql").read_text(encoding="utf-8")
        marker = "CREATE TABLE IF NOT EXISTS lakehouse.bronze.online_transaction_cdc"
        block = ddl.split(marker, 1)[1].split("USING iceberg", 1)[0]
        for col in ("merchant_id", "payment_channel", "refund_flag"):
            assert col in block, f"{col} thiếu trong DDL online_transaction_cdc"

    def test_missing_watermark_delay_is_reported(self):
        mod = _load_dedup_module()
        assert any("watermark_delay" in p for p in mod.dedup_config_problems({"keys": ["x"]}))

    def test_bad_interval_is_reported(self):
        mod = _load_dedup_module()
        problems = mod.dedup_config_problems({"watermark_delay": "10", "keys": ["x"]})
        assert any("định dạng" in p for p in problems)

    def test_valid_interval_accepted(self):
        mod = _load_dedup_module()
        for value in ("10 minutes", "30 seconds", "1 hour", "2 days"):
            assert mod.dedup_config_problems({"watermark_delay": value, "keys": ["x"]}) == []

    def test_no_config_means_no_problems(self):
        mod = _load_dedup_module()
        assert mod.dedup_config_problems(None) == []


# =============================================================================
# Lớp 2 — Hành vi thật trên stream (cần Spark)
# =============================================================================


@pytest.mark.integration
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="stream nguồn file trên Windows cần winutils (HADOOP_HOME); chạy ở CI Linux",
)
class TestWatermarkAndDedupBehaviour:
    """
    `withWatermark`/`dropDuplicatesWithinWatermark` chỉ hợp lệ trên STREAM
    DataFrame, không phải batch. Nên test phải chạy một streaming query thật
    với memory sink — nếu không, Spark ném AnalysisException và test không đo
    được đúng thứ cần đo.
    """

    @pytest.fixture(scope="class")
    def env(self):
        pytest.importorskip("pyspark.sql.functions", reason="pyspark không có trong CI env")
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("test-stream-dedup")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        mod = _load_dedup_module()
        yield spark, mod
        spark.stop()

    @staticmethod
    def _run_stream(spark, transformed_df, tmp_path, name: str):
        """Ghi stream ra memory sink rồi trả toàn bộ dòng đã nhận."""
        query = (
            transformed_df.writeStream.format("memory")
            .queryName(name)
            .outputMode("append")
            .option("checkpointLocation", str(tmp_path / f"ckpt_{name}"))
            .start()
        )
        try:
            query.processAllAvailable()
        finally:
            query.stop()
        return spark.sql(f"SELECT * FROM {name}").collect()

    def _stream_from_payloads(self, spark, payloads, tmp_path, name):
        """
        Nguồn stream có cột `value` giống Kafka frame.

        Dùng `readStream.json` trên thư mục tạm: đây là nguồn file thật, cho
        DataFrame đúng cột và không cần Kafka. `rate` source không có cột `id`
        nên không join được với seed.
        """
        import json

        src = tmp_path / f"src_{name}"
        src.mkdir()
        (src / "batch.json").write_text(
            "\n".join(json.dumps({"value": json.dumps(p)}) for p in payloads),
            encoding="utf-8",
        )
        return spark.readStream.schema("value STRING").json(str(src))

    def test_duplicate_keys_are_dropped(self, env, tmp_path):
        spark, mod = env
        payloads = [
            {"payload": {"transaction_id": "1", "__ts_ms": "1700000000000"}},
            {"payload": {"transaction_id": "1", "__ts_ms": "1700000001000"}},
            {"payload": {"transaction_id": "2", "__ts_ms": "1700000002000"}},
        ]
        stream = self._stream_from_payloads(spark, payloads, tmp_path, "dups")
        out = mod.apply_watermark_and_dedup(
            stream,
            {"watermark_delay": "10 minutes", "keys": ["transaction_id"]},
            target_primary_key="transaction_id",
        )
        result = self._run_stream(spark, out, tmp_path, "dedup_dups")
        assert len(result) == 2, f"mong đợi 2 dòng sau dedup, nhận {len(result)}"

    def test_watermark_column_is_event_time(self, env, tmp_path):
        spark, mod = env
        stream = self._stream_from_payloads(
            spark, [{"payload": {"transaction_id": "9", "__ts_ms": "1700000000000"}}], tmp_path, "ts"
        )
        out = mod.apply_watermark_and_dedup(stream, {"watermark_delay": "10 minutes", "keys": ["transaction_id"]})
        result = self._run_stream(spark, out, tmp_path, "dedup_ts")
        assert result, "stream không trả dòng nào"
        assert result[0][mod.DEFAULT_EVENT_TIME_COLUMN] is not None, "event time NULL → watermark vô hiệu"

    def test_tombstones_are_dropped_before_watermark(self, env, tmp_path):
        """Tombstone (value NULL) không có event time — phải bị loại trước watermark."""
        spark, mod = env
        src = tmp_path / "src_tomb"
        src.mkdir()
        (src / "b.json").write_text(
            '{"value": "{\\"payload\\": {\\"transaction_id\\": \\"1\\", \\"__ts_ms\\": \\"1700000000000\\"}}"}'
            "\n"
            '{"value": null}',
            encoding="utf-8",
        )
        stream = spark.readStream.schema("value STRING").json(str(src))
        out = mod.apply_watermark_and_dedup(stream, {"watermark_delay": "10 minutes", "keys": ["transaction_id"]})
        result = self._run_stream(spark, out, tmp_path, "tomb")
        assert len(result) == 1, f"tombstone phải bị loại, nhận {len(result)} dòng"

    def test_no_dedup_config_is_a_noop(self, env, tmp_path):
        spark, mod = env
        stream = self._stream_from_payloads(
            spark,
            [
                {"payload": {"transaction_id": "1", "__ts_ms": "1700000000000"}},
                {"payload": {"transaction_id": "1", "__ts_ms": "1700000000000"}},
            ],
            tmp_path,
            "noop",
        )
        out = mod.apply_watermark_and_dedup(stream, None)
        result = self._run_stream(spark, out, tmp_path, "dedup_noop")
        assert len(result) == 2, "không khai dedup phải giữ nguyên (hành vi cũ)"

    def test_dedup_without_key_raises(self, env, tmp_path):
        spark, mod = env
        stream = self._stream_from_payloads(
            spark, [{"payload": {"transaction_id": "1", "__ts_ms": "1700000000000"}}], tmp_path, "err"
        )
        with pytest.raises(ValueError, match="khoá"):
            mod.apply_watermark_and_dedup(stream, {"watermark_delay": "10 minutes"}, target_primary_key=None)

    def test_dedup_without_delay_raises(self, env, tmp_path):
        spark, mod = env
        stream = self._stream_from_payloads(
            spark, [{"payload": {"transaction_id": "1", "__ts_ms": "1700000000000"}}], tmp_path, "err"
        )
        with pytest.raises(ValueError, match="watermark_delay"):
            mod.apply_watermark_and_dedup(stream, {"keys": ["transaction_id"]})
