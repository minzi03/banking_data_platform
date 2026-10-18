"""
Verify watermark + dedup thật trên Spark (chạy trong container Linux).

Chứng minh hai điều kiện đề bài §4.3:
  1. Event trùng theo transaction_id bị loại khi replay.
  2. Watermark có event time đúng (không NULL).
"""

import json
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/project/code_etl/cdc/base_job")
from stream_dedup import DEFAULT_EVENT_TIME_COLUMN, apply_watermark_and_dedup  # noqa: E402

spark = (
    SparkSession.builder.appName("verify-stream-dedup")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

tmp = Path(tempfile.mkdtemp())


def stream_from(payloads, name):
    src = tmp / name
    src.mkdir()
    (src / "b.json").write_text(
        "\n".join(json.dumps({"value": json.dumps(p)}) for p in payloads), encoding="utf-8"
    )
    return spark.readStream.schema("value STRING").json(str(src))


def collect(stream, name):
    q = (
        stream.writeStream.format("memory")
        .queryName(name)
        .outputMode("append")
        .option("checkpointLocation", str(tmp / f"ck_{name}"))
        .start()
    )
    try:
        q.processAllAvailable()
    finally:
        q.stop()
    # Memory sink append cộng dồn qua các micro-batch; dedup đã chạy trước khi ghi.
    return spark.sql(f"SELECT * FROM {name}").collect()


ok = True

# ── Case 1: trùng khoá bị loại ────────────────────────────────────────────
payloads = [
    {"payload": {"transaction_id": "1", "__ts_ms": "1700000000000"}},
    {"payload": {"transaction_id": "1", "__ts_ms": "1700000001000"}},  # replay
    {"payload": {"transaction_id": "2", "__ts_ms": "1700000002000"}},
]
out = apply_watermark_and_dedup(
    stream_from(payloads, "case1"),
    {"watermark_delay": "10 minutes", "keys": ["transaction_id"]},
    target_primary_key="transaction_id",
)
rows = collect(out, "c1")
print(f"CASE1 dedup: in={len(payloads)} out={len(rows)} (mong đợi 2)")
ok &= len(rows) == 2

# ── Case 2: event time không NULL ─────────────────────────────────────────
out2 = apply_watermark_and_dedup(
    stream_from([{"payload": {"transaction_id": "9", "__ts_ms": "1700000000000"}}], "case2"),
    {"watermark_delay": "10 minutes", "keys": ["transaction_id"]},
)
rows2 = collect(out2, "c2")
ts_ok = bool(rows2) and rows2[0][DEFAULT_EVENT_TIME_COLUMN] is not None
print(f"CASE2 event_time: {rows2[0][DEFAULT_EVENT_TIME_COLUMN] if rows2 else 'EMPTY'}")
ok &= ts_ok

# ── Case 3: không khai dedup = hành vi cũ (giữ nguyên) ───────────────────
out3 = apply_watermark_and_dedup(
    stream_from(
        [
            {"payload": {"transaction_id": "1", "__ts_ms": "1700000000000"}},
            {"payload": {"transaction_id": "1", "__ts_ms": "1700000000000"}},
        ],
        "case3",
    ),
    None,
)
rows3 = collect(out3, "c3")
print(f"CASE3 no-dedup: out={len(rows3)} (mong đợi 2, giữ nguyên)")
ok &= len(rows3) == 2

print("\nRESULT:", "ALL PASS" if ok else "FAILED")
spark.stop()
sys.exit(0 if ok else 1)
