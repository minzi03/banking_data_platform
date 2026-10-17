"""
Event-time watermark + deduplication cho CDC streaming (khoá new2 §4.3).
=========================================================================

Đề bài (Buổi 10, §4.3 "Đúng đắn dữ liệu"):
    "Streaming dùng event time + watermark, chống trùng giao dịch"

Trước module này, luồng CDC là:

    Kafka → foreachBatch → validate → append Bronze

Không có watermark, không dedup. Hệ quả:

  1. **Trùng khi replay.** Debezium bảo đảm at-least-once. Một event phát lại
     (sau rebalance, sau khi job restart từ offset cũ, hoặc producer retry) sẽ
     vào Bronze **hai lần**. Vì Bronze là append-only, số dòng sai vĩnh viễn.
  2. **Không có khái niệm event time.** Mọi aggregate downstream
     (doanh thu theo phút, giờ cao điểm) phải tự suy thời điểm từ
     `__ingestion_time` — tức giờ máy Spark, không phải giờ giao dịch xảy ra.
     Một event đến trễ 5 phút bị tính vào phút SAI.

## Vì sao `dropDuplicatesWithinWatermark` (Spark 3.5+) chứ không `dropDuplicates`

`dropDuplicates(["transaction_id"])` trên stream sẽ **giữ khoá vĩnh viễn** để
dedup — state tăng mãi, job chết sau vài ngày với bộ nhớ đầy. Đây là lỗi kinh
điển khi làm streaming.

`dropDuplicatesWithinWatermark` chỉ nhớ khoá trong phạm vi watermark: event đến
trễ hơn watermark bị coi là "quá cũ", không dedup nữa nhưng cũng không còn
được xử lý như dữ liệu đúng giờ. State bị chặn trên bởi độ trễ watermark.

## Thứ tự áp dụng (quan trọng)

    stream_df
      .withWatermark(event_time, delay)        # 1. khai event time + độ trễ
      .dropDuplicatesWithinWatermark([pk])     # 2. dedup trong phạm vi watermark
      .writeStream.foreachBatch(...)           # 3. ghi

Watermark PHẢI đặt trước dedup. Nếu đảo, watermark không có tác dụng chặn state
của dedup và job vẫn phình bộ nhớ.

## Cấu hình (tuỳ chọn trong YAML)

    dedup:
      event_time_column: __cdc_ts                 # cột event time (micro-giây → ts)
      watermark_delay: "10 minutes"               # độ trễ chấp nhận
      keys: [transaction_id]                      # khoá nghiệp vụ để chống trùng

Không khai `dedup` → giữ nguyên hành vi cũ (không watermark, không dedup),
để các topic không phải giao dịch (ví dụ `core_customer`) không bị đổi ngữ nghĩa.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Debezium `ExtractNewRecordState` đặt timestamp sự kiện ở `__ts_ms`
# (milisecond, UTC). Tên cột này do `cdc_dlq.validate_and_split` sinh ra.
DEFAULT_EVENT_TIME_COLUMN = "__cdc_ts"


def apply_watermark_and_dedup(stream_df: DataFrame, dedup_cfg: dict | None, target_primary_key: str | None = None) -> DataFrame:
    """
    Áp watermark + dedup lên stream DataFrame trước khi `writeStream`.

    Args:
        stream_df: stream DataFrame đọc từ Kafka (có cột `value`).
        dedup_cfg: khối `dedup` trong YAML. None/rỗng → trả nguyên df (hành vi cũ).
        target_primary_key: `target.primary_key` — dùng làm khoá mặc định nếu
            `dedup.keys` không khai.

    Returns:
        DataFrame đã áp watermark (+ dedup nếu có khoá).

    Raises:
        ValueError: cấu hình sai (thiếu khoá, watermark_delay không hợp lệ).
    """
    if not dedup_cfg:
        return stream_df

    delay = str(dedup_cfg.get("watermark_delay", "")).strip()
    if not delay:
        raise ValueError(
            "dedup.watermark_delay là bắt buộc khi bật dedup (ví dụ '10 minutes'). "
            "Không có nó, state dedup KHÔNG bị chặn và job sẽ chết vì hết bộ nhớ."
        )

    keys = dedup_cfg.get("keys") or ([target_primary_key] if target_primary_key else [])
    if not keys:
        raise ValueError(
            "dedup.keys rỗng và target.primary_key cũng không khai — không có khoá "
            "nào để chống trùng."
        )

    # Tombstone (value NULL) theo sau mỗi delete để log compaction — không phải
    # event nghiệp vụ. Debezium đã gửi delete thật ở bản ghi `__op=d` trước đó.
    # Lọc TRƯỚC watermark: dòng NULL không có event time, để lại chỉ làm
    # watermark bỏ qua ngầm định (khó đoán) và làm nhiễu phép dedup.
    stream_df = stream_df.filter(F.col("value").isNotNull())

    # `__ts_ms` nằm trong payload JSON của Debezium (milisecond, UTC).
    # Ở đây stream_df vẫn là Kafka frame thô → trích từ `value`.
    ts_ms = F.get_json_object(F.col("value").cast("string"), "$.payload.__ts_ms")
    df = stream_df.withColumn(
        DEFAULT_EVENT_TIME_COLUMN,
        F.to_timestamp(F.from_unixtime(ts_ms.cast("double") / 1000)),
    )

    df = df.withWatermark(DEFAULT_EVENT_TIME_COLUMN, delay)

    # Khoá nằm trong payload JSON của Kafka frame thô → trích ra cột tạm rồi
    # dedup trên cột đó (Kafka frame không có cột tên khoá nghiệp vụ).
    key_cols = []
    for key in keys:
        col_name = f"__dedup_{key}"
        df = df.withColumn(col_name, F.get_json_object(F.col("value").cast("string"), f"$.payload.{key}"))
        key_cols.append(col_name)

    return df.dropDuplicatesWithinWatermark(key_cols)


def dedup_config_problems(dedup_cfg: dict | None) -> list[str]:
    """
    Kiểm tra cấu hình dedup mà KHÔNG cần Spark — dùng cho test static/CI.

    Trả danh sách lỗi (rỗng = hợp lệ).
    """
    if dedup_cfg is None:
        return []
    problems: list[str] = []
    if not isinstance(dedup_cfg, dict):
        return ["dedup phải là mapping"]

    delay = str(dedup_cfg.get("watermark_delay", "")).strip()
    if not delay:
        problems.append("thiếu dedup.watermark_delay")
    elif not _looks_like_duration(delay):
        problems.append(f"dedup.watermark_delay không đúng định dạng interval: {delay!r}")

    keys = dedup_cfg.get("keys")
    if keys is not None and (not isinstance(keys, list) or not keys):
        problems.append("dedup.keys phải là danh sách không rỗng")

    return problems


def _looks_like_duration(value: str) -> bool:
    """`10 minutes`, `30 seconds`, `1 hour` …"""
    parts = value.strip().split()
    if len(parts) != 2:
        return False
    number, unit = parts
    if not number.isdigit():
        return False
    return unit.rstrip("s") in {"second", "minute", "hour", "day"}
