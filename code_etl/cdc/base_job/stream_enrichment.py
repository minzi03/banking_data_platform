"""
Static Dimension Join for CDC Streaming (foreachBatch)
======================================================

Khoá new2 §4.3: "Join: static join dimension từ Silver trong foreachBatch, làm
mới theo từng micro-batch."

Ý tưởng: CDC event thô chỉ mang khoá ngoại (account_id, customer_id…). Để phục
vụ đọc gần thời gian thực, ta cần thuộc tính dimension (branch, segment, mcc…).
Dimension nằm ở Silver (Iceberg), ít đổi — nên ĐỌC LẠI mỗi micro-batch (không
cache vĩnh viễn): dimension có thể vừa được Silver cập nhật giữa hai batch.

    Kafka CDC → foreachBatch → validate → (Bronze raw)
                                          └→ join Silver dim (refresh/batch)
                                              → ghi bảng enriched riêng

QUAN TRỌNG — giữ Bronze RAW: kết quả join KHÔNG ghi vào bảng Bronze CDC (Bronze
phải khớp schema bảng nguồn, nếu thêm cột dimension thì `writeTo(...).append()`
sẽ vỡ, và consolidation đọc Bronze sẽ thấy cột lạ). Enrichment ghi vào một bảng
đích RIÊNG (silver.stream_enriched_*).

LEFT JOIN: giữ nguyên số dòng. Dimension thiếu → cột dimension NULL, KHÔNG mất
event. Đây là enrichment best-effort, không phải lọc.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def enrich_batch(spark: SparkSession, df: DataFrame, joins: list[dict], batch_id: int) -> DataFrame:
    """Join df với các dimension tĩnh (đọc lại mỗi lần gọi = mỗi micro-batch).

    joins: list các dict {dim_table, on, columns, prefix}
        dim_table : bảng Silver (vd lakehouse.silver.dim_branch) — đọc lại mỗi batch
        on        : list cặp [event_col, dim_col] để join
        columns   : list cột dimension cần mang sang (đổi tên bằng prefix để tránh trùng)
        prefix    : tiền tố cột dimension (mặc định rỗng)
    """
    out = df
    for j in joins:
        dim = spark.table(j["dim_table"])  # đọc lại mỗi micro-batch → refresh
        on_pairs = j["on"]
        prefix = j.get("prefix", "")

        # Rename khoá join phía dim sang tên tạm để điều kiện join không nhập nhằng
        # với cột cùng tên phía event (account_id xuất hiện ở cả hai phía).
        dim_key_aliases = {right_col: f"__dimkey_{right_col}" for _, right_col in on_pairs}
        dim_proj = dim
        for right_col, alias in dim_key_aliases.items():
            dim_proj = dim_proj.withColumnRenamed(right_col, alias)
        dim_proj = dim_proj.select(
            *[dim_proj[c].alias(f"{prefix}{c}") for c in j["columns"]],
            *[dim_proj[alias] for alias in dim_key_aliases.values()],
        )

        condition = None
        for left_col, right_col in on_pairs:
            eq = out[left_col] == dim_proj[dim_key_aliases[right_col]]
            condition = eq if condition is None else (condition & eq)

        out = out.join(dim_proj, condition, "left")
        # Bỏ khoá join tạm — cột dimension thật đã mang sang với prefix.
        out = out.drop(*dim_key_aliases.values())
    return out


def write_enrichment(spark: SparkSession, df: DataFrame, target: str, batch_id: int) -> int:
    """Ghi kết quả enriched vào bảng đích. Tạo bảng từ schema nếu chưa có."""
    if df.isEmpty():
        return 0
    rows = df.count()
    # Bảng đích do enrichment sở hữu — tạo từ schema kết quả ở lần đầu.
    tables = spark.sql("SHOW TABLES IN silver").filter("tableName LIKE 'stream_enriched_%'").collect()
    exists = any(r["tableName"] == target.split(".")[-1] for r in tables)
    if not exists:
        df.writeTo(target).using("iceberg").create()
    else:
        df.writeTo(target).append()
    print(f"[Batch {batch_id}] Enriched {rows} rows → {target}")
    return rows
