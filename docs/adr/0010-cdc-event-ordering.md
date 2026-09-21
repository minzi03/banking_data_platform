# ADR-0010 — Thứ tự sự kiện CDC dùng `(timestamp_ms, batch_id)`, không dùng Kafka offset

**Status**: Accepted
**Ngày**: 2026-09 · chuyển thể thành ADR 2026-09-22
**Liên quan**: [`0007`](0007-overwrite-partitions-by-cob-dt.md)

---

## Context

Luồng CDC: PostgreSQL WAL → Debezium → Kafka → Spark Structured Streaming → Bronze CDC (append-only) → consolidation → Silver Current.

Bước consolidation phải trả lời một câu: **trong nhiều sự kiện của cùng một khoá, cái nào mới nhất?** Trả lời sai thì một bản UPDATE cũ ghi đè bản mới, và Silver Current mang giá trị sai mà không có gì báo.

Cách chuẩn mực là dùng **Kafka offset**: nó đơn điệu tăng trong một partition, nên so sánh offset cho thứ tự chính xác tuyệt đối.

Nhưng bảng Bronze CDC **không lưu toạ độ Kafka**. Schema chỉ có:

```sql
__cdc_operation    VARCHAR(10)
__cdc_timestamp    TIMESTAMP
__cdc_timestamp_ms BIGINT
```

Không có `kafka_partition`, không có `kafka_offset`.

## Decision

Thứ tự sự kiện xác định bằng cặp **`(__cdc_timestamp_ms, __spark_batch_id)`**.

`__cdc_timestamp_ms` là thời điểm Debezium ghi nhận thay đổi. Khi hai sự kiện trùng millisecond, `__spark_batch_id` phân định — batch sau chắc chắn xử lý sự kiện đến sau.

Consolidation dùng MERGE (upsert/delete) nên **idempotent**: chạy lại cho cùng kết quả.

### Bất đối xứng với DLQ — và trạng thái thật của nó

DLQ **có** lưu toạ độ Kafka:

```sql
-- code_etl/cdc/base_job/cdc_dlq.py
kafka_partition  INT
kafka_offset     BIGINT
kafka_timestamp  TIMESTAMP
```

Bất đối xứng này **có lý do chính đáng**: DLQ tồn tại để điều tra và replay **một message cụ thể**, nên cần toạ độ chính xác. Bronze CDC là một luồng được xử lý theo lô, replay ở mức lô chứ không ở mức message.

Nhưng cần nói thẳng: docstring của `cdc_consolidation.py` ghi đây là **limitation**, không phải design:

> ```
> Limitations:
>     - Event ordering uses __cdc_timestamp_ms + __spark_batch_id (not Kafka offset)
>     - Kafka metadata not persisted in Bronze CDC yet
> ```

Chữ **"yet"** quan trọng. Đây là **khoảng trống đã chấp nhận**, không phải lựa chọn đã cân nhắc rồi chốt. ADR này ghi lại trạng thái thật chứ không hợp lý hoá ngược.

## Consequences

**Được**

- Consolidation chạy được mà không cần đổi schema Bronze CDC.
- MERGE idempotent, chạy lại an toàn.
- DLQ vẫn giữ đủ toạ độ để điều tra từng message hỏng.

**Mất**

- **Thứ tự phụ thuộc đồng hồ nguồn.** `__cdc_timestamp_ms` do Debezium gán theo đồng hồ Postgres. Đồng hồ nhảy lùi hoặc lệch giữa các node sẽ làm thứ tự sai — Kafka offset không có vấn đề này.
- **Độ phân giải millisecond có thể không đủ.** Hai UPDATE cùng khoá trong cùng millisecond phải nhờ `__spark_batch_id` phân định; nếu chúng rơi vào **cùng một batch**, thứ tự trong batch không được bảo đảm.
- **Không truy ngược được về Kafka.** Một dòng Bronze CDC đáng ngờ không chỉ ra được message gốc. Điều tra phải dựa vào timestamp, không dựa vào offset.
- Không có test nào chứng minh thứ tự đúng dưới điều kiện out-of-order. `reconcile_cdc.py` đối chiếu **số lượng** giữa ba tầng, không đối chiếu **thứ tự**.

## Evidence

```text
code_etl/cdc/consolidation/cdc_consolidation.py     docstring nêu rõ limitation
code_etl/cdc/create_cdc_tables.py                   schema Bronze CDC, không có cột Kafka
code_etl/cdc/base_job/cdc_dlq.py:36-38              DLQ có kafka_partition/offset/timestamp
code_etl/cdc/reconcile_cdc.py                       gate read-only, exit != 0 khi invariant vỡ
lakehouse.meta.cdc_watermark                        last_cdc_timestamp_ms + last_spark_batch_id
```

**Nếu xem lại quyết định này**: thêm `kafka_partition`/`kafka_offset` vào Bronze CDC và chuyển thứ tự sang offset sẽ loại bỏ cả ba nhược điểm đầu. Chi phí là một lần schema evolution trên bảng append-only — rẻ hơn nhiều so với sửa sau khi đã có sự cố thứ tự.
