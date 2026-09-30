# ADR-0010 — Thứ tự sự kiện CDC dùng `(timestamp_ms, batch_id)`, không dùng Kafka offset

**Status**: Superseded một phần (2026-09-30) — Bronze CDC giờ lưu `__kafka_partition` / `__kafka_offset`; xem mục cuối
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
docker/init_iceberg/04_ddl_bronze_cdc.sql           schema Bronze CDC (bản sao create_cdc_tables.py đã xoá 2026-09-30)
code_etl/cdc/base_job/cdc_dlq.py:36-38              DLQ có kafka_partition/offset/timestamp
code_etl/cdc/reconcile_cdc.py                       gate read-only, exit != 0 khi invariant vỡ
lakehouse.meta.cdc_watermark                        last_snapshot_id (tiến độ) + last_cdc_timestamp_ms / last_spark_batch_id (quan sát)
```

**Nếu xem lại quyết định này**: thêm `kafka_partition`/`kafka_offset` vào Bronze CDC và chuyển thứ tự sang offset sẽ loại bỏ cả ba nhược điểm đầu. Chi phí là một lần schema evolution trên bảng append-only — rẻ hơn nhiều so với sửa sau khi đã có sự cố thứ tự.

## Drift found and fixed (2026-09-27)

The code had drifted from this decision without anyone noticing. From `d84b0e3`
(2026-09-08), `cdc_dlq.validate_and_split` added six Kafka provenance columns
(`source_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `raw_payload`,
`payload_hash`) to the **valid** path. Neither Bronze CDC DDL (`04_ddl_bronze_cdc.sql`,
`create_cdc_tables.py`) has those columns, which matches this ADR. So every non-empty
micro-batch failed at `writeTo().append()` with `INSERT_COLUMN_ARITY_MISMATCH`. The CDC
path never ran in CI, so nothing caught it. It surfaced while verifying CDC against the
rotated credentials:

```text
before  core_customer stream, batch 0   INSERT_COLUMN_ARITY_MISMATCH (too many data columns)
after   core_customer stream, batch 0   10,000 valid -> bronze.core_customer_cdc, 0 -> DLQ
        Bronze: 10,000 rows, 10,000 distinct customer_id, all SNAPSHOT operations
```

The valid path now carries exactly the table's columns again, and Kafka coordinates stay
in the DLQ only. `tests/bronze/test_cdc_bronze_schema.py` covers this in two parts:

- **Static check (unit CI):** every CDC config plus the 5 metadata columns equals the
  table in both DDLs.
- **Spark check (Gold Spark Regression job):** the real `validate_and_split` produces
  exactly those columns and sends a bad `__op` to the DLQ. All 6 configs fail on the
  old code.

Persisting offsets into Bronze is still the "if revisited" option above. It is a schema
change, not something to reintroduce into the write path unannounced.

## Cập nhật 2026-09-30 — tách tiến độ khỏi thứ tự

Quyết định về **thứ tự** giữ nguyên: `(__cdc_timestamp_ms, __spark_batch_id)`. Cái đổi
là **tiến độ**. Watermark cũ `(max ts, max batch)` dùng chính khoá thứ tự làm mốc đã
đọc, và đọc lại bảng mới nhất ở mỗi action, nên có hai lỗ hổng:

- event về Bronze muộn với `ts` nhỏ hơn watermark bị bỏ qua vĩnh viễn;
- event append giữa lúc MERGE và lúc tính `max` bị tính là đã xử lý mà chưa MERGE.

Tiến độ giờ là **snapshot Iceberg** của bảng Bronze CDC (append-only): mỗi lượt chốt một
snapshot cuối và đọc đúng các append trong `(watermark, end]` bằng incremental read của
Iceberg. MERGE thêm guard `s.ts >= t.ts`, nên event muộn hoặc replay không kéo trạng
thái lùi. Snapshot watermark bị expire → đọc lại toàn bộ tại `end` (an toàn nhờ guard).
Test: `tests/cdc/test_cdc_consolidation.py`. Chưa chạy trên stack.

## Cập nhật 2026-09-30 (runtime) — offset vào Bronze CDC

Chạy trên stack thật, nhược điểm thứ hai ở trên xảy ra ngay: `INSERT` rồi `UPDATE` cùng
khoá trong **một transaction** có cùng `__ts_ms` (Debezium gán theo ms xử lý) và rơi vào
cùng micro-batch, nên `row_number()` chọn ngẫu nhiên giữa hai bản.

```text
customer 990004  INSERT  ts_ms 1790742688429  batch 4  offset 30012
customer 990004  UPDATE  ts_ms 1790742688429  batch 4  offset 30013
```

Đã làm theo đúng "nếu xem lại" của ADR này:

- `cdc_dlq.validate_and_split` giữ `__kafka_partition` / `__kafka_offset` trong luồng hợp lệ
  (chỉ hai cột; topic / timestamp / raw_payload vẫn chỉ ở DLQ).
- DDL `04_ddl_bronze_cdc.sql` có hai cột đó (bản sao `create_cdc_tables.py`, không ai chạy, đã xoá cùng ngày). Bảng tạo trước được
  `ensure_kafka_coordinate_columns` thêm cột khi stream khởi động (dòng cũ mang NULL).
- `deduplicate_latest` xếp `ts_ms DESC, batch DESC, offset DESC NULLS LAST`. Debezium key =
  khoá chính → mọi event của một khoá cùng partition, nên offset phân định dứt khoát.
- Guard MERGE giữa các lượt vẫn so `ts` (`s.ts >= t.ts`).

Kiểm chứng: sau thay đổi, 990003 / 990004 / 990005 (INSERT + UPDATE cùng transaction) đều ra
bản `UPDATE` cuối trong Silver Current; test `TestDeduplicateLatest` (hai thứ tự đầu vào,
repartition 4) và `TestKafkaCoordinateColumns`.

Còn lại: dòng Bronze CDC ghi trước thay đổi không có offset; đồng hồ nguồn vẫn là khoá thứ tự
chính giữa các lượt.
