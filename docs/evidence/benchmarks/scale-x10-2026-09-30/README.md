# Benchmark ×1 vs ×10 — batch pipeline (2026-09-30)

Câu hỏi: pipeline batch (Bronze → Silver → Gold → dbt) chịu dữ liệu gấp 10 lần thế nào, và
chi phí nằm ở đâu. Số dưới đây là đo, không ước lượng; số liệu gốc nằm cạnh file này.

## Điều kiện

| | ×1 | ×10 |
|---|---:|---:|
| Seed | `generate_all.py --as-of 2026-09-29` | `… --scale 10` |
| Dòng nguồn | 2.673.629 | 26.697.391 |
| Giao dịch (tài khoản + thẻ + online) | 2.300.000 | 23.000.000 |
| Thời gian seed | 180 s | 4.617 s |

- Cùng stack local (Docker Desktop 29.8.1, 23,5 GiB cho VM): Spark worker 8 core / 8 GiB,
  Airflow LocalExecutor, mọi job `docker exec … spark-submit` vào `spark-worker-1`.
- Cả hai lượt là **lần nạp đầu vào lakehouse trống** (`cob_dt` 2026-09-29), lượt ×1 chạy sau
  khi xoá toàn bộ bảng Iceberg và chạy lại DDL khởi tạo. CDC tắt trong cả hai lượt.
- Thời gian = khoảng từ task xử lý đầu tiên đến task xử lý cuối của mỗi DAG, **không tính
  sensor chờ cờ** (`scripts/benchmark/airflow_durations.py`). Dung lượng = byte file dữ liệu
  Iceberg của partition `cob_dt` đó (`scripts/benchmark/storage_stats.py`).

So lại: `py -3 scripts/benchmark/compare.py docs/evidence/benchmarks/scale-x10-2026-09-30`

## Kết quả

| DAG | ×1 | ×10 | Tỷ lệ |
|---|---:|---:|---:|
| bronze_core_banking | 252 s | 319 s | 1,3 |
| bronze_card_crm | 72 s | 153 s | 2,1 |
| bronze_digital_banking | 144 s | 205 s | 1,4 |
| silver_all | 329 s | 421 s | 1,3 |
| gold_all | 459 s | 834 s | 1,8 |
| dbt_serving_publish | 90 s | 107 s | 1,2 |
| **Tổng** | **1.345 s** | **2.039 s** | **1,5** |

| Tầng | Dòng ×1 → ×10 | Dung lượng ×1 → ×10 |
|---|---:|---:|
| Bronze | 2,67 M → 26,7 M | 73 MB → 770 MB |
| Silver | 2,66 M → 26,5 M | 80 MB → **1.935 MB** |
| Gold | 2,51 M → 25,1 M | 53 MB → 540 MB |

Task chậm nhất ở ×10: `aml_monitoring` 173 s (×1: 47 s), `fraud_risk_txn` 118 s (37 s),
`ingest_core_txn_account` 87 s cho 12 M dòng (28 s).

## Đọc kết quả

1. **Chi phí cố định chiếm phần lớn ở ×1.** Dữ liệu ×10 chỉ làm tổng thời gian ×1,5. Mỗi task
   là một `spark-submit` riêng (JVM, SparkSession, catalog) — khoảng 50 task, vài chục giây
   khởi động mỗi task. Gom các job cùng tầng vào ít Spark application hơn sẽ giảm trực tiếp phần này.
2. **Silver phình ở ×10, không phình ở ×1.** `fact_txn_account`: ×1 33,9 B/dòng (Bronze 30,5);
   ×10 99,1 B/dòng (Bronze 32,6). Đo từng cột ở ×1 (`scripts/benchmark/column_sizes.py`): hai
   surrogate key hex SHA-256 64 ký tự chỉ tốn 2,8 + 2,0 B/dòng — nén dictionary tốt vì mỗi
   snapshot có 30 K tài khoản. Giả thuyết cho ×10: 300 K key × 64 B ≈ 19 MB vượt giới hạn
   dictionary page của Parquet, rơi về plain encoding, chuỗi ngẫu nhiên không nén được.
   **Chưa kiểm ở mức cột cho ×10** (dữ liệu ×10 đã xoá trước khi có script); cần chạy lại.
3. **Fact là full snapshot mỗi ngày.** Bronze đọc lại toàn bộ bảng giao dịch nguồn mỗi `cob_dt`
   (`load.strategy: full_snapshot`), Silver ghi lại toàn bộ. Ở ×10, mỗi ngày đọc/ghi 23 M giao
   dịch dù ngày đó chỉ phát sinh vài chục nghìn. Dung lượng lịch sử tăng theo số ngày × toàn
   bộ lịch sử.

## Hướng tối ưu (theo thứ tự tác động)

| # | Thay đổi | Đo lại bằng |
|---|---|---|
| 1 | Fact nạp tăng dần theo ngày nghiệp vụ; KPI 30/90 ngày đọc khoảng partition | thời gian Bronze/Silver ngày thứ hai, dung lượng tăng mỗi ngày |
| 2 | Surrogate key dạng số (vd `xxhash64`) thay hex SHA-256, hoặc tăng dictionary page size | byte/dòng của fact ở ×10 |
| 3 | Gom job Silver/Gold vào ít Spark application | tổng thời gian ×1 |

## Không đo ở đây

- CDC (Debezium/Kafka/streaming) — tắt trong benchmark; ở ×10 riêng Kafka cần ~30 GB đĩa.
- Trino/API dưới tải; dbt chỉ publish bảng serving của một `cob_dt`.
