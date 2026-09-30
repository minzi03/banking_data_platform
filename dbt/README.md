# dbt — Current-Serving Layer (Banking Data Platform)

dbt ở đây **không** biến đổi dữ liệu phân tích. Spark sở hữu Bronze → Silver →
Gold lịch sử (`lakehouse.gold.*`, partition theo `cob_dt`). dbt, chạy qua Trino,
chỉ **xuất bản** lát cắt hiện hành của Gold cho **một `cob_dt` được chỉ định**
thành bảng `iceberg.serving.*_current`, rồi kiểm tra nó.

```text
lakehouse.gold.<model>  (Spark, nhiều cob_dt)
        │  dbt build --select serving --vars '{"cob_dt": "YYYY-MM-DD"}'
        ▼
iceberg.serving.<model>_current  (dbt/Trino, đúng một cob_dt, 1 dòng/khoá)
```

Tài liệu này thay thế `dbt/SUMMARY.md` và `docs/04-operations/DBT_DEPLOYMENT.md`
(mô tả "12 ephemeral semantic models" đã lỗi thời).

## Thành phần (đo bằng `dbt parse`, dbt-core 1.12.0 + dbt-trino 1.9.0)

| Loại | Số | Ở đâu |
|---|---:|---|
| Model serving (`materialized: table`) | 16 | `models/serving/*.sql` |
| Model semantic (time spine MetricFlow) | 1 | `models/semantic/metricflow_time_spine.sql` |
| Data test (generic + singular) | 137 | `models/serving/_serving_models.yml`, `tests/*.sql` |
| Source (bảng Gold) | 15 | `models/gold/_gold_sources.yml` |
| Semantic model / metric (MetricFlow) | 2 / 13 | `models/semantic/_semantic_models.yml` |

## Vì sao `table`, không phải view

Iceberg REST catalog của Trino không hỗ trợ `createView` (ADR-0003). Vì vậy serving
chỉ tươi khi được build lại: `dbt_serving_publish` chạy mỗi ngày sau `GOLD_COMPLETE`
của cùng `cob_dt`.

## Vì sao `cob_dt` tường minh

Mỗi model lọc `WHERE cob_dt = date '{{ var("cob_dt") }}'`, không dùng `MAX(cob_dt)`.
`MAX` sẽ âm thầm phục vụ dữ liệu hôm qua khi pipeline hôm nay hỏng. Thiếu var →
sentinel `1900-01-01` → bảng rỗng → `assert_serving_snapshot_alignment` FAIL.

## Chạy

Trong stack (container `banking-dbt`, user Trino `dbt`, HTTPS + mật khẩu từ
`secrets/trino/env/dbt.env`):

```bash
docker exec banking-dbt sh -lc "cd /usr/src/dbt && dbt deps && \
  dbt build --target docker --select serving --vars '{\"cob_dt\": \"2026-09-29\"}'"
```

Qua Airflow: `dbt_serving_publish` (07:00; chờ `GOLD_COMPLETE(cob_dt)`, `dbt build`,
rồi ghi `SERVING_COMPLETE(cob_dt)`). Chạy tay cho một ngày cụ thể:

```bash
docker exec banking-airflow-scheduler airflow dags trigger dbt_serving_publish \
  --conf '{"cob_dt": "2026-09-29"}'
```

Kiểm tra tĩnh (không cần Trino): `dbt parse --target docker`.

## Lưu ý

- `dbt build` dựng model trước rồi mới test: nếu test fail, bảng serving **đã bị
  thay** nhưng `SERVING_COMPLETE` không được ghi. Consumer nên dựa vào cờ đó.
- `serving` và `semantic` đều ghi vào schema `serving` vì user `dbt` chỉ được tạo
  bảng ở đó (ADR-0016, `governance/rbac.py`).
