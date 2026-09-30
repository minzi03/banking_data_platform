# ADR-0017 — Một định nghĩa `cob_dt` cho mọi DAG: ngày ICT của `data_interval_start`

**Status**: Accepted — cài đặt 2026-09-30, kiểm bằng Airflow 2.10.0 (chưa chạy trên stack)
**Ngày**: 2026-09-30
**Liên quan**: [`0004`](0004-business-date-under-utc-session.md) · [`0007`](0007-overwrite-partitions-by-cob-dt.md) · `airflow/plugins/cob_dt.py`

---

## Context

Mọi DAG dùng `cob_dt = "{{ ds }}"` và nối nhau bằng cờ `flag_job_etl` **cho cùng
`cob_dt`** (Silver chờ cờ Bronze, dbt chờ `GOLD_COMPLETE`, DQ/PII/contract chờ Gold).

`ds` là ngày của logical_date tính theo **UTC**, còn lịch đặt theo giờ ICT
(`start_date` tz `Asia/Ho_Chi_Minh`). Đã render bằng Airflow 2.10.0 thật cho các lần
chạy ngày 2026-09-30:

```text
cron 0 2 / 0 4 / 0 6 * * *   (Bronze, Silver, Gold)             ds = 2026-09-28
cron 0 7 / 0 8 / 0 9 * * *   (dbt, DQ, PII, lineage, contract…) ds = 2026-09-29
```

07:00 ICT = 00:00 UTC, nên DAG chạy từ 07:00 chờ cờ Gold của một ngày chưa chạy
và hết giờ mỗi ngày. Mọi evidence trước đó đến từ trigger tay — khi đó mọi DAG có
cùng `ds` — nên lỗi không lộ ra. Test `test_waiter_runs_daily_after_what_it_waits_for`
chỉ so giờ địa phương.

## Decision

`airflow/plugins/cob_dt.py` là định nghĩa duy nhất:

```text
cob_dt = dag_run.conf["cob_dt"]                              nếu có (chạy tay, backfill)
       = data_interval_start theo Asia/Ho_Chi_Minh, dạng ngày  nếu không
```

Với lịch hằng ngày, `data_interval_start` là cùng giờ của ngày hôm trước, nên mọi DAG
chạy trong một ngày nhận **cùng** `cob_dt` = ngày làm việc vừa kết thúc. Template được
render lúc task chạy; `etl_flag` và mọi DAG import `COB_DT` từ đây, PythonOperator dùng
`cob_dt_from_context`.

## Consequences

- Render lại bằng Airflow 2.10.0: 13/13 DAG có lịch hằng ngày ra `2026-09-29` cho ngày chạy
  2026-09-30. `--conf '{"cob_dt": "2026-09-15"}'` ghi đè cho mọi DAG.
- `cob_dt` của batch là D-1 so với ngày chạy (trước đây Bronze/Silver/Gold là D-2).
  Seed với `--as-of` = cob_dt định nạp (TD-16).
- Trigger tay **không** kèm `conf` trong khoảng giữa hai giờ chạy (vd 06:30) vẫn có thể
  cho hai DAG hai ngày khác nhau, vì Airflow suy interval của lần chạy tay theo lịch
  riêng của từng DAG. Quy ước: chạy tay luôn truyền `conf.cob_dt` (DEMO_GUIDE, RUNBOOK).
- Test tĩnh chặn quay lại: không DAG nào được dùng `{{ ds }}`, `ds_nodash` hay
  `context["ds"]` (`TestSingleCobDtDefinition`).
