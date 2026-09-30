"""
cob_dt dùng chung cho MỌI DAG — một định nghĩa, một múi giờ.

Vì sao không dùng `{{ ds }}`:
    `ds` là ngày của logical_date tính theo **UTC**, còn lịch chạy của các DAG đặt
    theo giờ ICT (start_date tz=Asia/Ho_Chi_Minh). 07:00 ICT = 00:00 UTC, nên DAG
    chạy trước 07:00 (bronze 02:00, silver 04:00, gold 06:00) nhận ds = D-2, còn
    DAG chạy từ 07:00 (dbt 07:00, DQ/PII/lineage 08:00, contract/quarantine 09:00)
    nhận ds = D-1. Đo bằng Airflow 2.10.0: cùng ngày 2026-09-30, cron `0 6 * * *`
    render ds=2026-09-28, cron `0 7 * * *` render ds=2026-09-29. Hệ quả: dbt chờ
    GOLD_COMPLETE của một ngày Gold chưa chạy → sensor timeout mỗi ngày.

Định nghĩa:
    cob_dt = ngày ICT của data_interval_start
           = ngày làm việc vừa kết thúc, GIỐNG NHAU cho mọi DAG hằng ngày chạy
             trong cùng một ngày (bất kể giờ chạy).
    dag_run.conf["cob_dt"] ghi đè — dùng cho chạy tay / backfill một ngày cụ thể:
        airflow dags trigger gold_all_dag --conf '{"cob_dt": "2026-09-29"}'
"""

from __future__ import annotations

from datetime import date

BUSINESS_TIMEZONE = "Asia/Ho_Chi_Minh"

# Template Jinja, render lúc task chạy (không truy cập DB lúc parse DAG).
COB_DT = (
    "{{ dag_run.conf['cob_dt'] if (dag_run and dag_run.conf and dag_run.conf.get('cob_dt')) "
    "else data_interval_start.in_timezone('" + BUSINESS_TIMEZONE + "').strftime('%Y-%m-%d') }}"
)


# Lần nạp đầu cho bảng incremental (ADR-0018): trigger với conf {"backfill_from": "1900-01-01"}.
# ds_format parse giá trị như một ngày — chuỗi khác làm task fail thay vì lọt vào bash_command.
# Chỉ gắn vào lệnh của job incremental; job full_snapshot từ chối --backfill_from.
BACKFILL_FROM_ARG = (
    "{{ ('--backfill_from ' ~ macros.ds_format(dag_run.conf['backfill_from'], '%Y-%m-%d', '%Y-%m-%d')) "
    "if (dag_run and dag_run.conf and dag_run.conf.get('backfill_from')) else '' }}"
)


def cob_dt_from_context(context) -> str:
    """Cùng quy tắc với COB_DT cho PythonOperator (nhận **context)."""
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    if conf.get("cob_dt"):
        return date.fromisoformat(str(conf["cob_dt"])).isoformat()
    return context["data_interval_start"].in_timezone(BUSINESS_TIMEZONE).strftime("%Y-%m-%d")
