"""
Thêm giao dịch của MỘT ngày nghiệp vụ vào nguồn — dữ liệu cho lượt chạy hằng ngày (ADR-0018).

`generate_all.py` sinh cả lịch sử một lần, neo giao dịch mới nhất vào --as-of. Để chạy "ngày
thứ hai" cần giao dịch của ngày kế tiếp. Script này sao chép giao dịch của ngày D-7 (cùng thứ
trong tuần — giữ phân phối giờ, kênh, số tiền) sang ngày D cho ba bảng giao dịch:

- id mới = max(id) hiện có + thứ tự trong ngày nguồn;
- các cột thời điểm sự kiện dời +7 ngày; last_updated = now();
- card_txn.reference_number sinh lại theo id mới (CDN + 10 chữ số, như generator);
- balance_after giữ nguyên giá trị nguồn (không mô phỏng lại số dư — đủ cho đo tải).

Từ chối khi ngày D đã có giao dịch (không nhân đôi). Ngày nghiệp vụ theo giờ ICT; cột lưu UTC
(ADR-0004).

    python data_generator/append_day.py --day 2026-09-30 --host localhost
    (user / password / db đọc từ POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB)
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta

logger = logging.getLogger("append_day")

# bảng → (cột id, cột thời điểm sự kiện, cột thời điểm dời theo sự kiện)
TABLES = {
    "core_banking.txn_account": ("txn_id", "txn_date", ("txn_date", "created_ts")),
    "card_crm.card_txn": ("txn_id", "txn_date", ("txn_date", "created_ts")),
    "digital_banking.online_transaction": ("transaction_id", "transaction_date", ("transaction_date", "created_ts")),
}
ICT_DATE = "(timezone('Asia/Ho_Chi_Minh', timezone('UTC', {col})))::date"


def build_insert_sql(table: str, columns: list[str], day: date, lag_days: int = 7) -> str:
    """INSERT … SELECT sao chép ngày (day - lag_days) sang day. Thuần chuỗi — test được không cần DB."""
    id_col, event_col, shifted = TABLES[table]
    source_day = day - timedelta(days=lag_days)
    new_id = f"(SELECT COALESCE(MAX({id_col}), 0) FROM {table}) + ROW_NUMBER() OVER (ORDER BY {id_col})"
    exprs = []
    for col in columns:
        if col == id_col:
            exprs.append(f"{new_id} AS {col}")
        elif col in shifted:
            exprs.append(f"{col} + INTERVAL '{lag_days} days' AS {col}")
        elif col == "last_updated":
            exprs.append("NOW() AS last_updated")
        elif col == "reference_number":
            exprs.append(f"'CDN' || LPAD(({new_id})::text, 10, '0') AS reference_number")
        else:
            exprs.append(col)
    select = ",\n       ".join(exprs)
    where = f"{ICT_DATE.format(col=event_col)} = DATE '{source_day.isoformat()}'"
    return f"INSERT INTO {table} ({', '.join(columns)})\nSELECT {select}\nFROM {table}\nWHERE {where}"


def day_count_sql(table: str, day: date) -> str:
    _, event_col, _ = TABLES[table]
    return f"SELECT COUNT(*) FROM {table} WHERE {ICT_DATE.format(col=event_col)} = DATE '{day.isoformat()}'"


def main() -> None:
    import psycopg2

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--day", required=True, help="Ngày nghiệp vụ cần thêm (YYYY-MM-DD)")
    parser.add_argument("--lag-days", type=int, default=7, help="Sao chép từ ngày day - lag (mặc định 7)")
    parser.add_argument("--host", default=os.environ.get("POSTGRES_HOST", "postgres"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("POSTGRES_PORT", 5432)))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    day = date.fromisoformat(args.day)

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=os.environ.get("POSTGRES_DB", "banking_db"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with conn, conn.cursor() as cur:
            for table in TABLES:
                cur.execute(day_count_sql(table, day))
                existing = cur.fetchone()[0]
                if existing:
                    raise SystemExit(f"{table} đã có {existing} giao dịch ngày {day} — không thêm lần nữa")
            for table in TABLES:
                schema, name = table.split(".")
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                    (schema, name),
                )
                columns = [r[0] for r in cur.fetchall()]
                cur.execute(build_insert_sql(table, columns, day, args.lag_days))
                logger.info(
                    "%s: +%s giao dịch ngày %s (từ %s)", table, cur.rowcount, day, day - timedelta(days=args.lag_days)
                )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
