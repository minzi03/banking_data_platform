"""
Serving Sync — Iceberg serving → PostgreSQL real-time serving
============================================================

Context: ADR-0019. Tầng serving hiện ghi vào Iceberg (`lakehouse.serving.*`) và
được đọc qua Trino — đúng cho analytics lịch sử. Nghiệp vụ Merchant QR/POS cần
thêm backend PostgreSQL cho point-lookup độ trễ thấp (app/dashboard real-time).

Job này đọc từng bảng Iceberg serving, tạo bảng PostgreSQL tương ứng (lần chạy
đầu), rồi **upsert idempotent** bằng staging + `INSERT ... ON CONFLICT DO UPDATE`.
Chạy lại cùng `cob_dt` không tạo dòng trùng.

Vì sao upsert theo khoá (không phải TRUNCATE + INSERT):
  1. An toàn khi job chết giữa đường — không có cửa sổ bảng rỗng.
  2. DAG chạy mỗi cob_dt; hai lượt liên tiếp có thể chồng lấn cùng khoá.
  3. App đang đọc bảng trong lúc sync — TRUNCATE sẽ tạo khoảng trống đọc được.

Bảng PostgreSQL tạo tự động từ schema Iceberg (metadata-driven, ADR-0006):
không viết DDL tay cho 14 bảng, tránh drift schema giữa Iceberg và Postgres.

Usage:
    spark-submit --master spark://spark-master:7077 \
        code_etl/serving/sync_postgres.py --cob_dt 2025-12-31

    # Chỉ sync một số bảng:
    spark-submit ... --cob_dt 2025-12-31 --only mart_customer_360_current,rfm_segment_current

Về "partition theo ngày" (yêu cầu new2 §4.3): tầng serving ở đây là CURRENT
SNAPSHOT — mỗi bảng chỉ giữ lát cắt của cob_dt hiện tại (đọc WHERE cob_dt = …),
không tích luỹ lịch sử. Vì vậy partition theo ngày nằm ĐÚNG ở bảng Iceberg
nguồn (`lakehouse.serving.*`, khai PARTITIONED BY (cob_dt)); bảng PostgreSQL
không cần khai partition vì nó chỉ có một ngày. Bù lại, mỗi bảng Postgres được
tạo INDEX trên khoá nghiệp vụ để app point-lookup nhanh (xem ensure_target_table).
Nếu về sau muốn tích luỹ lịch sử trong Postgres thì mới cần PARTITION BY RANGE
(cob_dt) — khi đó viết ADR mới, không sửa ADR-0019 đã Accepted.
"""

from __future__ import annotations

import argparse
import os
import sys
from logging import INFO, basicConfig, getLogger
from pathlib import Path

import yaml

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent / "shared"))

from spark.spark_session import get_spark_session  # noqa: E402

basicConfig(
    level=INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = getLogger("serving_sync")

CONFIG_FILE = _HERE / "config" / "serving_sync.yml"
JDBC_URL = "jdbc:postgresql://postgres:5432/banking_db"
SOURCE_PREFIX = "lakehouse.serving"

# Map Spark SQL type → Postgres type. Service serving chỉ dùng kiểu đơn giản;
# kiểu lạ (array/map/struct) không map được → raise để fail loud, không đoán.
SPARK_TO_PG = {
    "StringType": "TEXT",
    "IntegerType": "INTEGER",
    "LongType": "BIGINT",
    "ShortType": "SMALLINT",
    "DoubleType": "DOUBLE PRECISION",
    "FloatType": "REAL",
    "BooleanType": "BOOLEAN",
    "DateType": "DATE",
    "TimestampType": "TIMESTAMP",
    "DecimalType": "NUMERIC",
}


def _jdbc_props() -> dict[str, str]:
    """Đọc credential từ env — thiếu thì lỗi to tiếng, không dùng default đã commit."""
    missing = [name for name in ("POSTGRES_USER", "POSTGRES_PASSWORD") if not os.environ.get(name)]
    if missing:
        raise OSError(f"Thiếu biến môi trường {missing} để ghi serving vào PostgreSQL.")
    return {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def _pg_type(spark_type) -> str:
    """Chuyển một Spark DataType → tên kiểu PostgreSQL."""
    type_name = type(spark_type).__name__
    if type_name == "DecimalType":
        return f"NUMERIC({spark_type.precision},{spark_type.scale})"
    if type_name not in SPARK_TO_PG:
        raise TypeError(f"Không map được kiểu Spark {type_name} sang PostgreSQL.")
    return SPARK_TO_PG[type_name]


def _run_sql(spark, sql: str, props: dict[str, str]) -> None:
    """Chạy một câu DDL/DML trực tiếp qua JDBC (không qua DataFrame)."""
    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(JDBC_URL, props["user"], props["password"])
    try:
        stmt = conn.createStatement()
        stmt.executeUpdate(sql)
        stmt.close()
    finally:
        conn.close()


def ensure_target_table(
    spark, source_table: str, target_table: str, keys: list[str], props: dict[str, str]
) -> list[str]:
    """Tạo bảng PostgreSQL từ schema Iceberg nếu chưa có. Trả về danh sách cột."""
    schema = spark.table(source_table).schema
    columns = [(f.name, f.dataType) for f in schema.fields]
    col_names = [c[0] for c in columns]

    missing_keys = [k for k in keys if k not in col_names]
    if missing_keys:
        raise ValueError(f"{source_table}: khoá upsert {missing_keys} không có trong bảng.")

    col_defs = ", ".join(f'"{name}" {_pg_type(dtype)}' for name, dtype in columns)
    key_def = ", ".join(f'"{k}"' for k in keys)
    table_name = target_table.split(".")[-1]
    ddl = f"CREATE TABLE IF NOT EXISTS {target_table} ({col_defs}, CONSTRAINT pk_{table_name} PRIMARY KEY ({key_def}))"
    _run_sql(spark, ddl, props)

    # Index cho truy vấn của ứng dụng (khoá nghiệp vụ app hay tra). Primary key đã
    # có index ngầm, nhưng khoá upsert ở đây thường là 1 phần của truy vấn app
    # (vd app lọc merchant_id trên bảng merchant_ranking có PK gồm cả year/month).
    # Đặt tên index theo cột để idempotent, tránh tạo trùng mỗi lần chạy.
    idx_cols = "_".join(keys)
    idx_name = f"idx_{table_name}_{idx_cols}".lower()[:63]
    idx_cols_def = ", ".join(f'"{k}"' for k in keys)
    _run_sql(spark, f"CREATE INDEX IF NOT EXISTS {idx_name} ON {target_table} ({idx_cols_def})", props)

    return col_names


def upsert_table(
    spark, source_table: str, target_table: str, keys: list[str], cob_dt: str, props: dict[str, str]
) -> int:
    """Đọc Iceberg serving → upsert vào PostgreSQL theo khoá. Trả về số dòng nguồn."""
    df = spark.sql(f"SELECT * FROM {source_table} WHERE cob_dt = DATE '{cob_dt}'")
    col_names = ensure_target_table(spark, source_table, target_table, keys, props)

    count = df.count()
    if count == 0:
        log.warning(f"  {source_table}: 0 dòng cho cob_dt={cob_dt} — bỏ qua.")
        return 0

    # 1) Ghi vào staging (bảng tạm cùng schema, không ràng buộc) rồi upsert.
    staging = f"{target_table}__staging"
    _run_sql(spark, f"DROP TABLE IF EXISTS {staging}", props)
    _run_sql(
        spark,
        f"CREATE TABLE {staging} AS SELECT * FROM {target_table} WITH NO DATA",
        props,
    )
    df.write.jdbc(JDBC_URL, staging, mode="append", properties=props)

    # 2) INSERT ... ON CONFLICT DO UPDATE  → idempotent.
    cols = ", ".join(f'"{c}"' for c in col_names)
    update_cols = [c for c in col_names if c not in keys]
    if update_cols:
        set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        conflict = f"DO UPDATE SET {set_clause}"
    else:
        conflict = "DO NOTHING"
    key_def = ", ".join(f'"{k}"' for k in keys)
    upsert = f"INSERT INTO {target_table} ({cols}) SELECT {cols} FROM {staging} ON CONFLICT ({key_def}) {conflict}"
    _run_sql(spark, upsert, props)
    _run_sql(spark, f"DROP TABLE IF EXISTS {staging}", props)

    log.info(f"  {target_table}: upsert {count:,} dòng (khoá {keys})")
    return count


def load_config(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Iceberg serving → PostgreSQL serving")
    parser.add_argument("--cob_dt", required=True, help="Business date YYYY-MM-DD")
    parser.add_argument("--config", default=str(CONFIG_FILE), help="Đường dẫn YAML config")
    parser.add_argument("--only", default="", help="Danh sách tên bảng, phân tách bằng dấu phẩy")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cob_dt = args.cob_dt
    config = load_config(Path(args.config))
    target_schema = config["target_schema"]
    tables = config["tables"]

    if args.only:
        wanted = {t.strip() for t in args.only.split(",") if t.strip()}
        tables = [t for t in tables if t["name"] in wanted]

    log.info(f"Serving Sync — cob_dt={cob_dt}, {len(tables)} bảng → schema {target_schema}")
    spark = get_spark_session("serving-sync")
    props = _jdbc_props()

    # Schema đích: tạo một lần (idempotent).
    _run_sql(spark, f"CREATE SCHEMA IF NOT EXISTS {target_schema}", props)

    synced, skipped, failures = [], [], []
    try:
        for spec in tables:
            name = spec["name"]
            source_table = f"{SOURCE_PREFIX}.{name}"
            target_table = f"{target_schema}.{name}"
            try:
                if not spark.catalog.tableExists(source_table):
                    log.warning(f"  {source_table} không tồn tại — bỏ qua.")
                    skipped.append(name)
                    continue
                rows = upsert_table(spark, source_table, target_table, spec["key"], cob_dt, props)
                (synced if rows else skipped).append(name)
            except Exception as exc:  # noqa: BLE001
                # Một bảng lỗi không che các bảng khác: ghi nhận rồi tiếp tục.
                failures.append(f"{name}: {exc}")
                log.error(f"  {name} LỖI: {exc}")
    finally:
        spark.stop()

    log.info(f"Sync xong: {len(synced)} OK, {len(skipped)} bỏ qua, {len(failures)} lỗi.")
    if failures:
        for f in failures:
            log.error(f"  - {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
