"""
End-of-Day Reconciliation — Streaming vs Batch
==============================================

Tiêu chí nghiệm thu nghiệp vụ ngân hàng (nguồn: khoá Merchant QR/POS):
**số liệu đường streaming phải KHỚP số liệu đường batch cho cùng cob_dt.**

Giao dịch trong ngân hàng đi qua hai đường biểu diễn khác nhau:

    Đường batch      : Postgres → (Spark JDBC) → Bronze → Silver fact_txn_account
    Đường streaming  : Postgres → (Debezium CDC) → Kafka → Bronze CDC → Silver Current

Cả hai đường đều neo vào **cùng một nguồn sự thật** (Postgres core). Nếu một
đường đếm thiếu/đếm thừa, đó là silent corruption — báo cáo ngày và dashboard
real-time sẽ nói hai con số khác nhau, và không ai biết cho tới khi đối soát.

Module này so ba con số cho một `cob_dt`:

    source_count   = số dòng trong Postgres core (nguồn sự thật)
    bronze_count   = số dòng trong Bronze CDC (đường streaming)
    batch_count    = số dòng trong Silver fact (đường batch)

    |bronze_count - source_count| <= tolerance   (streaming bắt kịp nguồn)
    |batch_count  - source_count| <= tolerance   (batch bắt kịp nguồn)
    |bronze_count - batch_count|  <= tolerance   (hai đường khớp nhau)

Nếu tolerance = 0 (mặc định) thì ba con số phải bằng nhau tuyệt đối.

Module còn đối soát **control total** (số dư tổng, balance sheet tie-out) theo
pattern `gl_control_total` của dataset tham khảo `data2/`: tổng số dư tài khoản
từ Postgres phải khớp tổng số dư trong Silver dim_account (chỉ bản ghi còn hiệu
lực). Đếm dòng khớp mà tiền lệch vẫn là silent corruption — nên cần cả hai.

Ghi kết quả vào `opslakehouse.reconciliation_log`, và exit non-zero nếu có FAIL
— để dùng làm Airflow gate (cùng mô hình với data_quality.py, quarantine.py).

Usage:
    spark-submit --master spark://spark-master:7077 \
        code_etl/shared/ops/eod_reconciliation.py --cob_dt 2025-12-31

    # Nới ngưỡng cho bảng được ghi liên tục (giao dịch đang chạy trong ngày):
    spark-submit ... --cob_dt 2025-12-31 --tolerance 5
"""

# ruff: noqa: E402

from __future__ import annotations

import argparse
import os
import sys
from decimal import Decimal
from logging import INFO, basicConfig, getLogger
from typing import Any

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, ".."))

from ops.merchant_settlement_reconciliation import (  # noqa: E402, PLC0415
    reconcile_spark as reconcile_merchant_settlement,
)
from ops.merchant_settlement_reconciliation import to_reconciliation_log_rows  # noqa: E402, PLC0415
from spark.spark_session import get_spark_session  # noqa: E402

basicConfig(
    level=INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = getLogger("eod_reconciliation")

JDBC_URL = "jdbc:postgresql://postgres:5432/banking_db"
RECON_LOG_TABLE = "opslakehouse.reconciliation_log"

# Mỗi mục đối soát một cặp (nguồn sự thật ↔ hai đường biểu diễn).
#   source_query : SQL trên Postgres core, trả về ĐÚNG một số (nguyên hoặc numeric)
#   bronze_table : bảng Bronze CDC (đường streaming)
#   batch_table  : bảng Silver (đường batch)
#   kind         : "count" (đếm dòng) hoặc "control_total" (đối soát số dư tổng)
#
# Chỉ dùng cho bảng ĐÃ có cả hai đường. Khi đường streaming mở rộng thêm
# transaction fact, thêm mục ở đây — không cần sửa logic.

# ---- count: đối soát số dòng ----
COUNT_RECONCILIATIONS: list[dict[str, str]] = [
    {
        "entity": "customer",
        "kind": "count",
        "source_query": "SELECT COUNT(*) AS n FROM core_banking.customer",
        "bronze_table": "lakehouse.bronze.core_customer_cdc",
        "batch_table": "lakehouse.silver.dim_customer",
        "batch_is_scd": "true",  # SCD2: đếm theo customer_id distinct, không đếm dòng
    },
    {
        "entity": "account",
        "kind": "count",
        "source_query": "SELECT COUNT(*) AS n FROM core_banking.account",
        "bronze_table": "lakehouse.bronze.core_account_cdc",
        "batch_table": "lakehouse.silver.dim_account",
        "batch_is_scd": "true",
    },
]

# ---- control_total: đối soát SỐ DƯ TỔNG (balance sheet tie-out) ----
# Pattern lấy từ dataset tham khảo `data2/gl_control_total.csv` (GENERAL_LEDGER
# control total = SUM(closing_balance) GROUP BY business_date/currency, tolerance
# 1.0). Ở đây đối soát tổng số dư tài khoản giữa nguồn Postgres và Silver
# dim_account (trạng thái hiện tại). Một đường đếm khớp mà số dư lệch vẫn là
# silent corruption — cùng số dòng, khác tiền.
#
# bronze_table = None vì Bronze CDC là append-only event log, không giữ trạng
# thái số dư hiện tại; control total chỉ so source ↔ batch.
CONTROL_TOTAL_RECONCILIATIONS: list[dict[str, str]] = [
    {
        "entity": "account_balance",
        "kind": "control_total",
        "source_query": "SELECT COALESCE(SUM(balance), 0) AS n FROM core_banking.account",
        # dim_account là SCD2 → chỉ tính phiên bản còn hiệu lực — xem _control_total_batch.
        "batch_table": "lakehouse.silver.dim_account",
        "batch_balance_col": "balance",
        "batch_current_only": "true",
    },
]

RECONCILIATIONS: list[dict[str, str]] = COUNT_RECONCILIATIONS + CONTROL_TOTAL_RECONCILIATIONS

MERCHANT_COHORT = "merchant_qr_pos"
MERCHANT_METRICS = ("success_count", "gross_amount", "refund_count")


def _merchant_source_query(cob_dt: str) -> str:
    """Postgres source-of-truth aggregate for the shared Merchant cohort."""
    return f"""
        SELECT merchant_id, payment_channel, currency,
               COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_count,
               COALESCE(SUM(amount) FILTER (WHERE status = 'SUCCESS'), 0) AS gross_amount,
               COUNT(*) FILTER (WHERE status = 'SUCCESS' AND refund_flag = 1) AS refund_count
        FROM digital_banking.online_transaction
        WHERE merchant_id IS NOT NULL
          AND payment_channel IN ('QR', 'POS')
          AND (timezone('Asia/Ho_Chi_Minh', timezone('UTC', transaction_date)))::date = DATE '{cob_dt}'
        GROUP BY merchant_id, payment_channel, currency
    """


def _merchant_metric_sql(table: str, cob_dt: str, *, epoch_timestamp: bool = False) -> str:
    """Aggregate batch/current CDC at the exact source cohort and metric grain."""
    date_expr = (
        "CAST(from_utc_timestamp(timestamp_micros(CAST(transaction_date AS BIGINT)), 'Asia/Ho_Chi_Minh') AS DATE)"
        if epoch_timestamp
        else "CAST(from_utc_timestamp(transaction_date, 'Asia/Ho_Chi_Minh') AS DATE)"
    )
    return f"""
        SELECT merchant_id, payment_channel, currency,
               COUNT(CASE WHEN status = 'SUCCESS' THEN transaction_id END) AS success_count,
               COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END), 0) AS gross_amount,
               COUNT(CASE WHEN status = 'SUCCESS' AND refund_flag = 1 THEN transaction_id END) AS refund_count
        FROM {table}
        WHERE merchant_id IS NOT NULL
          AND payment_channel IN ('QR', 'POS')
          AND {date_expr} = DATE '{cob_dt}'
        GROUP BY merchant_id, payment_channel, currency
    """


def _metric_map(rows) -> dict[tuple, dict[str, float]]:
    """Convert Spark/JDBC rows to a comparable, stable metric map."""
    result = {}
    for row in rows:
        values = row.asDict() if hasattr(row, "asDict") else dict(row)
        key = (int(values["merchant_id"]), values["payment_channel"], values["currency"].strip())
        result[key] = {metric: float(values[metric] or 0) for metric in MERCHANT_METRICS}
    return result


def reconcile_merchant_transactions(spark, cob_dt: str, tolerance: float = 0.0) -> list[dict[str, Any]]:
    """Compare source, batch fact and CDC current state for one ICT business date."""
    source = _read_source_rows(spark, _merchant_source_query(cob_dt))
    batch_rows = spark.sql(_merchant_metric_sql("lakehouse.silver.fact_online_transaction", cob_dt)).collect()
    stream_rows = spark.sql(
        _merchant_metric_sql("lakehouse.silver.fact_online_transaction_current", cob_dt, epoch_timestamp=True)
    ).collect()
    source_map = _metric_map(source)
    batch_map = _metric_map(batch_rows)
    stream_map = _metric_map(stream_rows)
    results = []
    for key in sorted(set(source_map) | set(batch_map) | set(stream_map)):
        source_values = source_map.get(key, {metric: 0.0 for metric in MERCHANT_METRICS})
        batch_values = batch_map.get(key, {metric: 0.0 for metric in MERCHANT_METRICS})
        stream_values = stream_map.get(key, {metric: 0.0 for metric in MERCHANT_METRICS})
        merchant_id, channel, currency = key
        for metric in MERCHANT_METRICS:
            source_value = source_values[metric]
            batch_value = batch_values[metric]
            stream_value = stream_values[metric]
            status = (
                "PASS"
                if max(abs(source_value - batch_value), abs(source_value - stream_value)) <= tolerance
                else "FAIL"
            )
            results.append(
                {
                    "metric_name": f"{MERCHANT_COHORT}.{metric}",
                    "entity": f"merchant:{merchant_id}:{channel}:{currency}",
                    "stream_value": stream_value,
                    "batch_value": batch_value,
                    "delta": stream_value - batch_value,
                    "tolerance": tolerance,
                    "check_status": status,
                    "details": (
                        f"source={source_value} batch={batch_value} stream={stream_value} "
                        f"merchant_id={merchant_id} channel={channel} currency={currency}"
                    ),
                    "cob_dt": cob_dt,
                }
            )
    return results


def _read_source_rows(spark, query: str):
    """Read grouped source rows from Postgres with the platform credentials."""
    missing = [name for name in ("POSTGRES_USER", "POSTGRES_PASSWORD") if not os.environ.get(name)]
    if missing:
        raise OSError(f"Thiếu biến môi trường {missing} để đọc nguồn PostgreSQL.")
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"({query}) s")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .load()
        .collect()
    )


def _read_source_count(spark, query: str) -> int:
    """Đọc một con số từ Postgres core qua JDBC."""
    missing = [name for name in ("POSTGRES_USER", "POSTGRES_PASSWORD") if not os.environ.get(name)]
    if missing:
        raise OSError(f"Thiếu biến môi trường {missing} để đọc nguồn PostgreSQL.")
    return int(
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"({query}) s")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .load()
        .collect()[0][0]
    )


def _count_table(spark, table: str, cob_dt: str | None, distinct_key: str | None = None) -> int:
    """Đếm dòng một bảng Iceberg, tuỳ chọn lọc theo cob_dt hoặc đếm distinct key."""
    if not spark.catalog.tableExists(table):
        raise FileNotFoundError(f"Bảng không tồn tại: {table}")

    selects = f"COUNT(DISTINCT {distinct_key})" if distinct_key else "COUNT(*)"
    sql = f"SELECT {selects} AS n FROM {table}"
    if cob_dt:
        sql += f" WHERE cob_dt = DATE '{cob_dt}'"
    return int(spark.sql(sql).collect()[0][0])


def _sum_table(spark, spec: dict[str, str]) -> float:
    """Cộng một cột số của bảng Iceberg — dùng cho control-total (đối soát số dư).

    SCD2: chỉ tính phiên bản còn hiệu lực (`is_current`). Cộng toàn bộ lịch sử
    sẽ ra số dư gấp nhiều lần và lệch vô nghĩa so với nguồn.
    """
    table = spec["batch_table"]
    if not spark.catalog.tableExists(table):
        raise FileNotFoundError(f"Bảng không tồn tại: {table}")

    col = spec["batch_balance_col"]
    sql = f"SELECT COALESCE(SUM({col}), 0) AS n FROM {table}"
    if spec.get("batch_current_only") == "true":
        sql += " WHERE is_current = true"
    return float(spark.sql(sql).collect()[0][0])


def reconcile_one(spark, spec: dict[str, str], cob_dt: str, tolerance: float) -> list[dict[str, Any]]:
    """Đối soát một entity, trả về danh sách bản ghi kết quả."""
    entity = spec["entity"]
    kind = spec.get("kind", "count")
    log.info(f"Đối soát entity={entity} (kind={kind}) cho cob_dt={cob_dt}")

    if kind == "control_total":
        return _reconcile_control_total(spark, spec, cob_dt, tolerance)

    source_count = _read_source_count(spark, spec["source_query"])
    # Bronze CDC là append-only, KHÔNG partition theo cob_dt → đếm toàn bộ.
    bronze_count = _count_table(spark, spec["bronze_table"], cob_dt=None)
    # Silver dimension là SCD2 → nhiều dòng mỗi business key, đếm distinct.
    batch_key = "customer_id" if entity == "customer" else "account_id"
    batch_count = _count_table(
        spark,
        spec["batch_table"],
        cob_dt=None,
        distinct_key=batch_key if spec.get("batch_is_scd") == "true" else None,
    )

    log.info(f"  source={source_count} bronze={bronze_count} batch={batch_count}")

    checks = [
        ("streaming_vs_source", bronze_count, source_count),
        ("batch_vs_source", batch_count, source_count),
        ("streaming_vs_batch", bronze_count, batch_count),
    ]

    results: list[dict[str, Any]] = []
    for check_name, stream_value, batch_value in checks:
        delta = stream_value - batch_value
        status = "PASS" if abs(delta) <= tolerance else "FAIL"
        results.append(
            {
                "metric_name": check_name,
                "entity": entity,
                "stream_value": stream_value,
                "batch_value": batch_value,
                "delta": delta,
                "tolerance": tolerance,
                "check_status": status,
                "details": (
                    f"{entity}.{check_name}: stream={stream_value} batch={batch_value} "
                    f"delta={delta} tolerance={tolerance}"
                ),
                "cob_dt": cob_dt,
            }
        )
        if status == "FAIL":
            log.error(f"  FAIL {check_name}: delta={delta} vượt ngưỡng {tolerance}")
    return results


def _reconcile_control_total(spark, spec: dict[str, str], cob_dt: str, tolerance: float) -> list[dict[str, Any]]:
    """Đối soát số dư tổng (balance sheet tie-out): nguồn Postgres ↔ Silver.

    Khác với count: một sai lệch ở đây là SAI TIỀN, không chỉ sai số dòng — cùng
    số dòng mà tổng số dư lệch vẫn là silent corruption. Vì vậy tolerance mặc
    định của mục này nên để nhỏ (0 là lý tưởng; data2 dùng tolerance 1.0 cho sai
    số làm tròn).
    """
    entity = spec["entity"]
    source_value = float(_read_source_count(spark, spec["source_query"]))
    batch_value = _sum_table(spark, spec)

    delta = source_value - batch_value
    # Số tiền có thể âm; dùng sai số tương đối nhỏ để tránh floating-point noise.
    status = "PASS" if abs(delta) <= tolerance else "FAIL"

    log.info(f"  source_balance={source_value} batch_balance={batch_value} delta={delta}")
    if status == "FAIL":
        log.error(f"  FAIL {entity}: số dư lệch {delta} vượt ngưỡng {tolerance}")

    return [
        {
            "metric_name": "balance_control_total",
            "entity": entity,
            "stream_value": source_value,  # control total neo vào nguồn sự thật
            "batch_value": batch_value,
            "delta": delta,
            "tolerance": tolerance,
            "check_status": status,
            "details": (
                f"{entity}.balance_control_total: source={source_value} batch={batch_value} "
                f"delta={delta} tolerance={tolerance}"
            ),
            "cob_dt": cob_dt,
        }
    ]


def write_results_to_pg(spark, results: list[dict[str, Any]], cob_dt: str) -> None:
    """Ghi kết quả vào opslakehouse.reconciliation_log (idempotent theo cob_dt+entity)."""
    if not results:
        log.info("Không có kết quả để ghi.")
        return

    missing = [name for name in ("POSTGRES_USER", "POSTGRES_PASSWORD") if not os.environ.get(name)]
    if missing:
        raise OSError(f"Thiếu biến môi trường {missing} để ghi kết quả đối soát vào PostgreSQL.")
    props = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }

    import datetime

    from pyspark.sql import Row
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    entities = sorted({r["entity"] for r in results})
    entities_str = ",".join(f"'{e}'" for e in entities)

    # Chạy lại cùng cob_dt: xoá kết quả cũ trước khi ghi (idempotent re-run).
    try:
        conn = spark._sc._jvm.java.sql.DriverManager.getConnection(JDBC_URL, props["user"], props["password"])
        stmt = conn.createStatement()
        stmt.executeUpdate(
            f"DELETE FROM {RECON_LOG_TABLE} WHERE cob_dt = DATE '{cob_dt}' AND entity IN ({entities_str})"
        )
        stmt.close()
        conn.close()
    except Exception as exc:  # noqa: BLE001
        log.warning(f"Không xoá được kết quả cũ (lần chạy đầu?): {exc}")

    rows = [
        Row(
            metric_name=r["metric_name"],
            entity=r["entity"],
            stream_value=float(r["stream_value"]),
            batch_value=float(r["batch_value"]),
            delta=float(r["delta"]),
            tolerance=float(r["tolerance"]),
            check_status=r["check_status"],
            details=r["details"],
            cob_dt=datetime.datetime.strptime(cob_dt, "%Y-%m-%d").date(),
        )
        for r in results
    ]
    schema = StructType(
        [
            StructField("metric_name", StringType()),
            StructField("entity", StringType()),
            StructField("stream_value", DoubleType()),
            StructField("batch_value", DoubleType()),
            StructField("delta", DoubleType()),
            StructField("tolerance", DoubleType()),
            StructField("check_status", StringType()),
            StructField("details", StringType()),
            StructField("cob_dt", DateType()),
        ]
    )
    df = spark.createDataFrame(rows, schema=schema)
    log.info(f"Ghi {len(results)} kết quả đối soát vào {RECON_LOG_TABLE} ...")
    df.write.jdbc(JDBC_URL, RECON_LOG_TABLE, mode="append", properties=props)
    log.info("Ghi xong.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="End-of-day streaming vs batch reconciliation")
    parser.add_argument("--cob_dt", required=True, help="Business date YYYY-MM-DD")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.0,
        help="Ngưỡng lệch tuyệt đối cho phép (mặc định 0 — phải khớp tuyệt đối)",
    )
    parser.add_argument(
        "--merchant-eod",
        action="store_true",
        help="Đối soát cohort Merchant QR/POS source ↔ batch ↔ CDC current",
    )
    parser.add_argument(
        "--merchant-settlement",
        action="store_true",
        help="Đối soát transaction aggregate ↔ SFTP settlement aggregate",
    )
    parser.add_argument(
        "--settlement-amount-tolerance",
        type=float,
        default=0.0,
        help="Ngưỡng lệch gross amount settlement (mặc định 0)",
    )
    parser.add_argument(
        "--settlement-count-tolerance",
        type=int,
        default=0,
        help="Ngưỡng lệch txn_count settlement (mặc định 0)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cob_dt = args.cob_dt
    tolerance = args.tolerance

    log.info(f"End-of-Day Reconciliation — cob_dt={cob_dt}, tolerance={tolerance}")
    spark = get_spark_session("EodReconciliation")

    all_results: list[dict[str, Any]] = []
    failures: list[str] = []
    if args.merchant_eod:
        try:
            all_results.extend(reconcile_merchant_transactions(spark, cob_dt, tolerance))
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{MERCHANT_COHORT}: {exc}")
            log.error(f"Đối soát {MERCHANT_COHORT} lỗi: {exc}")
    if args.merchant_settlement:
        try:
            settlement_results = reconcile_merchant_settlement(
                spark,
                cob_dt,
                amount_tolerance=Decimal(str(args.settlement_amount_tolerance)),
                count_tolerance=args.settlement_count_tolerance,
            )
            all_results.extend(
                to_reconciliation_log_rows(
                    settlement_results,
                    cob_dt,
                    Decimal(str(args.settlement_amount_tolerance)),
                )
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"merchant_settlement: {exc}")
            log.error(f"Đối soát merchant settlement lỗi: {exc}")
    for spec in RECONCILIATIONS:
        if args.merchant_eod or args.merchant_settlement:
            break
        # Legacy dimension/account reconciliation remains the default path.

        try:
            results = reconcile_one(spark, spec, cob_dt, tolerance)
            all_results.extend(results)
        except Exception as exc:  # noqa: BLE001
            # Một entity lỗi không được che các entity khác: ghi nhận rồi tiếp tục.
            failures.append(f"{spec['entity']}: {exc}")
            log.error(f"Đối soát {spec['entity']} lỗi: {exc}")

    if all_results:
        write_results_to_pg(spark, all_results, cob_dt)

    fail_count = sum(1 for r in all_results if r["check_status"] == "FAIL")
    if fail_count or failures:
        log.error(f"ĐỐI SOÁT THẤT BẠI — {fail_count} check FAIL, {len(failures)} entity lỗi")
        for f in failures:
            log.error(f"  - {f}")
        return 1

    log.info(f"ĐỐI SOÁT ĐẠT — {len(all_results)} check PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
