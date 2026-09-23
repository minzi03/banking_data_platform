#!/usr/bin/env python3
"""
Contract Validation — kiểm snapshot của một ngày theo data contract
===================================================================

Nạp mọi contract của một tầng trong `governance/datasets/`, đọc bảng thật trong
phạm vi mà lượt chạy hằng ngày hỏi tới, kiểm theo `quality_rules` bằng
`ContractEnforcer`, ghi từng kết quả vào `opslakehouse.contract_validation_log`,
rồi exit 1 nếu có FAIL.

Trước file này, `ops_contract_validation_dag` gọi `governance/enforcement.py`
như một script. Đó là module thư viện: không có `__main__`, không nhận tham số.
Chạy như script nó còn không import được (`/opt/project` không nằm trong
`sys.path`), nên DAG chưa từng kiểm contract nào (TD-10).

Phạm vi đọc giống hệt DQ (TD-11) — dùng chung `_scoped_table`, không viết lại:

    bảng có cột cob_dt      → chỉ snapshot của ngày đang kiểm
    bảng có cột is_current  → chỉ phiên bản hiện hành

Đọc cả bảng thì `unique_check` đếm mỗi giao dịch một lần cho mỗi snapshot còn
giữ, và job đỏ trên dữ liệu lành.

Usage:
    spark-submit --master spark://spark-master:7077 \\
        contract_validation.py --cob_dt 2026-09-22 --layer silver
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from logging import INFO, basicConfig, getLogger

_HERE = os.path.dirname(os.path.abspath(__file__))
_SHARED = os.path.abspath(os.path.join(_HERE, ".."))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
# code_etl/shared cho `spark.*` và `ops.*`; gốc repo cho package `governance`.
# Thiếu gốc repo chính là lý do bản cũ chết với "No module named 'governance'".
for _path in (_SHARED, _REPO_ROOT):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from ops.data_quality import SCOPE_KEY, _scoped_table  # noqa: E402

from governance.contracts import DatasetContract  # noqa: E402
from governance.contracts_registry import ContractRegistry  # noqa: E402
from governance.enforcement import CheckResult, ContractEnforcer, ValidationResult  # noqa: E402

basicConfig(
    level=INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = getLogger("contract_validation")

LOG_TABLE = "opslakehouse.contract_validation_log"
JDBC_URL = "jdbc:postgresql://postgres:5432/banking_db"
LAYERS = ("bronze", "silver", "gold")


@dataclass
class ContractRun:
    """Kết quả kiểm một contract, kèm phạm vi đã đọc để ghi vào log."""

    contract: DatasetContract
    result: ValidationResult
    scope: str


# ---------------------------------------------------------------------------
# Kiểm một contract
# ---------------------------------------------------------------------------


def _single_failure(dataset_id: str, check_name: str, expected: str, error: Exception) -> ValidationResult:
    result = ValidationResult(dataset_id=dataset_id)
    result.add_check(
        CheckResult(
            check_name=check_name,
            status="FAIL",
            expected=expected,
            actual="error",
            details=f"{type(error).__name__}: {error}",
        )
    )
    return result


def validate_contract(spark, enforcer: ContractEnforcer, contract: DatasetContract, cob_dt: str) -> ContractRun:
    """
    Lỗi là FAIL, không phải crash và không phải PASS.

    Bảng không đọc được, hay một check nổ giữa chừng, đều thành một dòng FAIL
    cho đúng contract đó — các contract còn lại vẫn được kiểm và ghi log, nên
    một lượt đỏ vẫn để lại bằng chứng đầy đủ.
    """
    table = contract.physical_location.full_table_name
    try:
        df, scope = _scoped_table(spark, table, {SCOPE_KEY: cob_dt})
    except Exception as e:
        return ContractRun(contract, _single_failure(contract.dataset_id, "table_readable", table, e), "")

    try:
        result = enforcer.validate_before_write(spark, df, contract)
    except Exception as e:
        result = _single_failure(contract.dataset_id, "validation_error", "all checks run", e)
    return ContractRun(contract, result, scope)


# ---------------------------------------------------------------------------
# Log
# ---------------------------------------------------------------------------


def to_log_rows(runs: list[ContractRun], cob_dt: str, checked_at: datetime) -> list[dict]:
    """Mỗi check một dòng, đúng các cột của contract_validation_log."""
    rows = []
    for run in runs:
        for check in run.result.checks:
            rows.append(
                {
                    "dataset_id": run.contract.dataset_id,
                    "check_name": check.check_name,
                    "check_status": check.status,
                    "expected_value": str(check.expected),
                    "actual_value": str(check.actual),
                    "details": f"{check.details}{run.scope}",
                    "cob_dt": date.fromisoformat(cob_dt),
                    "checked_at": checked_at,
                }
            )
    return rows


def _jdbc_credentials() -> dict[str, str]:
    """
    Bắt buộc lấy từ môi trường. Không có mật khẩu mặc định trong code: thiếu
    biến thì fail to tiếng, thay vì âm thầm dùng một credential đã commit.
    """
    missing = [name for name in ("POSTGRES_USER", "POSTGRES_PASSWORD") if not os.environ.get(name)]
    if missing:
        raise OSError(
            f"Thiếu biến môi trường {missing} để ghi {LOG_TABLE}. "
            "spark-worker-1 nhận chúng từ docker/.env qua docker-compose.yml."
        )
    return {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def write_log(spark, rows: list[dict], dataset_ids: list[str], cob_dt: str) -> None:
    """
    Xoá kết quả cũ của cùng (cob_dt, dataset) rồi ghi mới — chạy lại một ngày
    không nhân đôi log. Cùng cách với data_quality_log.
    """
    from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

    props = _jdbc_credentials()
    schema = StructType(
        [
            StructField("dataset_id", StringType()),
            StructField("check_name", StringType()),
            StructField("check_status", StringType()),
            StructField("expected_value", StringType()),
            StructField("actual_value", StringType()),
            StructField("details", StringType()),
            StructField("cob_dt", DateType()),
            StructField("checked_at", TimestampType()),
        ]
    )
    df = spark.createDataFrame([tuple(r[f.name] for f in schema.fields) for r in rows], schema=schema)

    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(JDBC_URL, props["user"], props["password"])
    try:
        stmt = conn.prepareStatement(f"DELETE FROM {LOG_TABLE} WHERE cob_dt = ? AND dataset_id = ?")
        for dataset_id in dataset_ids:
            stmt.setDate(1, spark._sc._jvm.java.sql.Date.valueOf(cob_dt))
            stmt.setString(2, dataset_id)
            stmt.executeUpdate()
        stmt.close()
    finally:
        conn.close()

    df.write.jdbc(JDBC_URL, LOG_TABLE, mode="append", properties=props)
    log.info(f"Đã ghi {len(rows)} kết quả vào {LOG_TABLE}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def summarize(runs: list[ContractRun]) -> int:
    """In tóm tắt, trả về số contract có FAIL."""
    failed = 0
    for run in runs:
        r = run.result
        icon = "✅" if r.passed else "❌"
        log.info(f"{icon} {r.dataset_id}{run.scope}: {r.pass_count} PASS · {r.warn_count} WARN · {r.fail_count} FAIL")
        for check in r.checks:
            if check.status != "PASS":
                log.info(f"      {check.status:4s} {check.check_name}: {check.details}")
        failed += 0 if r.passed else 1
    log.info(f"CONTRACT SUMMARY: {len(runs)} contract · {len(runs) - failed} PASS · {failed} FAIL")
    return failed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kiểm snapshot của một ngày theo data contract")
    parser.add_argument("--cob_dt", required=True, help="Ngày snapshot (YYYY-MM-DD)")
    parser.add_argument("--layer", required=True, choices=LAYERS)
    return parser.parse_args(argv)


def select_contracts(registry: ContractRegistry, layer: str) -> list[DatasetContract]:
    """
    Registry lỗi hoặc tầng không có contract nào đều là lỗi, không phải PASS.
    Một lượt kiểm 0 contract sẽ xanh mà không kiểm gì.
    """
    if registry.has_errors:
        raise ValueError(f"{len(registry.errors)} contract không nạp được: {registry.errors}")
    contracts = registry.get_contracts_by_layer(layer)
    if not contracts:
        raise ValueError(f"Không có contract nào cho tầng '{layer}' — lượt kiểm sẽ xanh vô nghĩa.")
    return [contracts[k] for k in sorted(contracts)]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cob_dt = date.fromisoformat(args.cob_dt).isoformat()
    contracts = select_contracts(ContractRegistry(), args.layer)
    log.info(f"Contract validation — layer={args.layer} cob_dt={cob_dt} · {len(contracts)} contract")

    from spark.spark_session import get_spark_session

    spark = get_spark_session("ContractValidation")
    enforcer = ContractEnforcer()
    runs = [validate_contract(spark, enforcer, c, cob_dt) for c in contracts]

    failed = summarize(runs)
    write_log(spark, to_log_rows(runs, cob_dt, datetime.now()), [c.dataset_id for c in contracts], cob_dt)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
