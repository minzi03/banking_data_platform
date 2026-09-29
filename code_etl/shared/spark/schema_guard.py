"""
Guard schema trước khi ghi Gold: chỉ publish khi schema ổn định.

So kết quả vừa tính (result_df) với bảng Gold đang có, TRƯỚC khi ghi:

- Cột của bảng mà kết quả không có → BREAKING: dừng, partition cũ giữ nguyên.
  Consumer (dbt serving, Superset, API) đang đọc cột đó.
- Đổi HỌ kiểu (số ↔ chuỗi, date ↔ timestamp, …) → BREAKING: dừng. Ghi tiếp thì
  hoặc lỗi giữa chừng, hoặc consumer âm thầm đọc sai.
- Số → số (int, bigint, decimal, double — mọi chiều) → để Spark ép kiểu khi ghi
  như trước (store assignment, tràn số vẫn làm lệnh ghi lỗi). KHÔNG tự nới kiểu
  cột: SUM trên DECIMAL(18,2) ra DECIMAL(28,2), nới theo kết quả thì mỗi lần
  chạy lại đổi schema bảng khỏi DDL.
- Cột mới trong kết quả → ADD COLUMNS rồi ghi (schema evolution dạng additive).
  Trước đây là lỗi INSERT_COLUMN_ARITY_MISMATCH và phải ALTER tay (RUNBOOK §10).

Module không import governance/: gold_job chạy trên worker với sys.path chỉ có
code_etl/shared.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_NUMERIC = re.compile(r"(TINYINT|SMALLINT|INT|INTEGER|BIGINT|LONG|FLOAT|DOUBLE|DECIMAL\(\d+,\d+\))")


def _family(dtype: str) -> str:
    t = re.sub(r"\s+", "", dtype).upper()
    if _NUMERIC.fullmatch(t):
        return "NUMERIC"
    if re.fullmatch(r"(VAR)?CHAR(\(\d+\))?|STRING", t):
        return "STRING"
    return t  # DATE, TIMESTAMP, TIMESTAMP_NTZ, BOOLEAN, … — mỗi kiểu một họ


def assignable(result_type: str, table_type: str) -> bool:
    """Giá trị kiểu result_type ghi được vào cột table_type mà không đổi nghĩa."""
    return _family(result_type) == _family(table_type)


@dataclass
class WritePlan:
    """Kế hoạch ghi result vào bảng có sẵn."""

    missing: list[str] = field(default_factory=list)
    incompatible: list[tuple[str, str, str]] = field(default_factory=list)
    add_columns: list[tuple[str, str]] = field(default_factory=list)

    @property
    def breaking(self) -> bool:
        return bool(self.missing or self.incompatible)

    def ddl(self, table: str) -> list[str]:
        """Câu ALTER cần chạy trước khi ghi (chỉ khi không breaking)."""
        if not self.add_columns:
            return []
        cols = ", ".join(f"{c} {t}" for c, t in self.add_columns)
        return [f"ALTER TABLE {table} ADD COLUMNS ({cols})"]


def plan_write(table_types: dict[str, str], result_types: dict[str, str]) -> WritePlan:
    """Hàm thuần: {cột: kiểu Spark} của bảng và của kết quả."""
    table = {c.lower(): t for c, t in table_types.items()}
    result = {c.lower(): t for c, t in result_types.items()}
    plan = WritePlan()
    plan.missing = sorted(set(table) - set(result))
    plan.add_columns = [(c, result[c]) for c in sorted(set(result) - set(table))]
    plan.incompatible = [
        (c, table[c], result[c]) for c in sorted(set(table) & set(result)) if not assignable(result[c], table[c])
    ]
    return plan


class BreakingSchemaChange(RuntimeError):
    """Kết quả Gold không ghi được vào bảng mà không làm vỡ consumer."""


def guard_and_evolve(spark, result_df, target: str, logger) -> WritePlan:
    """
    Gọi TRƯỚC writeTo(...).overwritePartitions() khi bảng đã tồn tại.
    BREAKING → raise, không đụng tới bảng. Cột mới → ADD COLUMNS rồi trả về để ghi.
    """
    plan = plan_write(dict(spark.table(target).dtypes), dict(result_df.dtypes))
    if plan.breaking:
        problems = [f"thiếu cột {plan.missing}"] if plan.missing else []
        problems += [f"{c}: {o} → {n}" for c, o, n in plan.incompatible]
        raise BreakingSchemaChange(
            f"Không publish {target}: thay đổi schema BREAKING ({'; '.join(problems)}). "
            "Partition cũ giữ nguyên. Sửa SQL của job, hoặc đổi DDL + consumer có chủ đích."
        )
    for statement in plan.ddl(target):
        logger.warning(f"Schema evolution (ADDITIVE): {statement}")
        spark.sql(statement)
    return plan
