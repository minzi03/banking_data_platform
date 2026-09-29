"""
Schema drift có mức độ (ADDITIVE / BREAKING) và một entrypoint chạy thật.

Hai lỗi file này chặn:
1. ops_schema_drift_dag gọi `spark-submit governance/schema_drift.py --table …`
   trong khi file không có entrypoint — thoát 0, DAG luôn xanh mà không kiểm gì.
   Test chạy file ĐÚNG CÁCH spark-submit chạy: như một script.
2. Mọi thay đổi (thêm cột, mất cột, đổi kiểu) từng ngang hàng; giờ thêm cột và
   nâng kiểu theo luật Iceberg là ADDITIVE, còn lại là BREAKING.

Schema khai báo đọc từ DDL Iceberg; parser được đối chiếu với parser của từ điển
dữ liệu (scripts/generate_data_dictionary.py) trên toàn bộ DDL.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from governance.ddl_schema import declared_schemas, normalize_type, parse_ddl_text
from governance.schema_drift import (
    ADDITIVE,
    BREAKING,
    NONE,
    check_tables,
    classify,
    is_safe_promotion,
    main,
    select_tables,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DDL_DIR = REPO_ROOT / "docker" / "init_iceberg"
DAG = REPO_ROOT / "airflow" / "dags" / "ops" / "ops_schema_drift_dag.py"


@pytest.mark.parametrize(
    ("raw", "normal"),
    [
        ("decimal(18, 2)", "DECIMAL(18,2)"),
        ("integer", "INT"),
        ("long", "BIGINT"),
        ("VARCHAR(10)", "STRING"),
        ("char(3)", "STRING"),
        ("string", "STRING"),
        ("timestamp", "TIMESTAMP"),
    ],
)
def test_normalize_type(raw, normal):
    assert normalize_type(raw) == normal


@pytest.mark.parametrize(
    ("old", "new", "safe"),
    [
        ("INT", "BIGINT", True),
        ("FLOAT", "DOUBLE", True),
        ("DECIMAL(10,2)", "DECIMAL(18,2)", True),
        ("DECIMAL(18,2)", "DECIMAL(18,2)", True),
        ("BIGINT", "INT", False),  # thu hẹp
        ("DECIMAL(18,2)", "DECIMAL(10,2)", False),  # mất precision
        ("DECIMAL(18,2)", "DECIMAL(18,4)", False),  # đổi scale
        ("DECIMAL(18,2)", "STRING", False),
        ("DATE", "TIMESTAMP", False),
    ],
)
def test_iceberg_promotion_rules(old, new, safe):
    assert is_safe_promotion(old, new) is safe


def test_classify_levels():
    declared = {"id": "BIGINT", "amount": "DECIMAL(10,2)", "n": "INT"}
    assert classify("t", declared, dict(declared)).severity == NONE
    assert classify("t", declared, {**declared, "extra": "STRING"}).severity == ADDITIVE
    assert classify("t", declared, {**declared, "n": "BIGINT"}).severity == ADDITIVE
    assert classify("t", declared, {"id": "BIGINT", "n": "INT"}).severity == BREAKING
    assert classify("t", declared, {**declared, "amount": "STRING"}).severity == BREAKING
    assert classify("t", declared, None).severity == BREAKING
    # Spark trả kiểu viết thường; VARCHAR trong DDL là string trong Iceberg.
    assert classify("t", {"code": "VARCHAR(10)"}, {"code": "string"}).severity == NONE


def test_describe_names_every_change():
    report = classify("t", {"a": "INT", "b": "STRING"}, {"a": "BIGINT", "c": "DATE"})
    text = report.describe()
    assert "mất cột ['b']" in text and "thêm cột ['c']" in text and "INT → BIGINT (promotion)" in text


def test_declared_schemas_cover_the_lakehouse():
    declared = declared_schemas(DDL_DIR)
    assert len(declared) >= 50
    assert declared["lakehouse.gold.loan_delinquency"]["debt_group"] == "INT"
    # Cột đứng sau một dòng comment tiêu đề không được rơi mất.
    assert "__cdc_operation" in declared["lakehouse.silver.dim_customer_current"]
    assert "__source_spark_batch_id" in declared["lakehouse.silver.dim_customer_current"]


def test_parser_handles_comments_and_decimal():
    ddl = """
    CREATE TABLE IF NOT EXISTS lakehouse.gold.x (
        a   BIGINT,            -- khoá
        -- nhóm tiền
        b   DECIMAL(18, 2),
        c   VARCHAR(10)
    )
    USING iceberg;
    """
    assert parse_ddl_text(ddl) == {"lakehouse.gold.x": {"a": "BIGINT", "b": "DECIMAL(18,2)", "c": "STRING"}}


def test_parser_agrees_with_data_dictionary_parser():
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    import generate_data_dictionary as dictionary

    theirs = {}
    for path in sorted(DDL_DIR.glob("*.sql")):
        for table in dictionary.parse_ddl(path):
            theirs[table.fqn.lower()] = {c.name.lower(): normalize_type(c.dtype) for c in table.columns}
    assert declared_schemas(DDL_DIR) == theirs


def test_select_tables_by_layer_and_name():
    declared = declared_schemas(DDL_DIR)
    silver_gold = select_tables(declared, ["silver", "gold"], [])
    assert silver_gold and all(t.split(".")[1] in {"silver", "gold"} for t in silver_gold)
    assert "lakehouse.gold.loan_delinquency" in silver_gold
    assert select_tables(declared, ["silver"], ["LAKEHOUSE.GOLD.LOAN_DELINQUENCY"]) == [
        "lakehouse.gold.loan_delinquency"
    ]
    with pytest.raises(SystemExit):
        select_tables(declared, ["gold"], ["lakehouse.gold.khong_ton_tai"])


def _reader_from(declared, overrides):
    def read(table):
        if table in overrides:
            return overrides[table]
        return {c: t.lower() for c, t in declared[table].items()}

    return read


def test_main_exit_codes(capsys):
    declared = declared_schemas(DDL_DIR)
    target = "lakehouse.gold.loan_delinquency"
    args = ["--table", target]

    assert main(args, read_schema=_reader_from(declared, {})) == 0

    additive = {**declared[target], "new_col": "STRING"}
    assert main(args, read_schema=_reader_from(declared, {target: additive})) == 0
    assert "ADDITIVE" in capsys.readouterr().out

    dropped = {c: t for c, t in declared[target].items() if c != "debt_group"}
    assert main(args, read_schema=_reader_from(declared, {target: dropped})) == 1
    assert "BREAKING" in capsys.readouterr().out

    assert main(args, read_schema=_reader_from(declared, {target: None})) == 1


def test_check_tables_reports_every_table():
    declared = {"a.b.c": {"x": "INT"}, "a.b.d": {"y": "STRING"}}
    reports = check_tables(lambda t: {"x": "int"} if t == "a.b.c" else None, declared, ["a.b.c", "a.b.d"])
    assert [r.severity for r in reports] == [NONE, BREAKING]


def test_runs_as_a_script_like_spark_submit():
    """Chạy như spark-submit chạy: `python governance/schema_drift.py`. Không có
    entrypoint thì lệnh này in ra rỗng và thoát 0 — đúng lỗi của DAG cũ."""
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "governance" / "schema_drift.py"), "--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=REPO_ROOT / "airflow",  # không phải gốc repo: import phải tự tìm được governance/
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "--layer" in result.stdout and "BREAKING" in result.stdout


def test_dag_uses_the_real_entrypoint():
    text = DAG.read_text(encoding="utf-8")
    command = text.split("check_schema_drift = BashOperator(", 1)[1]
    assert "--layer silver --layer gold" in command
    assert "--columns" not in command, "schema_drift.py không có tham số --columns"
    assert "governance/schema_drift.py" in text
