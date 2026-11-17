"""Static contract tying the pipeline audit writer to pipeline_run_log DDL."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DDL = ROOT / "docker" / "init_postgres" / "04_ddl_ops_metadata.sql"
WRITER = ROOT / "code_etl" / "shared" / "ops" / "pipeline_run_audit.py"

EXPECTED_COLUMNS = {
    "dag_id",
    "task_id",
    "status",
    "cob_dt",
    "rows_processed",
    "execution_time_s",
    "error_message",
    "started_at",
    "completed_at",
}
EXPECTED_STATUSES = {"RUNNING", "SUCCESS", "FAILED", "SKIPPED"}


def _ddl_columns() -> set[str]:
    text = DDL.read_text(encoding="utf-8")
    marker = "CREATE TABLE IF NOT EXISTS opslakehouse.pipeline_run_log ("
    block = text.split(marker, maxsplit=1)[1].split(");", maxsplit=1)[0]
    return set(re.findall(r"^\s{4}([a-z_]+)\s", block, flags=re.MULTILINE))


def test_ddl_has_every_column_the_writer_inserts():
    assert _ddl_columns() == EXPECTED_COLUMNS | {"id"}


def test_ddl_status_check_matches_the_writer_statuses():
    ddl = DDL.read_text(encoding="utf-8")
    match = re.search(r"CHECK \(status IN \(([^)]*)\)\)", ddl)
    assert match, "pipeline_run_log missing status constraint"
    declared = set(re.findall(r"'([A-Z_]+)'", match.group(1)))
    assert declared == EXPECTED_STATUSES


def test_writer_uses_parameterized_insert_only():
    writer = WRITER.read_text(encoding="utf-8")
    assert "INSERT INTO {TABLE}" in writer
    assert "%s" in writer
    assert "DELETE" not in writer.upper()
