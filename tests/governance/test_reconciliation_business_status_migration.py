"""Static compatibility contract for persisted reconciliation outcomes."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DDL = ROOT / "docker" / "init_postgres" / "00_extensions.sql"
WRITER = ROOT / "code_etl" / "shared" / "ops" / "eod_reconciliation.py"
DICTIONARY = ROOT / "docs" / "03-data" / "DATA_DICTIONARY.md"
RUNBOOK = ROOT / "RUNBOOK.md"

EXPECTED = {
    "MATCHED",
    "MISMATCH",
    "MISSING",
    "DUPLICATE",
    "PENDING",
    "AMBIGUOUS",
    "NOT_COMPARABLE",
}


def test_reconciliation_business_status_is_an_additive_idempotent_migration():
    ddl = DDL.read_text(encoding="utf-8")
    assert re.search(
        r"ALTER TABLE\s+opslakehouse\.reconciliation_log\s+ADD COLUMN IF NOT EXISTS\s+"
        r"reconciliation_status\s+VARCHAR\(24\)\s+NOT NULL\s+DEFAULT\s+'NOT_COMPARABLE'",
        ddl,
        flags=re.IGNORECASE,
    )
    assert re.search(r"ADD CONSTRAINT chk_reconciliation_business_status CHECK", ddl, re.IGNORECASE)
    assert re.search(r"pg_constraint.*?chk_reconciliation_business_status", ddl, re.DOTALL)
    assert "check_status IN ('PASS', 'FAIL', 'WARN')" in ddl
    for status in EXPECTED:
        assert f"'{status}'" in ddl


def test_eod_writer_sends_legacy_and_business_status_columns():
    writer = WRITER.read_text(encoding="utf-8")
    assert 'reconciliation_status=r.get("reconciliation_status", "NOT_COMPARABLE")' in writer
    assert 'StructField("check_status", StringType())' in writer
    assert 'StructField("reconciliation_status", StringType())' in writer


def test_runbook_explains_existing_stack_migration_and_legacy_status():
    runbook = RUNBOOK.read_text(encoding="utf-8")
    assert "reconciliation_status" in runbook
    assert "check_status" in runbook
    assert "NOT_COMPARABLE" in runbook
    assert "historical rows" in runbook


def test_generated_dictionary_lists_the_new_column():
    dictionary = DICTIONARY.read_text(encoding="utf-8")
    section = dictionary.split("### `opslakehouse.reconciliation_log`", maxsplit=1)[1].split("### `", maxsplit=1)[0]
    assert "`reconciliation_status`" in section
    assert "VARCHAR(24)" in section
