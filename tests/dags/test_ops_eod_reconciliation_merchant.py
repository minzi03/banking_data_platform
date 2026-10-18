"""Contract checks for the Merchant EOD reconciliation DAG branch."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DAG = ROOT / "airflow" / "dags" / "ops" / "ops_eod_reconciliation_dag.py"


def test_eod_dag_waits_for_online_transaction_consolidation():
    text = DAG.read_text(encoding="utf-8")
    assert "wait_cdc_online_transaction_consolidation" in text
    assert 'upstream_success_sql("cdc_consolidation_pipeline", COB_DT)' in text


def test_eod_dag_runs_merchant_gate_separately():
    text = DAG.read_text(encoding="utf-8")
    assert "reconcile_merchant_qr_pos" in text
    assert "--merchant-eod" in text
    assert "merchant_reconcile" in text
