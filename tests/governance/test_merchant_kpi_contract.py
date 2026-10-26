"""Static contract for merchant business KPI ownership and actions."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "docs" / "04-operations" / "merchant_kpi_contract.yml"

REQUIRED_FIELDS = {
    "id",
    "owner",
    "definition",
    "unit",
    "source_column",
    "dimensions",
    "thresholds",
    "severity",
    "action",
}
VALID_SEVERITIES = {"info", "warning", "critical"}


def load_contract() -> dict:
    return yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))


def test_contract_has_expected_scope_and_source():
    contract = load_contract()
    assert contract["version"] == "1.0"
    assert contract["domain"] == "merchant_qr_pos"
    assert contract["business_timezone"] == "Asia/Ho_Chi_Minh"
    assert contract["source"]["table"] == "lakehouse.gold.mart_merchant_daily_summary"
    assert contract["source"]["eod_gate"] == "lakehouse.shared.merchant_eod_reconciliation"


def test_each_metric_is_actionable_and_owned():
    metrics = load_contract()["metrics"]
    assert len(metrics) >= 6
    ids = [metric["id"] for metric in metrics]
    assert len(ids) == len(set(ids))
    for metric in metrics:
        assert REQUIRED_FIELDS.issubset(metric)
        assert metric["owner"]
        assert metric["dimensions"]
        assert metric["severity"] in VALID_SEVERITIES
        assert metric["action"]
        assert isinstance(metric["thresholds"], dict)


def test_contract_covers_business_and_platform_personas():
    owners = {metric["owner"] for metric in load_contract()["metrics"]}
    assert {"Merchant Operations", "Finance", "Data Platform"} <= owners


def test_zero_tolerance_controls_are_explicit():
    metrics = {metric["id"]: metric for metric in load_contract()["metrics"]}
    assert metrics["merchant_eod_reconciliation_mismatch"]["thresholds"] == {
        "warning_above": 0,
        "critical_above": 0,
    }
    assert metrics["cdc_dead_letter_rows"]["thresholds"] == {
        "warning_above": 0,
        "critical_above": 0,
    }
