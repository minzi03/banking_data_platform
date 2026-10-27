"""Static contracts for the Prometheus streaming observability surface."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
PROMETHEUS = ROOT / "docker" / "monitoring" / "prometheus.yml"
ALERTS = ROOT / "docker" / "monitoring" / "alert_rules.yml"
EXPORTER = ROOT / "docker" / "monitoring" / "exporters" / "freshness_exporter.py"


def test_prometheus_scrapes_freshness_exporter():
    config = yaml.safe_load(PROMETHEUS.read_text(encoding="utf-8"))
    jobs = {job["job_name"]: job for job in config["scrape_configs"]}
    assert jobs["cdc-freshness"]["static_configs"][0]["targets"] == ["freshness-exporter:9119"]


def test_alerts_cover_streaming_failure_modes():
    config = yaml.safe_load(ALERTS.read_text(encoding="utf-8"))
    names = {rule["alert"] for group in config["groups"] for rule in group["rules"]}
    assert {
        "SparkStreamingQueryInactive",
        "SparkStreamingHeartbeatStale",
        "SparkStreamingKafkaLagHigh",
        "CDCDeadLetterGrowth",
    } <= names


def test_alerts_do_not_reference_unemitted_merchant_eod_metric():
    text = ALERTS.read_text(encoding="utf-8")
    assert "merchant_eod_reconciliation_failures_total" not in text
    assert "MerchantEODReconciliationFailed" not in text


def test_exporter_documents_existing_metric_surface():
    text = EXPORTER.read_text(encoding="utf-8")
    assert "cdc_freshness_seconds" in text
    assert "cdc_row_count" in text
    assert "exporter_up" in text
