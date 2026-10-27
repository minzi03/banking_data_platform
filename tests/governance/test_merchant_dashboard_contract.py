"""Static contract for the merchant Grafana dashboard query surface."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = ROOT / "docker" / "monitoring" / "grafana" / "dashboards" / "merchant-business.json"


def load_dashboard() -> dict:
    return json.loads(DASHBOARD.read_text(encoding="utf-8"))


def raw_queries() -> list[str]:
    queries = []
    for panel in load_dashboard()["panels"]:
        for target in panel.get("targets", []):
            if target.get("rawSql"):
                queries.append(target["rawSql"])
    return queries


def test_dashboard_json_and_queries_are_present():
    dashboard = load_dashboard()
    assert dashboard["uid"] == "merchant-business"
    assert dashboard["timezone"] == "Asia/Ho_Chi_Minh"
    assert len(raw_queries()) >= 7


def test_dashboard_uses_only_known_merchant_serving_tables():
    allowed = {
        "serving.merchant_daily_summary_current",
        "serving.merchant_ranking_current",
        "serving.merchant_360_current",
    }
    queries = raw_queries()
    referenced = {table for table in allowed if any(table in query for query in queries)}
    assert referenced == allowed


def test_dashboard_does_not_expose_raw_merchant_name():
    text = "\n".join(raw_queries()).lower()
    assert "select merchant_name," not in text
    assert "merchant_name_masked" in text
