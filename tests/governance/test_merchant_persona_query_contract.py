"""Static contract for persona-based merchant semantic queries."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "docs" / "04-operations" / "merchant_persona_query_contract.yml"
KPI_CONTRACT = ROOT / "docs" / "04-operations" / "merchant_kpi_contract.yml"


def load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_persona_contract_points_to_kpi_contract_and_has_all_personas():
    contract = load(CONTRACT)
    assert contract["kpi_contract"] == "docs/04-operations/merchant_kpi_contract.yml"
    assert {persona["id"] for persona in contract["personas"]} == {
        "merchant_operations",
        "finance",
        "sales_commercial",
        "data_platform",
    }
    assert KPI_CONTRACT.exists()


def test_each_query_is_executable_or_explicitly_bound_to_an_engine():
    contract = load(CONTRACT)
    for persona in contract["personas"]:
        assert persona["owner"]
        assert persona["purpose"]
        assert persona["queries"]
        for query in persona["queries"]:
            assert query["id"]
            assert query["engine"] in {"postgres", "prometheus", "airflow"}
            assert query["metric_refs"] is not None
            if query["engine"] == "postgres":
                assert query["source_tables"]
                assert query["sql"].strip().upper().startswith("SELECT")
            elif query["engine"] == "prometheus":
                assert query["expression"]
            else:
                assert query["dag"] and query["task"]


def test_query_metric_references_exist_in_kpi_contract():
    kpi_ids = {metric["id"] for metric in load(KPI_CONTRACT)["metrics"]}
    query_refs = {
        metric_ref
        for persona in load(CONTRACT)["personas"]
        for query in persona["queries"]
        for metric_ref in query["metric_refs"]
    }
    assert query_refs <= kpi_ids


def test_queries_cover_existing_serving_surfaces():
    queries = [query for persona in load(CONTRACT)["personas"] for query in persona["queries"]]
    tables = {table for query in queries for table in query["source_tables"]}
    assert {
        "serving.merchant_daily_summary_current",
        "serving.merchant_ranking_current",
        "serving.merchant_360_current",
    } <= tables
