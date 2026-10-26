"""Static runtime smoke contracts for merchant persona queries."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
QUERY_CONTRACT = ROOT / "docs" / "04-operations" / "merchant_persona_query_contract.yml"

EXPECTED_COLUMNS = {
    "serving.merchant_daily_summary_current": {
        "txn_date",
        "merchant_id",
        "txn_count",
        "gross_amount",
        "refund_rate",
    },
    "serving.merchant_ranking_current": {
        "merchant_name_masked",
        "mcc_code",
        "revenue",
        "rank_in_mcc",
        "pop_growth_rate",
    },
    "serving.merchant_360_current": {
        "advisory_product_hint",
        "revenue_90d",
    },
}


def load_queries() -> list[dict]:
    contract = yaml.safe_load(QUERY_CONTRACT.read_text(encoding="utf-8"))
    return [query for persona in contract["personas"] for query in persona["queries"]]


def postgres_queries() -> list[dict]:
    return [query for query in load_queries() if query["engine"] == "postgres"]


def test_postgres_queries_reference_declared_serving_columns():
    for query in postgres_queries():
        sql = query["sql"].lower()
        referenced_tables = set(query["source_tables"])
        for table in referenced_tables:
            assert table in EXPECTED_COLUMNS
            assert any(column in sql for column in EXPECTED_COLUMNS[table]), (
                f"{query['id']} không tham chiếu column đã khai báo của {table}"
            )


def test_postgres_queries_do_not_reference_unknown_merchant_surface():
    for query in postgres_queries():
        sql = query["sql"].lower()
        assert "merchant_daily_summary" in sql or "merchant_ranking" in sql or "merchant_360" in sql
        assert "raw_merchant_name" not in sql


def test_query_contract_does_not_expose_raw_merchant_name():
    for query in postgres_queries():
        sql = query["sql"].lower()
        assert "select merchant_name," not in sql
        assert "merchant_name_masked" in sql or "merchant_name" not in sql


def test_postgres_query_sources_match_known_serving_models():
    known = set(EXPECTED_COLUMNS)
    for query in postgres_queries():
        assert set(query["source_tables"]) <= known
