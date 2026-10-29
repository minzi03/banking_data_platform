"""Tests for read-only Schema Registry subject verification."""

from scripts.verify_schema_registry import subject_for, verify_subject


class FakeRegistry:
    def __init__(self):
        self.requests = []

    def get(self, registry_url, path):
        self.requests.append(path)
        if path == "/subjects/orders-value/versions":
            return [1, 2]
        return {"id": 42, "version": 2, "schemaType": "JSON"}


def test_subject_uses_topic_name_strategy():
    assert subject_for("orders") == "orders-value"


def test_verifier_reads_versions_and_selected_schema(monkeypatch):
    registry = FakeRegistry()
    monkeypatch.setattr("scripts.verify_schema_registry.fetch_json", registry.get)

    result = verify_subject("http://registry", "orders-value", 2)

    assert result["id"] == 42
    assert registry.requests == ["/subjects/orders-value/versions", "/subjects/orders-value/versions/2"]
