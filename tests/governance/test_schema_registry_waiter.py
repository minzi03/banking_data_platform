"""Tests for Schema Registry readiness waiting."""

from scripts.wait_for_schema_registry import wait_for_registry


def test_waiter_returns_after_registry_is_ready(monkeypatch):
    calls = []

    def fake_fetch(url, path):
        calls.append((url, path))
        return []

    monkeypatch.setattr("scripts.wait_for_schema_registry.fetch_json", fake_fetch)
    monkeypatch.setattr("scripts.wait_for_schema_registry.time.sleep", lambda _: None)

    wait_for_registry("http://registry", 1)

    assert calls == [("http://registry", "/subjects")]


def test_waiter_retries_transient_errors(monkeypatch):
    responses = [OSError("starting"), ["orders-value"]]

    def fake_fetch(url, path):
        result = responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr("scripts.wait_for_schema_registry.fetch_json", fake_fetch)
    monkeypatch.setattr("scripts.wait_for_schema_registry.time.sleep", lambda _: None)

    wait_for_registry("http://registry", 1)

    assert responses == []
