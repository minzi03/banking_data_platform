"""Tests for the non-mutating stack readiness diagnostic."""

from unittest.mock import MagicMock

import pytest

from scripts.check_stack_readiness import Check, _docker_health, _trino_query, check_stack, main


def test_missing_docker_is_reported_as_environment_not_failure(monkeypatch):
    monkeypatch.setattr("scripts.check_stack_readiness.shutil.which", lambda _name: None)
    result = _docker_health("banking-postgres")
    assert result.status == "ENV"
    assert "docker CLI" in result.detail


def test_unavailable_container_is_environment_not_failure(monkeypatch):
    monkeypatch.setattr("scripts.check_stack_readiness.shutil.which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(
        "scripts.check_stack_readiness.subprocess.run",
        lambda *args, **kwargs: MagicMock(returncode=1, stdout="", stderr="not found"),
    )
    result = _docker_health("banking-postgres")
    assert result.status == "ENV"
    assert "not found" in result.detail


def test_unhealthy_running_container_is_failure(monkeypatch):
    monkeypatch.setattr("scripts.check_stack_readiness.shutil.which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(
        "scripts.check_stack_readiness.subprocess.run",
        lambda *args, **kwargs: MagicMock(returncode=0, stdout="running|unhealthy", stderr=""),
    )
    result = _docker_health("banking-postgres")
    assert result.status == "FAIL"
    assert "unhealthy" in result.detail


def test_healthy_container_passes(monkeypatch):
    monkeypatch.setattr("scripts.check_stack_readiness.shutil.which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(
        "scripts.check_stack_readiness.subprocess.run",
        lambda *args, **kwargs: MagicMock(returncode=0, stdout="running|healthy", stderr=""),
    )
    result = _docker_health("banking-postgres")
    assert result.status == "PASS"


def test_serving_profile_runs_authenticated_trino_check(monkeypatch):
    seen = []
    monkeypatch.setattr("scripts.check_stack_readiness._docker_health", lambda name: Check(name, "PASS", "ok"))

    def trino_check():
        seen.append("trino")
        return Check("trino_query", "PASS", "ok")

    monkeypatch.setattr("scripts.check_stack_readiness._trino_query", trino_check)
    results = check_stack({"serving"})
    assert "trino" in seen
    assert any(result.name == "banking-trino" for result in results)
    assert any(result.name == "trino_query" for result in results)


def test_trino_query_uses_https_auth_and_waits_for_finished_state(monkeypatch, tmp_path):
    secret_dir = tmp_path / "docker" / "secrets" / "trino"
    secret_dir.mkdir(parents=True)
    (secret_dir / "passwords.env").write_text("TRINO_PASSWORD_MANIFEST_COLLECTOR=secret\n", encoding="utf-8")
    certificate = secret_dir / "trino.pem"
    certificate.write_text("test-cert", encoding="utf-8")
    certificate.touch()
    monkeypatch.setattr("scripts.check_stack_readiness.ROOT", tmp_path)
    monkeypatch.setattr("ssl.create_default_context", lambda **_kwargs: object())
    calls = []
    responses = [
        (200, b'{"nextUri":"https://localhost:8453/next","stats":{"state":"RUNNING"}}'),
        (200, b'{"stats":{"state":"FINISHED"}}'),
    ]

    def fake_http(url, **kwargs):
        calls.append((url, kwargs))
        return responses.pop(0)

    monkeypatch.setattr("scripts.check_stack_readiness._http", fake_http)
    result = _trino_query()
    assert result.status == "PASS"
    assert calls[0][0] == "https://localhost:8453/v1/statement"
    assert calls[0][1]["data"] == b"SELECT 1"
    assert calls[1][0] == "https://localhost:8453/next"


def test_trino_query_fails_on_statement_error(monkeypatch, tmp_path):
    secret_dir = tmp_path / "docker" / "secrets" / "trino"
    secret_dir.mkdir(parents=True)
    (secret_dir / "passwords.env").write_text("TRINO_PASSWORD_MANIFEST_COLLECTOR=secret\n", encoding="utf-8")
    (secret_dir / "trino.pem").write_text("test-cert", encoding="utf-8")
    monkeypatch.setattr("scripts.check_stack_readiness.ROOT", tmp_path)
    monkeypatch.setattr("ssl.create_default_context", lambda **_kwargs: object())
    monkeypatch.setattr(
        "scripts.check_stack_readiness._http",
        lambda *_args, **_kwargs: (200, b'{"stats":{"state":"FAILED"},"error":{"message":"bad query"}}'),
    )
    assert _trino_query().status == "FAIL"
    assert "bad query" in _trino_query().detail


def test_streaming_profile_skips_connect_probe_if_debezium_container_is_missing(
    monkeypatch,
):
    def docker_check(name):
        if name == "banking-debezium":
            return Check(name, "ENV", "container not running")
        return Check(name, "PASS", "ok")

    monkeypatch.setattr("scripts.check_stack_readiness._docker_health", docker_check)
    monkeypatch.setattr(
        "scripts.check_stack_readiness._http",
        lambda *_args, **_kwargs: pytest.fail("should not probe an absent service"),
    )
    results = check_stack({"streaming"})
    probe = next(result for result in results if result.name == "debezium_connectors_api")
    assert probe.status == "ENV"


def test_streaming_profile_probes_connect_rest(monkeypatch):
    urls = []
    monkeypatch.setattr("scripts.check_stack_readiness._docker_health", lambda name: Check(name, "PASS", "ok"))
    monkeypatch.setattr("scripts.check_stack_readiness._http", lambda url, **kwargs: urls.append(url) or (200, b"[]"))
    results = check_stack({"streaming"})
    assert "http://localhost:8083/connectors" in urls
    assert any(result.name == "debezium_connectors_api" and result.status == "PASS" for result in results)


def test_main_returns_distinct_fail_and_environment_exit_codes(monkeypatch, capsys):
    monkeypatch.setattr("scripts.check_stack_readiness.check_stack", lambda _profiles: [Check("x", "ENV", "off")])
    assert main([]) == 2
    assert "ENV=1" in capsys.readouterr().out
    monkeypatch.setattr("scripts.check_stack_readiness.check_stack", lambda _profiles: [Check("x", "FAIL", "bad")])
    assert main([]) == 1


def test_invalid_profile_is_rejected_by_cli():
    with pytest.raises(SystemExit):
        main(["--profile", "unknown"])
