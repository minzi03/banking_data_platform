"""
Mọi client của Trino phải biết xác thực (ADR-0016, bước PR-A)
=============================================================

Khi Trino bật password auth, cổng HTTP trả 403 cho client và HTTPS đòi mật khẩu
(đo trên Trino 443). Một client bị quên sẽ gãy ngay lúc bật — hoặc tệ hơn, được
"sửa" bằng cách dùng user admin. Test này bắt client bị quên TRƯỚC khi bật.

Quy ước: có TRINO_PASSWORD → HTTPS + mật khẩu; không có → HTTP như trước.
"""

from __future__ import annotations

import base64
import importlib.util
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOTS = ("api", "ml", "streamlit", "code_etl", "docker", "scripts", "airflow")


def _load(rel: str, name: str):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / rel)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _dbapi_clients() -> list[Path]:
    out = []
    for root in RUNTIME_ROOTS:
        for path in (REPO_ROOT / root).rglob("*.py"):
            if "from trino.dbapi import connect" in path.read_text(encoding="utf-8", errors="ignore"):
                out.append(path)
    return sorted(out)


def _connect_calls(source: str) -> list[str]:
    """Thân của mọi lời gọi connect(...), tách theo ngoặc cân bằng."""
    calls = []
    for match in re.finditer(r"\bconnect\(", source):
        depth, i = 1, match.end()
        while depth and i < len(source):
            depth += {"(": 1, ")": -1}.get(source[i], 0)
            i += 1
        calls.append(source[match.end() : i - 1])
    return calls


def test_dbapi_clients_are_found():
    names = {p.relative_to(REPO_ROOT).as_posix() for p in _dbapi_clients()}
    assert {"api/main.py", "streamlit/app.py", "ml/pipeline/churn_prediction.py"} <= names


@pytest.mark.parametrize("path", _dbapi_clients(), ids=lambda p: p.relative_to(REPO_ROOT).as_posix())
def test_every_connect_call_can_authenticate(path: Path):
    source = path.read_text(encoding="utf-8")
    calls = _connect_calls(source)
    assert calls, "import connect nhưng không gọi"
    for call in calls:
        assert "trino_auth_kwargs(" in call, f"connect() không truyền trino_auth_kwargs: connect({call.strip()[:80]}…)"


def test_manifest_client_uses_https_and_basic_auth_only_with_a_password():
    gen = _load("scripts/generate_metrics_manifest.py", "gmm_auth")
    plain = gen.TrinoClient()
    assert plain.url == "http://localhost:8085/v1/statement"
    assert "Authorization" not in plain.headers

    auth = gen.TrinoClient(password="pw", ca_cert=None)
    assert auth.url == "https://localhost:8443/v1/statement"
    assert auth.headers["Authorization"] == "Basic " + base64.b64encode(b"manifest_collector:pw").decode()
    assert auth.ssl_context is not None


def test_manifest_does_not_pick_up_a_password_implicitly(monkeypatch):
    """PR-A không được tự đọc docker/.env: máy đã bootstrap sẽ gãy với Trino chưa bật auth."""
    gen = _load("scripts/generate_metrics_manifest.py", "gmm_env")
    monkeypatch.delenv("TRINO_PASSWORD", raising=False)
    assert gen.trino_credentials("manifest_collector") == (None, None)


def test_superset_uri(monkeypatch):
    mod = _load("docker/superset/add_trino_connection.py", "superset_conn")
    monkeypatch.delenv("TRINO_PASSWORD", raising=False)
    assert mod.trino_uri() == "trino://superset@trino:8080/iceberg"
    monkeypatch.setenv("TRINO_PASSWORD", "p@ss/w:rd")
    uri = mod.trino_uri()
    assert uri.startswith("trino://superset:") and uri.endswith("@trino:8443/iceberg?protocol=https")
    assert "p@ss/w:rd" not in uri, "mật khẩu phải được URL-encode"


def test_dbt_docker_target_switches_on_trino_password():
    target = yaml.safe_load((REPO_ROOT / "dbt" / "profiles.yml").read_text(encoding="utf-8"))["banking"]["outputs"][
        "docker"
    ]
    for key in ("method", "http_scheme", "port", "password"):
        assert "env_var('TRINO_PASSWORD'" in str(target[key]), (
            f"dbt docker target: {key} không phụ thuộc TRINO_PASSWORD"
        )


def test_freshness_exporter_sends_basic_auth_when_configured(monkeypatch):
    monkeypatch.setenv("TRINO_PASSWORD", "pw")
    exporter = _load("docker/monitoring/exporters/freshness_exporter.py", "fresh_auth")
    assert exporter.TRINO_URL.startswith("https://")
    assert exporter._auth_headers()["Authorization"] == "Basic " + base64.b64encode(b"freshness_exporter:pw").decode()
