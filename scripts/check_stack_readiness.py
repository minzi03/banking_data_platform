"""Non-mutating readiness diagnostic for selected Banking Data Platform profiles."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "docker" / "docker-compose.yml"
PROFILES = {"foundation", "orchestration", "serving", "streaming", "observability", "governance", "apps"}


@dataclass
class Check:
    name: str
    status: str
    detail: str


def _http(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    context: Any = None,
    data: bytes | None = None,
) -> tuple[int, bytes]:
    request = urllib.request.Request(url, data=data, headers=headers or {})
    with urllib.request.urlopen(request, timeout=5, context=context) as response:
        return response.status, response.read()


def _docker_health(container: str) -> Check:
    if shutil.which("docker") is None:
        return Check(container, "ENV", "docker CLI not installed or not on PATH")
    try:
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}",
                container,
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return Check(container, "ENV", f"cannot reach Docker daemon: {exc}")
    if result.returncode != 0:
        return Check(container, "ENV", "container not found/running; start its Compose profile first")
    state, _, health = result.stdout.strip().partition("|")
    if state != "running":
        return Check(container, "FAIL", f"container state={state}")
    if health == "healthy":
        return Check(container, "PASS", "container healthy")
    if health == "no-healthcheck":
        return Check(container, "ENV", "container is running but has no Docker healthcheck")
    return Check(container, "FAIL", f"container health={health}")


def _trino_query() -> Check:
    secrets_dir = ROOT / "docker" / "secrets" / "trino"
    password = os.environ.get("TRINO_PASSWORD")
    if not password:
        env_file = secrets_dir / "passwords.env"
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                if line.startswith("TRINO_PASSWORD_MANIFEST_COLLECTOR="):
                    password = line.partition("=")[2]
                    break
    if not password:
        return Check("trino_query", "ENV", "manifest_collector password unavailable; run bootstrap_trino_auth.py")
    pem = os.environ.get("TRINO_CA_CERT") or str(secrets_dir / "trino.pem")
    if not Path(pem).is_file():
        return Check("trino_query", "ENV", f"Trino CA certificate missing: {pem}")
    import base64
    import ssl

    token = base64.b64encode(f"manifest_collector:{password}".encode()).decode()
    context = ssl.create_default_context(cafile=pem)
    try:
        _, body = _http(
            "https://localhost:8453/v1/statement",
            headers={
                "X-Trino-User": "manifest_collector",
                "Authorization": f"Basic {token}",
                "Content-Type": "text/plain",
            },
            context=context,
            data=b"SELECT 1",
        )
        payload = json.loads(body)
        next_uri = payload.get("nextUri")
        while next_uri:
            _, body = _http(
                next_uri,
                headers={"X-Trino-User": "manifest_collector", "Authorization": f"Basic {token}"},
                context=context,
            )
            payload = json.loads(body)
            query_state = payload.get("stats", {}).get("state")
            if query_state == "FAILED":
                return Check("trino_query", "FAIL", payload.get("error", {}).get("message", "query failed"))
            if query_state == "FINISHED":
                return Check("trino_query", "PASS", "authenticated HTTPS SELECT 1 completed")
            next_uri = payload.get("nextUri")
        if payload.get("stats", {}).get("state") == "FAILED":
            return Check("trino_query", "FAIL", payload.get("error", {}).get("message", "query failed"))
        return Check("trino_query", "PASS", "authenticated HTTPS SELECT 1 completed")
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            return Check("trino_query", "FAIL", f"authenticated HTTPS query rejected: HTTP {exc.code}")
        return Check("trino_query", "FAIL", f"authenticated HTTPS query failed: HTTP {exc.code}")
    except urllib.error.URLError as exc:
        return Check("trino_query", "ENV", f"Trino HTTPS endpoint unavailable: {exc.reason}")
    except (TimeoutError, OSError, ValueError, KeyError) as exc:
        return Check("trino_query", "FAIL", f"authenticated HTTPS query failed: {exc}")


def check_stack(profiles: set[str]) -> list[Check]:
    checks = [
        _docker_health("banking-postgres"),
        _docker_health("banking-minio"),
        _docker_health("banking-iceberg-rest"),
        _docker_health("banking-spark-master"),
        _docker_health("banking-spark-worker-1"),
    ]
    profile_containers = {
        "orchestration": ["banking-airflow-webserver", "banking-airflow-scheduler"],
        "serving": ["banking-trino"],
        "streaming": ["banking-zookeeper", "banking-kafka", "banking-debezium"],
        "observability": [
            "banking-prometheus",
            "banking-alertmanager",
            "banking-grafana",
            "banking-freshness-exporter",
        ],
        "governance": ["banking-om-mysql", "banking-om-elasticsearch", "banking-openmetadata"],
        "apps": ["banking-streamlit", "banking-api", "banking-superset", "banking-mlflow"],
    }
    for profile in sorted(profiles):
        for container in profile_containers.get(profile, []):
            checks.append(_docker_health(container))
    if "serving" in profiles:
        checks.append(_trino_query())
    if "streaming" in profiles:
        debezium = next(check for check in checks if check.name == "banking-debezium")
        if debezium.status != "PASS":
            checks.append(Check("debezium_connectors_api", debezium.status, "skipped because Debezium is not ready"))
        else:
            try:
                _http("http://localhost:8083/connectors")
                checks.append(Check("debezium_connectors_api", "PASS", "Connect REST API responded"))
            except urllib.error.URLError as exc:
                checks.append(Check("debezium_connectors_api", "ENV", f"Connect REST API unavailable: {exc.reason}"))
            except (TimeoutError, OSError) as exc:
                checks.append(Check("debezium_connectors_api", "FAIL", f"Connect REST API failed: {exc}"))
    return checks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", action="append", choices=sorted(PROFILES), default=[])
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)
    profiles = set(args.profile)
    checks = check_stack(profiles)
    if args.as_json:
        print(json.dumps([asdict(item) for item in checks], ensure_ascii=False, indent=2))
    else:
        for item in checks:
            print(f"{item.status:4} {item.name}: {item.detail}")
    states = {item.status for item in checks}
    counts = {status: sum(check.status == status for check in checks) for status in ("PASS", "FAIL", "ENV")}
    print(f"Summary: PASS={counts['PASS']} FAIL={counts['FAIL']} ENV={counts['ENV']}")
    if "FAIL" in states:
        return 1
    if "ENV" in states:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
