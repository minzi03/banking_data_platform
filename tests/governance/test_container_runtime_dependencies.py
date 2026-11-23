"""Static contracts connecting Compose builds, Dockerfiles, and runtime imports."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = REPO_ROOT / "docker" / "docker-compose.yml"


def _build_spec(service: dict) -> tuple[Path, Path] | None:
    build = service.get("build")
    if not isinstance(build, dict):
        return None
    context = (COMPOSE_PATH.parent / build["context"]).resolve()
    dockerfile = (context / build.get("dockerfile", "Dockerfile")).resolve()
    return context, dockerfile


def _copy_sources(dockerfile: Path) -> list[Path]:
    sources = []
    for line in dockerfile.read_text(encoding="utf-8").splitlines():
        match = re.match(r"\s*COPY\s+(?:--from=\S+\s+)?([^\s]+)\s+", line)
        if match and not match.group(1).startswith("--"):
            source = match.group(1)
            if source.startswith("["):
                continue
            sources.append(source)
    return sources


def _python_imports(paths: list[Path]) -> set[str]:
    imported = set()
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".", 1)[0])
    return imported


def _requirements(path: Path) -> set[str]:
    if not path.exists():
        return set()
    packages = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = re.split(r"#", raw_line, maxsplit=1)[0].strip()
        if line and not line.startswith(("-", "http:", "https:")):
            packages.add(re.split(r"[<=>~!\[]", line, maxsplit=1)[0].lower().replace("_", "-"))
    return packages


def test_compose_build_contexts_contain_dockerfiles_and_copied_sources():
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    services = compose["services"]
    checked = 0
    for name, service in services.items():
        build_spec = _build_spec(service)
        if build_spec is None:
            continue
        context, dockerfile = build_spec
        assert dockerfile.is_file(), f"{name}: Dockerfile missing at {dockerfile}"
        for source in _copy_sources(dockerfile):
            if source.startswith("/"):
                continue  # COPY --from=<stage> reads from another image, not the build context.
            assert (context / source).exists(), (
                f"{name}: COPY source {source!r} is outside/missing in build context {context}"
            )
        checked += 1
    assert checked >= 6, f"Only checked {checked} Compose builds; add the maintained app builds"


def test_spark_image_requirements_cover_runtime_imports():
    spark_requirements = _requirements(REPO_ROOT / "docker" / "requirements-spark.txt")
    runtime_roots = [
        REPO_ROOT / "code_etl",
        REPO_ROOT / "governance",
        REPO_ROOT / "scripts" / "resolve_quarantine.py",
    ]
    python_files = []
    for root in runtime_roots:
        python_files.extend([root] if root.is_file() else root.rglob("*.py"))
    imports = _python_imports(python_files)
    package_map = {
        "yaml": "pyyaml",
        "jinja2": "jinja2",
        "pydantic": "pydantic",
        "paramiko": "paramiko",
        "kafka": "kafka-python",
        "jsonschema": "jsonschema",
    }
    expected = {package_map[module] for module in imports if module in package_map}
    assert expected <= spark_requirements, (
        f"Spark runtime imports lack requirements: {sorted(expected - spark_requirements)}"
    )


def test_airflow_dag_external_python_imports_are_installed():
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    airflow_build = _build_spec(compose["services"]["airflow-webserver"])
    assert airflow_build is not None
    _context, dockerfile = airflow_build
    dockerfile_text = dockerfile.read_text(encoding="utf-8")
    dag_files = sorted((REPO_ROOT / "airflow" / "dags").rglob("*.py"))
    imports = _python_imports(dag_files)
    package_map = {"yaml": "apache-airflow", "requests": "requests"}
    installed = set()
    for package in re.findall(r"(?:pip install[^\n]*|requirements[^\n]*)", dockerfile_text):
        installed.update(_requirements_from_text(package))
    requirements_match = re.search(r"-r\s+(\S+)", dockerfile_text)
    if requirements_match:
        installed |= _requirements(dockerfile.parent / requirements_match.group(1))
    direct_packages = set(re.findall(r"pip install[^\n]*", dockerfile_text))
    for line in direct_packages:
        installed |= _requirements_from_text(line.removeprefix("pip install"))
    expected = {package_map[module] for module in imports if module in package_map}
    expected.discard("apache-airflow")  # Provided by the pinned apache/airflow base image.
    assert expected <= installed, (
        f"Airflow DAG imports lack declared image dependencies: {sorted(expected - installed)}"
    )
    for provider in ("apache-airflow-providers-sftp", "apache-airflow-providers-ssh"):
        assert provider in installed, f"Airflow image must explicitly install {provider} for DAG operators/sensors"


def _requirements_from_text(text: str) -> set[str]:
    packages = set()
    for raw_token in re.split(r"\s+", text):
        token = raw_token.strip("\\'\";,)")
        if token and not token.startswith("-"):
            package = re.split(r"[<=>~!\[]", token, maxsplit=1)[0].lower().replace("_", "-")
            if package and package not in {"no-cache-dir", "pip"}:
                packages.add(package)
    return packages


def test_api_and_streamlit_requirements_cover_entrypoint_imports():
    cases = (
        (
            REPO_ROOT / "api",
            "requirements.txt",
            ["main.py"],
            {"fastapi": "fastapi", "pydantic": "pydantic", "trino": "trino"},
        ),
        (
            REPO_ROOT / "streamlit",
            "requirements.txt",
            ["app.py"],
            {"streamlit": "streamlit", "pandas": "pandas", "plotly": "plotly", "trino": "trino"},
        ),
    )
    for context, requirements_name, entrypoints, package_map in cases:
        requirements = _requirements(context / requirements_name)
        imports = _python_imports([context / entrypoint for entrypoint in entrypoints])
        expected = {package_map[module] for module in imports if module in package_map}
        assert expected <= requirements, (
            f"{context.name}: missing runtime requirements {sorted(expected - requirements)}"
        )


def test_spark_worker_pins_driver_and_executor_to_worker_python():
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    environment = compose["services"]["spark-worker-1"].get("environment", {})
    if isinstance(environment, list):
        environment = dict(item.split("=", 1) for item in environment if "=" in item)
    assert environment.get("PYSPARK_PYTHON") == "python3"
    assert environment.get("PYSPARK_DRIVER_PYTHON") == "python3"
    assert environment["PYSPARK_PYTHON"] == environment["PYSPARK_DRIVER_PYTHON"]


def test_exporter_uses_only_python_standard_library():
    exporter = REPO_ROOT / "docker" / "monitoring" / "exporters" / "freshness_exporter.py"
    imports = _python_imports([exporter])
    stdlib = {"http", "json", "urllib", "base64", "os", "ssl", "time"}
    assert imports <= stdlib, f"Exporter has undeclared third-party imports: {sorted(imports - stdlib)}"
