#!/usr/bin/env python3
"""
Generate / verify docs/evidence/metrics-manifest.yaml.

Không phải script dump số. Nó là VERIFIER: thu evidence, chạy invariant, và chỉ
promote canonical manifest khi mọi blocking invariant xanh.

    COLLECT  →  luôn ghi timestamped run artifact
       ↓
    VERIFY   →  blocking invariants
       ↓
    ERROR == 0 ?  yes → promote canonical
                  no  → giữ canonical cũ, exit != 0

Nhờ tách hai bước, một rebuild fail vẫn để lại forensic evidence trong
docs/evidence/generated/ thay vì mất trắng.

Usage:
    python scripts/generate_metrics_manifest.py --cob-dt 2026-09-06
    python scripts/generate_metrics_manifest.py --validate-contract
    python scripts/generate_metrics_manifest.py --render-sql --cob-dt 2026-09-06
    python scripts/generate_metrics_manifest.py --collect-only --cob-dt 2026-09-06
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import subprocess
import sys
import urllib.request
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


def _force_utf8_output() -> None:
    """
    Console Windows mặc định cp1258 → mọi thông báo tiếng Việt sẽ ném
    UnicodeEncodeError và giết script trước cả khi in được lỗi thật.
    """
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, OSError):
                pass


_force_utf8_output()

REPO_ROOT = Path(__file__).resolve().parent.parent
EVIDENCE_DIR = REPO_ROOT / "docs" / "evidence"
MANIFEST_PATH = EVIDENCE_DIR / "metrics-manifest.yaml"
SQL_PATH = EVIDENCE_DIR / "metrics-manifest.sql"
GENERATED_DIR = EVIDENCE_DIR / "generated"

MISSING = object()


# =============================================================================
# Path helpers — đường dẫn dotted, hỗ trợ wildcard `*`
# =============================================================================

def get_path(root: Any, dotted: str) -> Any:
    node = root
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return MISSING
        node = node[part]
    return node


def set_path(root: dict, dotted: str, value: Any) -> None:
    parts = dotted.split(".")
    node = root
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    node[parts[-1]] = value


def expand_wildcards(root: Any, dotted: str) -> list[str]:
    """`a.*.c` → [`a.x.c`, `a.y.c`] theo các key thực tế có mặt."""
    if "*" not in dotted:
        return [dotted]
    head, _, tail = dotted.partition(".*.")
    node = get_path(root, head)
    if not isinstance(node, dict):
        return []
    return [p for key in node for p in expand_wildcards(root, f"{head}.{key}.{tail}")]


# =============================================================================
# load_contract
# =============================================================================

def load_contract(path: Path = MANIFEST_PATH) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def validate_contract(manifest: dict) -> list[str]:
    """Kiểm tra contract tự nhất quán TRƯỚC khi đụng tới Trino."""
    errors: list[str] = []

    for section in ("manifest", "metrics", "invariants", "readme_bindings"):
        if section not in manifest:
            errors.append(f"thiếu section bắt buộc: {section}")
    if errors:
        return errors

    if manifest["manifest"].get("schema_version") != "1.1":
        errors.append("schema_version phải là '1.1'")

    valid_ops = {"eq", "ne", "lt", "le", "gt", "ge"}
    for inv_id, inv in manifest["invariants"].items():
        if inv.get("severity") not in {"error", "warn"}:
            errors.append(f"invariant {inv_id}: severity phải là error|warn")
        if inv.get("operator") not in valid_ops:
            errors.append(f"invariant {inv_id}: operator không hợp lệ")
        if "expected" not in inv and "compare_to" not in inv:
            errors.append(f"invariant {inv_id}: cần expected hoặc compare_to")
        # Kiểm tra path CÓ THẬT, không chỉ nở wildcard: expand_wildcards trả về
        # nguyên chuỗi input khi không có `*`, nên nếu chỉ dựa vào nó thì một
        # path rác như `metrics.does.not.exist` vẫn lọt qua.
        for field in ("metric", "compare_to"):
            path = inv.get(field)
            if not path or path.startswith("__"):
                continue
            expanded = expand_wildcards(manifest, path)
            if not expanded:
                errors.append(f"invariant {inv_id}: {field} không khớp node nào: {path}")
                continue
            for resolved in expanded:
                if get_path(manifest, resolved) is MISSING:
                    errors.append(
                        f"invariant {inv_id}: {field} trỏ tới node không tồn tại: {resolved}"
                    )

    for binding in manifest["readme_bindings"]:
        if get_path(manifest, binding["manifest_path"]) is MISSING:
            errors.append(f"readme_binding trỏ tới node không tồn tại: {binding['manifest_path']}")

    return errors


# =============================================================================
# collect_repo_metrics — static, sinh từ repo chứ không gõ tay
# =============================================================================

def _load_yaml_files(pattern: str) -> list[tuple[Path, Any]]:
    out = []
    for path in sorted(REPO_ROOT.glob(pattern)):
        try:
            out.append((path, yaml.safe_load(path.read_text(encoding="utf-8"))))
        except yaml.YAMLError:
            continue
    return out


def _bronze_ingestion_workloads(manifest: dict) -> int:
    """
    Đếm ingestion workload theo SEMANTICS, không theo tên file.
    templates/source_registry.yml tự bị loại vì thiếu contract shape.
    """
    required = manifest["metrics"]["source"]["source_datasets"]["required_keys"]
    return sum(
        1
        for _path, cfg in _load_yaml_files("code_etl/bronze/*/*.yml")
        if isinstance(cfg, dict) and all(k in cfg for k in required)
    )


def _cdc_stream_configs(_manifest: dict) -> int:
    return sum(
        1
        for _p, cfg in _load_yaml_files("code_etl/cdc/config/*.yml")
        if isinstance(cfg, dict) and "kafka" in cfg and "target" in cfg
    )


def _silver_dims(job_type: str | None) -> int:
    configs = [c for _p, c in _load_yaml_files("code_etl/silver/dims/*.yml") if isinstance(c, dict)]
    if job_type is None:
        return len(configs)
    return sum(1 for c in configs if (c.get("job") or {}).get("type") == job_type)


def _gold_ddl_tables(_manifest: dict) -> int:
    ddl = (REPO_ROOT / "docker/init_iceberg/03_ddl_gold.sql").read_text(encoding="utf-8")
    return len(re.findall(r"CREATE TABLE IF NOT EXISTS", ddl, re.IGNORECASE))


def _gold_ddl_objects(_manifest: dict) -> int:
    """CREATE TABLE + CREATE VIEW — object phục vụ, không chỉ bảng."""
    ddl = (REPO_ROOT / "docker/init_iceberg/03_ddl_gold.sql").read_text(encoding="utf-8")
    return (
        len(re.findall(r"CREATE TABLE IF NOT EXISTS", ddl, re.IGNORECASE))
        + len(re.findall(r"CREATE (?:OR REPLACE )?VIEW", ddl, re.IGNORECASE))
    )


def _debezium_source() -> str:
    return (REPO_ROOT / "code_etl/cdc/register_connectors.py").read_text(encoding="utf-8")


def _debezium_connectors(_manifest: dict) -> int:
    return len(re.findall(r'"name":\s*"banking-[\w-]+"', _debezium_source()))


def _debezium_topics(_manifest: dict) -> int:
    """
    1 topic / bảng. Đọc CONFIG (table.include.list), không đọc docstring —
    docstring đầu file ghi 8/3/5, sai so với config 6/3/3.
    """
    src = _debezium_source()
    total = 0
    for block in re.findall(r'"table\.include\.list":\s*\((.*?)\)', src, re.DOTALL):
        joined = "".join(re.findall(r'"([^"]*)"', block))
        total += len([t for t in joined.split(",") if t.strip()])
    return total


def _airflow_dag_sources() -> list[str]:
    return [
        p.read_text(encoding="utf-8")
        for p in sorted((REPO_ROOT / "airflow/dags").rglob("*.py"))
        if p.name != "__init__.py"
    ]


def _airflow_dag_files(_manifest: dict) -> int:
    return sum(1 for src in _airflow_dag_sources() if re.search(r"\bDAG\(", src))


def _airflow_dag_objects(_manifest: dict) -> int:
    return sum(len(re.findall(r"(?:with\s+DAG\(|=\s*DAG\()", src)) for src in _airflow_dag_sources())


def _test_functions(_manifest: dict) -> int:
    return sum(
        len(re.findall(r"^\s*def test_", p.read_text(encoding="utf-8"), re.MULTILINE))
        for p in sorted((REPO_ROOT / "tests").rglob("*.py"))
    )


def _pytest_collected_nodes(_manifest: dict) -> int | None:
    """Cần chạy pytest — trả None nếu không chạy được, không làm hỏng collect."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "--collect-only", "-q", "tests/"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=300, check=False,
        )
        match = re.search(r"(\d+)\s+tests? collected", result.stdout)
        return int(match.group(1)) if match else None
    except (OSError, subprocess.SubprocessError):
        return None


def _compose_services(manifest: dict) -> tuple[int, int, int]:
    compose = yaml.safe_load(
        (REPO_ROOT / "docker/docker-compose.yml").read_text(encoding="utf-8")
    )
    services = list(compose.get("services", {}))
    one_shot_declared = set(
        manifest["metrics"]["platform"]["docker_services"]["one_shot_services"]
    )
    one_shot = [s for s in services if s in one_shot_declared]
    return len(services), len(services) - len(one_shot), len(one_shot)


def _dq_check_types(_manifest: dict) -> int:
    src = (REPO_ROOT / "code_etl/shared/ops/data_quality.py").read_text(encoding="utf-8")
    block = re.search(r"CHECK_DISPATCH\s*=\s*\{(.*?)\n\}", src, re.DOTALL)
    return len(re.findall(r'"\w+":', block.group(1))) if block else 0


def collect_repo_metrics(manifest: dict) -> dict[str, Any]:
    """Trả về {dotted_path_trong_metrics: value} cho mọi metric static."""
    compose_defined, long_running, one_shot = _compose_services(manifest)
    simple: dict[str, Callable[[dict], Any]] = {
        "source.source_datasets.value": _bronze_ingestion_workloads,
        "bronze.batch_tables.value": _bronze_ingestion_workloads,
        "bronze.cdc_tables.value": _cdc_stream_configs,
        "cdc.bronze_cdc_tables.value": _cdc_stream_configs,
        "gold.tables.value": _gold_ddl_tables,
        "serving.gold_objects_declared.value": _gold_ddl_objects,
        "cdc.connectors.value": _debezium_connectors,
        "cdc.topics.value": _debezium_topics,
        "platform.airflow_dag_files.value": _airflow_dag_files,
        "platform.airflow_dag_objects.value": _airflow_dag_objects,
        "platform.automated_tests.test_functions.value": _test_functions,
        "platform.automated_tests.collected_pytest_nodes.value": _pytest_collected_nodes,
        "platform.dq_check_types.value": _dq_check_types,
    }
    values = {path: fn(manifest) for path, fn in simple.items()}
    values.update({
        "silver.dimensions.total.value": _silver_dims(None),
        "silver.dimensions.scd_type_1.value": _silver_dims("scd_type1"),
        "silver.dimensions.scd_type_2.value": _silver_dims("scd_type2"),
        "silver.facts.total.value": len(_load_yaml_files("code_etl/silver/facts/*.yml")),
        "silver.current_state_tables.total.value":
            len(_load_yaml_files("code_etl/cdc/consolidation/config/*.yml")),
        "platform.data_contracts.value": len(list(REPO_ROOT.glob("governance/datasets/*.yaml"))),
        "platform.dbt_models.value": len(list(REPO_ROOT.glob("dbt/models/serving/*.sql"))),
        "platform.docker_services.compose_defined.value": compose_defined,
        "platform.docker_services.long_running.value": long_running,
        "platform.docker_services.one_shot.value": one_shot,
    })
    return values


def collect_build_metadata() -> dict:
    """
    Provenance PHẢI phản ánh sự thật, không bao giờ bị cờ CLI viết lại.

    Trước đây hàm này nhận `allow_dirty` và ghi `git_dirty: False` khi cây bẩn
    mà người chạy truyền `--allow-dirty`. Nghĩa là tác dụng DUY NHẤT của cờ đó
    là làm manifest nói dối về chính nguồn gốc của nó — trong đúng script chịu
    trách nhiệm promote canonical. Cờ được phép quyết định CÓ CHẠY hay không;
    nó không được phép quyết định SỰ THẬT là gì.
    """
    def git(*args: str) -> str | None:
        try:
            return subprocess.run(
                ["git", *args], cwd=REPO_ROOT, capture_output=True,
                text=True, timeout=30, check=True,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return None

    status = git("status", "--porcelain")
    dirty = bool(status) if status is not None else None
    return {
        "git_commit": git("rev-parse", "HEAD"),
        "git_branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "git_dirty": dirty,
        "portfolio_release": git("describe", "--tags", "--abbrev=0"),
    }


# =============================================================================
# verification scope
# =============================================================================

def scope_skips(manifest: dict) -> list[str]:
    """
    Prefix bị bỏ qua theo scope hiện tại.

    `not_collected` KHÔNG phải `verified`: query bị bỏ qua thì metric vẫn null
    và invariant tương ứng đi vào `verification.skipped` — được ghi tên rõ ràng,
    không âm thầm PASS. Canonical manifest cuối cùng phải chạy scope `full`.
    """
    scope = manifest["manifest"]["runtime"]["verification_scope"]
    scopes = manifest["manifest"]["verification_scopes"]
    if scope not in scopes:
        raise ValueError(f"verification_scope không hợp lệ: {scope}")
    return scopes[scope]["skips"]


def _in_scope(name: str, skips: list[str]) -> bool:
    return not any(name.startswith(prefix) for prefix in skips)


# =============================================================================
# render_query_bundle
# =============================================================================

class Query:
    __slots__ = ("id", "phase", "sql")

    def __init__(self, qid: str, sql: str, phase: int):
        self.id, self.sql, self.phase = qid, sql, phase

    def __repr__(self) -> str:
        return f"Query({self.id!r}, phase={self.phase})"


# Thứ tự thực thi: rẻ và quyết định trước, đắt sau.
# Snapshot D3 không tồn tại thì dừng trước khi chạy 30 query còn lại.
PHASES: list[tuple[int, tuple[str, ...]]] = [
    (1, ("environment.",)),
    (2, ("snapshot.",)),
    (3, ("bronze.partitions_present",)),
    (4, ("bronze.snapshot_rows", "silver.snapshot_rows", "cdc.current_state")),
    (5, ("silver.scd2", "gold.grain_checks", "gold.stale_current_tables")),
    (6, ("transaction_scale.", "gold.reconciliation")),
]


def _phase_of(qid: str) -> int:
    for phase, prefixes in PHASES:
        if any(qid.startswith(p) for p in prefixes):
            return phase
    return 6


def render_query_bundle(manifest: dict, cob_dt: str, sql_path: Path = SQL_PATH) -> list[Query]:
    """
    Parse marker `--@id`, thay :cob_dt, nở template.

    gold.grain_checks.TEMPLATE có `{table}` — nở thành 1 query cho mỗi bảng khai
    báo trong manifest thay vì copy 9 lần trong file SQL.
    """
    text = sql_path.read_text(encoding="utf-8")
    blocks = re.split(r"^--@id\s+(\S+)\s*$", text, flags=re.MULTILINE)[1:]

    # Spark gọi warehouse là `lakehouse`, Trino gọi là `iceberg`. Bundle chạy
    # trên Trino nên phải dùng trino_catalog, không phải tên trong ETL YAML.
    env = manifest["manifest"]["environment"]
    catalog = env["trino_catalog"]
    # Business timezone của contract. Sau timezone migration đây KHÔNG còn là
    # workaround bắt chước Spark: cả hai engine chạy session UTC và cùng derive
    # ngày nghiệp vụ tường minh theo business timezone này.
    business_tz = env["business_timezone"]

    def _render(sql: str) -> str:
        return (sql.replace(":catalog", catalog)
                   .replace(":business_tz", business_tz)
                   .replace(":cob_dt", cob_dt))

    queries: list[Query] = []
    for qid, body in zip(blocks[::2], blocks[1::2], strict=True):
        sql = body.split(";")[0].strip()
        if not sql:
            continue
        if qid.endswith(".TEMPLATE"):
            prefix = qid[: -len(".TEMPLATE")]
            for table in get_path(manifest, f"metrics.{prefix}") or {}:
                queries.append(Query(
                    f"{prefix}.{table}",
                    _render(sql.replace("{table}", table)),
                    _phase_of(prefix),
                ))
        else:
            queries.append(Query(qid, _render(sql), _phase_of(qid)))

    queries.sort(key=lambda q: q.phase)
    return queries


# =============================================================================
# collect_trino_metrics
# =============================================================================

class MetricQueryError(RuntimeError):
    """Lỗi query kèm đủ ngữ cảnh để sửa ngay, không phải stack trace chung chung."""

    def __init__(self, query: Query, cob_dt: str, cause: str):
        super().__init__(
            "\n".join([
                "",
                "Metric query failed",
                "",
                f"  id:      {query.id}",
                f"  phase:   {query.phase}",
                f"  cob_dt:  {cob_dt}",
                "",
                "  Trino error:",
                *(f"    {line}" for line in str(cause).splitlines()),
                "",
                "  Rendered SQL:",
                *(f"    {line}" for line in query.sql.splitlines()),
                "",
            ])
        )
        self.query_id = query.id


class TrinoClient:
    """Client tối thiểu qua REST API. Test inject fake thay cho class này."""

    def __init__(self, host: str = "localhost", port: int = 8085, user: str = "admin"):
        self.url = f"http://{host}:{port}/v1/statement"
        self.user = user

    def query(self, sql: str) -> list[dict]:
        request = urllib.request.Request(
            self.url, data=sql.encode("utf-8"),
            headers={"Content-Type": "text/plain", "X-Trino-User": self.user},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            result = json.loads(response.read().decode("utf-8"))

        columns, rows = [], []
        while True:
            if "columns" in result and "data" in result:
                columns = [c["name"] for c in result["columns"]]
                rows = result["data"]
            if result.get("stats", {}).get("state") == "FAILED":
                raise RuntimeError(result.get("error", {}).get("message", "unknown"))
            next_uri = result.get("nextUri")
            if not next_uri:
                break
            with urllib.request.urlopen(
                urllib.request.Request(next_uri, headers={"X-Trino-User": self.user}),
                timeout=120,
            ) as response:
                result = json.loads(response.read().decode("utf-8"))

        return [dict(zip(columns, row, strict=True)) for row in rows]


def collect_trino_metrics(
    queries: list[Query],
    client: Any,
    cob_dt: str,
    stop_after_phase: int | None = None,
    continue_on_error: bool = False,
) -> tuple[dict[str, dict], list[MetricQueryError]]:
    """
    Trả về (results, failures).

    continue_on_error=True: chạy hết mọi query rồi báo cáo TẤT CẢ lỗi một lượt.
    Dùng cho lần runtime-validate đầu tiên — dừng ở lỗi đầu nghĩa là mỗi vòng
    chỉ lộ được một mismatch, phải chạy lại nhiều lần vô ích.
    Metric của query fail vẫn để null nên invariant vẫn FAIL, không bị che.
    """
    results: dict[str, dict] = {}
    failures: list[MetricQueryError] = []
    for query in queries:
        if stop_after_phase is not None and query.phase > stop_after_phase:
            break
        try:
            rows = client.query(query.sql)
        except Exception as exc:
            error = MetricQueryError(query, cob_dt, str(exc))
            if not continue_on_error:
                raise error from exc
            failures.append(error)
            continue
        results[query.id] = rows[0] if rows else {}
    return results, failures


# =============================================================================
# collect_manual_metrics
# =============================================================================

def collect_manual_metrics(manifest: dict) -> dict[str, Any]:
    """
    Metric manual được GIỮ NGUYÊN từ contract. Generator không tính lại và
    không set null — cdc_freshness là benchmark người đo, không query được.
    """
    return {
        path: copy.deepcopy(node)
        for path, node in _iter_metric_nodes(manifest["metrics"])
        if node.get("metric_type") == "manual"
    }


def _iter_metric_nodes(root: Any, path: str = ""):
    if isinstance(root, dict):
        if "metric_type" in root:
            yield path, root
        for key, child in root.items():
            yield from _iter_metric_nodes(child, f"{path}.{key}" if path else key)


# =============================================================================
# evaluate_invariants
# =============================================================================

OPERATORS: dict[str, Callable[[Any, Any], bool]] = {
    "eq": lambda a, b: a == b,
    "ne": lambda a, b: a != b,
    "lt": lambda a, b: a < b,
    "le": lambda a, b: a <= b,
    "gt": lambda a, b: a > b,
    "ge": lambda a, b: a >= b,
}


def evaluate_invariants(
    manifest: dict, skips: list[str] | None = None
) -> tuple[list[str], list[str], list[str]]:
    """
    Trả về (errors, warnings, skipped).

    Metric chưa đo (None) coi là FAIL, KHÔNG phải pass — trừ khi metric nằm
    ngoài verification_scope, khi đó nó vào `skipped` kèm tên invariant.
    """
    errors: list[str] = []
    warnings: list[str] = []
    skipped: list[str] = []
    skips = skips or []

    for inv_id, inv in manifest["invariants"].items():
        metric_name = inv["metric"].removeprefix("metrics.")
        if not _in_scope(metric_name, skips):
            skipped.append(f"{inv_id} (ngoài verification_scope)")
            continue
        bucket = errors if inv["severity"] == "error" else warnings

        if inv["metric"] == "__all_static__":
            warnings.extend(_static_drift(manifest))
            continue

        for metric_path in expand_wildcards(manifest, inv["metric"]):
            actual = get_path(manifest, metric_path)
            if "compare_to" in inv:
                compare_path = _mirror_wildcard(inv["metric"], metric_path, inv["compare_to"])
                expected = get_path(manifest, compare_path)
                label = compare_path
            else:
                expected, label = inv["expected"], repr(inv["expected"])

            if actual is MISSING or actual is None or expected is MISSING or expected is None:
                bucket.append(f"{inv_id}: {metric_path} chưa có giá trị (actual={actual!r})")
                continue
            if not OPERATORS[inv["operator"]](actual, expected):
                bucket.append(
                    f"{inv_id}: {metric_path}={actual!r} {inv['operator']} {label} → FAIL"
                )

    return errors, warnings, skipped


def _mirror_wildcard(pattern: str, resolved: str, compare_pattern: str) -> str:
    """`a.*.x` + `a.foo.x` + `a.*.y` → `a.foo.y`."""
    if "*" not in pattern:
        return compare_pattern
    idx = pattern.split(".").index("*")
    return ".".join(
        part if i != idx else resolved.split(".")[idx]
        for i, part in enumerate(compare_pattern.split("."))
    )


def _static_drift(manifest: dict) -> list[str]:
    out = []
    for path, node in _iter_metric_nodes(manifest["metrics"]):
        if node.get("metric_type") != "static":
            continue
        value, declared = node.get("value"), node.get("declared")
        if value is not None and declared is not None and value != declared:
            out.append(f"README drift: {path} value={value} declared={declared}")
    return out


# =============================================================================
# Assembly + output
# =============================================================================

def apply_results(
    manifest: dict, static: dict, runtime: dict, manual: dict, build: dict, cob_dt: str
) -> dict:
    out = copy.deepcopy(manifest)

    out["manifest"]["build"].update(build)
    out["manifest"]["runtime"]["generated_at_utc"] = (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    out["manifest"]["runtime"]["requested_cob_dt"] = cob_dt
    out["manifest"]["snapshot"]["requested_cob_dt"] = cob_dt

    for path, value in static.items():
        set_path(out["metrics"], path, value)

    for qid, row in runtime.items():
        # snapshot.* và environment.* nằm dưới `manifest:`, không phải `metrics:`
        if qid.startswith(("snapshot.", "environment.")):
            section, key = qid.split(".", 1)
            if key in out["manifest"][section]:
                out["manifest"][section][key] = row.get("value")
            continue
        # Ba hình dạng target khác nhau, phải phân biệt:
        #   dict          → merge từng cột của row vào node
        #   leaf scalar   → gán thẳng (vd silver.scd2.*.overlapping_intervals,
        #                   khai báo trong skeleton là `overlapping_intervals: null`)
        #   chưa tồn tại  → tạo node mới dạng {value: ...}
        node = get_path(out["metrics"], qid)
        if isinstance(node, dict):
            for column, value in row.items():
                node[column] = value
        elif not row:
            continue
        elif node is not MISSING:
            set_path(out["metrics"], qid, next(iter(row.values())))
        else:
            set_path(out["metrics"], f"{qid}.value", next(iter(row.values())))

    for path, node in manual.items():
        set_path(out["metrics"], path, node)

    snapshot = out["manifest"]["snapshot"]
    snapshot["layers_aligned"] = bool(
        snapshot["requested_cob_dt"]
        and snapshot["requested_cob_dt"]
        == snapshot["bronze_max_cob_dt"]
        == snapshot["silver_max_cob_dt"]
        == snapshot["gold_max_cob_dt"]
    )
    return out


def write_run_artifact(manifest: dict) -> Path:
    """LUÔN ghi, kể cả khi verification fail — giữ forensic evidence."""
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    stamp = (manifest["manifest"]["runtime"]["generated_at_utc"] or "unknown").replace(
        ":", ""
    ).replace("-", "")
    path = GENERATED_DIR / f"metrics-run-{stamp}.yaml"
    path.write_text(
        yaml.safe_dump(manifest, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )
    return path


def promote_canonical_if_verified(
    manifest: dict, errors: list[str], allow_dirty: bool = False
) -> bool:
    """
    Canonical CHỈ được ghi đè khi không còn blocking error VÀ cây sạch.

    Cây bẩn nghĩa là số đo không quy được về một commit nào: `git_commit` trỏ
    HEAD trong khi thứ được đo là HEAD cộng các sửa đổi chưa commit. Đó là một
    manifest không tái lập được. `--allow-dirty` nới điều kiện này cho lần chạy
    thăm dò ở local, và khi đó `git_dirty: true` vẫn được ghi trung thực.
    """
    if errors:
        return False
    if manifest["manifest"]["build"].get("git_dirty") and not allow_dirty:
        print(
            "Từ chối promote canonical: worktree bẩn nên số đo không quy được "
            "về một commit. Commit thay đổi rồi chạy lại, hoặc dùng --allow-dirty "
            "nếu chấp nhận một manifest không tái lập được.",
            file=sys.stderr,
        )
        return False
    tmp = MANIFEST_PATH.with_suffix(".yaml.tmp")
    tmp.write_text(
        yaml.safe_dump(manifest, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )
    tmp.replace(MANIFEST_PATH)   # atomic
    return True


# =============================================================================
# CLI
# =============================================================================

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate/verify metrics manifest")
    parser.add_argument("--cob-dt", help="Snapshot cần đo (YYYY-MM-DD)")
    parser.add_argument("--validate-contract", action="store_true",
                        help="Chỉ kiểm tra contract tự nhất quán rồi thoát")
    parser.add_argument("--render-sql", action="store_true",
                        help="In query đã render (gồm template đã nở) rồi thoát — không cần Trino")
    parser.add_argument("--scope", choices=["batch", "cdc", "full"],
                        help="Ghi đè verification_scope trong contract")
    parser.add_argument("--continue-on-error", action="store_true",
                        help="Chạy hết mọi query rồi báo cáo TẤT CẢ lỗi một lượt "
                             "(dùng cho lần runtime-validate đầu tiên)")
    parser.add_argument("--collect-only", action="store_true",
                        help="Thu evidence + ghi run artifact, KHÔNG promote canonical")
    parser.add_argument("--allow-dirty", action="store_true",
                        help="Cho phép promote canonical từ worktree bẩn (chỉ dùng khi "
                             "chạy thăm dò ở local). git_dirty VẪN được ghi đúng sự thật.")
    parser.add_argument("--output", type=Path, help="Ghi kết quả ra file này thay vì canonical")
    parser.add_argument("--trino-host", default="localhost")
    parser.add_argument("--trino-port", type=int, default=8085)
    args = parser.parse_args(argv)

    manifest = load_contract()

    contract_errors = validate_contract(manifest)
    if contract_errors:
        print("Contract không hợp lệ:", file=sys.stderr)
        for err in contract_errors:
            print(f"  - {err}", file=sys.stderr)
        return 2
    if args.validate_contract:
        print("Contract hợp lệ.")
        return 0

    if not args.cob_dt:
        parser.error("--cob-dt là bắt buộc trừ khi dùng --validate-contract")

    if args.render_sql:
        for query in render_query_bundle(manifest, args.cob_dt):
            print(f"--@id {query.id}  (phase {query.phase})")
            print(query.sql)
            print()
        return 0

    if args.scope:
        manifest["manifest"]["runtime"]["verification_scope"] = args.scope
    skips = scope_skips(manifest)
    scope_name = manifest["manifest"]["runtime"]["verification_scope"]

    static = collect_repo_metrics(manifest)
    manual = collect_manual_metrics(manifest)
    build = collect_build_metadata()
    queries = [q for q in render_query_bundle(manifest, args.cob_dt) if _in_scope(q.id, skips)]

    client = TrinoClient(args.trino_host, args.trino_port)
    try:
        runtime, query_failures = collect_trino_metrics(
            queries, client, args.cob_dt, continue_on_error=args.continue_on_error
        )
    except MetricQueryError as exc:
        print(exc, file=sys.stderr)
        return 3

    result = apply_results(manifest, static, runtime, manual, build, args.cob_dt)

    # Ghi rõ nhánh nào KHÔNG được thu, kèm lý do — để người đọc artifact không
    # nhầm "không có số" với "đã kiểm và đạt".
    if not _in_scope("cdc.", skips):
        result["metrics"]["cdc"]["collection_status"] = "not_collected"
        result["metrics"]["cdc"]["collection_reason"] = (
            f"verification_scope={scope_name}"
        )
    else:
        result["metrics"]["cdc"]["collection_status"] = "collected"

    errors, warnings, skipped = evaluate_invariants(result, skips)

    # Query fail là blocking error: metric của nó để null nên invariant cũng
    # fail, nhưng ghi riêng ra đây để triage nhanh theo --@id.
    errors = [f"query_failed: {exc.query_id}" for exc in query_failures] + errors

    result["manifest"]["verification"]["errors"] = errors
    result["manifest"]["verification"]["warnings"] = warnings
    result["manifest"]["verification"]["skipped"] = skipped
    if errors:
        status = "failed"
    elif scope_name != "full":
        # Chỉ scope `full` mới được gọi là verified. Một lượt batch sạch là
        # "verified_batch", không phải bằng chứng toàn platform.
        status = f"verified_{scope_name}" if not warnings else f"warning_{scope_name}"
    else:
        status = "warning" if warnings else "verified"
    result["manifest"]["verification"]["status"] = status

    artifact = write_run_artifact(result)
    print(f"Run artifact: {artifact.relative_to(REPO_ROOT)}")

    for failure in query_failures:
        print(failure, file=sys.stderr)

    for item in skipped:
        print(f"  SKIP  {item}")

    for warning in warnings:
        print(f"  WARN  {warning}")
    for error in errors:
        print(f"  ERROR {error}", file=sys.stderr)

    if args.output:
        args.output.write_text(
            yaml.safe_dump(result, sort_keys=False, allow_unicode=True), encoding="utf-8"
        )
        return 1 if errors else 0

    if args.collect_only:
        print("--collect-only: canonical không đổi.")
        return 1 if errors else 0

    if promote_canonical_if_verified(result, errors, args.allow_dirty):
        print(f"Canonical promoted: status={result['manifest']['verification']['status']}")
        return 0

    print(
        f"KHÔNG promote canonical — {len(errors)} blocking invariant fail. "
        "Canonical giữ nguyên; xem run artifact để triage.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
