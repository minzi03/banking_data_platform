"""
Contract tests cho Airflow DAG — bắt các lỗi chỉ lộ ra khi rebuild sạch.

Hai lỗi dưới đây đều đã xảy ra thật và đều KHÔNG bị test nào bắt:

1. `cdc_consolidation_dag` gọi `spark-submit` ngay trong container Airflow.
   Image Airflow chỉ có wheel pyspark, không có Iceberg runtime jar, nên job
   chết với `Cannot find catalog plugin class for catalog 'lakehouse'`. Mọi
   DAG Spark khác đều `docker exec` vào spark-worker; đây là ngoại lệ duy nhất.

2. Ba conn_id `postgres-core-banking` / `-card-crm` / `-digital-banking` không
   được init nào tạo. Vì DAG Bronze đọc connection ở top-level để sinh task,
   thiếu conn_id làm DAG *lỗi import* — nó biến mất khỏi UI thay vì hiện đỏ,
   nên nhìn lướt qua tưởng hệ thống bình thường.

Test tĩnh, chạy trong CI, không cần stack.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"
COMPOSE = REPO_ROOT / "docker" / "docker-compose.yml"

DAG_FILES = sorted(p for p in DAGS_DIR.rglob("*.py") if p.name != "__init__.py")

SPARK_WORKER_EXEC = "docker exec"


def _dag_id(path: Path) -> str:
    return str(path.relative_to(DAGS_DIR)).replace("\\", "/")


@pytest.fixture(scope="module")
def compose_text() -> str:
    return COMPOSE.read_text(encoding="utf-8")


class TestSparkSubmitStaysOnTheWorker:
    """Spark job phải chạy trong container có Iceberg jar, không phải Airflow."""

    @pytest.mark.parametrize("dag_path", DAG_FILES, ids=_dag_id)
    def test_spark_submit_goes_through_docker_exec(self, dag_path):
        text = dag_path.read_text(encoding="utf-8")

        offenders = []
        for line_no, line in enumerate(text.splitlines(), start=1):
            if "spark-submit" not in line:
                continue
            # Bỏ qua comment/docstring nhắc tên lệnh chứ không gọi nó.
            stripped = line.strip()
            if stripped.startswith("#") or "Kill any running" in line:
                continue
            # `docker exec` có thể nằm ở dòng trước trong chuỗi nối nhiều dòng;
            # xét cả cửa sổ 3 dòng trước đó.
            window = "\n".join(text.splitlines()[max(0, line_no - 4) : line_no])
            if SPARK_WORKER_EXEC not in window:
                offenders.append(f"{_dag_id(dag_path)}:{line_no}: {stripped}")

        assert not offenders, (
            "spark-submit chạy trong container Airflow (không có Iceberg jar):\n  "
            + "\n  ".join(offenders)
            + "\nDùng: /usr/bin/docker exec banking-spark-worker-1 /opt/spark/bin/spark-submit"
        )

    @pytest.mark.parametrize("dag_path", DAG_FILES, ids=_dag_id)
    def test_no_catalog_credentials_redeclared_in_dag(self, dag_path):
        """
        Catalog/MinIO config sống trong spark-defaults.conf của image worker.
        Dán lại vào DAG vừa trùng lặp vừa đưa secret vào file orchestration.
        """
        text = dag_path.read_text(encoding="utf-8")
        leaked = re.findall(r"s3\.secret-access-key=\S+", text)
        assert not leaked, f"{_dag_id(dag_path)}: khai báo lại secret catalog trong DAG: {leaked}"


class TestEveryConnIdIsProvisioned:
    """conn_id dùng trong DAG phải được airflow-init tạo, nếu không DAG chết."""

    @staticmethod
    def _referenced_conn_ids() -> set[str]:
        pattern = re.compile(r"""(?:conn_id|CONN_ID)\s*=\s*["']([^"']+)["']""")
        found: set[str] = set()
        for path in DAG_FILES:
            found.update(pattern.findall(path.read_text(encoding="utf-8")))
        # spark_default là connection Airflow tạo sẵn, không do init này quản.
        return {c for c in found if c.startswith("postgres")}

    @staticmethod
    def _provisioned_conn_ids(compose_text: str) -> set[str]:
        """
        Đọc cả hai dạng trong compose: literal và vòng lặp shell.

            airflow connections add 'postgres-etl'
            for src in core-banking card-crm; do
              airflow connections add "postgres-$${src}"
        """
        creates = r"""(?:airflow connections add|upsert_pg_conn)"""
        provisioned = set(re.findall(creates + r""" ["']([a-z0-9-]+)["']""", compose_text))
        loop_vars = dict((var, items.split()) for var, items in re.findall(r"for (\w+) in ([^;\n]+); do", compose_text))
        for tmpl in re.findall(creates + r""" ["']([^"']*\$\$\{\w+\}[^"']*)["']""", compose_text):
            var = re.search(r"\$\$\{(\w+)\}", tmpl).group(1)
            for item in loop_vars.get(var, []):
                provisioned.add(re.sub(r"\$\$\{\w+\}", item, tmpl))
        return provisioned

    def test_all_referenced_conn_ids_are_created(self, compose_text):
        referenced = self._referenced_conn_ids()
        provisioned = self._provisioned_conn_ids(compose_text)

        assert referenced, "không tìm thấy conn_id nào trong DAG — regex hỏng?"
        missing = sorted(referenced - provisioned)
        assert not missing, (
            f"conn_id được DAG dùng nhưng airflow-init không tạo: {missing}\n"
            f"init đang tạo: {sorted(provisioned)}\n"
            "Trên môi trường sạch, thiếu conn_id ở top-level làm DAG lỗi import."
        )

    def test_connections_are_upserted_and_failures_are_loud(self, compose_text):
        """
        `connections add … || true` trên Airflow DB đã có connection: add lỗi, bị nuốt, và
        connection giữ mật khẩu cũ sau khi POSTGRES_PASSWORD được đổi. Mỗi add phải đi sau
        một delete của cùng conn_id, và lỗi add không được nuốt.
        """
        adds = list(
            re.finditer(
                r"airflow connections add (\S+)(.*?)(?=\n\s*(?:\}|airflow|upsert_pg_conn|for |done|echo))",
                compose_text,
                re.DOTALL,
            )
        )
        assert adds, "không tìm thấy `airflow connections add` — regex hỏng?"
        for m in adds:
            conn = m.group(1)
            before = compose_text[: m.start()].rstrip().splitlines()[-1]
            assert f"airflow connections delete {conn}" in before, f"add {conn} không đi sau delete — không phải upsert"
            assert "|| true" not in m.group(2), f"add {conn} nuốt lỗi bằng `|| true`"


# ---------------------------------------------------------------------------
# Quyền của workflow benchmark
# ---------------------------------------------------------------------------

BENCHMARK_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark.yml"


def _benchmark_policy_violations() -> dict[str, str | None]:
    """
    Trả về {tên luật: mô tả vi phạm hoặc None}.

    `Performance Benchmark` chạy theo lịch. Trước đây nó xin `contents: write`
    và `git push` baseline thẳng lên `main` — đo lường và sửa source of truth
    gộp vào một workflow. Nhánh ghi đó nằm sau bước dựng stack vốn fail suốt
    nhiều tuần nên chưa từng chạy, và nó kết thúc bằng `git push || true`, nuốt
    luôn lỗi. Cập nhật baseline nếu cần phải đi đường bot branch → PR → CI →
    review như mọi thay đổi source khác.

    Gộp ba luật vào một hàm test có parametrize: mỗi luật vẫn là một test case
    riêng để đọc được khi hỏng, nhưng `automated_tests` (đếm `def test_*`) chỉ
    tăng một.
    """
    raw = BENCHMARK_WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.safe_load(raw)
    # Bỏ comment: phần giải thích ở đầu file có nhắc `git push` và
    # `contents: write` như mô tả lịch sử, không phải cấu hình.
    code = "\n".join(line for line in raw.splitlines() if not line.lstrip().startswith("#"))

    permissions = workflow.get("permissions") or {}
    write_scopes = sorted(k for k, v in permissions.items() if v == "write")

    pushes = [line.strip() for line in code.splitlines() if re.search(r"\bgit\s+(push|commit)\b", line)]

    steps = workflow["jobs"]["benchmark"]["steps"]
    uploads = [s for s in steps if str(s.get("uses", "")).startswith("actions/upload-artifact")]

    return {
        "no_write_permission": (f"workflow xin quyền ghi: {write_scopes}" if write_scopes else None),
        "no_repository_mutation": ("workflow còn lệnh ghi vào repo:\n  " + "\n  ".join(pushes) if pushes else None),
        "publishes_artifact": (None if uploads else "workflow không upload artifact nào — đo mà không báo cáo"),
    }


@pytest.mark.parametrize("rule", ["no_write_permission", "no_repository_mutation", "publishes_artifact"])
def test_scheduled_benchmark_measures_without_mutating_the_repository(rule):
    violation = _benchmark_policy_violations()[rule]
    assert violation is None, violation


def _daily_hour(cron: str | None) -> int | None:
    m = re.fullmatch(r"\d+ (\d+) \* \* \*", cron or "")
    return int(m.group(1)) if m else None


def _dag_schedules() -> dict[str, tuple[str, str | None, list[str]]]:
    """DAG_ID → (file, schedule_interval, các job_name mà sensor của nó chờ cờ)."""
    dags = {}
    for path in DAG_FILES:
        text = path.read_text(encoding="utf-8")
        dag_id = re.search(r'^DAG_ID\s*=\s*"(\w+)"', text, re.MULTILINE)
        if not dag_id:
            continue
        sched = re.search(r"""schedule_interval\s*=\s*(?:"([^"]*)"|'([^']*)'|None)""", text)
        cron = (sched.group(1) or sched.group(2)) if sched else None
        # Upstream = đối số đầu của upstream_success_sql(...) — chuỗi hằng hoặc hằng *_FLAG.
        waits = set(re.findall(r'upstream_success_sql\(\s*"(\w+)"', text))
        for const in re.findall(r"upstream_success_sql\(\s*([A-Z_]+_FLAG)\b", text):
            value = re.search(rf'^{const}\s*=\s*"(\w+)"', text, re.MULTILINE)
            waits.update([value.group(1)] if value else [])
        for var in re.findall(r"_check_dag_flag_sql\(\s*\"(\w+)\"", text):
            waits.add(var)
        dags[dag_id.group(1)] = (_dag_id(path), cron, sorted(waits))
    return dags


# Chạy tay có chủ đích: churn ML cần ml/requirements.txt trên worker, không chạy hằng ngày
# (audit 2026-09-30). Sensor của nó vẫn chờ SERVING_COMPLETE khi được trigger.
MANUAL_DAGS = {"ops_ml_churn_dag"}


class TestFlagWaitingDagsAreScheduled:
    """
    DAG chờ cờ `flag_job_etl` của DAG khác cho `{{ ds }}` phải TỰ chạy, và chạy SAU DAG nó chờ.

    `ops_lineage_dag` có đủ sensor chờ silver/gold nhưng `schedule_interval=None` và không
    DAG nào trigger nó — lineage_log chỉ có dữ liệu khi có người bấm tay (TD-13).
    """

    DAGS = _dag_schedules()
    MANUAL_BY_DESIGN = MANUAL_DAGS
    WAITERS = sorted(d for d, (_, _, waits) in DAGS.items() if waits and d not in MANUAL_DAGS)

    def test_waiters_are_found(self):
        assert {"ops_lineage_dag", "ops_data_quality_dag"} <= set(self.WAITERS)

    def test_manual_exemptions_really_are_unscheduled(self):
        for dag_id in self.MANUAL_BY_DESIGN:
            assert self.DAGS[dag_id][1] is None, f"{dag_id} có lịch — bỏ khỏi MANUAL_BY_DESIGN"

    @pytest.mark.parametrize("dag_id", WAITERS)
    def test_waiter_runs_daily_after_what_it_waits_for(self, dag_id):
        path, cron, waits = self.DAGS[dag_id]
        hour = _daily_hour(cron)
        assert hour is not None, f"{path}: chờ cờ {waits} nhưng không có lịch hằng ngày (schedule_interval={cron!r})"
        for upstream in waits:
            if upstream not in self.DAGS:
                continue  # cờ không phải của một DAG (vd. SERVING_COMPLETE)
            up_hour = _daily_hour(self.DAGS[upstream][1])
            if up_hour is not None:
                assert hour >= up_hour, f"{path} chạy {hour}h, trước {upstream} ({up_hour}h) mà nó chờ"


# ---------------------------------------------------------------------------
# cob_dt: một định nghĩa cho mọi DAG (airflow/plugins/cob_dt.py)
# ---------------------------------------------------------------------------
# `{{ ds }}` là ngày UTC của logical_date. Với lịch theo giờ ICT, DAG chạy trước
# 07:00 nhận D-2, DAG chạy từ 07:00 nhận D-1 (render bằng Airflow 2.10.0 thật),
# nên dbt / DQ / PII chờ cờ của một ngày Gold chưa chạy và timeout mỗi ngày.

PLUGINS_DIR = REPO_ROOT / "airflow" / "plugins"


def _load_cob_dt_plugin():
    import importlib.util

    spec = importlib.util.spec_from_file_location("cob_dt_plugin", PLUGINS_DIR / "cob_dt.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestSingleCobDtDefinition:
    @pytest.mark.parametrize("dag_path", DAG_FILES, ids=_dag_id)
    def test_no_dag_uses_utc_ds(self, dag_path):
        text = dag_path.read_text(encoding="utf-8")
        offenders = [
            f"{_dag_id(dag_path)}:{n}: {line.strip()}"
            for n, line in enumerate(text.splitlines(), start=1)
            if re.search(r"\{\{\s*ds\s*\}\}|\bds_nodash\b|context\[['\"]ds['\"]\]", line)
            and not line.strip().startswith("#")
        ]
        assert not offenders, "Dùng cob_dt.COB_DT / cob_dt_from_context thay cho ds:\n  " + "\n  ".join(offenders)

    def test_flag_helpers_default_to_shared_cob_dt(self):
        text = (PLUGINS_DIR / "etl_flag.py").read_text(encoding="utf-8")
        assert "{{ ds }}" not in text
        assert "cob_dt: str = COB_DT" in text

    def _render(self, conf, interval_start):
        jinja2 = pytest.importorskip("jinja2")
        module = _load_cob_dt_plugin()

        class FakeDagRun:
            def __init__(self, conf):
                self.conf = conf

        return jinja2.Template(module.COB_DT).render(dag_run=FakeDagRun(conf), data_interval_start=interval_start)

    def test_conf_override_wins(self):
        assert self._render({"cob_dt": "2026-09-15"}, None) == "2026-09-15"

    def test_default_is_ict_date_of_interval_start(self):
        """2026-09-28 23:00 UTC = 2026-09-29 06:00 ICT → cob_dt 2026-09-29 (ds sẽ là 2026-09-28)."""
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo

        class PendulumLike:
            def __init__(self, dt):
                self.dt = dt

            def in_timezone(self, tz):
                return self.dt.astimezone(ZoneInfo(tz))

        start = PendulumLike(datetime(2026, 9, 28, 23, 0, tzinfo=timezone.utc))
        assert self._render({}, start) == "2026-09-29"
        assert self._render(None, start) == "2026-09-29"


class TestNoSparkSubmitOperator:
    """
    SparkSubmitOperator chạy spark-submit trong container Airflow (không có Iceberg
    jar) — cùng lỗi mà test docker-exec ở trên bắt, nhưng test đó chỉ tìm chuỗi
    "spark-submit" nên từng bỏ sót ops_pii_masking_daily_dag và ops_maintenance_weekly_dag.
    """

    @pytest.mark.parametrize("dag_path", DAG_FILES, ids=_dag_id)
    def test_dag_does_not_use_spark_submit_operator(self, dag_path):
        code = [ln for ln in dag_path.read_text(encoding="utf-8").splitlines() if not ln.strip().startswith("#")]
        assert not any("SparkSubmitOperator(" in ln or "import SparkSubmitOperator" in ln for ln in code), _dag_id(
            dag_path
        )


class TestCdcStreamingFitsTheWorker:
    """
    Standalone cấp core cho app theo `spark.cores.max` (spark-defaults: 6), không theo
    `spark.executor.instances`. Thiếu giới hạn, query CDC đầu lấy 6/8 core, query thứ
    hai 2, bốn query còn lại và consolidation WAITING vô hạn — Airflow vẫn báo success
    (đo trên stack 2026-09-30).
    """

    STREAMING_DAG = DAGS_DIR / "cdc" / "cdc_streaming_dag.py"
    CDC_CONFIGS = sorted((REPO_ROOT / "code_etl" / "cdc" / "config").glob("cdc_*.yml"))
    ENV_EXAMPLE = REPO_ROOT / "docker" / ".env.example"

    def test_each_query_caps_its_cores(self):
        caps = re.findall(r"spark\.cores\.max=(\d+)", self.STREAMING_DAG.read_text(encoding="utf-8"))
        assert caps, "cdc_streaming_dag không đặt --conf spark.cores.max cho query streaming"

    def test_all_queries_leave_room_for_consolidation(self):
        cap = int(re.findall(r"spark\.cores\.max=(\d+)", self.STREAMING_DAG.read_text(encoding="utf-8"))[0])
        worker_cores = int(re.search(r"^SPARK_WORKER_CORES=(\d+)", self.ENV_EXAMPLE.read_text(), re.M).group(1))
        used = cap * len(self.CDC_CONFIGS)
        assert len(self.CDC_CONFIGS) == 6
        assert used < worker_cores, (
            f"{len(self.CDC_CONFIGS)} query x {cap} core = {used} >= {worker_cores} core của worker"
        )


SCHEDULED_DAGS = [p for p in DAG_FILES if re.search(r'schedule_interval\s*=\s*"[^"]+"', p.read_text(encoding="utf-8"))]


def test_scheduled_dags_are_found():
    assert len(SCHEDULED_DAGS) >= 13, [_dag_id(p) for p in SCHEDULED_DAGS]


@pytest.mark.parametrize("dag_path", SCHEDULED_DAGS, ids=_dag_id)
def test_scheduled_dag_runs_one_at_a_time(dag_path):
    """
    Unpause một DAG có lịch tạo ngay lượt chạy cho khoảng gần nhất; DEMO_GUIDE trigger
    thêm lượt tay cho cùng cob_dt. Không giới hạn (mặc định 16), hai lượt cùng ghi đè một
    partition, cùng MERGE SCD2 một bảng dim (batch, 2026-09-30) hoặc cùng CREATE bảng
    sandbox (`ops_pii_masking_daily_dag`: "table already exists", 2026-09-30).
    """
    text = dag_path.read_text(encoding="utf-8")
    assert re.search(r"max_active_runs=1\b", text), f"{_dag_id(dag_path)} thiếu max_active_runs=1"


@pytest.mark.parametrize("dag_path", DAG_FILES, ids=_dag_id)
def test_flag_sensors_use_the_latest_flag(dag_path):
    """
    Sensor cờ phải đi qua etl_flag.upstream_success_sql (dòng cờ MỚI NHẤT là S). SQL tự viết
    "có dòng S nào" cho downstream chạy ngay khi chạy lại một cob_dt đã có S (2026-09-30).
    """
    lines = dag_path.read_text(encoding="utf-8").splitlines()
    code = "\n".join(ln for ln in lines if not ln.strip().startswith("#"))
    assert not re.search(r"status\s*=\s*'S'", code), f"{_dag_id(dag_path)} tự viết SQL kiểm cờ S"


@pytest.mark.parametrize(
    ("relative", "flag"),
    [("gold/gold_mart360_dag.py", "GOLD_COMPLETE_FLAG"), ("dbt/dbt_run_dag.py", "SERVING_COMPLETE_FLAG")],
)
def test_derived_completion_flag_is_reset_at_start(relative, flag):
    """GOLD_COMPLETE / SERVING_COMPLETE chỉ được ghi S lúc kết thúc; lượt chạy lại phải chèn R
    ngay đầu, nếu không S cũ vẫn là dòng mới nhất và consumer chạy trên dữ liệu đang ghi."""
    text = (DAGS_DIR / relative).read_text(encoding="utf-8")
    assert re.search(rf"make_start_flag_task\(\s*\"\w+_reset\", {flag}", text), f"{relative}: thiếu R cho {flag}"


@pytest.mark.parametrize(
    "relative",
    [
        "bronze/bronze_core_banking_dag.py",
        "bronze/bronze_card_crm_dag.py",
        "bronze/bronze_digital_banking_dag.py",
        "silver/silver_all_dag.py",
    ],
)
def test_backfill_arg_is_only_added_to_incremental_jobs(relative):
    """
    ADR-0018: lần nạp đầu truyền conf backfill_from. Job full_snapshot từ chối --backfill_from,
    nên DAG phải gắn tham số CÓ ĐIỀU KIỆN theo config, không gắn cho mọi task.
    """
    text = (DAGS_DIR / relative).read_text(encoding="utf-8")
    assert "BACKFILL_FROM_ARG" in text, f"{relative}: không truyền backfill_from"
    guarded = re.search(r'if\s+\(?config\["load"\]\["strategy"\]\s*==\s*"incremental"', text) or re.search(
        r'\.get\("incremental"\)', text
    )
    assert guarded, f"{relative}: BACKFILL_FROM_ARG gắn vô điều kiện"
