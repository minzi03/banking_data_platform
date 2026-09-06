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
            window = "\n".join(text.splitlines()[max(0, line_no - 4):line_no])
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
        assert not leaked, (
            f"{_dag_id(dag_path)}: khai báo lại secret catalog trong DAG: {leaked}"
        )


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
        provisioned = set(
            re.findall(r"""airflow connections add ["']([a-z0-9-]+)["']""", compose_text)
        )
        loop_vars = dict(
            (var, items.split())
            for var, items in re.findall(r"for (\w+) in ([^;\n]+); do", compose_text)
        )
        for tmpl in re.findall(
            r"""airflow connections add ["']([^"']*\$\$\{\w+\}[^"']*)["']""", compose_text
        ):
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
