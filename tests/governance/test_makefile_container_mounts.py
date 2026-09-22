"""
TD-8 Contract Test — Makefile exec targets phải trỏ vào container có mount repo
==============================================================================

`make seed` chạy generator trong container `postgres`, nơi không mount repo:
`/opt/project/data_generator/` không tồn tại ở đó, nên target chưa từng chạy
được (TD-8). TD-4 là cùng một hình dạng lỗi: bootstrap target chạy ở working
directory container không có.

Hai lần, cùng một nguyên nhân: Makefile chọn container mà không đối chiếu với
volume trong docker-compose.yml. Không test nào bắt được, vì lỗi chỉ lộ ra khi
có người thật gõ lệnh — CI không chạy `make seed`.

Test tĩnh: với mỗi `$(DC) exec` trong Makefile có tham chiếu đường dẫn repo,
service được chọn phải là service mount `..:/opt/project`.

Chạy: pytest tests/governance/test_makefile_container_mounts.py -v
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
MAKEFILE = REPO_ROOT / "Makefile"
COMPOSE_FILE = REPO_ROOT / "docker" / "docker-compose.yml"

REPO_MOUNT = "/opt/project"

# Dấu hiệu một lệnh đang chạy code lấy từ repo chứ không phải binary có sẵn
# trong image (psql, trino, spark-class...).
REPO_PATH_MARKERS = (
    REPO_MOUNT,
    "code_etl/",
    "data_generator/",
    "scripts/",
    "governance/",
)

# Option của `docker compose exec` có nhận giá trị đứng sau.
_OPTS_WITH_VALUE = {"-w", "--workdir", "-u", "--user", "-e", "--env", "--index"}


def _load_compose() -> dict:
    if not COMPOSE_FILE.exists():
        pytest.skip(f"{COMPOSE_FILE} không tồn tại")
    return yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))


def _services_mounting_repo() -> set[str]:
    """Service nào mount repo root vào /opt/project."""
    compose = _load_compose()
    mounted = set()
    for name, spec in (compose.get("services") or {}).items():
        if not isinstance(spec, dict):
            continue
        for volume in spec.get("volumes") or []:
            # Chỉ xử lý dạng chuỗi "source:target[:mode]"; dạng long syntax
            # (dict) chưa dùng trong repo này.
            if isinstance(volume, str) and volume.split(":")[:2] == ["..", REPO_MOUNT]:
                mounted.add(name)
    return mounted


def _logical_lines(text: str) -> list[str]:
    """Nối dòng bị ngắt bằng backslash thành một dòng logic."""
    return [line.strip() for line in re.sub(r"\\\n\s*", " ", text).splitlines()]


def _exec_invocations() -> list[tuple[str, str]]:
    """Trả về (service, lệnh đầy đủ) cho mỗi `$(DC) exec` trong Makefile."""
    if not MAKEFILE.exists():
        pytest.skip("Makefile không tồn tại")

    invocations = []
    for line in _logical_lines(MAKEFILE.read_text(encoding="utf-8")):
        if "$(DC) exec" not in line:
            continue
        tokens = line.split("$(DC) exec", 1)[1].split()
        index = 0
        while index < len(tokens):
            token = tokens[index]
            if token in _OPTS_WITH_VALUE:
                index += 2
                continue
            if token.startswith("-"):
                index += 1
                continue
            break
        if index < len(tokens):
            invocations.append((tokens[index], line))
    return invocations


class TestMakefileExecTargets:
    def test_repo_paths_run_in_a_container_that_mounts_the_repo(self):
        """
        Lệnh chạy code từ repo phải nằm trong container có mount repo.

        Đây là invariant bị vi phạm ở cả TD-4 lẫn TD-8. Nó kiểm tra quan hệ
        giữa hai file — Makefile chọn service, docker-compose.yml quyết định
        service đó thấy gì — nên đọc riêng từng file không phát hiện được.
        """
        mounted = _services_mounting_repo()
        assert mounted, "Không service nào mount ..:/opt/project — compose đã đổi cấu trúc?"

        violations = [
            f"service '{service}' không mount {REPO_MOUNT}: {command}"
            for service, command in _exec_invocations()
            if any(marker in command for marker in REPO_PATH_MARKERS) and service not in mounted
        ]
        assert not violations, "Makefile chạy đường dẫn repo trong container không mount repo:\n" + "\n".join(
            violations
        )

    def test_seed_does_not_run_in_the_postgres_container(self):
        """
        Regression TD-8, neo trực tiếp vào target đã hỏng.

        Test trên là invariant tổng quát; test này giữ đúng trường hợp cụ thể
        để một lần refactor nới lỏng bộ marker không âm thầm mở lại lỗ cũ.
        """
        for service, command in _exec_invocations():
            if "data_generator/" in command:
                assert service != "postgres", (
                    "seed chạy trong container postgres — container này chỉ mount data volume "
                    "và init scripts, không có data_generator/ (TD-8)"
                )

    def test_generator_dependencies_are_absent_from_the_spark_image(self):
        """
        Vì sao seed KHÔNG chuyển sang spark-worker-1 dù nó cũng mount repo.

        requirements-ci-seed.txt ghi rõ: image Spark không mang package mà ETL
        không dùng. Nếu psycopg2 xuất hiện trong requirements-spark.txt thì
        quyết định đó đã đổi, và comment trong Makefile thành sai — fail ở đây
        để buộc cập nhật cả hai cùng lúc.
        """
        spark_requirements = REPO_ROOT / "docker" / "requirements-spark.txt"
        if not spark_requirements.exists():
            pytest.skip("requirements-spark.txt không tồn tại")
        content = spark_requirements.read_text(encoding="utf-8").lower()
        assert "psycopg2" not in content, (
            "psycopg2 đã được thêm vào image Spark — cập nhật lại lý do chọn container "
            "trong Makefile target `seed` và trong requirements-ci-seed.txt"
        )
