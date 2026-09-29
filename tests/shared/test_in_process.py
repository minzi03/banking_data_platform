"""
Bootstrap `--in-process`: nhiều job Silver/Gold trong MỘT Spark app.

Không cần Spark: session là mock. Kiểm cái mà sai thì hỏng âm thầm — mỗi job một session
con, cache được xoá kể cả khi job lỗi, lỗi job trả False như exit code của subprocess,
session gốc tạo một lần và luôn được dừng, chính sách dừng/bỏ qua của từng tầng giữ nguyên,
và tên hàm job mà runner gọi thật sự tồn tại trong module job.

Chạy thật trên stack (2026-09-28, cob_dt 2026-09-22, dữ liệu đầy đủ): Silver 16/16 trong
53 s, Gold 14/14 trong 90 s; số dòng 30 bảng Silver/Gold và 4 checksum nội dung trùng khớp
với lần chạy từng spark-submit.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# Nạp THẲNG từ file: vài test khác thay sys.modules["spark"] bằng MagicMock ở module scope,
# nên `from spark import in_process` nhận mock khi file này được collect sau chúng.
_spec = importlib.util.spec_from_file_location(
    "_in_process_under_test", REPO_ROOT / "code_etl" / "shared" / "spark" / "in_process.py"
)
in_process = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(in_process)


def _load_bootstrap(layer: str):
    """Nạp initial_load.py của một tầng với `utils` stub CHỈ trong lúc exec (như test_initial_load.py)."""
    stubs = {"utils": MagicMock(), "utils.logger": MagicMock()}
    saved = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        spec = importlib.util.spec_from_file_location(
            f"_bootstrap_{layer}_in_process", REPO_ROOT / "code_etl" / layer / "bootstrap" / "initial_load.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for name, previous in saved.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous


SILVER = _load_bootstrap("silver")
GOLD = _load_bootstrap("gold")


def _job_module(runner_name: str, runner):
    module = MagicMock()
    module.load_config.return_value = {"target": {"table": "t"}}
    setattr(module, runner_name, runner)
    return module


# ── run_job_in_process ───────────────────────────────────────────────────────


def test_each_job_runs_on_a_child_session_and_cache_is_cleared():
    spark, logger, runner = MagicMock(), MagicMock(), MagicMock()
    child = spark.newSession.return_value
    checked = []
    module = _job_module("run_x", runner)

    ok = in_process.run_job_in_process(
        spark, module, "run_x", "cfg.yml", "2026-09-22", logger, check_session=checked.append
    )

    assert ok is True
    assert checked == [child], "session con phải qua kiểm UTC như get_spark_session"
    module.load_config.assert_called_once_with("cfg.yml")
    module.validate_config.assert_called_once_with(module.load_config.return_value)
    runner.assert_called_once_with(child, module.load_config.return_value, "2026-09-22", logger)
    spark.catalog.clearCache.assert_called_once()


def test_a_failing_job_returns_false_and_still_clears_the_cache():
    spark, logger = MagicMock(), MagicMock()
    module = _job_module("run_x", MagicMock(side_effect=RuntimeError("boom")))

    ok = in_process.run_job_in_process(spark, module, "run_x", "cfg.yml", "d", logger, check_session=lambda s: None)

    assert ok is False
    logger.exception.assert_called_once()
    spark.catalog.clearCache.assert_called_once()


def test_job_modules_load_once_and_their_main_block_does_not_run(tmp_path):
    job = tmp_path / "layer" / "base_job" / "fake_job.py"
    job.parent.mkdir(parents=True)
    job.write_text(
        "LOADS = []\nLOADS.append(1)\nif __name__ == '__main__':\n    raise SystemExit(9)\n", encoding="utf-8"
    )
    first = in_process.load_job_module(job)
    second = in_process.load_job_module(job)
    assert first is second and first.LOADS == [1]


# ── Tên hàm job mà runner gọi phải có thật ───────────────────────────────────


@pytest.mark.parametrize("job_type", sorted(SILVER.IN_PROCESS_RUNNER))
def test_silver_runner_functions_exist(job_type):
    src = (SILVER.BASE_JOB_DIR / f"{job_type}.py").read_text(encoding="utf-8")
    assert re.search(rf"^def {SILVER.IN_PROCESS_RUNNER[job_type]}\(spark, config", src, re.MULTILINE)


def test_every_silver_job_type_has_a_runner():
    assert {job["type"] for job in SILVER.SILVER_JOB_ORDER} <= set(SILVER.IN_PROCESS_RUNNER)


def test_gold_runner_function_exists():
    assert re.search(r"^def run_gold_job\(spark, config", GOLD.GOLD_JOB_FILE.read_text(encoding="utf-8"), re.MULTILINE)


# ── main --in-process ────────────────────────────────────────────────────────


@pytest.fixture
def fake_session(monkeypatch):
    session = MagicMock()
    factory = MagicMock(return_value=session)
    fake_module = MagicMock(get_spark_session=factory)
    monkeypatch.setitem(sys.modules, "spark.spark_session", fake_module)
    return session, factory


def test_silver_in_process_uses_one_session_and_no_subprocess(monkeypatch, fake_session):
    session, factory = fake_session
    ran = []
    monkeypatch.setattr(
        SILVER, "run_silver_job_in_process", lambda job, cob, spark, log: ran.append(job["name"]) or True
    )
    monkeypatch.setattr(SILVER, "run_silver_job", MagicMock(side_effect=AssertionError("subprocess path used")))
    monkeypatch.setattr(sys, "argv", ["initial_load.py", "--cob_dt", "2026-09-22", "--in-process"])

    SILVER.main()

    factory.assert_called_once()
    assert ran == [job["name"] for job in SILVER.SILVER_JOB_ORDER]
    session.stop.assert_called_once()


def test_silver_in_process_stops_on_a_failed_dimension_and_exits_1(monkeypatch, fake_session):
    session, _ = fake_session
    ran = []

    def run(job, cob, spark, log):
        ran.append(job["name"])
        return job["name"] != "dim_product"

    monkeypatch.setattr(SILVER, "run_silver_job_in_process", run)
    monkeypatch.setattr(sys, "argv", ["initial_load.py", "--cob_dt", "2026-09-22", "--in-process"])

    with pytest.raises(SystemExit) as exc:
        SILVER.main()

    assert exc.value.code == 1
    assert ran == ["dim_branch", "dim_product"], "dimension lỗi thì dừng, như chế độ subprocess"
    session.stop.assert_called_once()


def test_gold_in_process_uses_one_session_and_no_subprocess(monkeypatch, fake_session):
    session, factory = fake_session
    monkeypatch.setattr(GOLD, "run_gold_job_in_process", lambda job, cob, spark, log: True)
    monkeypatch.setattr(GOLD, "run_gold_job", MagicMock(side_effect=AssertionError("subprocess path used")))
    monkeypatch.setattr(sys, "argv", ["initial_load.py", "--cob_dt", "2026-09-22", "--in-process"])

    GOLD.main()

    factory.assert_called_once()
    session.stop.assert_called_once()


def test_default_is_still_one_spark_submit_per_job(monkeypatch):
    calls = []
    monkeypatch.setattr(GOLD, "run_gold_job", lambda job, cob, submit, log: calls.append(job["name"]) or True)
    monkeypatch.setattr(sys, "argv", ["initial_load.py", "--cob_dt", "2026-09-22"])
    GOLD.main()
    assert calls == [job["name"] for job in GOLD.GOLD_JOB_ORDER]
