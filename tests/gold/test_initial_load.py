"""
Tests for code_etl/gold/bootstrap/initial_load.py

Covers:
  - GOLD_JOB_ORDER: structure, dependency ordering
  - parse_arguments: CLI arg parsing
  - run_gold_job: success, failure, timeout scenarios
  - main: dependency checking logic
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Mock shared modules CHỈ trong lúc exec initial_load.py, rồi khôi phục ngay.
#
# Trước đây hai dòng stub này chạy ở module scope và không bao giờ restore, nên
# `sys.modules["utils"]` bị thay bằng MagicMock cho MỌI test module được collect
# sau file này — bất kỳ test nào import module thật có `from utils.x import y`
# đều nổ `ModuleNotFoundError: 'utils' is not a package`.
#
# Việc restore an toàn: initial_load.py đã bind xong tên nó cần ngay tại thời
# điểm exec, nên nó vẫn giữ mock trong globals của chính nó sau khi restore.
_STUBBED = {"utils": MagicMock(), "utils.logger": MagicMock()}
_SAVED = {name: sys.modules.get(name) for name in _STUBBED}
sys.modules.update(_STUBBED)

try:
    # Import via importlib
    _spec = importlib.util.spec_from_file_location(
        "initial_load_mod",
        str(PROJECT_ROOT / "code_etl" / "gold" / "bootstrap" / "initial_load.py")
    )
    _ilmod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_ilmod)
finally:
    for name, previous in _SAVED.items():
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous

GOLD_JOB_ORDER = _ilmod.GOLD_JOB_ORDER
parse_arguments = _ilmod.parse_arguments
run_gold_job = _ilmod.run_gold_job


class TestGoldJobOrder:
    """Tests for the job ordering configuration."""

    def test_has_jobs(self):
        """Should define at least one job."""
        assert len(GOLD_JOB_ORDER) > 0

    def test_all_jobs_have_required_fields(self):
        """Each job should have name, type, and config."""
        for job in GOLD_JOB_ORDER:
            assert "name" in job, f"Job missing 'name': {job}"
            assert "type" in job, f"Job missing 'type': {job}"
            assert "config" in job, f"Job missing 'config': {job}"

    def test_all_jobs_have_unique_names(self):
        """All job names should be unique."""
        names = [job["name"] for job in GOLD_JOB_ORDER]
        assert len(names) == len(set(names))

    def test_campaign_target_depends_on_phase1(self):
        """campaign_target should depend on Phase 1 jobs."""
        campaign_job = next(j for j in GOLD_JOB_ORDER if j["name"] == "campaign_target")
        assert "depends_on" in campaign_job
        deps = campaign_job["depends_on"]
        assert "rfm_segment" in deps
        assert "churn_prediction" in deps
        assert "cross_sell_segment" in deps
        assert "mart_customer_360" in deps

    def test_phase1_jobs_have_no_depends_on(self):
        """Phase 1 jobs should not have depends_on."""
        for job in GOLD_JOB_ORDER:
            if job["name"] != "campaign_target":
                assert "depends_on" not in job, \
                    f"Phase 1 job '{job['name']}' should not have depends_on"

    def test_mart360_jobs_count(self):
        """Should have 5 mart360 jobs."""
        mart360_jobs = [j for j in GOLD_JOB_ORDER if j["type"] == "mart360"]
        assert len(mart360_jobs) == 5

    def test_segment_jobs_count(self):
        """Should have 4 segment jobs (rfm, churn, cross_sell, campaign)."""
        segment_jobs = [j for j in GOLD_JOB_ORDER if j["type"] == "segment"]
        assert len(segment_jobs) == 4

    def test_time_analytics_jobs_count(self):
        """Should have 1 time_analytics job."""
        time_jobs = [j for j in GOLD_JOB_ORDER if j["type"] == "time_analytics"]
        assert len(time_jobs) == 1


class TestParseArguments:
    """Tests for CLI argument parsing."""

    def test_cob_dt_required(self):
        """Should require --cob_dt argument."""
        with patch("sys.argv", ["initial_load.py"]), pytest.raises(SystemExit):
            parse_arguments()

    def test_cob_dt_parsed(self):
        """Should parse --cob_dt value."""
        with patch("sys.argv", ["initial_load.py", "--cob_dt", "2025-01-15"]):
            args = parse_arguments()
            assert args.cob_dt == "2025-01-15"

    def test_default_spark_submit(self):
        """Should have default spark-submit path."""
        with patch("sys.argv", ["initial_load.py", "--cob_dt", "2025-01-15"]):
            args = parse_arguments()
            assert args.spark_submit == "spark-submit"


class TestRunGoldJob:
    """Tests for individual Gold job execution."""

    def test_success_returns_true(self):
        """Should return True when job succeeds."""
        mock_logger = MagicMock()
        job_def = {"name": "test_job", "type": "mart360", "config": "test.yml"}

        with patch.object(_ilmod.subprocess, "run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = run_gold_job(job_def, "2025-01-15", "spark-submit", mock_logger)
            assert result is True

    def test_failure_returns_false(self):
        """Should return False when job fails."""
        mock_logger = MagicMock()
        job_def = {"name": "test_job", "type": "mart360", "config": "test.yml"}

        with patch.object(_ilmod.subprocess, "run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="error output")
            result = run_gold_job(job_def, "2025-01-15", "spark-submit", mock_logger)
            assert result is False

    def test_timeout_returns_false(self):
        """Should return False on timeout."""
        mock_logger = MagicMock()
        job_def = {"name": "test_job", "type": "mart360", "config": "test.yml"}

        import subprocess
        with patch.object(_ilmod.subprocess, "run", side_effect=subprocess.TimeoutExpired(cmd="test", timeout=600)):
            result = run_gold_job(job_def, "2025-01-15", "spark-submit", mock_logger)
            assert result is False

    def test_exception_returns_false(self):
        """Should return False on unexpected exception."""
        mock_logger = MagicMock()
        job_def = {"name": "test_job", "type": "mart360", "config": "test.yml"}

        with patch.object(_ilmod.subprocess, "run", side_effect=Exception("Unexpected")):
            result = run_gold_job(job_def, "2025-01-15", "spark-submit", mock_logger)
            assert result is False

    def test_builds_correct_command(self):
        """Should build spark-submit command with correct arguments."""
        mock_logger = MagicMock()
        job_def = {"name": "test_job", "type": "mart360", "config": "test.yml"}

        with patch.object(_ilmod.subprocess, "run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            run_gold_job(job_def, "2025-01-15", "spark-submit", mock_logger)

            cmd = mock_run.call_args[0][0]
            assert "spark-submit" in cmd[0]
            assert "--cob_dt" in cmd
            assert "2025-01-15" in cmd
            assert "test.yml" in cmd
