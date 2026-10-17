"""
Contract Test — Docker Compose Profiles cho máy 16GB RAM (Khoá new2 C8)
========================================================================

Ngữ cảnh:
  Khóa Master Class (Cole.vn / mentor Hồ Đức Huy - VietinBank) yêu cầu:
  "RAM 16GB - chạy theo docker-compose profiles, không bật toàn bộ stack cùng lúc."
  Trước đây platform kéo 30 container cùng lúc, chiếm ~18GB RAM → crash trên máy 16GB.

Quy tắc kiến trúc:
  1. Base Lakehouse (postgres, sftp, minio, mc, iceberg-rest, spark-master,
     spark-worker-1, iceberg-init) KHÔNG gán profile → luôn khởi động nền tảng (~4.5GB).
  2. Mọi service tuỳ chọn PHẢI thuộc ít nhất một domain profile VÀ profile 'all'.
  3. Batch Lakehouse (orchestration + serving) <= 15 services → ~9GB RAM, chạy mượt trên 16GB.
  4. Real-time Streaming (streaming + serving) <= 15 services → ~10GB RAM, chạy mượt trên 16GB.
  5. Không có lỗi cross-profile dependency (mọi depends_on của service trong profile
     đều phải thuộc profile đó hoặc là base service).

Chạy: pytest tests/governance/test_compose_profiles.py -v
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker" / "docker-compose.yml"

BASE_SERVICES = {
    "postgres",
    "sftp",
    "minio",
    "mc",
    "iceberg-rest",
    "spark-master",
    "spark-worker-1",
    "iceberg-init",
}

DOMAIN_PROFILES = {
    "streaming",
    "orchestration",
    "serving",
    "governance",
    "observability",
    "apps",
}


def _load_compose() -> dict:
    return yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))


def test_base_services_have_no_profile():
    """Các dịch vụ nền tảng Lakehouse luôn khởi động mặc định (không profile)."""
    services = _load_compose().get("services", {})
    for name in BASE_SERVICES:
        assert name in services, f"thiếu base service `{name}` trong docker-compose.yml"
        spec = services[name]
        assert "profiles" not in spec or not spec["profiles"], (
            f"Base service `{name}` không được gán profiles — nó là nền tảng của mọi subsystem"
        )


def test_optional_services_have_domain_profile_and_all():
    """Mọi service tuỳ chọn phải thuộc ít nhất một domain profile VÀ có tag 'all'."""
    services = _load_compose().get("services", {})
    for name, spec in services.items():
        if name in BASE_SERVICES:
            continue
        profiles = set(spec.get("profiles", []))
        assert profiles, f"Service tuỳ chọn `{name}` chưa được gán profile — sẽ bị bật ngoài ý muốn"
        assert "all" in profiles, f"Service `{name}` thiếu tag 'all' trong profiles: {profiles}"
        assert profiles & DOMAIN_PROFILES, (
            f"Service `{name}` phải thuộc ít nhất một domain {DOMAIN_PROFILES}, có: {profiles}"
        )


def test_profile_combinations_have_no_broken_dependencies():
    """
    Kiểm tra docker compose config giải quyết được mọi phụ thuộc dịch vụ
    cho từng profile riêng rẽ mà không văng lỗi undefined service.
    """
    test_profiles = [
        [],  # base
        ["streaming"],
        ["orchestration"],
        ["serving"],
        ["governance"],
        ["apps"],
        ["observability"],
        ["orchestration", "serving"],  # Batch Lakehouse
        ["streaming", "serving"],      # Streaming Lakehouse
        ["all"],
    ]

    for p in test_profiles:
        cmd = ["docker", "compose", "-f", str(COMPOSE_FILE)]
        for prof in p:
            cmd.extend(["--profile", prof])
        cmd.extend(["config", "--services"])
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        assert res.returncode == 0, (
            f"Profile combination {p} bị lỗi dependency:\n{res.stderr}"
        )


def test_16gb_workloads_stay_within_service_budget():
    """
    Kịch bản chạy trên máy 16GB RAM:
      - Batch Lakehouse (orchestration + serving) <= 15 services.
      - Streaming Lakehouse (streaming + serving) <= 15 services.
    """
    services = _load_compose().get("services", {})

    def count_active(target_profiles: set[str]) -> int:
        count = 0
        for name, spec in services.items():
            if name in BASE_SERVICES or set(spec.get("profiles", [])) & target_profiles:
                count += 1
        return count

    batch_count = count_active({"orchestration", "serving"})
    assert batch_count <= 14, f"Batch Lakehouse có {batch_count} services — vượt ngân sách 16GB"

    streaming_count = count_active({"streaming", "serving"})
    assert streaming_count <= 15, f"Streaming Lakehouse có {streaming_count} services — vượt ngân sách 16GB"
