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

import os
import subprocess
from pathlib import Path

import yaml

COMPOSE_TEST_ENV = {
    **os.environ,
    # Compose config validation must not depend on a developer's .env. These
    # values are interpolation-only and are never used to start a service.
    "SUPERSET_SECRET_KEY": "ci-compose-validation-only",
    "SUPERSET_ADMIN_PASSWORD": "ci-compose-validation-only",
}

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker" / "docker-compose.yml"
COMPOSE_FILES = (
    COMPOSE_FILE,
    REPO_ROOT / "docker" / "docker-compose.ci.yml",
    REPO_ROOT / "docker" / "docker-compose.e2e-merchant.yml",
)

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


def _load_compose_file(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _compose_images() -> dict[tuple[str, str], str]:
    return {
        (compose_file.name, service): spec["image"]
        for compose_file in COMPOSE_FILES
        for service, spec in _load_compose_file(compose_file).get("services", {}).items()
        if spec.get("image")
    }


def test_images_are_version_pinned_and_do_not_use_latest():
    """Every Compose file must use an explicit image version or digest."""
    floating = []
    for (compose_name, service), image in _compose_images().items():
        reference = image.split("@", maxsplit=1)[0]
        tag = reference.rsplit("/", maxsplit=1)[-1]
        if tag == "latest" or tag.endswith(":latest"):
            floating.append(f"{compose_name} {service}: {image}")
        elif ":" not in tag and "@sha256:" not in image:
            floating.append(f"{compose_name} {service}: image has no explicit tag or digest ({image})")
    assert not floating, "Unpinned Compose images:\n  " + "\n  ".join(floating)


def test_frozen_minio_mirrors_are_digest_pinned_consistently():
    images = _compose_images()
    for image_name, digest in (
        ("ghcr.io/minzi03/minio", "sha256:a1a8bd4ac40ad7881a245bab97323e18f971e4d4cba2c2007ec1bedd21cbaba2"),
        ("ghcr.io/minzi03/mc", "sha256:eb4ea9884b77704230e2423e9004d2fa738dc272876b9cc41a297d29443b8780"),
    ):
        references = [
            image for image in images.values() if image.split("@", maxsplit=1)[0].startswith(image_name + ":")
        ]
        assert references, f"No Compose reference found for frozen mirror {image_name}"
        assert all(image.endswith("@" + digest) for image in references), (
            f"{image_name} must use {digest} in every Compose file: {references}"
        )


def test_all_compose_images_have_digest_pins_or_documented_transitional_tags():
    images = _compose_images()
    unpinned = {key: image for key, image in images.items() if "@sha256:" not in image}
    inventory = (REPO_ROOT / "docs" / "05-quality" / "image-reference-inventory.md").read_text(encoding="utf-8")
    for (compose_name, service), image in unpinned.items():
        assert image in inventory, (
            f"{compose_name}:{service} tag-pinned reference missing from image inventory: {image}"
        )


def test_trino_has_authenticated_readiness_configuration():
    trino = _load_compose_file(COMPOSE_FILE)["services"]["trino"]
    assert "healthcheck" in trino
    healthcheck = " ".join(trino["healthcheck"]["test"])
    assert "/v1/info" in healthcheck
    assert "8443" in str(trino.get("ports", []))
    assert "password-authenticator.properties" in " ".join(trino.get("volumes", []))


def test_optional_service_names_and_profiles_match_the_readme_matrix():
    expected = {
        "trino": {"serving", "analytics", "apps", "observability", "all"},
        "kafka": {"streaming", "all"},
        "debezium": {"streaming", "all"},
        "airflow-scheduler": {"orchestration", "all"},
        "prometheus": {"observability", "all"},
        "openmetadata": {"governance", "all"},
        "streamlit": {"apps", "all"},
        "superset": {"apps", "bi", "all"},
    }
    services = _load_compose().get("services", {})
    for name, profiles in expected.items():
        assert name in services
        assert set(services[name].get("profiles", [])) == profiles


def test_profile_services_have_health_or_explicit_one_shot_semantics():
    one_shot = {"mc", "iceberg-init", "airflow-init", "om-migrate", "superset-init"}
    missing = []
    for compose_file in COMPOSE_FILES:
        for name, spec in _load_compose_file(compose_file).get("services", {}).items():
            if name in one_shot or "healthcheck" in spec or "build" in spec:
                continue
            if spec.get("profiles"):
                missing.append(f"{compose_file.name}:{name}")
    assert not missing, "Profile services without readiness/explicit one-shot contract: " + ", ".join(missing)


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
        ["streaming", "serving"],  # Streaming Lakehouse
        ["all"],
    ]

    for p in test_profiles:
        cmd = ["docker", "compose", "-f", str(COMPOSE_FILE)]
        for prof in p:
            cmd.extend(["--profile", prof])
        cmd.extend(["config", "--services"])
        res = subprocess.run(cmd, capture_output=True, text=True, env=COMPOSE_TEST_ENV, check=False)
        assert res.returncode == 0, f"Profile combination {p} bị lỗi dependency:\n{res.stderr}"


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
