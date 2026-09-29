"""
Heap của Trino nằm gọn trong giới hạn bộ nhớ của container
==========================================================

Image trinodb/trino đặt heap = 80% bộ nhớ mà JVM THẤY (`-XX:MaxRAMPercentage=80`). Trên
Docker Desktop (WSL2) JVM không thấy giới hạn cgroup mà thấy RAM của máy: đo 2026-09-27,
container giới hạn 6,000 MiB chạy với MaxHeapSize ~19 GB, commit 10.3 GB heap, và bị kernel
OOM-kill (exit 137). `docker/init_trino/jvm.config` đặt heap cố định; test này giữ nó.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
JVM_CONFIG = REPO_ROOT / "docker" / "init_trino" / "jvm.config"
COMPOSE_FILES = ("docker/docker-compose.yml", "docker/docker-compose.ci.yml")
MOUNT = "./init_trino/jvm.config:/etc/trino/jvm.config:ro"
MAX_HEAP_SHARE = 0.75  # phần còn lại cho metaspace, code cache, direct buffer, thread stack

_UNITS = {"k": 1 / 1024, "m": 1, "g": 1024}


def _mib(value: str) -> float:
    m = re.fullmatch(r"(\d+)([kKmMgG])[bB]?", value.strip())
    assert m, f"không đọc được kích thước {value!r}"
    return int(m.group(1)) * _UNITS[m.group(2).lower()]


def _jvm_lines() -> list[str]:
    return [line.strip() for line in JVM_CONFIG.read_text(encoding="utf-8").splitlines() if line.strip()]


def _trino_service(compose: str) -> dict:
    return yaml.safe_load((REPO_ROOT / compose).read_text(encoding="utf-8"))["services"]["trino"]


def test_both_compose_files_mount_the_jvm_config():
    for compose in COMPOSE_FILES:
        assert MOUNT in _trino_service(compose)["volumes"], f"{compose}: trino không mount {MOUNT}"


def test_heap_is_fixed_not_a_share_of_visible_ram():
    lines = _jvm_lines()
    assert any(line.startswith("-Xmx") for line in lines), "jvm.config phải đặt -Xmx"
    percent = [line for line in lines if "RAMPercentage" in line and not line.startswith("#")]
    assert not percent, f"heap theo % RAM phụ thuộc JVM nhận ra container: {percent}"


def test_heap_leaves_room_below_the_container_limit():
    xmx = next(line for line in _jvm_lines() if line.startswith("-Xmx"))
    limit = _mib(_trino_service("docker/docker-compose.yml")["mem_limit"])
    assert _mib(xmx[len("-Xmx") :]) <= MAX_HEAP_SHARE * limit, (
        f"{xmx} vượt {MAX_HEAP_SHARE:.0%} của mem_limit {limit:.0f} MiB — không còn chỗ cho bộ nhớ ngoài heap"
    )


def test_jvm_config_is_ascii():
    """Launcher đọc file này (không phải Trino); giữ ASCII để không phụ thuộc locale container."""
    assert JVM_CONFIG.read_bytes().isascii()
