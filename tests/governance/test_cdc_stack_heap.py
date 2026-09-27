"""
Heap của Zookeeper / Kafka / Debezium nằm gọn trong giới hạn bộ nhớ container (TD-19)
====================================================================================

Mặc định của image, đo bằng `jcmd VM.flags` so với `mem_limit` ngày 2026-09-27:

    zookeeper   -Xmx512M   trong 256m    heap gấp đôi giới hạn
    kafka       -Xmx1G     trong 1024m   không còn chỗ ngoài heap
    debezium    -Xmx2G     trong 2048m   không còn chỗ ngoài heap (1.88 GiB RSS khi snapshot)

Trino cùng loại lỗi đã bị kernel OOM-kill thật (exit 137). Tên biến đọc từ script khởi động
của chính image: cp-zookeeper / cp-kafka dùng KAFKA_HEAP_OPTS; debezium/connect dùng HEAP_OPTS
rồi chép sang KAFKA_HEAP_OPTS.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICES = yaml.safe_load((REPO_ROOT / "docker" / "docker-compose.yml").read_text(encoding="utf-8"))["services"]
HEAP_VAR = {"zookeeper": "KAFKA_HEAP_OPTS", "kafka": "KAFKA_HEAP_OPTS", "debezium": "HEAP_OPTS"}
MAX_HEAP_SHARE = 0.75
_UNITS = {"k": 1 / 1024, "m": 1, "g": 1024}


def _mib(value: str) -> float:
    m = re.fullmatch(r"(\d+)([kKmMgG])[bB]?", value.strip())
    assert m, f"không đọc được kích thước {value!r}"
    return int(m.group(1)) * _UNITS[m.group(2).lower()]


@pytest.mark.parametrize("service", sorted(HEAP_VAR))
def test_heap_is_set_and_leaves_room_below_the_limit(service):
    env = SERVICES[service].get("environment", {})
    opts = env.get(HEAP_VAR[service])
    assert opts, f"{service}: {HEAP_VAR[service]} chưa đặt — image dùng heap mặc định, bằng hoặc vượt mem_limit"
    xmx = re.search(r"-Xmx(\S+)", opts)
    assert xmx, f"{service}: {HEAP_VAR[service]}={opts!r} không có -Xmx"
    limit = _mib(SERVICES[service]["mem_limit"])
    assert _mib(xmx.group(1)) <= MAX_HEAP_SHARE * limit, (
        f"{service}: -Xmx{xmx.group(1)} vượt {MAX_HEAP_SHARE:.0%} của mem_limit {limit:.0f} MiB"
    )
