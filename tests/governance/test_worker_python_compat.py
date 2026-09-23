"""
Contract Test — Code chạy trên spark-worker phải chạy được trên Python của worker
================================================================================

Mọi task ops/governance trong Airflow `docker exec` vào `banking-spark-worker-1`
rồi `spark-submit`, hoặc đi qua `SparkSubmitOperator` với executor ở đó. Nên
Python của **image Spark** là runtime thật của `code_etl/` và `governance/` —
không phải Python của CI.

Lịch sử (TD-10): image dựng trên `apache/spark:3.5.3`, tức Ubuntu 20.04 với
**Python 3.8.10**, trong khi repo khai `requires-python >=3.10`, ruff
`target-version = "py310"` và CI chạy 3.11. Ruff đẩy cú pháp 3.9+ vào code, CI
xanh, còn trên worker 11/14 entry point chết ngay lúc import — từ commit đầu
tiên, không test nào thấy.

Bản sửa là nâng image lên biến thể jammy (Python 3.10). Test này giữ cho lỗi
đó không quay lại, theo hai hướng:

1. **Năm nơi khai phiên bản phải khớp nhau.** `WORKER_PYTHON` dưới đây,
   Python đã đo trong base image của `docker/Dockerfile.spark`,
   `requires-python`, `target-version` của ruff, và `python-version` của mọi
   workflow CI. Đổi một nơi mà quên nơi khác thì đỏ. Đổi dòng `FROM` sang tag
   chưa đo thì đỏ — phải đo Python trong image mới rồi thêm vào
   `MEASURED_BASE_IMAGES`.

2. **Code worker không dùng thứ mới hơn `WORKER_PYTHON`.** Giờ CI chạy đúng
   Python của worker nên suite tự bắt được phần lớn. Mục này là lớp thứ hai,
   rẻ và chạy được ở máy dev có Python mới hơn: cú pháp
   (`ast.parse(feature_version=...)`), module stdlib, và các tên cụ thể trong
   `typing`/`datetime`/`enum` mà người viết code 3.11 hay dùng.

Giới hạn: mục 2 là xấp xỉ tĩnh. Nó không thấy `getattr(typing, "Self")`, và
bảng tên ở dưới là danh sách chọn lọc, không phải toàn bộ API mới của 3.11+.

Chạy: pytest tests/governance/test_worker_python_compat.py -v
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# Runtime thật của mọi thứ spark-submit trên worker.
WORKER_PYTHON = (3, 10)

# Base image → Python ĐO ĐƯỢC trong image đó. Không suy từ tên tag.
#
#   docker run --rm --entrypoint python3 <image> --version
#
# 2026-09-23: Python 3.10.12 · Ubuntu 22.04.5 (jammy) · OpenJDK 17.
MEASURED_BASE_IMAGES = {
    "apache/spark:3.5.3-scala2.12-java17-python3-ubuntu": (3, 10),
}

# Mọi thứ spark-worker có thể import hoặc spark-submit.
WORKER_ROOTS = ("code_etl", "governance")
WORKER_EXTRA_FILES = ("scripts/resolve_quarantine.py",)

# Module stdlib xuất hiện SAU 3.10 → (phiên bản xuất hiện).
STDLIB_ADDED = {"tomllib": (3, 11)}

# Tên cụ thể mà code 3.11+ hay dùng, CI 3.11 chấp nhận, worker 3.10 thì không.
NAMES_ADDED = {
    ("typing", "Self"): (3, 11),
    ("typing", "Never"): (3, 11),
    ("typing", "LiteralString"): (3, 11),
    ("typing", "NotRequired"): (3, 11),
    ("typing", "Required"): (3, 11),
    ("typing", "Unpack"): (3, 11),
    ("typing", "TypeVarTuple"): (3, 11),
    ("typing", "assert_never"): (3, 11),
    ("typing", "assert_type"): (3, 11),
    ("typing", "reveal_type"): (3, 11),
    ("typing", "dataclass_transform"): (3, 11),
    ("typing", "override"): (3, 12),
    ("typing", "TypeAliasType"): (3, 12),
    ("datetime", "UTC"): (3, 11),
    ("enum", "StrEnum"): (3, 11),
}


def _worker_files() -> list[Path]:
    files: list[Path] = []
    for root in WORKER_ROOTS:
        files.extend(sorted((REPO_ROOT / root).rglob("*.py")))
    files.extend(REPO_ROOT / f for f in WORKER_EXTRA_FILES)
    return files


WORKER_FILES = _worker_files()


def _rel(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def _fmt(version: tuple[int, int]) -> str:
    return f"{version[0]}.{version[1]}"


def _dockerfile_base_image() -> str:
    text = (REPO_ROOT / "docker" / "Dockerfile.spark").read_text(encoding="utf-8")
    froms = re.findall(r"^FROM\s+(\S+)", text, flags=re.MULTILINE)
    assert len(froms) == 1, f"Dockerfile.spark phải có đúng một FROM, tìm thấy {froms}"
    return froms[0]


def _pyproject() -> str:
    return (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")


def _newer_stdlib_modules(tree: ast.AST) -> list[str]:
    used: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            used.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            used.add(node.module.split(".")[0])
    return sorted(m for m in used if m in STDLIB_ADDED and STDLIB_ADDED[m] > WORKER_PYTHON)


def _newer_names(tree: ast.AST) -> list[tuple[int, str]]:
    """`from typing import Self` và `typing.Self` — cả hai dạng."""
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            for alias in node.names:
                added = NAMES_ADDED.get((node.module, alias.name))
                if added and added > WORKER_PYTHON:
                    found.append((node.lineno, f"{node.module}.{alias.name} ({_fmt(added)})"))
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            added = NAMES_ADDED.get((node.value.id, node.attr))
            if added and added > WORKER_PYTHON:
                found.append((node.lineno, f"{node.value.id}.{node.attr} ({_fmt(added)})"))
    return found


# ---------------------------------------------------------------------------
# 1. Bốn nơi khai phiên bản phải khớp nhau
# ---------------------------------------------------------------------------


def test_spark_base_image_python_was_measured():
    """
    Đổi dòng FROM mà không đo lại là cách TD-10 xảy ra: không ai hỏi base image
    mang Python nào, vì Python không nằm trong tên tag `apache/spark:3.5.3`.
    """
    image = _dockerfile_base_image()
    assert image in MEASURED_BASE_IMAGES, (
        f"docker/Dockerfile.spark dùng `{image}` nhưng Python của image này chưa được đo.\n"
        "Build image, chạy `docker run --rm --entrypoint python3 <image> --version`, "
        "rồi thêm kết quả vào MEASURED_BASE_IMAGES."
    )
    measured = MEASURED_BASE_IMAGES[image]
    assert measured == WORKER_PYTHON, (
        f"`{image}` mang Python {_fmt(measured)} nhưng WORKER_PYTHON là {_fmt(WORKER_PYTHON)}. "
        "Đổi WORKER_PYTHON, requires-python và ruff target-version cùng lúc."
    )


def test_requires_python_floor_is_the_worker_python():
    """
    Nếu mức sàn cao hơn worker, code hợp lệ theo pyproject sẽ chết trên worker.
    Nếu thấp hơn, pyproject hứa hỗ trợ một phiên bản không ai chạy.
    """
    match = re.search(r'^requires-python\s*=\s*">=(\d+)\.(\d+)"', _pyproject(), flags=re.MULTILINE)
    assert match, 'không tìm thấy requires-python dạng ">=X.Y" trong pyproject.toml'
    floor = (int(match.group(1)), int(match.group(2)))
    assert floor == WORKER_PYTHON, (
        f"pyproject.toml khai requires-python >={_fmt(floor)}, worker chạy {_fmt(WORKER_PYTHON)}."
    )


def test_ruff_target_is_the_worker_python():
    """
    Luật `UP` của ruff viết lại code theo `target-version`. Target cao hơn worker
    thì chính ruff đưa cú pháp mà worker không chạy được vào code — đúng cơ chế
    đã gây ra TD-10.
    """
    match = re.search(r'^target-version\s*=\s*"py(\d)(\d+)"', _pyproject(), flags=re.MULTILINE)
    assert match, "không tìm thấy target-version của ruff trong pyproject.toml"
    target = (int(match.group(1)), int(match.group(2)))
    assert target == WORKER_PYTHON, (
        f"ruff target-version là py{target[0]}{target[1]}, worker chạy {_fmt(WORKER_PYTHON)}."
    )


def test_ci_runs_the_worker_python():
    """
    CI chạy Python nào thì suite chỉ chứng minh được code chạy trên Python đó.
    CI đi trước worker một bậc là đúng kiểu lỗi TD-10: cú pháp và API mới xanh ở
    CI rồi chết trên worker. Test tĩnh ở mục 2 chỉ bắt được một phần — CI chạy
    đúng Python của worker mới bắt được hết.
    """
    workflows = sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml"))
    found: list[tuple[str, tuple[int, int]]] = []
    for wf in workflows:
        for major, minor in re.findall(r"""python-version:\s*["']?(\d+)\.(\d+)""", wf.read_text(encoding="utf-8")):
            found.append((wf.name, (int(major), int(minor))))

    assert found, "Không tìm thấy python-version nào trong .github/workflows — guard này sẽ xanh vô nghĩa."
    wrong = [f"{name}: {_fmt(version)}" for name, version in found if version != WORKER_PYTHON]
    assert not wrong, f"Workflow chạy Python khác worker ({_fmt(WORKER_PYTHON)}): {wrong}"


# ---------------------------------------------------------------------------
# 2. Code worker không dùng thứ mới hơn WORKER_PYTHON
# ---------------------------------------------------------------------------


def test_worker_files_are_found():
    """Guard chống pass rỗng: đổi cấu trúc thư mục không được làm test xanh vô nghĩa."""
    assert len(WORKER_FILES) >= 40, f"Chỉ tìm thấy {len(WORKER_FILES)} file Python chạy trên worker."
    for rel in (
        "code_etl/shared/ops/data_quality.py",
        "code_etl/shared/ops/quarantine.py",
        "governance/schema_drift.py",
        "governance/contracts.py",
    ):
        assert REPO_ROOT / rel in WORKER_FILES, f"{rel} không nằm trong phạm vi quét"


def test_detectors_flag_known_offenders():
    """
    Guard cho chính các bộ dò: một hàm dò luôn trả rỗng sẽ làm mọi test dưới
    xanh mà không kiểm gì.
    """
    tree = ast.parse(
        "import tomllib\nimport typing\nfrom typing import Self\nfrom datetime import UTC\nx: typing.Never\n"
    )
    assert _newer_stdlib_modules(tree) == ["tomllib"]
    names = [name for _, name in _newer_names(tree)]
    assert names == ["typing.Self (3.11)", "datetime.UTC (3.11)", "typing.Never (3.11)"]

    # Và không báo nhầm những thứ worker có sẵn.
    ok = ast.parse("import zoneinfo\nfrom typing import Optional\nfrom datetime import timezone\n")
    assert _newer_stdlib_modules(ok) == []
    assert _newer_names(ok) == []


@pytest.mark.parametrize("path", WORKER_FILES, ids=_rel)
def test_parses_on_worker_python(path: Path):
    """Cú pháp mà parser của worker từ chối (vd. `except*` của 3.11) làm file không load được."""
    source = path.read_text(encoding="utf-8")
    try:
        ast.parse(source, filename=_rel(path), feature_version=WORKER_PYTHON)
    except SyntaxError as e:
        pytest.fail(f"{_rel(path)}:{e.lineno}: cú pháp không có trong Python {_fmt(WORKER_PYTHON)} — {e.msg}")


@pytest.mark.parametrize("path", WORKER_FILES, ids=_rel)
def test_no_stdlib_modules_newer_than_worker(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    bad = _newer_stdlib_modules(tree)
    assert not bad, f"{_rel(path)} import {bad} — không có trong stdlib Python {_fmt(WORKER_PYTHON)} của spark-worker."


@pytest.mark.parametrize("path", WORKER_FILES, ids=_rel)
def test_no_api_names_newer_than_worker(path: Path):
    """CI chạy 3.11 nên dùng `typing.Self` vẫn xanh ở CI — và chết trên worker."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    bad = _newer_names(tree)
    assert not bad, f"{_rel(path)} dùng API mới hơn Python {_fmt(WORKER_PYTHON)} của spark-worker:\n" + "\n".join(
        f"  dòng {line}: {name}" for line, name in bad[:10]
    )
