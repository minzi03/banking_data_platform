"""
Contract Test — Code chạy trên spark-worker phải import được bằng Python 3.8
===========================================================================

`banking-spark-worker-1` chạy **Python 3.8.10**. Mọi task ops/governance trong
Airflow đều `docker exec` vào đó rồi `spark-submit`. Trong khi đó:

    pyproject.toml     requires-python >=3.10 · ruff target-version py310 · UP
    ci.yml job test    Python 3.11

Nên ruff chủ động đẩy cú pháp 3.9+ vào code (`Dict` → `dict`, `Optional[X]` →
`X | None`), CI chạy trên 3.11 và xanh, còn trên worker thì job chết ngay lúc
import:

    TypeError: 'type' object is not subscriptable
    TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'
    ModuleNotFoundError: No module named 'zoneinfo'

Tới 2026-09-23, 11/14 entry point chạy trên worker chết như vậy — kể cả
`data_quality.py` và `quarantine.py` — từ commit đầu tiên, và không test nào
thấy. Xem `docs/05-quality/technical-debt.md` TD-10.

Test này không chạy Python 3.8. Nó kiểm TĨNH ba điều mà 3.8 không chịu được:

1. Cú pháp mà parser 3.8 từ chối (`ast.parse(..., feature_version=(3, 8))`).
2. Annotation dùng `dict[...]`/`list[...]`/`X | Y` trong file không có
   `from __future__ import annotations` — annotation được đánh giá lúc import.
3. Import module stdlib chưa có trong 3.8 (`zoneinfo`, `graphlib`, `tomllib`).

Giới hạn: nó không bắt được `dict[...]` hay `X | Y` dùng ở runtime NGOÀI
annotation (ví dụ `isinstance(x, int | str)`) khi đi kèm future import. Điều 2
chỉ kiểm annotation; bảo vệ đầy đủ cần chạy suite trên 3.8 hoặc nâng Python
của worker (quyết định còn mở trong TD-10).

Chạy: pytest tests/governance/test_worker_python38_compat.py -v
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# Mọi thứ spark-worker có thể import hoặc spark-submit.
WORKER_ROOTS = ("code_etl", "governance")
WORKER_EXTRA_FILES = ("scripts/resolve_quarantine.py",)

# Miễn trừ phải có lý do. Không miễn im lặng.
EXEMPT = {
    "governance/contracts.py": (
        "pydantic đánh giá annotation lúc tạo model kể cả khi có future import, "
        "nên future import không cứu được nó trên 3.8. Worker không cài pydantic "
        "nên file này (và ops_contract_validation_dag) không chạy được ở đó dù "
        "cú pháp thế nào — lỗi thiếu dependency, ghi ở TD-10."
    ),
}

BUILTIN_GENERICS = {"dict", "list", "tuple", "set", "frozenset", "type"}
STDLIB_AFTER_38 = {"zoneinfo", "graphlib", "tomllib"}


def _worker_files() -> list[Path]:
    files: list[Path] = []
    for root in WORKER_ROOTS:
        files.extend(sorted((REPO_ROOT / root).rglob("*.py")))
    files.extend(REPO_ROOT / f for f in WORKER_EXTRA_FILES)
    return files


WORKER_FILES = _worker_files()


def _rel(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def _has_future_annotations(tree: ast.Module) -> bool:
    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == "__future__"
        and any(alias.name == "annotations" for alias in node.names)
        for node in tree.body
    )


def _annotations(tree: ast.Module) -> list[ast.expr]:
    """Annotation được đánh giá lúc import: tham số, return, và AnnAssign ngoài thân hàm."""
    found: list[ast.expr] = []

    def visit(node: ast.AST, in_function: bool) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = node.args
            for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs, args.vararg, args.kwarg):
                if arg is not None and arg.annotation is not None:
                    found.append(arg.annotation)
            if node.returns is not None:
                found.append(node.returns)
            for child in node.body:
                visit(child, True)
            return
        # PEP 526: annotation biến CỤC BỘ trong thân hàm không bao giờ được đánh giá.
        if isinstance(node, ast.AnnAssign) and not in_function:
            found.append(node.annotation)
        for child in ast.iter_child_nodes(node):
            visit(child, in_function)

    visit(tree, False)
    return found


def _py39_construct(annotation: ast.expr) -> str | None:
    for node in ast.walk(annotation):
        if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name) and node.value.id in BUILTIN_GENERICS:
            return f"{node.value.id}[...]"
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            return "X | Y"
    return None


def test_worker_files_are_found():
    """Guard chống pass rỗng: đổi cấu trúc thư mục không được làm test xanh vô nghĩa."""
    assert len(WORKER_FILES) >= 40, f"Chỉ tìm thấy {len(WORKER_FILES)} file Python chạy trên worker."
    for rel in (
        "code_etl/shared/ops/data_quality.py",
        "code_etl/shared/ops/quarantine.py",
        "governance/schema_drift.py",
    ):
        assert REPO_ROOT / rel in WORKER_FILES, f"{rel} không nằm trong phạm vi quét"


def test_exemptions_still_exist():
    """Miễn trừ cho file đã xoá/đổi tên là rác — nó sẽ che file mới cùng tên."""
    missing = [rel for rel in EXEMPT if not (REPO_ROOT / rel).exists()]
    assert not missing, f"EXEMPT trỏ vào file không tồn tại: {missing}"


@pytest.mark.parametrize("path", WORKER_FILES, ids=_rel)
def test_parses_as_python38(path: Path):
    """Cú pháp mà parser 3.8 từ chối (match, with-ngoặc, ...) làm file không load được."""
    source = path.read_text(encoding="utf-8")
    try:
        ast.parse(source, filename=_rel(path), feature_version=(3, 8))
    except SyntaxError as e:
        pytest.fail(f"{_rel(path)}:{e.lineno}: cú pháp không có trong Python 3.8 — {e.msg}")


@pytest.mark.parametrize("path", WORKER_FILES, ids=_rel)
def test_annotations_are_lazy_on_python38(path: Path):
    """
    `dict[str, Any]` hoặc `X | None` trong annotation → TypeError lúc import
    trên 3.8, trừ khi file có `from __future__ import annotations`.
    """
    rel = _rel(path)
    if rel in EXEMPT:
        pytest.skip(EXEMPT[rel])
    tree = ast.parse(path.read_text(encoding="utf-8"))
    if _has_future_annotations(tree):
        return
    offenders = [(a.lineno, c) for a in _annotations(tree) if (c := _py39_construct(a))]
    assert not offenders, (
        f"{rel} dùng annotation cú pháp 3.9+ mà không có `from __future__ import annotations`. "
        f"spark-worker chạy Python 3.8 nên file này chết lúc import:\n"
        + "\n".join(f"  dòng {line}: {construct}" for line, construct in offenders[:10])
        + "\n\nThêm `from __future__ import annotations` ngay sau docstring của module."
    )


@pytest.mark.parametrize("path", WORKER_FILES, ids=_rel)
def test_no_stdlib_modules_newer_than_38(path: Path):
    """`zoneinfo` (3.9), `graphlib` (3.9), `tomllib` (3.11) không tồn tại trên worker."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    used = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            used.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            used.add(node.module.split(".")[0])
    bad = sorted(used & STDLIB_AFTER_38)
    assert not bad, (
        f"{_rel(path)} import {bad} — không có trong stdlib Python 3.8 của spark-worker. "
        "Với Asia/Ho_Chi_Minh, dùng timezone(timedelta(hours=7)) (không có giờ mùa hè)."
    )
