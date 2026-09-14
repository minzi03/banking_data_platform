"""
TD-7 Contract Test — Trino Catalog Naming
=========================================

Spark catalog = "lakehouse" (spark-defaults.conf).
Trino catalog = "iceberg" (catalog properties).

Code kết nối Trino dùng "lakehouse" sẽ fail runtime:
`Catalog 'lakehouse' not found`. Đã xảy ra 4 lần (bao gồm TD-7 Streamlit).

Lỗi này KHÔNG crash khi import — CI vẫn xanh. Test tĩnh scan code
Trino-facing: nếu file kết nối Trino, 'lakehouse' không được dùng
làm catalog parameter.

Chạy: pytest tests/governance/test_trino_catalog_contract.py -v
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _trino_connecting_files() -> list[Path]:
    """Find .py/.sql files that actually connect to or invoke Trino."""
    search_dirs = [
        "streamlit", "api", "scripts", "dbt",
        "docker/monitoring", "tests/integration",
    ]
    trino_signatures = re.compile(
        r"trino\.dbapi\.connect|run_trino_query|--catalog|trino.*execute",
        re.IGNORECASE,
    )
    results = []
    for rel_dir in search_dirs:
        d = REPO_ROOT / rel_dir
        if not d.exists():
            continue
        for ext in ("*.py", "*.sql"):
            for f in d.rglob(ext):
                text = f.read_text(encoding="utf-8", errors="replace")
                if trino_signatures.search(text):
                    results.append(f)
    return sorted(results)


def _has_lakehouse_catalog_usage(path: Path) -> list[str]:
    """Check if file uses 'lakehouse' as a Trino catalog parameter."""
    text = path.read_text(encoding="utf-8", errors="replace")
    offenders = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith(("#", "--", "*")):
            continue
        # Match Trino catalog parameter set to "lakehouse"
        if re.search(
            r"""catalog\s*=\s*["']lakehouse["']"""
            r"""|["']catalog["'].*["']lakehouse["']"""
            r"""|--catalog[=\s]+lakehouse""",
            stripped,
            re.IGNORECASE,
        ):
            offenders.append(f"{line_no}: {stripped}")
    return offenders


class TestSparkCatalogNameNeverReachesTrinoConsumers:
    """
    Code kết nối Trino không được dùng tên Spark catalog 'lakehouse'.
    Nếu file kết nối Trino mà chứa catalog='lakehouse' → runtime fail.
    """

    def test_no_lakehouse_in_trino_connecting_code(self):
        files = _trino_connecting_files()
        assert files, "Không tìm thấy file nào kết nối Trino"

        violations = []
        for f in files:
            for line in _has_lakehouse_catalog_usage(f):
                rel = f.relative_to(REPO_ROOT).as_posix()
                violations.append(f"{rel}:{line}")

        assert not violations, (
            "Spark catalog name 'lakehouse' dùng trong Trino connection.\n"
            "  Spark catalog = 'lakehouse' (spark-defaults.conf)\n"
            "  Trino catalog = 'iceberg' (Iceberg REST)\n"
            "Vi phạm:\n  " + "\n  ".join(violations)
        )

    def test_streamlit_uses_iceberg_catalog(self):
        """Streamlit đã từng fail vì catalog='lakehouse' (TD-7 origin)."""
        app = REPO_ROOT / "streamlit" / "app.py"
        if not app.exists():
            pytest.skip("streamlit/app.py không tồn tại")
        text = app.read_text(encoding="utf-8")
        connect_match = re.search(
            r"def\s+get_connection\(.*?\).*?(?=\ndef\s|\Z)",
            text, re.DOTALL,
        )
        assert connect_match, "get_connection() không tìm thấy"
        body = connect_match.group(0)
        assert "catalog=\"iceberg\"" in body or "catalog='iceberg'" in body, (
            "get_connection() phải dùng catalog='iceberg'"
        )
        assert "catalog=\"lakehouse\"" not in body and "catalog='lakehouse'" not in body, (
            "get_connection() chứa catalog='lakehouse' — phải là 'iceberg'"
        )

    def test_trino_cli_defaults_to_iceberg(self):
        """Makefile trino: target phải dùng --catalog iceberg."""
        makefile = REPO_ROOT / "Makefile"
        if not makefile.exists():
            pytest.skip("Makefile không tồn tại")
        text = makefile.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            if "trino" in line.lower() and "--catalog" in line:
                assert "lakehouse" not in line, (
                    f"Makefile:{line_no}: Trino CLI dùng 'lakehouse' — "
                    f"phải dùng 'iceberg'"
                )
