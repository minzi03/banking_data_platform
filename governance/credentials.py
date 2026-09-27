"""
Credential PostgreSQL cho các ghi JDBC — lấy từ môi trường, không có mặc định.

Trước đây audit.py và lineage.py đọc POSTGRES_PASSWORD với một giá trị mặc
định là mật khẩu dev: thiếu biến thì âm thầm dùng một mật khẩu đã commit, và ai
đọc repo cũng biết mật khẩu đó. Thiếu biến giờ là lỗi to tiếng — cùng cách
code_etl/shared/ops/contract_validation.py đã làm từ #46.

Đọc lúc GHI, không lúc khởi tạo: AuditLogger / LineageTracker vẫn dùng được để
ghi nhận trong bộ nhớ (test, DAG) ở nơi không có credential.
"""

from __future__ import annotations

import os


def postgres_jdbc_properties(purpose: str) -> dict[str, str]:
    missing = [name for name in ("POSTGRES_USER", "POSTGRES_PASSWORD") if not os.environ.get(name)]
    if missing:
        raise OSError(
            f"Thiếu biến môi trường {missing} để {purpose}. Container nhận chúng từ docker/.env qua docker-compose.yml."
        )
    return {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }
