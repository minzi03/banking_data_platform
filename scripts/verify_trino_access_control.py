"""
Kiểm access control trên Trino ĐANG CHẠY — thứ mà test tĩnh không kiểm được.

    py -3 scripts/verify_trino_access_control.py                     # stack chính
    py -3 scripts/verify_trino_access_control.py --container ci-trino

Test tĩnh (tests/governance/test_trino_access_control.py) kiểm rules.json đúng
ngữ nghĩa. Script này kiểm hai điều chỉ engine thật trả lời được:

  1. Trino CÓ nạp luật. Nếu access-control.properties không được mount, Trino
     chạy access control `default` — cho phép tất cả — và mọi "phải bị chặn"
     dưới đây sẽ thành công. Script fail đúng lúc đó.
  2. Mask CHẠY được: Trino chỉ kiểm kiểu của biểu thức mask lúc có người query
     bảng, nên mask lệch kiểu cột là lỗi runtime, không phải lỗi khởi động.

Chạy bằng `docker exec <container> trino --user <user>`: user do client tự khai
(chưa có xác thực), nên script đóng vai từng client.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RULES_PATH = REPO_ROOT / "docker" / "init_trino" / "rules.json"

ADMIN = "admin"
# Bảng PII chắc chắn có dữ liệu sau khi pipeline batch chạy. Mask trên các
# bảng khác vẫn được kiểm nếu bảng tồn tại, nhưng hai bảng này là bắt buộc.
REQUIRED_MASKED_TABLES = {("silver", "dim_customer"), ("bronze", "core_customer")}


class Trino:
    def __init__(self, container: str):
        self.container = container

    def run(self, user: str, sql: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["docker", "exec", self.container, "trino", "--user", user,
             "--output-format", "CSV_UNQUOTED", "--execute", sql],
            capture_output=True, text=True, encoding="utf-8",
        )


class Checker:
    def __init__(self, trino: Trino):
        self.trino = trino
        self.failures: list[str] = []
        self.passed = 0

    def _record(self, ok: bool, label: str, detail: str = "") -> bool:
        if ok:
            self.passed += 1
            print(f"  ✓ {label}")
        else:
            self.failures.append(f"{label}: {detail}".strip())
            print(f"  ✗ {label}\n      {detail}")
        return ok

    def allowed(self, user: str, sql: str, label: str) -> str | None:
        result = self.trino.run(user, sql)
        ok = result.returncode == 0
        self._record(ok, f"{user}: {label}", result.stderr.strip()[-400:])
        return result.stdout if ok else None

    def denied(self, user: str, sql: str, label: str) -> None:
        result = self.trino.run(user, sql)
        # Phải là "Access Denied" — một lỗi khác (bảng không tồn tại, cú pháp)
        # cũng làm lệnh fail nhưng không chứng minh gì về quyền.
        ok = result.returncode != 0 and "Access Denied" in result.stderr
        detail = "lệnh THÀNH CÔNG — luật không được áp" if result.returncode == 0 else result.stderr.strip()[-400:]
        self._record(ok, f"{user}: bị chặn — {label}", detail)

    def check(self, ok: bool, label: str, detail: str = "") -> None:
        self._record(ok, label, detail)


def _masked_tables(rules: dict) -> dict[tuple[str, str], dict[str, list]]:
    """(schema, table) → {user_regex: [cột bị che]} lấy từ rules.json."""
    out: dict[tuple[str, str], dict[str, list]] = {}
    for rule in rules["tables"]:
        if rule.get("columns"):
            out.setdefault((rule["schema"], rule["table"]), {})[rule["user"]] = [c["name"] for c in rule["columns"]]
    return out


def verify(trino: Trino) -> Checker:
    c = Checker(trino)
    rules = json.loads(RULES_PATH.read_text(encoding="utf-8"))

    print("=== Luật có được nạp ===")
    c.denied("analytics_report", "SELECT 1 FROM iceberg.bronze.core_customer LIMIT 1", "analytics đọc Bronze")
    c.denied("nobody", "SELECT 1 FROM iceberg.gold.mart_customer_360 LIMIT 1", "user lạ đọc Gold")
    c.denied("customer_api", "SELECT 1 FROM iceberg.silver.dim_customer LIMIT 1", "client serving đọc Silver")

    print("=== Mask chạy được, đúng kiểu, trên mọi bảng có mask ===")
    exercised = set()
    for (schema, table), by_user in sorted(_masked_tables(rules).items()):
        exists = trino.run(ADMIN, f"SHOW TABLES FROM iceberg.{schema} LIKE '{table}'")
        if exists.returncode != 0 or table not in exists.stdout.split():
            print(f"  - bỏ qua iceberg.{schema}.{table}: bảng chưa tồn tại")
            continue
        exercised.add((schema, table))
        for user_regex, columns in by_user.items():
            user = user_regex.split("|")[0]
            c.allowed(user, f"SELECT {', '.join(columns)} FROM iceberg.{schema}.{table} LIMIT 1",
                      f"đọc {schema}.{table} với {len(columns)} cột bị che")
    missing = REQUIRED_MASKED_TABLES - exercised
    c.check(not missing, "các bảng PII bắt buộc đều được kiểm", f"chưa tồn tại: {sorted(missing)}")

    print("=== Giá trị: bị che với analytics, nguyên bản với admin ===")
    # ORDER BY để hai lần đọc lấy CÙNG một dòng — so được 4 số cuối.
    one_cccd = "SELECT cccd FROM iceberg.silver.dim_customer WHERE cccd IS NOT NULL ORDER BY customer_sk LIMIT 1"
    masked = c.allowed("analytics_report", one_cccd, "đọc cccd")
    raw = c.allowed(ADMIN, one_cccd, "đọc cccd")
    if masked is not None and raw is not None:
        masked_value, raw_value = masked.strip(), raw.strip()
        c.check(masked_value.startswith("***********"), "analytics thấy cccd đã che", f"nhận: {masked_value[:4]}…")
        c.check(raw_value.isdigit() and not raw_value.startswith("*"), "admin thấy cccd nguyên bản",
                "giá trị admin không phải chuỗi số")
        c.check(masked_value[-4:] == raw_value[-4:], "mask giữ 4 số cuối như khai trong rbac.py")

    print("=== Quyền ghi ===")
    c.denied("analytics_report", "CREATE TABLE iceberg.sandbox.acl_probe AS SELECT 1 AS x", "role read-only tạo bảng")
    c.denied("dbt", "CREATE TABLE iceberg.gold.acl_probe AS SELECT 1 AS x", "dbt tạo bảng ngoài serving")
    if c.allowed("dbt", "CREATE TABLE iceberg.serving.acl_probe AS SELECT 1 AS x", "tạo bảng trong serving") is not None:
        c.allowed("dbt", "DROP TABLE iceberg.serving.acl_probe", "xoá bảng trong serving")

    print("=== Metadata mà client cần ===")
    c.allowed("dbt", "SELECT count(*) FROM system.metadata.materialized_views", "đọc system.metadata (dbt-trino cần)")
    c.allowed("customer_api", "SHOW TABLES FROM iceberg.serving", "liệt kê bảng serving")
    c.denied("customer_api", "SELECT count(*) FROM system.runtime.queries", "xem truy vấn của người khác")

    return c


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Kiểm access control trên Trino đang chạy")
    parser.add_argument("--container", default="banking-trino")
    args = parser.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")

    checker = verify(Trino(args.container))
    print(f"\n{checker.passed} đạt, {len(checker.failures)} không đạt")
    for failure in checker.failures:
        print(f"::error::{failure}")
    return 1 if checker.failures else 0


if __name__ == "__main__":
    sys.exit(main())
