#!/usr/bin/env python3
"""
Sinh docs/DATA_DICTIONARY.md từ DDL + data contract.

KHÔNG viết tay tài liệu này. Viết tay nghĩa là nó sẽ lệch khỏi schema ngay ở
commit kế tiếp, và lệch một cách âm thầm — không có gì đỏ khi một cột được
thêm vào DDL mà tài liệu không biết.

Hai nguồn, mỗi nguồn một vai:

    docker/init_iceberg/*.sql     cột + kiểu của lakehouse (bronze/silver/gold)
    docker/init_postgres/*.sql    cột + kiểu của source system
    governance/datasets/*.yaml    owner, mục đích nghiệp vụ, SLA, quality rule,
                                  AI governance, upstream

DDL là nguồn sự thật cho CỘT. Contract là nguồn sự thật cho NGỮ NGHĨA. Cố ý
tách như vậy: `quality_rules.required_columns` trong contract là tập con có
chủ đích, không phải danh sách đầy đủ — dùng nó để liệt kê cột sẽ thiếu.

Usage:
    py -3 scripts/generate_data_dictionary.py              # ghi file
    py -3 scripts/generate_data_dictionary.py --check      # so, exit 1 nếu lệch
    py -3 scripts/generate_data_dictionary.py --stdout     # in ra stdout
"""

from __future__ import annotations

import argparse
import contextlib
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml


def _force_utf8_output() -> None:
    """
    Console Windows mặc định cp1258 → mọi thông báo tiếng Việt sẽ ném
    UnicodeEncodeError và giết script sau khi đã ghi file, làm exit code
    khác 0 dù công việc đã xong. Cùng lý do với generate_metrics_manifest.py.
    """
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            with contextlib.suppress(AttributeError, OSError):
                stream.reconfigure(encoding="utf-8", errors="replace")


_force_utf8_output()

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT = REPO_ROOT / "docs" / "03-data" / "DATA_DICTIONARY.md"

ICEBERG_DDL = REPO_ROOT / "docker" / "init_iceberg"
POSTGRES_DDL = REPO_ROOT / "docker" / "init_postgres"
CONTRACTS = REPO_ROOT / "governance" / "datasets"

# File DDL bị loại, kèm lý do. Không loại im lặng.
SKIP_DDL = {
    "04_ddl_bronze_cdc_old.sql": "bản cũ đã thay bằng 04_ddl_bronze_cdc.sql",
    "08_ddl_data_vault_example.sql": "DDL minh hoạ cho DATA_VAULT_MAPPING, không triển khai",
    "05_security.sql": "role và grant, không phải bảng dữ liệu",
    "06_ddl_superset.sql": "schema nội bộ của Superset",
    "07_ddl_mlflow.sql": "schema nội bộ của MLflow",
}

# Cột chứa dữ liệu cá nhân. Nguồn: masking áp ở Gold/serving (xem SECURITY.md).
PII_HINTS = re.compile(
    r"full_name|first_name|last_name|email|phone|address|id_number|dob|date_of_birth|national_id",
    re.IGNORECASE,
)

CREATE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.\"]+)\s*\((.*?)\n\s*\)",
    re.IGNORECASE | re.DOTALL,
)


@dataclass
class Column:
    name: str
    dtype: str
    comment: str = ""

    @property
    def is_pii(self) -> bool:
        return bool(PII_HINTS.search(self.name))


@dataclass
class Table:
    fqn: str
    source_file: str
    columns: list[Column] = field(default_factory=list)
    contract: dict | None = None

    @property
    def layer(self) -> str:
        if self.contract and self.contract.get("layer"):
            return str(self.contract["layer"])
        parts = self.fqn.split(".")
        return parts[-2] if len(parts) >= 2 else "unknown"


def _split_column_defs(body: str) -> list[str]:
    """Tách thân CREATE TABLE thành từng dòng định nghĩa, tôn trọng ngoặc lồng."""
    out, depth, cur = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if cur:
        out.append("".join(cur))
    return out


CONSTRAINT_START = re.compile(
    r"^\s*(CONSTRAINT|PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|CHECK|PARTITIONED|USING|TBLPROPERTIES)\b",
    re.IGNORECASE,
)


def parse_ddl(path: Path) -> list[Table]:
    text = path.read_text(encoding="utf-8", errors="replace")
    tables: list[Table] = []

    for match in CREATE_RE.finditer(text):
        fqn = match.group(1).replace('"', "")
        body = match.group(2)
        table = Table(fqn=fqn, source_file=path.name)

        for raw in _split_column_defs(body):
            line = raw.strip()
            if not line or line.startswith("--") or CONSTRAINT_START.match(line):
                continue
            # Bỏ comment cuối dòng nhưng giữ lại làm mô tả
            comment = ""
            if "--" in line:
                line, _, comment = line.partition("--")
                comment = comment.strip()
                line = line.strip()
            parts = line.split()
            if len(parts) < 2:
                continue
            name = parts[0].replace('"', "")
            dtype = parts[1].rstrip(",")
            # Gom kiểu có ngoặc bị tách: DECIMAL (18,2)
            if len(parts) > 2 and parts[2].startswith("("):
                dtype += parts[2]
            table.columns.append(Column(name=name, dtype=dtype.upper(), comment=comment))

        if table.columns:
            tables.append(table)

    return tables


def load_contracts() -> dict[str, dict]:
    """Ánh xạ tên bảng vật lý → contract."""
    by_table: dict[str, dict] = {}
    for path in sorted(CONTRACTS.glob("*.yaml")):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(contract, dict):
            continue
        loc = contract.get("physical_location") or {}
        table = loc.get("table")
        namespace = loc.get("namespace")
        if table:
            by_table[f"{namespace}.{table}" if namespace else str(table)] = contract
    return by_table


def collect_tables() -> list[Table]:
    contracts = load_contracts()
    tables: list[Table] = []

    for ddl_dir in (ICEBERG_DDL, POSTGRES_DDL):
        for path in sorted(ddl_dir.glob("*.sql")):
            if path.name in SKIP_DDL:
                continue
            for table in parse_ddl(path):
                short = ".".join(table.fqn.split(".")[-2:])
                table.contract = contracts.get(short)
                tables.append(table)

    return sorted(tables, key=lambda t: (t.layer, t.fqn))


LAYER_ORDER = ["source", "bronze", "silver", "gold", "serving", "ops", "meta"]


def render(tables: list[Table]) -> str:
    lines: list[str] = []
    a = lines.append

    a("# Data Dictionary")
    a("")
    a("> ⚠️ **File này được SINH TỰ ĐỘNG. Đừng sửa tay.**")
    a(">")
    a("> Sinh bởi `scripts/generate_data_dictionary.py` từ DDL (`docker/init_*/`)")
    a("> và data contract (`governance/datasets/`). Sửa tay sẽ bị")
    a("> `tests/governance/test_data_dictionary_current.py` bắt.")
    a(">")
    a("> Sinh lại: `py -3 scripts/generate_data_dictionary.py`")
    a("")

    with_contract = sum(1 for t in tables if t.contract)
    total_cols = sum(len(t.columns) for t in tables)
    pii_cols = sum(1 for t in tables for c in t.columns if c.is_pii)

    a(
        f"**{len(tables)} bảng · {total_cols} cột · {with_contract} bảng có data contract · "
        f"{pii_cols} cột nghi chứa PII**"
    )
    a("")

    by_layer: dict[str, list[Table]] = {}
    for t in tables:
        by_layer.setdefault(t.layer, []).append(t)

    ordered = [x for x in LAYER_ORDER if x in by_layer] + sorted(k for k in by_layer if k not in LAYER_ORDER)

    a("## Mục lục")
    a("")
    a("| Tầng | Số bảng | Số cột |")
    a("|---|---:|---:|")
    for layer in ordered:
        cols = sum(len(t.columns) for t in by_layer[layer])
        a(f"| [{layer}](#{layer}) | {len(by_layer[layer])} | {cols} |")
    a("")
    a("---")
    a("")

    for layer in ordered:
        a(f"## {layer}")
        a("")
        for table in by_layer[layer]:
            a(f"### `{table.fqn}`")
            a("")
            contract = table.contract
            if contract:
                purpose = " ".join(str(contract.get("business_purpose", "")).split())
                if purpose:
                    a(purpose)
                    a("")
                meta = []
                if contract.get("owner"):
                    meta.append(f"**Owner**: {contract['owner']}")
                if contract.get("refresh_sla"):
                    meta.append(f"**SLA**: {contract['refresh_sla']}")
                if contract.get("quality_class"):
                    meta.append(f"**Quality class**: {contract['quality_class']}")
                if contract.get("dag_id"):
                    meta.append(f"**DAG**: `{contract['dag_id']}`")
                if meta:
                    a(" · ".join(meta))
                    a("")

                rules = contract.get("quality_rules") or {}
                bits = []
                if rules.get("unique_column_sets"):
                    keys = ", ".join("(" + ", ".join(s) + ")" for s in rules["unique_column_sets"])
                    bits.append(f"**Grain**: {keys}")
                if rules.get("min_row_count") is not None:
                    bits.append(f"**Tối thiểu**: {rules['min_row_count']:,} dòng")
                if rules.get("freshness_sla_hours") is not None:
                    bits.append(f"**Freshness**: {rules['freshness_sla_hours']}h")
                if bits:
                    a(" · ".join(bits))
                    a("")

                gov = contract.get("ai_governance") or {}
                if gov.get("risk_tier"):
                    prohibited = gov.get("prohibited_uses") or []
                    line = f"**AI risk tier**: `{gov['risk_tier']}`"
                    if prohibited:
                        line += " · **Cấm dùng cho**: " + ", ".join(f"`{p}`" for p in prohibited)
                    a(line)
                    a("")

                upstream = contract.get("upstream_dataset_ids") or []
                if upstream:
                    a("**Upstream**: " + ", ".join(f"`{u}`" for u in upstream))
                    a("")
            else:
                a("_Chưa có data contract trong `governance/datasets/`._")
                a("")

            a("| Cột | Kiểu | PII | Ghi chú |")
            a("|---|---|:-:|---|")
            for col in table.columns:
                pii = "⚠️" if col.is_pii else ""
                note = col.comment.replace("|", "\\|")
                a(f"| `{col.name}` | `{col.dtype}` | {pii} | {note} |")
            a("")
            a(f"<sub>Nguồn DDL: `{table.source_file}`</sub>")
            a("")

        a("---")
        a("")

    a("## Ghi chú")
    a("")
    a("**Cột PII được đánh dấu bằng heuristic tên cột**, không phải bằng phân loại")
    a("thủ công. Hai giới hạn cần biết:")
    a("")
    a("- **Sẽ bỏ sót** cột nhạy cảm đặt tên không theo mẫu thông dụng.")
    a("- **Đánh dấu cả cột đã masking** (ví dụ `full_name_masked`). Đây là chủ ý:")
    a("  cột đã che vẫn nằm trong lineage PII và vẫn cần kiểm soát truy cập.")
    a("")
    a("Đây là chỉ báo, không phải bản kiểm kê đầy đủ — bản kiểm kê thật là")
    a("`PII_INVENTORY.md`, hiện chưa có (xem `DOCUMENTATION_PLAN.md` §3 nhóm F).")
    a("")
    a("**File DDL bị loại khỏi tài liệu này**, kèm lý do:")
    a("")
    for name, reason in sorted(SKIP_DDL.items()):
        a(f"- `{name}` — {reason}")
    a("")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Sinh Data Dictionary từ DDL + contract")
    parser.add_argument("--check", action="store_true", help="So với file đã commit, exit 1 nếu lệch")
    parser.add_argument("--stdout", action="store_true", help="In ra stdout thay vì ghi file")
    args = parser.parse_args()

    tables = collect_tables()
    if not tables:
        print("LỖI: không parse được bảng nào từ DDL — nghi ngờ regex hỏng", file=sys.stderr)
        return 2

    content = render(tables)

    if args.stdout:
        sys.stdout.write(content)
        return 0

    if args.check:
        if not OUTPUT.exists():
            print(f"LỖI: {OUTPUT.relative_to(REPO_ROOT)} chưa tồn tại", file=sys.stderr)
            return 1
        current = OUTPUT.read_text(encoding="utf-8")
        if current != content:
            print(
                f"LỖI: {OUTPUT.relative_to(REPO_ROOT)} đã lệch khỏi DDL/contract.\n"
                "Chạy: py -3 scripts/generate_data_dictionary.py",
                file=sys.stderr,
            )
            return 1
        print(f"OK: {OUTPUT.relative_to(REPO_ROOT)} khớp với nguồn ({len(tables)} bảng)")
        return 0

    # newline="\n" là BẮT BUỘC, không phải tuỳ chọn. .gitattributes đặt
    # `* text=auto eol=lf` vì host Windows và container Linux dùng chung
    # working tree. Ghi CRLF sẽ làm git báo file này modified ngay sau khi
    # sinh — đúng lỗi mà header .gitattributes ghi là đã phá
    # generate_metrics_manifest.py (nó stamp git_dirty và từ chối promote).
    OUTPUT.write_text(content, encoding="utf-8", newline="\n")
    cols = sum(len(t.columns) for t in tables)
    print(f"Đã ghi {OUTPUT.relative_to(REPO_ROOT)} — {len(tables)} bảng, {cols} cột")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
