#!/usr/bin/env python3
"""
Sinh docs/DATA_CONTRACTS.md và docs/LINEAGE.md từ governance/datasets/*.yaml.

Cả hai đều là PHÉP CHIẾU của 33 data contract. Viết tay sẽ lệch ngay khi ai đó
thêm hoặc sửa một contract, và lệch âm thầm — không có gì đỏ.

LINEAGE.md không chỉ vẽ đồ thị. Nó còn kiểm hai thứ mà con người khó thấy khi
đọc từng file riêng lẻ:

  1. Tham chiếu treo — upstream_dataset_ids trỏ tới dataset_id không tồn tại.
     Lineage đứt ở đó, và không có gì báo.
  2. Dataset không có consumer — có thể hợp lệ (bảng serving là lá), cũng có
     thể là dấu hiệu một nhánh được xây rồi bỏ quên.

Usage:
    py -3 scripts/generate_governance_docs.py              # ghi file
    py -3 scripts/generate_governance_docs.py --check      # so, exit 1 nếu lệch
"""

from __future__ import annotations

import argparse
import contextlib
import sys
from collections import defaultdict
from pathlib import Path

import yaml


def _force_utf8_output() -> None:
    """Console Windows mặc định cp1258 → thông báo tiếng Việt ném UnicodeEncodeError."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            with contextlib.suppress(AttributeError, OSError):
                stream.reconfigure(encoding="utf-8", errors="replace")


_force_utf8_output()

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = REPO_ROOT / "governance" / "datasets"
CONTRACTS_OUT = REPO_ROOT / "docs" / "03-data" / "DATA_CONTRACTS.md"
LINEAGE_OUT = REPO_ROOT / "docs" / "03-data" / "LINEAGE.md"

LAYER_ORDER = ["bronze", "silver", "gold", "serving"]

# Dataset là lá một cách hợp lệ: bảng serving/current là điểm cuối, không ai
# tiêu thụ chúng trong đồ thị contract vì consumer nằm ngoài (Superset, API).
TERMINAL_SUFFIXES = ("_current_gold",)


def load_contracts() -> list[dict]:
    out: list[dict] = []
    for path in sorted(CONTRACTS_DIR.glob("*.yaml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data.get("dataset_id"):
            data["_file"] = path.name
            out.append(data)
    return out


def _table_of(contract: dict) -> str:
    loc = contract.get("physical_location") or {}
    ns, tb = loc.get("namespace"), loc.get("table")
    return f"{ns}.{tb}" if ns and tb else str(tb or "?")


def _grain_of(contract: dict) -> str:
    rules = contract.get("quality_rules") or {}
    sets = rules.get("unique_column_sets") or []
    if not sets:
        return "—"
    return ", ".join("(" + ", ".join(s) + ")" for s in sets)


def render_contracts(contracts: list[dict]) -> str:
    lines: list[str] = []
    a = lines.append

    a("# Data Contracts")
    a("")
    a("> ⚠️ **File này được SINH TỰ ĐỘNG. Đừng sửa tay.**")
    a(">")
    a("> Sinh bởi `scripts/generate_governance_docs.py` từ `governance/datasets/*.yaml`.")
    a("> Sinh lại: `py -3 scripts/generate_governance_docs.py`")
    a("")
    a("Data contract khai báo ràng buộc của một dataset: grain, cột bắt buộc,")
    a("số dòng tối thiểu, SLA freshness, và phân loại rủi ro AI. Chúng được")
    a("`governance/enforcement.py` dùng để chặn dữ liệu không đạt.")
    a("")

    by_layer: dict[str, list[dict]] = defaultdict(list)
    for c in contracts:
        by_layer[str(c.get("layer", "unknown"))].append(c)

    crit = sum(1 for c in contracts if c.get("quality_class") == "critical")
    high = sum(1 for c in contracts if (c.get("ai_governance") or {}).get("risk_tier") == "high_risk")
    a(f"**{len(contracts)} contract · {crit} quality_class `critical` · {high} AI `high_risk`**")
    a("")

    a("| Tầng | Số contract |")
    a("|---|---:|")
    ordered = [x for x in LAYER_ORDER if x in by_layer] + sorted(k for k in by_layer if k not in LAYER_ORDER)
    for layer in ordered:
        a(f"| {layer} | {len(by_layer[layer])} |")
    a("")
    a("---")
    a("")

    for layer in ordered:
        a(f"## {layer}")
        a("")
        a("| Dataset | Bảng vật lý | Grain | Quality | AI risk | DAG |")
        a("|---|---|---|---|---|---|")
        for c in sorted(by_layer[layer], key=lambda x: str(x["dataset_id"])):
            gov = c.get("ai_governance") or {}
            a(
                f"| `{c['dataset_id']}` "
                f"| `{_table_of(c)}` "
                f"| {_grain_of(c)} "
                f"| {c.get('quality_class', '—')} "
                f"| `{gov.get('risk_tier', '—')}` "
                f"| `{c.get('dag_id', '—')}` |"
            )
        a("")

    a("---")
    a("")
    a("## Ràng buộc chi tiết")
    a("")
    a("| Dataset | Cột bắt buộc | Không null | Tối thiểu | Freshness |")
    a("|---|---:|---:|---:|---:|")
    for c in sorted(contracts, key=lambda x: (str(x.get("layer")), str(x["dataset_id"]))):
        r = c.get("quality_rules") or {}
        req = len(r.get("required_columns") or [])
        nn = len(r.get("non_null_columns") or [])
        minrow = r.get("min_row_count")
        fresh = r.get("freshness_sla_hours")
        a(
            f"| `{c['dataset_id']}` | {req} | {nn} "
            f"| {f'{minrow:,}' if minrow is not None else '—'} "
            f"| {f'{fresh}h' if fresh is not None else '—'} |"
        )
    a("")
    a("> `required_columns` là **tập con có chủ đích**, không phải bản kiểm kê cột.")
    a("> Danh sách cột đầy đủ nằm ở [`DATA_DICTIONARY.md`](DATA_DICTIONARY.md),")
    a("> sinh từ DDL chứ không từ contract.")
    a("")

    return "\n".join(lines) + "\n"


def analyse_lineage(contracts: list[dict]) -> dict:
    ids = {str(c["dataset_id"]) for c in contracts}
    by_id = {str(c["dataset_id"]): c for c in contracts}

    downstream: dict[str, list[str]] = defaultdict(list)
    dangling: dict[str, list[str]] = {}

    for c in contracts:
        cid = str(c["dataset_id"])
        missing = []
        for up in c.get("upstream_dataset_ids") or []:
            if up in ids:
                downstream[up].append(cid)
            else:
                missing.append(up)
        if missing:
            dangling[cid] = missing

    consumed = set(downstream.keys())
    orphans = sorted(i for i in ids if i not in consumed)
    roots = sorted(i for i in ids if not (by_id[i].get("upstream_dataset_ids") or []))

    return {
        "by_id": by_id,
        "downstream": downstream,
        "dangling": dangling,
        "orphans": orphans,
        "roots": roots,
    }


def render_lineage(contracts: list[dict], g: dict) -> str:
    lines: list[str] = []
    a = lines.append
    by_id, downstream = g["by_id"], g["downstream"]
    dangling, orphans, roots = g["dangling"], g["orphans"], g["roots"]

    a("# Lineage — Bản Đồ Phụ Thuộc")
    a("")
    a("> ⚠️ **File này được SINH TỰ ĐỘNG. Đừng sửa tay.**")
    a(">")
    a("> Sinh bởi `scripts/generate_governance_docs.py` từ `upstream_dataset_ids`")
    a("> trong `governance/datasets/*.yaml`.")
    a("")
    a("Đây là lineage **khai báo** — thứ contract nói. Lineage **quan sát được**")
    a("(thực sự chạy) nằm ở OpenMetadata. Hai cái lệch nhau là tín hiệu đáng điều tra.")
    a("")
    a(f"**{len(contracts)} dataset · {sum(len(v) for v in downstream.values())} cạnh phụ thuộc**")
    a("")

    if dangling:
        a("---")
        a("")
        a("## ⚠️ Tham chiếu treo")
        a("")
        a("`upstream_dataset_ids` trỏ tới `dataset_id` **không tồn tại**. Lineage")
        a("đứt tại đây: dataset nguồn trông như không có ai dùng, còn dataset đích")
        a("trông như không có nguồn.")
        a("")
        a("| Dataset | Upstream không tồn tại |")
        a("|---|---|")
        for cid, missing in sorted(dangling.items()):
            a(f"| `{cid}` | {', '.join(f'`{m}`' for m in missing)} |")
        a("")
        total = sum(len(v) for v in dangling.values())
        a(f"**{len(dangling)} contract · {total} tham chiếu treo.**")
        a("")

    a("---")
    a("")
    a("## Gốc — không có upstream")
    a("")
    a("Dataset nhận dữ liệu từ ngoài đồ thị contract (source system, Kafka).")
    a("")
    for r in roots:
        a(f"- `{r}` — {by_id[r].get('layer', '?')}")
    a("")

    a("---")
    a("")
    a("## Lá — không có consumer")
    a("")
    a("Dataset không dataset nào khác tiêu thụ. **Không phải lỗi mặc định**:")
    a("bảng serving là điểm cuối hợp lệ, consumer của chúng (Superset, API)")
    a("nằm ngoài đồ thị contract.")
    a("")
    a("Đáng chú ý là các lá **không** phải serving — chúng được xây, được bảo trì,")
    a("và chưa có gì dùng tới.")
    a("")
    terminal = [o for o in orphans if o.endswith(TERMINAL_SUFFIXES)]
    notable = [o for o in orphans if not o.endswith(TERMINAL_SUFFIXES)]

    a("| Dataset | Tầng | Ghi chú |")
    a("|---|---|---|")
    for o in notable:
        a(f"| `{o}` | {by_id[o].get('layer', '?')} | **đáng xem lại** |")
    for o in terminal:
        a(f"| `{o}` | {by_id[o].get('layer', '?')} | serving — lá hợp lệ |")
    a("")
    a(f"**{len(notable)} lá đáng xem lại · {len(terminal)} lá serving hợp lệ.**")
    a("")

    a("---")
    a("")
    a("## Đồ thị đầy đủ")
    a("")
    a("Mỗi dataset kèm upstream (cái nó đọc) và downstream (cái đọc nó).")
    a("")

    by_layer: dict[str, list[str]] = defaultdict(list)
    for cid, c in by_id.items():
        by_layer[str(c.get("layer", "unknown"))].append(cid)

    ordered = [x for x in LAYER_ORDER if x in by_layer] + sorted(k for k in by_layer if k not in LAYER_ORDER)

    for layer in ordered:
        a(f"### {layer}")
        a("")
        for cid in sorted(by_layer[layer]):
            c = by_id[cid]
            a(f"**`{cid}`** · `{_table_of(c)}` · DAG `{c.get('dag_id', '—')}`")
            a("")
            ups = c.get("upstream_dataset_ids") or []
            if ups:
                marked = [f"`{u}`" + ("" if u in by_id else " ⚠️") for u in ups]
                a(f"- ↑ đọc từ: {', '.join(marked)}")
            else:
                a("- ↑ đọc từ: _(gốc)_")
            downs = sorted(downstream.get(cid, []))
            if downs:
                a(f"- ↓ được đọc bởi: {', '.join(f'`{d}`' for d in downs)}")
            else:
                a("- ↓ được đọc bởi: _(không có)_")
            a("")
        a("---")
        a("")

    a("## Giới hạn")
    a("")
    a("- Đây là lineage **cấp dataset**, không phải cấp cột. Không trả lời được")
    a('  *"cột này bắt nguồn từ đâu"*.')
    a("- Chỉ phản ánh **khai báo trong contract**. Một job đọc bảng mà không khai")
    a("  báo sẽ không xuất hiện ở đây. Ràng buộc khai báo ↔ SQL do")
    a("  `tests/governance/test_declared_sources_match_sql.py` giữ, nhưng nó kiểm")
    a("  YAML của job chứ không kiểm contract.")
    a("- Không có dataset nào của tầng CDC trong đồ thị này.")
    a("")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Sinh DATA_CONTRACTS.md và LINEAGE.md")
    parser.add_argument("--check", action="store_true", help="So với file đã commit, exit 1 nếu lệch")
    args = parser.parse_args()

    contracts = load_contracts()
    if len(contracts) < 20:
        print(
            f"LỖI: chỉ đọc được {len(contracts)} contract từ {CONTRACTS_DIR} — nghi ngờ glob hỏng",
            file=sys.stderr,
        )
        return 2

    graph = analyse_lineage(contracts)
    outputs = {
        CONTRACTS_OUT: render_contracts(contracts),
        LINEAGE_OUT: render_lineage(contracts, graph),
    }

    if args.check:
        drifted = []
        for path, content in outputs.items():
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                drifted.append(path.relative_to(REPO_ROOT).as_posix())
        if drifted:
            print(
                "LỖI: đã lệch khỏi governance/datasets/: " + ", ".join(drifted) + "\n"
                "Chạy: py -3 scripts/generate_governance_docs.py",
                file=sys.stderr,
            )
            return 1
        print(f"OK: cả hai file khớp với {len(contracts)} contract")
        return 0

    for path, content in outputs.items():
        # newline="\n": .gitattributes đặt eol=lf. Ghi CRLF sẽ làm git báo
        # file modified ngay sau khi sinh.
        path.write_text(content, encoding="utf-8", newline="\n")

    dangling = sum(len(v) for v in graph["dangling"].values())
    print(f"Đã ghi DATA_CONTRACTS.md + LINEAGE.md — {len(contracts)} contract, {dangling} tham chiếu treo")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
