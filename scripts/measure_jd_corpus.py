"""
Đo mật độ từ khoá trong corpus JD (`thamkhao/JD/`) — nguồn số liệu của
docs/09-analysis/JD_MARKET_ANALYSIS.md.

Corpus nằm ngoài repo nên số đo là `metric_type: manual`: script này tồn tại để
người sau đo lại được bằng đúng regex, không phải để CI chạy.

Đơn vị: số lần regex khớp / 100.000 ký tự (văn bản decode UTF-8).
Hai chế độ:
  raw     — đếm trên văn bản nguyên trạng (cách đo của bản 2026-09-21).
  dedup   — chỉ giữ mỗi dòng không rỗng MỘT lần trong cả nhóm. Trang scrape lặp lại
            nguyên đoạn "About us"/JD của cùng công ty; dedup cho biết con số raw có bị
            vài bài đăng lặp thổi phồng không.

Dùng:
  py -3 scripts/measure_jd_corpus.py ../thamkhao/JD            # bảng markdown
  py -3 scripts/measure_jd_corpus.py ../thamkhao/JD --json     # JSON
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

# Nhóm theo thời điểm file được đưa vào corpus. jd14 được thay bằng bản scrape lớn
# hơn (76k → 266k byte) sau lần đo 2026-09-21, nên chuyển sang nhóm C cùng jd15–16.
GROUPS: dict[str, list[str]] = {
    "A": [f"jd{i}.md" for i in range(1, 8)] + ["jd8_bank.md"],
    "B": [f"jd{i}.md" for i in range(9, 14)],
    "C": ["jd14.md", "jd15.md", "jd16.md"],
}

IC = re.IGNORECASE

# (nhãn, regex, flags). Regex viết hoa-sensitive khi viết thường sẽ khớp nhầm
# (AWS, MCP, RAG, CI/CD, SCD, DPD, NPL...).
TERMS: list[tuple[str, str, int]] = [
    # — nền tảng —
    ("SQL", r"\bSQL\b", IC),
    ("Python", r"\bpython\b", IC),
    ("Spark / PySpark", r"\b(?:py)?spark\b", IC),
    ("AWS", r"\bAWS\b", 0),
    ("GCP / BigQuery", r"\bGCP\b|google cloud|bigquery", IC),
    ("Azure", r"\bazure\b", IC),
    ("Data Quality", r"data quality|chất lượng dữ liệu", IC),
    ("Real-time / streaming", r"real[- ]?time|streaming", IC),
    ("Airflow", r"\bairflow\b", IC),
    ("Lakehouse", r"lakehouse", IC),
    ("CI/CD", r"\bCI\s*/\s*CD\b", IC),
    ("Kafka", r"\bkafka\b", IC),
    ("Data Governance", r"data governance|quản trị dữ liệu", IC),
    ("Docker", r"\bdocker\b", IC),
    ("Kubernetes", r"\bkubernetes\b|\bk8s\b", IC),
    ("Terraform / IaC", r"\bterraform\b|infrastructure as code|\bIaC\b", IC),
    # — lakehouse / engine —
    ("Iceberg", r"\biceberg\b", IC),
    ("Delta Lake", r"\bdelta lake\b|\bdelta\b(?= ?/)", IC),
    ("Trino / Presto", r"\btrino\b|\bpresto\b", IC),
    ("StarRocks", r"starrocks", IC),
    ("ClickHouse", r"clickhouse", IC),
    ("Snowflake", r"snowflake", IC),
    ("Databricks", r"databricks", IC),
    ("MS Fabric", r"\bfabric\b", IC),
    ("dbt", r"\bdbt\b", IC),
    ("Flink", r"\bflink\b", IC),
    ("CDC", r"\bCDC\b|change data capture", IC),
    ("Debezium", r"debezium", IC),
    ("Oracle", r"\boracle\b", IC),
    ("Informatica / ODI / SSIS", r"informatica|\bODI\b|\bSSIS\b", IC),
    # — vận hành / độ tin cậy —
    ("Data Contract", r"data contracts?", IC),
    ("Schema drift / evolution", r"schema (?:drift|evolution)", IC),
    ("Backfill", r"backfill", IC),
    ("Idempotency", r"idempoten", IC),
    ("Reconciliation / đối soát", r"reconcil|đối soát", IC),
    ("Lineage", r"lineage", IC),
    ("Observability", r"observability", IC),
    ("Freshness", r"freshness", IC),
    ("SLA", r"\bSLAs?\b", 0),
    ("Runbook", r"runbook", IC),
    ("RCA / root cause", r"\bRCA\b|root[- ]cause", IC),
    ("Incident", r"\bincident", IC),
    ("Anomaly detection", r"anomal", IC),
    ("Great Expectations / dbt-expectations", r"great expectations|dbt[_ -]expectations", IC),
    ("RTO / RPO", r"\bRTO\b|\bRPO\b", 0),
    # — mô hình hoá —
    ("SCD", r"\bSCD\b|slowly[- ]changing", IC),
    ("Star schema / dimensional", r"star schema|dimensional model|kimball", IC),
    ("Data Vault", r"data vault", IC),
    ("Data Mart", r"data ?marts?", IC),
    ("Semantic layer / metric layer", r"semantic (?:layer|model)|metrics? layer|metric definition", IC),
    ("Data product", r"data products?", IC),
    ("Data mesh", r"data mesh", IC),
    ("OBT / One Big Table", r"\bOBT\b|one big table", IC),
    # — AI / ML —
    ("Feature store", r"feature stores?", IC),
    ("MLOps", r"\bmlops\b", IC),
    ("MLflow", r"mlflow", IC),
    ("LLM / GenAI", r"\bLLMs?\b|generative ai|\bgenai\b", IC),
    ("RAG", r"\bRAG\b", 0),
    ("Vector DB / embeddings", r"vector (?:db|database|search|index|store)|embeddings?", IC),
    ("AI agent / agentic", r"\bagentic\b|ai agents?", IC),
    ("MCP", r"\bMCP\b", 0),
    ("AI-assisted dev (Cursor/Claude/Copilot)", r"\bcursor\b|\bclaude\b|\bcopilot\b", IC),
    # — BI —
    ("Power BI", r"power ?bi", IC),
    ("Tableau", r"tableau", IC),
    ("Superset", r"superset", IC),
    ("Looker / Metabase", r"looker|metabase", IC),
    ("A/B test / experiment", r"a/b test|experiment", IC),
    # — bảo mật / tuân thủ —
    ("PII / masking", r"\bPII\b|masking|pseudonymi", IC),
    ("RBAC / access control", r"\bRBAC\b|access control", IC),
    ("Decree 13 / PDPL", r"decree 13|nghị định 13|\bPDPL?\b|personal data protection", IC),
    ("Data residency", r"data residency|data localization", IC),
    ("BCBS 239 / Basel", r"bcbs|basel", IC),
    ("DAMA / DCAM", r"\bDAMA\b|\bDCAM\b|\bCDMP\b", IC),
    ("Unity Catalog", r"unity catalog", IC),
    # — nghiệp vụ ngân hàng / tài chính —
    ("Core banking", r"core banking|corebank", IC),
    ("AML / KYC", r"\bAML\b|anti[- ]money|\bKYC\b", IC),
    ("Fraud", r"\bfraud", IC),
    ("Credit risk / scoring", r"credit risk|credit scor|chấm điểm tín dụng", IC),
    ("NPL / DPD / roll rate / vintage", r"\bNPL\b|\bDPD\b|roll rate|vintage|delinquen", IC),
    # "collection" trơn khớp cả "data collection" — chỉ đếm nghĩa thu nợ.
    (
        "Collection / thu hồi nợ",
        r"debt collection|thu hồi nợ|collections? (?:strategy|strategies|analyst|analytics|portfolio|team|model|score)",
        IC,
    ),
    ("CASA / NIM / CIR", r"\bCASA\b|\bNIM\b|\bCIR\b", 0),
    ("Regulatory reporting", r"regulatory report|báo cáo (?:thống kê|tuân thủ)|\bSBV\b|ngân hàng nhà nước", IC),
    ("Payment / settlement / ledger", r"settlement|\bledger\b|payment", IC),
    # "provisioning" trơn khớp cả "provisioning services" (cloud) — chỉ đếm nghĩa dự phòng rủi ro.
    (
        "IFRS 9 / ECL / dự phòng",
        r"ifrs ?9|\bECL\b|loan loss provision|dự phòng rủi ro|trích lập dự phòng|\bprovisioning\b(?= (?:is|or|and|,))",
        IC,
    ),
    ("Customer 360", r"customer 360|single customer view", IC),
    ("Churn", r"\bchurn", IC),
    ("CLV / LTV", r"\bCLV\b|lifetime value|\bLTV\b", IC),
    ("ODS", r"\bODS\b", 0),
]


def md5(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def load(corpus: Path, names: list[str]) -> str:
    return "\n".join((corpus / n).read_text(encoding="utf-8") for n in names)


def dedup_lines(text: str) -> str:
    seen: set[str] = set()
    out = []
    for line in text.splitlines():
        key = line.strip()
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    return "\n".join(out)


def measure(text: str) -> dict[str, dict[str, float]]:
    n = len(text)
    res = {}
    for label, pattern, flags in TERMS:
        count = len(re.findall(pattern, text, flags))
        res[label] = {"count": count, "per_100k": round(count * 100_000 / n, 1) if n else 0.0}
    return res


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("corpus", type=Path)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    corpus = args.corpus
    files = sorted(p.name for p in corpus.glob("*.md"))
    known = {n for names in GROUPS.values() for n in names} | {"repo.md"}
    unknown = [f for f in files if f not in known]
    if unknown:
        print(f"File chưa được xếp nhóm: {unknown} — thêm vào GROUPS trước khi đo.", file=sys.stderr)
        return 2

    out: dict = {"files": {}, "groups": {}}
    for f in files:
        p = corpus / f
        out["files"][f] = {"bytes": p.stat().st_size, "md5": md5(p)}

    texts = {g: load(corpus, names) for g, names in GROUPS.items()}
    texts["ALL"] = "\n".join(texts[g] for g in GROUPS)
    for g, text in texts.items():
        d = dedup_lines(text)
        out["groups"][g] = {
            "chars": len(text),
            "chars_dedup": len(d),
            "raw": measure(text),
            "dedup": measure(d),
        }

    if args.json:
        json.dump(out, sys.stdout, ensure_ascii=False, indent=2)
        print()
        return 0

    print("| File | Bytes | MD5 |\n|---|---:|---|")
    for f, meta in out["files"].items():
        print(f"| `{f}` | {meta['bytes']:,} | `{meta['md5']}` |")
    print()
    gs = list(texts)
    print("| Nhóm | Ký tự | Ký tự sau dedup |\n|---|---:|---:|")
    for g in gs:
        print(f"| {g} | {out['groups'][g]['chars']:,} | {out['groups'][g]['chars_dedup']:,} |")
    print()
    head = " | ".join(f"{g}" for g in gs)
    print(f"| Term (/100k, raw) | {head} | C dedup | ALL count | ALL count dedup |")
    print("|---|" + "---:|" * (len(gs) + 3))
    for label, _, _ in TERMS:
        cells = " | ".join(str(out["groups"][g]["raw"][label]["per_100k"]) for g in gs)
        cd = out["groups"]["C"]["dedup"][label]["per_100k"]
        cnt = out["groups"]["ALL"]["raw"][label]["count"]
        cntd = out["groups"]["ALL"]["dedup"][label]["count"]
        print(f"| {label} | {cells} | {cd} | {cnt} | {cntd} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
