"""So sánh hai lượt benchmark (x1 / x10) trong một thư mục kết quả.

    py -3 scripts/benchmark/compare.py docs/evidence/benchmarks/scale-x10-2026-09-30

Đầu vào: x1_airflow.json, x10_airflow.json (airflow_durations.py) và
x1_storage.json, x10_storage.json (storage_stats.py).
"""

import json
import sys
from pathlib import Path


def load(name: str) -> dict:
    return json.loads((Path(sys.argv[1]) / name).read_text(encoding="utf-8"))


def in_layer(stats: dict, layer: str) -> list[dict]:
    return [v for k, v in stats.items() if k.startswith(layer)]


a, b = load("x1_airflow.json"), load("x10_airflow.json")
ta = tb = 0.0
for dag in a:
    x, y = a[dag]["work_wall_s"], b[dag]["work_wall_s"]
    ta, tb = ta + x, tb + y
    print(f"{dag:28s} x1={x:7.1f}s  x10={y:7.1f}s  ratio={y / x:4.1f}")
print(f"{'sum of DAG work':28s} x1={ta:7.1f}s  x10={tb:7.1f}s  ratio={tb / ta:4.1f}")

tasks = [(d, a[dag]["tasks"].get(t), dag.replace("_dag", ""), t) for dag in b for t, d in b[dag]["tasks"].items()]
print("-- slowest tasks at x10")
for d, x1, dag, t in sorted(tasks, reverse=True)[:8]:
    print(f"  {d:6.1f}s (x1 {x1}s) {dag}.{t}")

sa, sb = load("x1_storage.json"), load("x10_storage.json")
for layer in ("bronze", "silver", "gold"):
    la, lb = in_layer(sa, layer), in_layer(sb, layer)
    ra, rb = sum(x["rows"] for x in la), sum(x["rows"] for x in lb)
    ma, mb = sum(x["bytes"] for x in la) / 1e6, sum(x["bytes"] for x in lb) / 1e6
    fa, fb = sum(x["files"] for x in la), sum(x["files"] for x in lb)
    print(f"{layer:7s} rows {ra:>10,} -> {rb:>11,}  MB {ma:7.1f} -> {mb:7.1f}  files {fa} -> {fb}")

print("-- biggest tables at x10 (MB, bytes/row)")
for k, v in sorted(sb.items(), key=lambda kv: -kv[1]["bytes"])[:6]:
    print(f"  {k:40s} {v['bytes'] / 1e6:7.1f} MB  {v['bytes'] / max(v['rows'], 1):5.1f} B/row  rows {v['rows']:,}")
