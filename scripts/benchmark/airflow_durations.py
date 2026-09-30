"""Thời gian chạy của chuỗi batch cho một cob_dt (lượt manual mới nhất có conf cob_dt đó).

python bench_airflow.py 2026-09-29  → in JSON: mỗi DAG {wall_s, tasks: {task_id: duration_s}}
"""

import json
import sys

from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session

COB = sys.argv[1]
DAGS = [
    "bronze_core_banking_dag",
    "bronze_card_crm_dag",
    "bronze_digital_banking_dag",
    "silver_all_dag",
    "gold_all_dag",
    "dbt_serving_publish",
]
out = {}
with create_session() as s:
    for dag in DAGS:
        runs = (
            s.query(DagRun)
            .filter(DagRun.dag_id == dag, DagRun.run_id.like("manual__%"), DagRun.state == "success")
            .order_by(DagRun.start_date.desc())
            .all()
        )
        run = next((r for r in runs if (r.conf or {}).get("cob_dt") == COB), None)
        if run is None:
            out[dag] = None
            continue
        tis = s.query(TaskInstance).filter(TaskInstance.dag_id == dag, TaskInstance.run_id == run.run_id).all()
        work = [t for t in tis if t.duration and not t.task_id.startswith(("check_", "wait_", "dag_", "start", "end"))]
        first = min((t.start_date for t in work), default=run.start_date)
        last = max((t.end_date for t in work), default=run.end_date)
        out[dag] = {
            "run_id": run.run_id,
            "work_wall_s": round((last - first).total_seconds(), 1),
            "tasks": {t.task_id: round(t.duration, 1) for t in sorted(work, key=lambda t: t.task_id)},
        }
print("JSON_BEGIN")
print(json.dumps(out, indent=1))
print("JSON_END")
