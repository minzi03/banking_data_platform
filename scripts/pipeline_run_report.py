"""Read-only report for pipeline lifecycle audit rows."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from typing import Any

TABLE = "opslakehouse.pipeline_run_log"
STATUSES = {"RUNNING", "SUCCESS", "FAILED", "SKIPPED"}


def _json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    return value


def build_query(
    status: str | None = None,
    dag_id: str | None = None,
    stale_minutes: int | None = None,
    limit: int = 50,
) -> tuple[str, list[Any]]:
    if limit < 1:
        raise ValueError("limit must be positive")
    if status is not None and status not in STATUSES:
        raise ValueError(f"unknown pipeline status: {status}")
    if stale_minutes is not None and stale_minutes < 1:
        raise ValueError("stale_minutes must be positive")

    clauses: list[str] = []
    params: list[Any] = []
    if status is not None:
        clauses.append("status = %s")
        params.append(status)
    if dag_id is not None:
        clauses.append("dag_id = %s")
        params.append(dag_id)
    if stale_minutes is not None:
        clauses.append("status = 'RUNNING' AND started_at < NOW() - (%s * INTERVAL '1 minute')")
        params.append(stale_minutes)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    query = (
        "SELECT dag_id, task_id, status, cob_dt, rows_processed, execution_time_s, "
        "error_message, started_at, completed_at "
        f"FROM {TABLE}{where} ORDER BY started_at DESC LIMIT %s"
    )
    params.append(limit)
    return query, params


def fetch_rows(connection: Any, query: str, params: list[Any]) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        return [_row_dict(columns, row) for row in rows]


def _row_dict(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    return dict(zip(columns, map(_json_value, row), strict=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", choices=sorted(STATUSES))
    parser.add_argument("--dag-id")
    parser.add_argument("--stale-minutes", type=int, help="Show RUNNING rows older than N minutes")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    try:
        query, params = build_query(args.status, args.dag_id, args.stale_minutes, args.limit)
        import psycopg2

        connection = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            dbname=os.environ.get("POSTGRES_DB", "banking_db"),
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        try:
            rows = fetch_rows(connection, query, params)
        finally:
            connection.close()
    except (KeyError, OSError, ValueError, ImportError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # database-driver errors vary by installed version
        print(f"ERROR: pipeline audit report query failed: {exc}", file=sys.stderr)
        return 1

    if args.as_json:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        for row in rows:
            print(f"{row['started_at']} {row['status']} {row['dag_id']} {row['task_id']} cob_dt={row['cob_dt']}")
        print(f"Rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
