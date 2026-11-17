"""Read reconciliation exceptions from PostgreSQL without mutating state."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from decimal import Decimal
from typing import Any

TABLE = "opslakehouse.reconciliation_log"
STATUSES = {
    "MATCHED",
    "MISMATCH",
    "MISSING",
    "DUPLICATE",
    "PENDING",
    "AMBIGUOUS",
    "NOT_COMPARABLE",
}


def _json_value(value: Any) -> Any:
    if isinstance(value, (date, Decimal)):
        return str(value)
    return value


def build_query(cob_dt: str | None = None, status: str | None = None, limit: int = 50) -> tuple[str, list[Any]]:
    """Build a parameterized read-only query for reconciliation results."""
    if limit < 1:
        raise ValueError("limit must be positive")
    if status is not None and status not in STATUSES:
        raise ValueError(f"unknown reconciliation status: {status}")
    if cob_dt is not None:
        date.fromisoformat(cob_dt)

    clauses: list[str] = []
    params: list[Any] = []
    if cob_dt is not None:
        clauses.append("cob_dt = %s")
        params.append(cob_dt)
    if status is not None:
        clauses.append("reconciliation_status = %s")
        params.append(status)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    query = (
        "SELECT cob_dt, entity, metric_name, reconciliation_status, check_status, "
        "stream_value, batch_value, delta, tolerance, details, checked_at "
        f"FROM {TABLE}{where} ORDER BY checked_at DESC LIMIT %s"
    )
    params.append(limit)
    return query, params


def _row_dict(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    return dict(zip(columns, map(_json_value, row), strict=True))


def fetch_rows(connection: Any, query: str, params: list[Any]) -> list[dict[str, Any]]:
    """Fetch rows through a DB-API connection and return JSON-ready dictionaries."""
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        return [_row_dict(columns, row) for row in rows]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cob-dt", help="Filter business date YYYY-MM-DD")
    parser.add_argument("--status", choices=sorted(STATUSES), help="Filter business reconciliation status")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--json", action="store_true", dest="as_json", help="Emit JSON instead of table-like text")
    args = parser.parse_args(argv)

    try:
        query, params = build_query(args.cob_dt, args.status, args.limit)
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
        print(f"ERROR: reconciliation report query failed: {exc}", file=sys.stderr)
        return 1

    if args.as_json:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        for row in rows:
            print(
                f"{row['cob_dt']} {row['reconciliation_status']} {row['check_status']} "
                f"{row['entity']} {row['metric_name']} delta={row['delta']}"
            )
        print(f"Rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
