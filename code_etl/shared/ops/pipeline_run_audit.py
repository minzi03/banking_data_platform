"""DB-API audit writer for pipeline task lifecycle events."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

TABLE = "opslakehouse.pipeline_run_log"
STATUSES = frozenset({"RUNNING", "SUCCESS", "FAILED", "SKIPPED"})


@dataclass(frozen=True)
class PipelineRun:
    dag_id: str
    task_id: str
    status: str
    cob_dt: date
    rows_processed: int | None = None
    execution_time_s: float | None = None
    error_message: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


def _validate(run: PipelineRun) -> None:
    if not run.dag_id or not run.task_id:
        raise ValueError("dag_id and task_id are required")
    if run.status not in STATUSES:
        raise ValueError(f"unknown pipeline status: {run.status}")
    if run.rows_processed is not None and run.rows_processed < 0:
        raise ValueError("rows_processed must be non-negative")
    if run.execution_time_s is not None and run.execution_time_s < 0:
        raise ValueError("execution_time_s must be non-negative")


def insert_run(connection: Any, run: PipelineRun) -> None:
    """Insert one lifecycle record using parameterized SQL."""
    _validate(run)
    query = f"""
        INSERT INTO {TABLE}
            (dag_id, task_id, status, cob_dt, rows_processed, execution_time_s,
             error_message, started_at, completed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s,
                COALESCE(%s, NOW()), %s)
    """
    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (
                run.dag_id,
                run.task_id,
                run.status,
                run.cob_dt,
                run.rows_processed,
                run.execution_time_s,
                run.error_message,
                run.started_at,
                run.completed_at,
            ),
        )
    connection.commit()


def connect_from_env() -> Any:
    """Create a PostgreSQL connection without committed secret defaults."""
    missing = [name for name in ("POSTGRES_USER", "POSTGRES_PASSWORD") if not os.environ.get(name)]
    if missing:
        raise OSError(f"missing required environment variables: {', '.join(missing)}")
    import psycopg2

    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "banking_db"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
