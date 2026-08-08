"""
CDC Freshness Exporter — Prometheus Metrics for Banking Data Platform

Queries Trino for CDC freshness (seconds since last consolidation)
and exposes metrics at /metrics endpoint.

Metrics:
    cdc_freshness_seconds{table="dim_customer_current"}  — seconds since last consolidation
    cdc_freshness_seconds{table="dim_account_current"}   — seconds since last consolidation
    cdc_event_count{table="dim_customer_current"}        — row count in current-state table
    exporter_up                                          — 1 if Trino is reachable
"""

import http.server
import json
import urllib.request
import time
import os

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = os.getenv("TRINO_PORT", "8080")
TRINO_URL = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/statement"
FRESHNESS_TABLES = [
    "lakehouse.silver.dim_customer_current",
    "lakehouse.silver.dim_account_current",
]

# Cache to avoid hammering Trino
_cache = {"data": None, "timestamp": 0}
CACHE_TTL = 30  # seconds


def query_trino(sql: str) -> list:
    """Execute SQL via Trino REST API and return rows.

    Trino REST API: data is only present in RUNNING-state responses.
    The FINISHED response drops the data field. So we capture data
    as soon as columns+data appear, and stop when state != QUEUED.
    """
    data_bytes = sql.encode("utf-8")
    req = urllib.request.Request(
        TRINO_URL,
        data=data_bytes,
        headers={
            "Content-Type": "text/plain",
            "X-Trino-User": "admin",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))

        columns = []
        rows_data = []

        # Follow nextUri — capture data as soon as it appears
        for _ in range(30):
            state = result.get("stats", {}).get("state", "")

            # Capture data if present (happens during RUNNING state)
            if "columns" in result and "data" in result:
                columns = [col["name"] for col in result["columns"]]
                rows_data = result["data"]

            if state in ("FINISHED", "FAILED"):
                break
            next_uri = result.get("nextUri")
            if not next_uri:
                break
            time.sleep(0.3)
            next_req = urllib.request.Request(
                next_uri,
                headers={"X-Trino-User": "admin"},
            )
            with urllib.request.urlopen(next_req, timeout=30) as resp:
                result = json.loads(resp.read().decode("utf-8"))

        if state == "FAILED":
            error_msg = result.get("error", {}).get("message", "unknown")
            raise Exception(f"Trino FAILED: {error_msg}")

        return [dict(zip(columns, row)) for row in rows_data]
    except Exception as e:
        print(f"Trino query error: {e}")
        return []


def get_freshness_data() -> dict:
    """Get freshness metrics from Trino (with caching)."""
    now = time.time()
    if _cache["data"] and (now - _cache["timestamp"]) < CACHE_TTL:
        return _cache["data"]

    metrics = {"tables": {}, "up": 0}

    try:
        # Check Trino is up
        info_req = urllib.request.Request(
            f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info",
        )
        with urllib.request.urlopen(info_req, timeout=5):
            metrics["up"] = 1
    except Exception:
        _cache["data"] = metrics
        _cache["timestamp"] = now
        return metrics

    # Query freshness for each table
    for table in FRESHNESS_TABLES:
        table_short = table.split(".")[-1]
        try:
            # Freshness: seconds since last __consolidated_at
            sql = f"""
                SELECT
                    COALESCE(
                        CAST(DATE_DIFF('second', MAX(__consolidated_at), CURRENT_TIMESTAMP) AS BIGINT),
                        999999
                    ) AS freshness_seconds,
                    COUNT(*) AS row_count
                FROM {table}
            """
            rows = query_trino(sql)
            if rows:
                metrics["tables"][table_short] = {
                    "freshness_seconds": rows[0].get("freshness_seconds", 999999),
                    "row_count": rows[0].get("row_count", 0),
                }
        except Exception as e:
            print(f"Error querying {table}: {e}")
            metrics["tables"][table_short] = {
                "freshness_seconds": 999999,
                "row_count": 0,
            }

    # Query Kafka consumer lag (from CDC topic)
    try:
        sql = """
            SELECT COUNT(*) as total_events
            FROM lakehouse.bronze.core_customer_cdc
            WHERE __cdc_timestamp_ms > CAST(
                (TO_UNIXTIME(CURRENT_TIMESTAMP) - 3600) * 1000 AS BIGINT
            )
        """
        rows = query_trino(sql)
        if rows:
            metrics["recent_events"] = rows[0].get("total_events", 0)
    except Exception:
        metrics["recent_events"] = 0

    _cache["data"] = metrics
    _cache["timestamp"] = now
    return metrics


def format_prometheus_metrics(data: dict) -> str:
    """Format metrics in Prometheus text exposition format."""
    lines = []

    # CDC freshness
    lines.append("# HELP cdc_freshness_seconds Seconds since last CDC consolidation")
    lines.append("# TYPE cdc_freshness_seconds gauge")
    for table, info in data.get("tables", {}).items():
        lines.append(f'cdc_freshness_seconds{{table="{table}"}} {info["freshness_seconds"]}')

    # Row counts
    lines.append("# HELP cdc_row_count Number of rows in current-state table")
    lines.append("# TYPE cdc_row_count gauge")
    for table, info in data.get("tables", {}).items():
        lines.append(f'cdc_row_count{{table="{table}"}} {info["row_count"]}')

    # Recent events
    lines.append("# HELP cdc_recent_events CDC events in last hour")
    lines.append("# TYPE cdc_recent_events gauge")
    lines.append(f"cdc_recent_events {data.get('recent_events', 0)}")

    # Up metric
    lines.append("# HELP exporter_up 1 if Trino is reachable")
    lines.append("# TYPE exporter_up gauge")
    lines.append(f"exporter_up {data['up']}")

    return "\n".join(lines) + "\n"


class MetricsHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler for /metrics endpoint."""

    def do_GET(self):
        if self.path == "/metrics":
            data = get_freshness_data()
            output = format_prometheus_metrics(data)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            self.wfile.write(output.encode("utf-8"))
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress default logging to reduce noise
        pass


if __name__ == "__main__":
    port = int(os.getenv("PORT", "9119"))
    server = http.server.HTTPServer(("0.0.0.0", port), MetricsHandler)
    print(f"Freshness exporter running on port {port}")
    server.serve_forever()
