#!/usr/bin/env python3
"""
Measure end-to-end CDC freshness: PostgreSQL commit → observable in Silver Current.

Methodology (fixed, do not change between releases without saying so):

    t0 = PostgreSQL transaction COMMIT completed
    t1 = the expected new value becomes observable in
         silver.dim_customer_current through Trino
    freshness = t1 - t0

Deliberately measured source → Silver Current, not Kafka arrival or Bronze
arrival: Silver Current is the consumer-visible CDC state the architecture
actually claims.

The number therefore INCLUDES consolidation scheduling delay. That is intended —
it measures user-observable pipeline freshness, not a component microbenchmark.
Record the consolidation cadence alongside the result; a different cadence gives
a different number and the two are not comparable.

PHASE SAMPLING — why there is a random sleep between trials
-----------------------------------------------------------
Consolidation runs on a cron (`*/10`), so freshness is dominated by *where in
the cadence window the commit lands*. Running trials back to back does not
sample that offset randomly: each trial ends the instant a consolidation run
completes, so the next trial commits at offset ~0 and waits a nearly full
window. Measured that way, every trial after the first reports the worst case.

Observed directly: back-to-back trials gave 355s, then 596s, then 599s against
a 600s cadence — the series locks onto the ceiling.

A real source change arrives at an arbitrary point in the window, so the offset
is uniform over [0, cadence). Sleeping a uniform random 0..cadence between
trials restores that. The reported median then answers the question a reader
actually has ("a change happens now — how long until I can see it?") instead of
"how long if I commit at the worst possible moment?".

Usage:
    python scripts/measure_cdc_freshness.py --trials 5
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone


def _force_utf8_output() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, OSError):
                pass


def psql(sql: str) -> str:
    result = subprocess.run(  # noqa: S603
        ["docker", "exec", "banking-postgres", "psql", "-U", "banking_admin",
         "-d", "banking_db", "-tAc", sql],
        capture_output=True, text=True, timeout=60, check=True,
        env={"MSYS_NO_PATHCONV": "1", "PATH": __import__("os").environ["PATH"]},
    )
    return result.stdout.strip()


def trino(sql: str) -> str:
    result = subprocess.run(  # noqa: S603
        ["docker", "exec", "banking-trino", "trino", "--execute", sql],
        capture_output=True, text=True, timeout=120, check=False,
        env={"MSYS_NO_PATHCONV": "1", "PATH": __import__("os").environ["PATH"]},
    )
    return result.stdout.strip().replace('"', "")


def run_trial(index: int, customer_id: int, timeout_s: int, poll_s: int) -> dict:
    """One trial: write a unique value, then poll until Silver Current shows it."""
    marker = f"cdc.freshness.{int(time.time())}.{index}@bank.vn"

    psql(
        f"UPDATE core_banking.customer SET email = '{marker}', last_updated = NOW() "
        f"WHERE customer_id = {customer_id};"
    )
    t0 = time.monotonic()
    committed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    deadline = t0 + timeout_s
    while time.monotonic() < deadline:
        observed = trino(
            "SELECT email FROM iceberg.silver.dim_customer_current "
            f"WHERE customer_id = {customer_id}"
        )
        if marker in observed:
            elapsed = round(time.monotonic() - t0, 1)
            print(f"  trial {index}: customer={customer_id}  {elapsed}s")
            return {
                "trial": index,
                "customer_id": customer_id,
                "marker": marker,
                "committed_at_utc": committed_at,
                "freshness_seconds": elapsed,
                "observed": True,
            }
        time.sleep(poll_s)

    print(f"  trial {index}: customer={customer_id}  TIMEOUT (>{timeout_s}s)")
    return {
        "trial": index,
        "customer_id": customer_id,
        "marker": marker,
        "committed_at_utc": committed_at,
        "freshness_seconds": None,
        "observed": False,
    }


def main(argv: list[str] | None = None) -> int:
    _force_utf8_output()
    parser = argparse.ArgumentParser(description="Measure CDC freshness")
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=1200,
                        help="Max wait per trial (s). Must exceed consolidation cadence.")
    parser.add_argument("--poll", type=int, default=5)
    parser.add_argument("--first-customer-id", type=int, default=2001,
                        help="Each trial uses a different customer to avoid interference")
    parser.add_argument("--cadence-seconds", type=int, default=600,
                        help="Deployed consolidation cadence. Recorded in the output; "
                             "the result is only comparable across runs at the same cadence.")
    parser.add_argument("--no-jitter", action="store_true",
                        help="Run trials back to back. Measures the WORST case, not the "
                             "median — see the module docstring before using this.")
    parser.add_argument("--seed", type=int, default=None, help="Seed the jitter for reproducibility")
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)
    print(f"CDC freshness — {args.trials} trials, timeout {args.timeout}s each")
    print(f"consolidation cadence {args.cadence_seconds}s, "
          f"phase sampling: {'back-to-back (worst case)' if args.no_jitter else 'randomised'}")
    print("t0 = PostgreSQL COMMIT, t1 = observable in silver.dim_customer_current via Trino\n")

    results = []
    for i in range(args.trials):
        if i and not args.no_jitter:
            # Decorrelate the commit from the cadence window; without this every
            # trial commits just after a run and reports the ceiling.
            wait = rng.uniform(0, args.cadence_seconds)
            print(f"  (phase jitter {wait:.0f}s)")
            time.sleep(wait)
        results.append(
            run_trial(i + 1, args.first_customer_id + i, args.timeout, args.poll)
        )

    observed = [r["freshness_seconds"] for r in results if r["observed"]]
    summary = {
        "methodology": "postgres_commit_to_trino_silver_current",
        "consolidation_cadence_seconds": args.cadence_seconds,
        "phase_sampling": "back_to_back" if args.no_jitter else "randomised_uniform",
        "sample_size": len(results),
        "observed_count": len(observed),
        "trials": results,
        "trials_seconds": observed,
        "min_seconds": min(observed) if observed else None,
        "median_seconds": round(statistics.median(observed), 1) if observed else None,
        "max_seconds": max(observed) if observed else None,
    }
    print("\n" + json.dumps(summary, indent=2))
    return 0 if len(observed) == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
