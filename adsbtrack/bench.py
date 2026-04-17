"""Clean benchmark harness for the async fetcher.

Skips the CLI's ``ensure_airports`` and auto-extract steps so the only
timed work is ``fetcher.fetch_traces`` itself. Prints the exact
config values used so there's no ambiguity about rate_limit /
concurrency / rate_limit_max.

Usage:
    uv run python scripts/bench_fetcher.py \
        --hex a66ad3 --start 2024-06-01 --end 2024-07-20 \
        --source adsbx --concurrency 4 --rate 0.5 \
        --db bench-c4.db
"""

from __future__ import annotations

import argparse
import time
from datetime import date
from pathlib import Path

from adsbtrack.config import Config
from adsbtrack.db import Database
from adsbtrack.fetcher import fetch_traces


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--hex", required=True)
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--source", default="adsbx")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--rate", type=float, default=0.5)
    p.add_argument("--rate-max", type=float, default=30.0)
    p.add_argument("--db", required=True)
    args = p.parse_args()

    db_path = Path(args.db)
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    with Database(db_path) as db:
        config = Config(db_path=db_path)
        config.rate_limit = args.rate
        config.rate_limit_max = args.rate_max
        config.fetch_concurrency = args.concurrency

        # Print exactly what the run will use so there's no ambiguity.
        print(
            f"[bench] hex={args.hex} start={args.start} end={args.end} "
            f"source={args.source} concurrency={config.fetch_concurrency} "
            f"rate_limit={config.rate_limit}s rate_limit_max={config.rate_limit_max}s "
            f"airports_in_db={db.airport_count()}"
        )

        t0 = time.perf_counter()
        stats = fetch_traces(db, config, args.hex, start, end, source=args.source)
        elapsed = time.perf_counter() - t0

        print(
            f"[bench] fetch_traces elapsed={elapsed:.2f}s "
            f"fetched={stats['fetched']} with_data={stats['with_data']} "
            f"skipped={stats['skipped']} errors={stats['errors']}"
        )

        # Per-run response histogram for this source only.
        rows = db.conn.execute(
            "SELECT status, COUNT(*) AS n FROM fetch_log WHERE icao = ? AND source = ? GROUP BY status ORDER BY status",
            (args.hex, args.source),
        ).fetchall()
        print("[bench] status histogram:")
        for r in rows:
            print(f"  {r['status']}: {r['n']}")

        # Orphan consistency check: fetch_log 200s must have matching trace_days rows.
        orphans = db.conn.execute(
            "SELECT COUNT(*) AS n FROM fetch_log f WHERE f.status = 200 "
            "AND NOT EXISTS (SELECT 1 FROM trace_days t "
            "  WHERE t.icao = f.icao AND t.date = f.date AND t.source = f.source)",
        ).fetchone()
        print(f"[bench] orphan 200s (must be 0): {orphans['n']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
