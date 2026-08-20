"""Alert-evaluation core for the `watch` command (issue #24; CLI wiring is a
separate task).

The intended workflow is: call snapshot_state() for a hex BEFORE running a
fetch, run the fetch/extract, then call evaluate() AFTER with that earlier
snapshot as `pre`. Comparing what existed before the run to what exists
after is what makes alerts self-suppressing across repeated runs -- a
trace day, flight, or spoof row that was already present when
snapshot_state() ran can never trigger an alert on a later run, because it
fails the `pre` comparison. There is no separate "already alerted" ledger
to maintain; the database's own before/after state is the ledger.

Leaf module: imports only db/config types, never adsbtrack.cli.
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass
from datetime import date

from .config import Config
from .db import Database


@dataclass
class WatchAlert:
    kind: str  # "reactivation" | "emergency" | "spoof"
    icao: str
    summary: str
    detail: dict


@dataclass
class WatchState:
    has_any_trace: bool
    last_data_day: str | None
    max_flight_takeoff_time: str | None


def snapshot_state(db: Database, icao: str) -> WatchState:
    """Capture this hex's trace/flight high-water marks. Call this BEFORE a
    fetch/extract run; the result becomes `pre` for a later evaluate() call."""
    trace_row = db.conn.execute(
        "SELECT COUNT(*) AS cnt, MAX(date) AS last_day FROM trace_days WHERE icao = ?",
        (icao,),
    ).fetchone()
    flight_row = db.conn.execute(
        "SELECT MAX(takeoff_time) AS max_takeoff FROM flights WHERE icao = ?",
        (icao,),
    ).fetchone()
    return WatchState(
        has_any_trace=trace_row["cnt"] > 0,
        last_data_day=trace_row["last_day"],
        max_flight_takeoff_time=flight_row["max_takeoff"],
    )


def evaluate(db: Database, icao: str, pre: WatchState, run_started_at: str, config: Config) -> list[WatchAlert]:
    """Compare the current DB state to `pre` and return the alerts a run
    produced. `pre` must come from a snapshot_state() call taken before the
    run; `run_started_at` gates spoof rows to ones detected during this run."""
    if not pre.has_any_trace:
        # A first-ever fetch backfills an aircraft's whole history in one
        # run; treating that backfill as a flood of alerts would swamp
        # anything meaningful. The CLI labels this hex "baselined" instead.
        return []

    alerts: list[WatchAlert] = []
    alerts.extend(_check_reactivation(db, icao, pre, config))
    alerts.extend(_check_emergency(db, icao, pre))
    alerts.extend(_check_spoof(db, icao, run_started_at))
    return alerts


def _check_reactivation(db: Database, icao: str, pre: WatchState, config: Config) -> list[WatchAlert]:
    if pre.last_data_day is None:
        return []
    row = db.conn.execute(
        "SELECT MIN(date) AS earliest FROM trace_days WHERE icao = ? AND date > ?",
        (icao, pre.last_data_day),
    ).fetchone()
    first_new_day = row["earliest"]
    if first_new_day is None:
        return []
    gap_days = (date.fromisoformat(first_new_day) - date.fromisoformat(pre.last_data_day)).days
    if gap_days < config.watch_dormancy_days:
        return []
    summary = f"{icao} active again after {gap_days} days (last seen {pre.last_data_day})"
    detail = {"dormant_since": pre.last_data_day, "reactivated_on": first_new_day, "gap_days": gap_days}
    return [WatchAlert(kind="reactivation", icao=icao, summary=summary, detail=detail)]


def _check_emergency(db: Database, icao: str, pre: WatchState) -> list[WatchAlert]:
    query = """SELECT takeoff_time, callsign, emergency_squawk, squawks_observed FROM flights
               WHERE icao = ? AND (had_emergency = 1 OR emergency_squawk IS NOT NULL)"""
    params: list = [icao]
    if pre.max_flight_takeoff_time is not None:
        query += " AND takeoff_time > ?"
        params.append(pre.max_flight_takeoff_time)
    query += " ORDER BY takeoff_time"
    rows = db.conn.execute(query, params).fetchall()

    alerts = []
    for row in rows:
        squawk = row["emergency_squawk"] or "emergency"
        summary = f"{icao} squawked {squawk} on flight {row['takeoff_time']}"
        detail = {
            "takeoff_time": row["takeoff_time"],
            "callsign": row["callsign"],
            "emergency_squawk": row["emergency_squawk"],
            "squawks_observed": row["squawks_observed"],
        }
        alerts.append(WatchAlert(kind="emergency", icao=icao, summary=summary, detail=detail))
    return alerts


def _check_spoof(db: Database, icao: str, run_started_at: str) -> list[WatchAlert]:
    rows = db.conn.execute(
        """SELECT takeoff_time, callsign, reason, reason_detail FROM spoofed_broadcasts
           WHERE icao = ? AND detected_at >= ?
           ORDER BY takeoff_time""",
        (icao, run_started_at),
    ).fetchall()

    alerts = []
    for row in rows:
        summary = f"{icao} new spoof quarantine ({row['reason']})"
        detail = {
            "takeoff_time": row["takeoff_time"],
            "callsign": row["callsign"],
            "reason": row["reason"],
            "reason_detail": row["reason_detail"],
        }
        alerts.append(WatchAlert(kind="spoof", icao=icao, summary=summary, detail=detail))
    return alerts


def _post_webhook(url: str, payload: dict, timeout: float) -> None:
    """POST payload as a JSON document to url. Raises on any failure
    (connection error, timeout, non-2xx status via HTTPError) -- the CLI
    catches it and reports a warning without changing the run's exit code."""
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    urllib.request.urlopen(request, timeout=timeout)
