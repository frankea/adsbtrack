"""Alert-evaluation core for the `watch` command (issue #24; CLI wiring is a
separate task).

The intended workflow is: call snapshot_state() for a hex BEFORE running a
fetch, run the fetch/extract, then call evaluate() AFTER with that earlier
snapshot as `pre`. Comparing what existed before the run to what exists
after is what makes alerts self-suppressing across repeated runs -- a
trace day or flight that was already present when snapshot_state() ran can
never trigger an alert on a later run, because it fails the `pre`
comparison. There is no separate "already alerted" ledger to maintain; the
database's own before/after state is the ledger.

Spoof rows are the one exception to "row presence is the ledger": a full
reprocess (parser.py's incremental-refusal fallback, an extractor-version
bump, etc.) deletes and re-inserts spoofed_broadcasts wholesale, stamping a
fresh detected_at on content that never actually changed. Row presence and
detected_at are therefore both unreliable signals of "new" for this table.
Instead, `pre.spoof_keys` captures the (takeoff_time, reason) pairs that
existed at snapshot time, and a spoof row only fires when its own
(takeoff_time, reason) is absent from that set -- so a row survives being
rewritten underneath it without re-firing.

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
    spoof_keys: frozenset[tuple[str, str]]


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
    spoof_rows = db.conn.execute(
        "SELECT takeoff_time, reason FROM spoofed_broadcasts WHERE icao = ?",
        (icao,),
    ).fetchall()
    return WatchState(
        has_any_trace=trace_row["cnt"] > 0,
        last_data_day=trace_row["last_day"],
        max_flight_takeoff_time=flight_row["max_takeoff"],
        spoof_keys=frozenset((row["takeoff_time"], row["reason"]) for row in spoof_rows),
    )


def evaluate(db: Database, icao: str, pre: WatchState, run_started_at: str, config: Config) -> list[WatchAlert]:
    """Compare the current DB state to `pre` and return the alerts a run
    produced. `pre` must come from a snapshot_state() call taken before the
    run; `run_started_at` gates reactivation to gaps a prior run already
    observed (see _check_reactivation)."""
    if not pre.has_any_trace:
        # A first-ever fetch backfills an aircraft's whole history in one
        # run; treating that backfill as a flood of alerts would swamp
        # anything meaningful. The CLI labels this hex "baselined" instead.
        return []

    alerts: list[WatchAlert] = []
    alerts.extend(_check_reactivation(db, icao, pre, run_started_at, config))
    alerts.extend(_check_emergency(db, icao, pre))
    alerts.extend(_check_spoof(db, icao, pre))
    return alerts


def _check_reactivation(
    db: Database, icao: str, pre: WatchState, run_started_at: str, config: Config
) -> list[WatchAlert]:
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

    # Observation evidence: a "gap" with no fetch_log row inside it (logged
    # by an earlier run, before this one started) is indistinguishable from
    # watch simply never having looked -- e.g. its first run against a DB
    # that already had sporadic history from `fetch` before watch was
    # adopted. The aircraft may have been flying the whole time; nobody
    # asked. A dormancy alert requires proof someone actually asked and got
    # nothing, which a daily cron's own 404/204 fetch_log rows provide from
    # its second run onward.
    observed = db.conn.execute(
        """SELECT 1 FROM fetch_log WHERE icao = ? AND date > ? AND date < ? AND fetched_at < ? LIMIT 1""",
        (icao, pre.last_data_day, first_new_day, run_started_at),
    ).fetchone()
    if observed is None:
        return []

    summary = f"{icao} active again after {gap_days} days (last seen {pre.last_data_day})"
    detail = {"dormant_since": pre.last_data_day, "reactivated_on": first_new_day, "gap_days": gap_days}
    return [WatchAlert(kind="reactivation", icao=icao, summary=summary, detail=detail)]


def _check_emergency(db: Database, icao: str, pre: WatchState) -> list[WatchAlert]:
    if pre.max_flight_takeoff_time is None:
        # Trace history exists (the caller's overall baseline guard already
        # ruled that out) but no flight had been extracted yet at snapshot
        # time -- e.g. this run's incremental extract hit parser.py's
        # incremental-refusal fallback and silently upgraded to a full
        # reprocess, writing years of flights in one pass. Matching every
        # row unconditionally here would flood the run with historical
        # emergencies instead of surfacing a genuinely new one; baseline
        # instead, same as evaluate()'s no-prior-trace guard.
        return []
    rows = db.conn.execute(
        """SELECT takeoff_time, callsign, emergency_squawk, squawks_observed FROM flights
           WHERE icao = ? AND (had_emergency = 1 OR emergency_squawk IS NOT NULL) AND takeoff_time > ?
           ORDER BY takeoff_time""",
        (icao, pre.max_flight_takeoff_time),
    ).fetchall()

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


def _check_spoof(db: Database, icao: str, pre: WatchState) -> list[WatchAlert]:
    rows = db.conn.execute(
        """SELECT takeoff_time, callsign, reason, reason_detail FROM spoofed_broadcasts
           WHERE icao = ?
           ORDER BY takeoff_time""",
        (icao,),
    ).fetchall()

    alerts = []
    for row in rows:
        if (row["takeoff_time"], row["reason"]) in pre.spoof_keys:
            continue
        summary = f"{icao} new spoof quarantine ({row['reason']})"
        detail = {
            "takeoff_time": row["takeoff_time"],
            "callsign": row["callsign"],
            "reason": row["reason"],
            "reason_detail": row["reason_detail"],
        }
        alerts.append(WatchAlert(kind="spoof", icao=icao, summary=summary, detail=detail))
    return alerts


def post_webhook(url: str, payload: dict, timeout: float) -> None:
    """POST payload as a JSON document to url. Raises on any failure
    (connection error, timeout, non-2xx status via HTTPError) -- the CLI
    catches it and reports a warning without changing the run's exit code."""
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        response.read()
