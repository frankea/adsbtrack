"""Per-flight event timeline over already-computed flight columns.

Emits notable events (emergencies, off-airport landings, long hovers,
multiple go-arounds) from the flights table. Most predicates check an
existing column; the spoof-detection predicate is the exception --
it scans the raw readsb trace JSON for bimodal-integrity signatures
(see _detect_spoof_events) and is opt-in via the `include_spoof_checks`
flag on collect_events.

Thresholds (`long hover >= 300s`, `multiple go-arounds >= 2`) are
deliberately set to cut everyday noise: one go-around happens all the
time, two in a row is a pattern worth looking at.

The spoof detector's own thresholds (`Config.spoof_v2_sil0_pct`,
`Config.spoof_min_v2_samples`) and its pooling scan live in
`parser.pool_spoof_scores`. The reject-in-extract gate in parser.py no
longer calls that day-level scan (issue #22 moved it to a flight-scoped
gate over FlightMetrics counters), but both consumers still read the
identical per-point integrity predicate via `integrity.count_v2_integrity`,
so day-scoped and flight-scoped stats can never disagree about a point.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .config import Config
from .db import Database, iter_parsed_trace_days
from .integrity import count_v2_integrity
from .parser import pool_spoof_scores

# ---------------------------------------------------------------------------
# Thresholds (centralized for audit)
# ---------------------------------------------------------------------------

_LONG_HOVER_SECS = 300  # 5 minutes; below this is approach-phase noise
_MULTI_GO_AROUNDS = 2  # one missed approach per flight is routine


# ---------------------------------------------------------------------------
# Event type
# ---------------------------------------------------------------------------


@dataclass
class Event:
    ts: datetime
    icao: str
    callsign: str | None
    event_type: str
    severity: str  # "emergency" | "unusual"
    summary: str
    context: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Spoof detector (optional, trace-scan)
# ---------------------------------------------------------------------------


def _event_from_spoof_agg(icao: str, date_str: str, agg: dict) -> Event:
    """Build the spoof_bimodal_integrity Event for one flagged (icao, date).

    Shared by both detection routes (decode-based pooling in
    pool_spoof_scores and the stats-column pooling in
    _pool_spoof_scores_from_stats) so a flagged date produces the exact
    same Event regardless of which route found it.
    """
    base_ts = agg["timestamp"]
    if isinstance(base_ts, (int, float)):
        ts = datetime.fromtimestamp(base_ts, tz=UTC)
    else:
        ts = datetime.fromisoformat(date_str).replace(tzinfo=UTC)
    callsigns = agg["callsigns"]
    callsign = callsigns[0] if callsigns else None
    source_names = agg["sources"]
    v2 = agg["v2_samples"]
    sil_pct = agg["v2_sil0_pct"]
    return Event(
        ts=ts,
        icao=icao,
        callsign=callsign,
        event_type="spoof_bimodal_integrity",
        severity="unusual",
        summary=(
            f"pooled v2 samples with sil=0: {sil_pct:.1f}% ({v2} v2 samples across {len(source_names)} source(s))"
        ),
        context={
            "date": date_str,
            "sources": source_names,
            "source_rates": agg["source_rates"],
            "v2_samples": v2,
            "v2_sil0_pct": round(sil_pct, 2),
            "v2_nic0_pct": round(agg["v2_nic0_pct"], 2),
            "callsigns": callsigns,
        },
    )


def _trace_days_needs_fallback(db: Database, icao: str, since: datetime | None) -> bool:
    """True if any of icao's trace_days rows (matching `since` when given)
    is missing one of the four Task 12 materialized stat columns.

    A True result forces the decode-based path for correctness (some rows
    haven't been through `db optimize` yet, so the stat columns can't be
    trusted for pooling). False -- including "icao has no trace_days rows
    at all" -- means the SQL-only path is safe.
    """
    params: list[Any] = [icao]
    sql = (
        "SELECT COUNT(*) AS cnt FROM trace_days WHERE icao = ? "
        "AND (v2_samples IS NULL OR v2_sil0 IS NULL OR v2_nic0 IS NULL OR v2_callsigns IS NULL)"
    )
    if since is not None:
        sql += " AND date >= ?"
        params.append(since.strftime("%Y-%m-%d"))
    return db.conn.execute(sql, params).fetchone()["cnt"] > 0


def _pool_spoof_scores_from_stats(
    db: Database, icaos: Iterable[str], since: datetime | None, config: Config
) -> dict[tuple[str, str], dict]:
    """SQL-only equivalent of parser.pool_spoof_scores for aircraft whose
    trace_days rows are all stat-filled (Task 12).

    Aggregates the materialized v2_samples/v2_sil0/v2_nic0 columns instead
    of decoding trace_json -- avoiding exactly the multi-GB decode this
    task exists to cut -- then does one small targeted decode covering
    only the (icao, date) pairs that end up flagged, to recover the
    actual callsigns (stored only as a count, v2_callsigns, on the row).
    Every other field is computed straight from the row data, so the
    result is field-for-field identical to pool_spoof_scores's output for
    the same rows. Callers must have already confirmed (via
    _trace_days_needs_fallback) that none of these icaos' rows are
    missing a stat column.
    """
    icao_list = list(icaos)
    if not icao_list:
        return {}
    placeholders = ",".join("?" for _ in icao_list)
    params: list[Any] = [*icao_list]
    sql = (
        f"SELECT icao, date, source, timestamp, v2_samples, v2_sil0, v2_nic0 "
        f"FROM trace_days WHERE icao IN ({placeholders})"
    )
    if since is not None:
        sql += " AND date >= ?"
        params.append(since.strftime("%Y-%m-%d"))
    sql += " ORDER BY icao, date, source"

    by_key: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"v2": 0, "sil0": 0, "nic0": 0, "sources": set(), "source_rates": [], "timestamp": None}
    )
    for row in db.conn.execute(sql, params).fetchall():
        v2 = row["v2_samples"] or 0
        if v2 == 0:
            continue
        sil0 = row["v2_sil0"] or 0
        nic0 = row["v2_nic0"] or 0
        key = (row["icao"], row["date"])
        agg = by_key[key]
        agg["v2"] += v2
        agg["sil0"] += sil0
        agg["nic0"] += nic0
        agg["sources"].add(row["source"])
        agg["source_rates"].append((row["source"], round(100.0 * sil0 / v2, 2)))
        if agg["timestamp"] is None:
            agg["timestamp"] = row["timestamp"]

    flagged: dict[tuple[str, str], dict] = {}
    for key, agg in by_key.items():
        v2 = agg["v2"]
        if v2 < config.spoof_min_v2_samples:
            continue
        sil_pct = 100.0 * agg["sil0"] / v2
        if sil_pct < config.spoof_v2_sil0_pct:
            continue
        flagged[key] = {
            "v2_samples": v2,
            "v2_sil0_pct": sil_pct,
            "v2_nic0_pct": 100.0 * agg["nic0"] / v2,
            "sources": sorted(agg["sources"]),
            "source_rates": sorted(agg["source_rates"]),
            "timestamp": agg["timestamp"],
        }

    # Targeted decode: only the (icao, date) pairs that ended up flagged,
    # and only to recover the callsigns set. Everything else above is
    # already field-identical to pool_spoof_scores's output.
    for (icao, date_str), agg in flagged.items():
        rows = db.conn.execute(
            "SELECT date, source, trace_json, timestamp FROM trace_days WHERE icao = ? AND date = ?",
            (icao, date_str),
        ).fetchall()
        callsigns: set[str] = set()
        for _row, samples in iter_parsed_trace_days(rows, icao):
            callsigns |= count_v2_integrity(samples)[3]
        agg["callsigns"] = sorted(callsigns)

    return flagged


def _detect_spoof_events(db: Database, icao: str, since: datetime | None, config: Config) -> list[Event]:
    """Scan stored trace_days for bimodal-integrity spoof signatures.

    Pools v2 samples across every aggregator that fetched the same date,
    so a single aggregator's transient integrity-field glitch does not by
    itself produce an event; real spoofs hit every receiver that could
    hear them. Emits one `spoof_bimodal_integrity` Event per date when the
    pooled sil=0 share crosses `config.spoof_v2_sil0_pct` on a date with
    >= `config.spoof_min_v2_samples` pooled v2 samples.

    Uses the Task 12 materialized stat columns (no trace_json decode) when
    every trace_days row for `icao` has them filled; falls back to
    decoding every row (db.iter_parsed_trace_days + parser.pool_spoof_scores)
    the moment any row is missing a stat column, so correctness never
    depends on `db optimize` having run.
    """
    if _trace_days_needs_fallback(db, icao, since):
        params: list[Any] = [icao]
        sql = "SELECT date, source, trace_json, timestamp FROM trace_days WHERE icao = ?"
        if since is not None:
            sql += " AND date >= ?"
            params.append(since.strftime("%Y-%m-%d"))
        sql += " ORDER BY date, source"
        rows = db.conn.execute(sql, params).fetchall()
        flagged_by_date = pool_spoof_scores(iter_parsed_trace_days(rows, icao), config)
    else:
        flagged = _pool_spoof_scores_from_stats(db, [icao], since, config)
        flagged_by_date = {date_str: agg for (_icao, date_str), agg in flagged.items()}

    return [_event_from_spoof_agg(icao, date_str, agg) for date_str, agg in sorted(flagged_by_date.items())]


def bulk_detect_spoof_events(db: Database, icaos: Iterable[str], config: Config | None = None) -> list[Event]:
    """Spoof-event detection across many aircraft in one grouped SQL scan.

    Used by the all-aircraft events view (tui/queries.py) so scanning N
    aircraft doesn't mean decoding N full trace histories. One query
    splits `icaos` into "fully stat-filled" (Task 12 materialized columns,
    no NULLs) -- handled by a single grouped _pool_spoof_scores_from_stats
    call covering all of them -- and "needs fallback" -- decoded one
    aircraft at a time via _detect_spoof_events, same as before this
    function existed.
    """
    config = config or Config()
    icao_list = list(dict.fromkeys(icaos))  # de-dupe, keep first-seen order
    if not icao_list:
        return []

    placeholders = ",".join("?" for _ in icao_list)
    needs_fallback = {
        row["icao"]
        for row in db.conn.execute(
            f"SELECT DISTINCT icao FROM trace_days WHERE icao IN ({placeholders}) "
            "AND (v2_samples IS NULL OR v2_sil0 IS NULL OR v2_nic0 IS NULL OR v2_callsigns IS NULL)",
            icao_list,
        ).fetchall()
    }
    optimized = [icao for icao in icao_list if icao not in needs_fallback]

    events: list[Event] = []
    for icao in sorted(needs_fallback):
        events.extend(_detect_spoof_events(db, icao, None, config))

    flagged = _pool_spoof_scores_from_stats(db, optimized, None, config)
    for (icao, date_str), agg in sorted(flagged.items()):
        events.append(_event_from_spoof_agg(icao, date_str, agg))
    return events


# ---------------------------------------------------------------------------
# Event extractors
# ---------------------------------------------------------------------------


def _event_from_row(row: dict[str, Any]) -> list[Event]:
    """Inspect a single flight row and emit 0+ events."""
    events: list[Event] = []
    ts = datetime.fromisoformat(row["takeoff_time"])
    icao = row["icao"]
    callsign = row["callsign"]

    if row["emergency_squawk"]:
        events.append(
            Event(
                ts=ts,
                icao=icao,
                callsign=callsign,
                event_type="emergency_squawk",
                severity="emergency",
                summary=f"squawk {row['emergency_squawk']}",
                context={"emergency_squawk": row["emergency_squawk"]},
            )
        )

    if row["emergency_flag"]:
        events.append(
            Event(
                ts=ts,
                icao=icao,
                callsign=callsign,
                event_type="emergency_flag",
                severity="emergency",
                summary=f"emergency flag: {row['emergency_flag']}",
                context={"emergency_flag": row["emergency_flag"]},
            )
        )

    if row["landing_type"] == "confirmed" and row["destination_icao"] is None and row["destination_helipad_id"] is None:
        events.append(
            Event(
                ts=ts,
                icao=icao,
                callsign=callsign,
                event_type="off_airport_landing",
                severity="unusual",
                summary="confirmed landing, no airport or helipad match",
                context={
                    "landing_lat": row["landing_lat"],
                    "landing_lon": row["landing_lon"],
                },
            )
        )

    if row["max_hover_secs"] and row["max_hover_secs"] >= _LONG_HOVER_SECS:
        events.append(
            Event(
                ts=ts,
                icao=icao,
                callsign=callsign,
                event_type="long_hover",
                severity="unusual",
                summary=f"hover {row['max_hover_secs']}s ({row['max_hover_secs'] / 60:.1f} min)",
                context={"max_hover_secs": row["max_hover_secs"]},
            )
        )

    if row["go_around_count"] and row["go_around_count"] >= _MULTI_GO_AROUNDS:
        events.append(
            Event(
                ts=ts,
                icao=icao,
                callsign=callsign,
                event_type="multiple_go_arounds",
                severity="unusual",
                summary=f"{row['go_around_count']} go-arounds on this flight",
                context={"go_around_count": row["go_around_count"]},
            )
        )

    return events


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def collect_events(
    db: Database,
    icao: str,
    *,
    since: datetime | None = None,
    severity: str = "all",
    include_spoof_checks: bool = False,
    config: Config | None = None,
) -> list[Event]:
    """Return a chronological (newest first) list of events for `icao`.

    `severity` filters to "emergency", "unusual", or "all" (default).
    `since` filters to flights with takeoff_time >= the given datetime.
    `include_spoof_checks` toggles the bimodal-integrity trace scan; it
    defaults to False so historical queries do not retroactively tag
    trace_days without an explicit opt-in. `config` supplies the spoof
    thresholds (spoof_v2_sil0_pct, spoof_min_v2_samples); defaults to
    Config() when omitted.
    """
    params: list[Any] = [icao]
    sql = "SELECT * FROM flights WHERE icao = ?"
    if since is not None:
        sql += " AND takeoff_time >= ?"
        params.append(since.isoformat())
    sql += " ORDER BY takeoff_time DESC"

    rows = db.conn.execute(sql, params).fetchall()
    events: list[Event] = []
    for row in rows:
        events.extend(_event_from_row(dict(row)))

    if include_spoof_checks:
        events.extend(_detect_spoof_events(db, icao, since, config or Config()))

    # Chronological (newest first) after merging spoof events into the
    # flight-derived list.
    events.sort(key=lambda e: e.ts, reverse=True)

    if severity != "all":
        events = [e for e in events if e.severity == severity]

    return events
