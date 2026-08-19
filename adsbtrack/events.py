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
`parser.pool_spoof_scores`, shared with the reject-in-extract gate in
parser.py so there is one bimodal-integrity implementation, not two.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .config import Config
from .db import Database, iter_parsed_trace_days
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


def _detect_spoof_events(db: Database, icao: str, since: datetime | None, config: Config) -> list[Event]:
    """Scan stored trace_days for bimodal-integrity spoof signatures.

    Decodes each row's trace_json once (db.iter_parsed_trace_days) and
    pools v2 samples across every aggregator that fetched the same date
    via parser.pool_spoof_scores, so a single aggregator's transient
    integrity-field glitch does not by itself produce an event; real
    spoofs hit every receiver that could hear them. Emits one
    `spoof_bimodal_integrity` Event per date when the pooled sil=0 share
    crosses `config.spoof_v2_sil0_pct` on a date with
    >= `config.spoof_min_v2_samples` pooled v2 samples.
    """
    params: list[Any] = [icao]
    sql = "SELECT date, source, trace_json, timestamp FROM trace_days WHERE icao = ?"
    if since is not None:
        sql += " AND date >= ?"
        params.append(since.strftime("%Y-%m-%d"))
    sql += " ORDER BY date, source"

    rows = db.conn.execute(sql, params).fetchall()
    flagged = pool_spoof_scores(iter_parsed_trace_days(rows, icao), config)

    events: list[Event] = []
    for date_str, agg in sorted(flagged.items()):
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
        events.append(
            Event(
                ts=ts,
                icao=icao,
                callsign=callsign,
                event_type="spoof_bimodal_integrity",
                severity="unusual",
                summary=(
                    f"pooled v2 samples with sil=0: {sil_pct:.1f}% "
                    f"({v2} v2 samples across {len(source_names)} source(s))"
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
        )
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
