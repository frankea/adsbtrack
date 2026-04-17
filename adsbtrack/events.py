"""Per-flight event timeline over already-computed flight columns.

Emits notable events (emergencies, off-airport landings, long hovers,
multiple go-arounds) from the flights table. No new signal derivation;
every predicate checks an existing column. The classifier / features
modules are the single source of truth for whether a flight has an
emergency squawk, a long hover, etc. -- this module just renders.

Thresholds (`long hover >= 300s`, `multiple go-arounds >= 2`) are
deliberately set to cut everyday noise: one go-around happens all the
time, two in a row is a pattern worth looking at.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .db import Database

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
) -> list[Event]:
    """Return a chronological (newest first) list of events for `icao`.

    `severity` filters to "emergency", "unusual", or "all" (default).
    `since` filters to flights with takeoff_time >= the given datetime.
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

    if severity != "all":
        events = [e for e in events if e.severity == severity]

    return events
