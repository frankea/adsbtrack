"""Read-only SQL layer for the TUI.

Every screen's data fetch lives here so individual screen modules stay
small and so the queries themselves can be unit-tested without needing
a running Textual app. Returns plain ``sqlite3.Row`` objects; the
screens render them.

All queries accept an ``adsbtrack.db.Database`` instance rather than a
raw connection so tests can wire a throwaway DB without re-implementing
the migration plumbing.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from ..db import Database
from ..events import collect_events

# ---------------------------------------------------------------------------
# Aircraft list
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AircraftRow:
    icao: str
    registration: str | None
    type_code: str | None
    description: str | None  # full type description, e.g. "AIRBUS A-380-800"
    total_flights: int
    total_hours: float
    home_base_icao: str | None
    last_seen: str | None
    spoof_count: int
    is_military: int
    flags: str  # pre-rendered MIL/SPF/HOVER badge string

    @property
    def display_reg(self) -> str:
        return self.registration or "-"

    @property
    def display_type(self) -> str:
        """Prefer the human-friendly description over the terse type code.

        `description` holds values like "AIRBUS A-380-800" while `type_code`
        is the four-letter Mode S tag like "A388". The design specifies the
        full description; we fall back to the type code then a dash.
        """
        return self.description or self.type_code or "-"

    @property
    def display_home(self) -> str:
        return self.home_base_icao or "-"

    @property
    def display_last_seen(self) -> str:
        return self.last_seen or "-"


def list_aircraft(db: Database, *, filter_substr: str | None = None, limit: int = 5000) -> list[AircraftRow]:
    """Return an aircraft list keyed on ICAO hex.

    Source of truth for the aircraft-list screen. ``filter_substr`` does
    a case-insensitive substring match against ICAO hex, registration,
    type code, and home-base ICAO (the four columns a user is most
    likely to be typing when they hit the filter bar). The query joins
    aircraft_stats (for utilisation + home base), hex_crossref /
    aircraft_registry (for registration + type), mil_hex_ranges (via
    hex_crossref.is_military), and spoofed_broadcasts (for the SPF
    count).
    """
    sql = """
        SELECT s.icao AS icao,
               COALESCE(r.registration, x.registration) AS registration,
               COALESCE(r.type_code, x.type_code) AS type_code,
               COALESCE(r.description, x.type_description) AS description,
               s.total_flights AS total_flights,
               s.total_hours AS total_hours,
               s.home_base_icao AS home_base_icao,
               s.last_seen AS last_seen,
               (SELECT COUNT(*) FROM spoofed_broadcasts sb WHERE sb.icao = s.icao) AS spoof_count,
               COALESCE(x.is_military, 0) AS is_military
          FROM aircraft_stats s
          LEFT JOIN aircraft_registry r ON r.icao = s.icao
          LEFT JOIN hex_crossref x ON x.icao = s.icao
    """
    params: list[Any] = []
    if filter_substr:
        sql += (
            " WHERE lower(s.icao) LIKE ? "
            "    OR lower(COALESCE(r.registration, x.registration, '')) LIKE ? "
            "    OR lower(COALESCE(r.type_code, x.type_code, '')) LIKE ? "
            "    OR lower(COALESCE(s.home_base_icao, '')) LIKE ?"
        )
        needle = f"%{filter_substr.lower()}%"
        params.extend([needle, needle, needle, needle])
    sql += " ORDER BY s.last_seen DESC, s.total_flights DESC LIMIT ?"
    params.append(limit)

    rows = db.conn.execute(sql, params).fetchall()
    out = []
    for row in rows:
        flags = _render_flags(
            is_military=row["is_military"],
            spoof_count=row["spoof_count"],
            type_code=row["type_code"],
        )
        out.append(
            AircraftRow(
                icao=row["icao"],
                registration=row["registration"],
                type_code=row["type_code"],
                description=row["description"],
                total_flights=row["total_flights"] or 0,
                total_hours=row["total_hours"] or 0.0,
                home_base_icao=row["home_base_icao"],
                last_seen=row["last_seen"],
                spoof_count=row["spoof_count"],
                is_military=row["is_military"],
                flags=flags,
            )
        )
    return out


def _render_flags(*, is_military: int, spoof_count: int, type_code: str | None) -> str:
    """Render a compact MIL/SPF/TYP badge string for the flags column."""
    flags: list[str] = []
    if is_military:
        flags.append("MIL")
    if spoof_count:
        flags.append("SPF")
    if type_code and type_code in {"B407", "B429", "S76", "S92", "H60", "UH60", "EC35", "EC45"}:
        flags.append("HELI")
    return " ".join(flags)


def count_aircraft(db: Database) -> int:
    row = db.conn.execute("SELECT COUNT(*) AS n FROM aircraft_stats").fetchone()
    return row["n"] if row else 0


def count_flights(db: Database) -> int:
    row = db.conn.execute("SELECT COUNT(*) AS n FROM flights").fetchone()
    return row["n"] if row else 0


def count_trace_bytes(db: Database) -> int:
    """Total size on disk of all ``trace_days`` JSON payloads.

    Used by the status strip to match the concept's `traces 3.4 GB`
    field. Computed via ``length(trace_json)`` so we don't have to stat
    the DB file and avoid counting index overhead.
    """
    row = db.conn.execute("SELECT COALESCE(SUM(length(trace_json)), 0) AS n FROM trace_days").fetchone()
    return int(row["n"]) if row else 0


@dataclass(frozen=True)
class JumpMatch:
    icao: str
    registration: str | None
    type_code: str | None
    description: str | None


def search_aircraft(db: Database, query: str, *, limit: int = 8) -> list[JumpMatch]:
    """Return aircraft matching ``query`` across icao / reg / type / desc.

    Backs the jump-to-hex overlay. Matches are case-insensitive substring.
    """
    q = (query or "").strip().lower()
    sql = (
        "SELECT s.icao AS icao, "
        "       COALESCE(r.registration, x.registration) AS registration, "
        "       COALESCE(r.type_code, x.type_code) AS type_code, "
        "       COALESCE(r.description, x.type_description) AS description "
        "  FROM aircraft_stats s "
        "  LEFT JOIN aircraft_registry r ON r.icao = s.icao "
        "  LEFT JOIN hex_crossref x ON x.icao = s.icao"
    )
    params: list[Any] = []
    if q:
        sql += (
            " WHERE lower(s.icao) LIKE ?"
            "    OR lower(COALESCE(r.registration, x.registration, '')) LIKE ?"
            "    OR lower(COALESCE(r.type_code, x.type_code, '')) LIKE ?"
            "    OR lower(COALESCE(r.description, x.type_description, '')) LIKE ?"
        )
        needle = f"%{q}%"
        params.extend([needle, needle, needle, needle])
    sql += " ORDER BY s.last_seen DESC LIMIT ?"
    params.append(limit)
    return [
        JumpMatch(
            icao=r["icao"],
            registration=r["registration"],
            type_code=r["type_code"],
            description=r["description"],
        )
        for r in db.conn.execute(sql, params).fetchall()
    ]


# ---------------------------------------------------------------------------
# Flight timeline
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FlightRow:
    takeoff_time: str
    takeoff_date: str
    origin_icao: str | None
    destination_icao: str | None
    duration_minutes: float | None
    callsign: str | None
    mission_type: str | None
    max_altitude: int | None
    cruise_gs_kt: int | None
    landing_type: str
    landing_confidence: float | None
    emergency_squawk: str | None
    had_go_around: int | None
    max_hover_secs: int | None


def list_flights(db: Database, icao: str, *, limit: int = 2000) -> list[FlightRow]:
    """Return flights for one aircraft, newest first.

    The column set mirrors what the existing ``trips`` CLI command
    renders so the TUI and CLI agree on what a "flight" looks like.
    """
    sql = """
        SELECT takeoff_time, takeoff_date,
               origin_icao, destination_icao,
               duration_minutes, callsign,
               mission_type, max_altitude, cruise_gs_kt,
               landing_type, landing_confidence,
               emergency_squawk, had_go_around, max_hover_secs
          FROM flights
         WHERE icao = ?
         ORDER BY takeoff_time DESC
         LIMIT ?
    """
    return [
        FlightRow(
            takeoff_time=r["takeoff_time"],
            takeoff_date=r["takeoff_date"],
            origin_icao=r["origin_icao"],
            destination_icao=r["destination_icao"],
            duration_minutes=r["duration_minutes"],
            callsign=r["callsign"],
            mission_type=r["mission_type"],
            max_altitude=r["max_altitude"],
            cruise_gs_kt=r["cruise_gs_kt"],
            landing_type=r["landing_type"] or "unknown",
            landing_confidence=r["landing_confidence"],
            emergency_squawk=r["emergency_squawk"],
            had_go_around=r["had_go_around"],
            max_hover_secs=r["max_hover_secs"],
        )
        for r in db.conn.execute(sql, (icao, limit)).fetchall()
    ]


# ---------------------------------------------------------------------------
# Event feed (delegates to events.collect_events)
# ---------------------------------------------------------------------------


def list_events(
    db: Database,
    icao: str | None = None,
    *,
    include_spoof_checks: bool = True,
    limit: int = 500,
) -> list[Any]:
    """Return recent events, optionally scoped to one ICAO.

    When ``icao`` is None we collect events for every aircraft in the
    DB and merge - expensive on huge datasets but manageable for the
    single-user TUI case. When scoped to one hex we delegate directly
    to ``events.collect_events``.
    """
    if icao is not None:
        events = collect_events(db, icao, include_spoof_checks=include_spoof_checks)
        return events[:limit]

    hexes = [r["icao"] for r in db.conn.execute("SELECT DISTINCT icao FROM flights").fetchall()]
    out: list[Any] = []
    for hex_code in hexes:
        out.extend(collect_events(db, hex_code, include_spoof_checks=include_spoof_checks))
    out.sort(key=lambda e: e.ts, reverse=True)
    return out[:limit]


# ---------------------------------------------------------------------------
# Spoofed broadcasts audit
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SpoofedBroadcast:
    icao: str
    takeoff_time: str
    takeoff_date: str
    callsign: str | None
    max_altitude: int | None
    reason: str
    reason_detail: dict[str, Any]
    detected_at: str


def list_spoofed_broadcasts(
    db: Database,
    *,
    icao: str | None = None,
    limit: int = 500,
) -> list[SpoofedBroadcast]:
    """Return rejected broadcasts in reverse chronological order."""
    sql = (
        "SELECT icao, takeoff_time, takeoff_date, callsign, max_altitude, "
        "       reason, reason_detail, detected_at "
        "  FROM spoofed_broadcasts"
    )
    params: list[Any] = []
    if icao is not None:
        sql += " WHERE icao = ?"
        params.append(icao)
    sql += " ORDER BY takeoff_time DESC LIMIT ?"
    params.append(limit)

    out = []
    for r in db.conn.execute(sql, params).fetchall():
        try:
            detail = json.loads(r["reason_detail"]) if r["reason_detail"] else {}
        except (TypeError, ValueError):
            detail = {"raw": r["reason_detail"]}
        out.append(
            SpoofedBroadcast(
                icao=r["icao"],
                takeoff_time=r["takeoff_time"],
                takeoff_date=r["takeoff_date"],
                callsign=r["callsign"],
                max_altitude=r["max_altitude"],
                reason=r["reason"],
                reason_detail=detail,
                detected_at=r["detected_at"],
            )
        )
    return out


# ---------------------------------------------------------------------------
# Status dashboard
# ---------------------------------------------------------------------------


def status_snapshot(db: Database, icao: str) -> dict[str, Any]:
    """Return a single-aircraft status snapshot.

    Pulls the aircraft_stats row, the aircraft_registry row, weighted
    source-mix percentages, and top mission types. Mirrors the existing
    ``status`` CLI command so the TUI and CLI stay aligned.
    """
    stats = db.conn.execute("SELECT * FROM aircraft_stats WHERE icao = ?", (icao,)).fetchone()
    registry = db.conn.execute("SELECT * FROM aircraft_registry WHERE icao = ?", (icao,)).fetchone()

    source_row = db.conn.execute(
        """SELECT
               SUM(data_points) AS total_points,
               SUM(adsb_pct * data_points) / NULLIF(SUM(data_points), 0) AS adsb,
               SUM(mlat_pct * data_points) / NULLIF(SUM(data_points), 0) AS mlat,
               SUM(tisb_pct * data_points) / NULLIF(SUM(data_points), 0) AS tisb,
               SUM(COALESCE(other_pct, 0) * data_points) / NULLIF(SUM(data_points), 0) AS other,
               SUM(COALESCE(adsc_pct, 0) * data_points) / NULLIF(SUM(data_points), 0) AS adsc
             FROM flights
            WHERE icao = ? AND data_points > 0""",
        (icao,),
    ).fetchone()

    missions = db.conn.execute(
        """SELECT mission_type, COUNT(*) AS n FROM flights WHERE icao = ?
            GROUP BY mission_type ORDER BY n DESC LIMIT 6""",
        (icao,),
    ).fetchall()

    spoof_count_row = db.conn.execute("SELECT COUNT(*) AS n FROM spoofed_broadcasts WHERE icao = ?", (icao,)).fetchone()

    indicators_row = db.conn.execute(
        """SELECT
               SUM(CASE WHEN emergency_squawk IS NOT NULL AND emergency_squawk != ''
                        THEN 1 ELSE 0 END) AS emergency_flights,
               SUM(CASE WHEN had_go_around = 1
                        THEN 1 ELSE 0 END) AS go_around_flights,
               SUM(CASE WHEN max_hover_secs IS NOT NULL AND max_hover_secs >= 300
                        THEN 1 ELSE 0 END) AS long_hover_flights,
               SUM(CASE WHEN landing_type = 'confirmed'
                        THEN 1 ELSE 0 END) AS confirmed_landings,
               SUM(CASE WHEN landing_type = 'signal_lost'
                        THEN 1 ELSE 0 END) AS signal_lost_landings,
               SUM(CASE WHEN landing_type = 'confirmed'
                         AND (destination_icao IS NULL OR destination_icao = '')
                        THEN 1 ELSE 0 END) AS off_airport_landings
             FROM flights
            WHERE icao = ?""",
        (icao,),
    ).fetchone()

    days_row = db.conn.execute(
        "SELECT COUNT(DISTINCT date) AS n FROM trace_days WHERE icao = ?",
        (icao,),
    ).fetchone()

    stats_dict = dict(stats) if stats else None
    if stats_dict is not None:
        stats_dict["days_with_data"] = days_row["n"] if days_row else 0
        if indicators_row is not None:
            stats_dict["emergency_flights"] = indicators_row["emergency_flights"] or 0
            stats_dict["go_around_flights"] = indicators_row["go_around_flights"] or 0
            stats_dict["long_hover_flights"] = indicators_row["long_hover_flights"] or 0
            stats_dict["confirmed_landings"] = indicators_row["confirmed_landings"] or 0
            stats_dict["signal_lost_landings"] = indicators_row["signal_lost_landings"] or 0
            stats_dict["off_airport_landings"] = indicators_row["off_airport_landings"] or 0

    return {
        "icao": icao,
        "stats": stats_dict,
        "registry": dict(registry) if registry else None,
        "sources": {
            "total_points": source_row["total_points"] or 0,
            "adsb": source_row["adsb"] or 0.0,
            "mlat": source_row["mlat"] or 0.0,
            "tisb": source_row["tisb"] or 0.0,
            "other": source_row["other"] or 0.0,
            "adsc": source_row["adsc"] or 0.0,
        }
        if source_row and source_row["total_points"]
        else None,
        "missions": [(r["mission_type"], r["n"]) for r in missions if r["mission_type"]],
        "spoof_count": spoof_count_row["n"] if spoof_count_row else 0,
    }


# ---------------------------------------------------------------------------
# Map: trace points for one flight
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TracePoint:
    ts: float
    lat: float
    lon: float
    alt_ft: int | None  # None when the trace point was 'ground'
    source: str  # adsb_icao / mlat / tisb / other / adsc


def load_trace_points(db: Database, icao: str, date: str) -> list[TracePoint]:
    """Load all trace points for ``icao`` on ``date`` pooled across sources.

    Points are merged across aggregators, deduplicated on
    (ts, round(lat, 4), round(lon, 4)), and sorted by absolute
    timestamp. ``source`` preserves the readsb source tag so the map
    can overlay the position-source palette.
    """
    rows = db.conn.execute(
        "SELECT source, timestamp, trace_json FROM trace_days WHERE icao = ? AND date = ?",
        (icao, date),
    ).fetchall()
    seen: set[tuple[float, int, int]] = set()
    out: list[TracePoint] = []
    for r in rows:
        try:
            samples = json.loads(r["trace_json"])
        except (TypeError, ValueError):
            continue
        if not isinstance(samples, list):
            continue
        base = r["timestamp"]
        for s in samples:
            if not isinstance(s, list) or len(s) < 4:
                continue
            ts = base + s[0]
            lat = s[1]
            lon = s[2]
            key = (round(ts, 1), round(lat, 4), round(lon, 4))
            if key in seen:
                continue
            seen.add(key)
            alt_raw = s[3]
            alt = int(alt_raw) if isinstance(alt_raw, (int, float)) else None
            source = s[9] if len(s) > 9 and isinstance(s[9], str) else "unknown"
            out.append(TracePoint(ts=ts, lat=lat, lon=lon, alt_ft=alt, source=source))
    out.sort(key=lambda p: p.ts)
    return out


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------


def distinct_dates_for_icao(db: Database, icao: str) -> list[str]:
    """Return the sorted list of dates with trace data for ``icao``."""
    return [
        r["date"]
        for r in db.conn.execute(
            "SELECT DISTINCT date FROM trace_days WHERE icao = ? ORDER BY date DESC",
            (icao,),
        ).fetchall()
    ]


def iter_aircraft_hexes(db: Database) -> Iterable[sqlite3.Row]:
    """Iterate over every ICAO hex we have data for."""
    return db.conn.execute("SELECT DISTINCT icao FROM aircraft_stats ORDER BY icao").fetchall()
