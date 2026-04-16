"""ACARS / VDL2 message fetcher via the airframes.io REST API.

airframes.io exposes aircraft-linked ACARS (inc. VDL2, HFDL, and various
SATCOM) at api.airframes.io. The routes this module uses:

  GET /airframes/icao/{hex}   -> airframe record with numeric id
  GET /airframes/{id}         -> airframe with a `flights` array
  GET /flights/{id}           -> flight with up to 200 `messages`

The /messages endpoint accepts undocumented snake_case filters
(airframe_ids, labels, limit, offset) but hard-caps at 100 per call and
silently ignores timeframe/date filters, so we drive backfill via the
flight-centric path (list flights for the aircraft, then fetch each
flight's messages).

Rate limits observed live: 60 requests/minute, 50k/day on the paid tier.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .airframes import AirframesClient
    from .db import Database

# -----------------------------------------------------------------------------
# OOOI parser
# -----------------------------------------------------------------------------

# Labels documented by airframesio/acars-message-documentation as potentially
# carrying OOOI. Messages with other labels shortcut to {}.
_OOOI_LABELS: frozenset[str] = frozenset({"14", "44", "4T", "H1"})

# "OUT 0830" / "OFF 0855" / "ON 1230" / "IN 1245" with optional punctuation,
# case-insensitive, word-boundaried so we don't match "OUTBOUND" etc.
_KEYWORD_RE = re.compile(
    r"\b(OUT|OFF|ON|IN)\b[\s:=/]*([0-2]\d[0-5]\d)\b",
    re.IGNORECASE,
)

# Air Canada "AGFSR" (Aircraft Generated Flight State Report) is a fairly
# predictable slash-delimited 4T format where the trailing fields are
# OUT/OFF/ON/IN as HHMM (or `----` for events that haven't happened yet).
_AGFSR_RE = re.compile(r"\bAGFSR\b", re.IGNORECASE)


def _anchor_hhmm(hhmm: str, ref_time: datetime) -> datetime:
    """Anchor an HHMM string to a UTC datetime near ref_time.

    OOOI events are typically reported within a few hours of the event
    itself, so we pick the calendar day (ref_time's day, +/- 1) whose
    resulting datetime is closest to ref_time. This handles the two edge
    cases correctly:
      - OUT at 23:45 reported at 00:15 the next day -> yesterday
      - IN at 00:15 tomorrow reported at 22:00 today -> tomorrow
    """
    hh = int(hhmm[:2])
    mm = int(hhmm[2:])
    base = ref_time.replace(hour=hh, minute=mm, second=0, microsecond=0)
    best = base
    best_delta = abs((base - ref_time).total_seconds())
    for days in (-1, 1):
        candidate = base + timedelta(days=days)
        delta = abs((candidate - ref_time).total_seconds())
        if delta < best_delta:
            best = candidate
            best_delta = delta
    return best


def _parse_agfsr(text: str, ref_time: datetime) -> dict[str, datetime]:
    """Parse an Air Canada AGFSR 4T report. Trailing 4 /-delimited fields
    are OUT/OFF/ON/IN as HHMM or `----`."""
    parts = text.strip().split("/")
    if len(parts) < 4:
        return {}
    tail_fields = parts[-4:]
    keys = ("out", "off", "on", "in_")
    result: dict[str, datetime] = {}
    for key, raw in zip(keys, tail_fields, strict=True):
        raw = raw.strip()
        if len(raw) == 4 and raw.isdigit():
            result[key] = _anchor_hhmm(raw, ref_time)
    return result


def _parse_keyword(text: str, ref_time: datetime) -> dict[str, datetime]:
    """Scan for OUT/OFF/ON/IN keyword-HHMM pairs in free-form text."""
    result: dict[str, datetime] = {}
    for match in _KEYWORD_RE.finditer(text):
        key = match.group(1).upper()
        hhmm = match.group(2)
        out_key = "in_" if key == "IN" else key.lower()
        # Only take the first occurrence of each key
        result.setdefault(out_key, _anchor_hhmm(hhmm, ref_time))
    return result


def parse_oooi(label: str, text: str, ref_time: datetime) -> dict[str, datetime]:
    """Extract OOOI (Out / Off / On / In) timestamps from an ACARS message.

    Returns a dict with any of the keys ``out``, ``off``, ``on``, ``in_``
    (note trailing underscore to avoid the Python keyword) mapped to UTC
    datetimes. Partial results are returned when only some OOOI events are
    present. Unknown formats and labels outside the OOOI set return an
    empty dict - the caller should treat this as "no information" rather
    than "flight has no OOOI".

    Parsing scope:
      - Air Canada AGFSR 4T reports (trailing 4 slash-fields)
      - Generic OUT/OFF/ON/IN keyword patterns (14 / 44 / 4T / H1 text)
    Airline-specific H1 sublabel formats (CFB / DFB / EFB) are not parsed.
    """
    if label not in _OOOI_LABELS or not text:
        return {}

    if label == "4T" and _AGFSR_RE.search(text):
        parsed = _parse_agfsr(text, ref_time)
        if parsed:
            return parsed
        # AGFSR with all ---- placeholders -> no OOOI yet.
        return {}

    return _parse_keyword(text, ref_time)


# -----------------------------------------------------------------------------
# Fetch pipeline
# -----------------------------------------------------------------------------


def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp (with or without trailing Z) to UTC."""
    if not value:
        return None
    # Python 3.11+ fromisoformat accepts trailing Z, but 3.10 and earlier do not.
    # The project pins 3.12+ so this is fine, but be defensive anyway.
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _in_range(ts_iso: str | None, start: date, end: date) -> bool:
    """True iff the ISO timestamp falls on a date in [start, end]."""
    dt = _parse_iso(ts_iso)
    if dt is None:
        return False
    d = dt.date()
    return start <= d <= end


def _flatten_message(m: dict, *, icao: str, flight_id: int, registration: str | None) -> dict:
    """Map an airframes.io message dict (camelCase) to our schema (snake_case)."""
    return {
        "airframes_id": m.get("id"),
        "uuid": m.get("uuid"),
        "flight_id": flight_id,
        "icao": icao,
        "registration": m.get("tail") or registration,
        "timestamp": m.get("timestamp"),
        "source_type": m.get("sourceType"),
        "link_direction": m.get("linkDirection"),
        "from_hex": m.get("fromHex"),
        "to_hex": m.get("toHex"),
        "frequency": m.get("frequency"),
        "level": m.get("level"),
        "channel": m.get("channel"),
        "mode": m.get("mode"),
        "label": m.get("label"),
        "block_id": m.get("blockId"),
        "message_number": m.get("messageNumber"),
        "ack": m.get("ack"),
        "flight_number": m.get("flightNumber"),
        "text": m.get("text"),
        "data": m.get("data") if isinstance(m.get("data"), str) else None,
        "latitude": m.get("latitude"),
        "longitude": m.get("longitude"),
        "altitude": m.get("altitude"),
        "departing_airport": m.get("departingAirport"),
        "destination_airport": m.get("destinationAirport"),
    }


def _flatten_flight(record: dict, *, airframe_id: int, icao: str, registration: str | None) -> dict:
    msgs = record.get("messages") or []
    tss = [m.get("timestamp") for m in msgs if m.get("timestamp")]
    return {
        "flight_id": record.get("id"),
        "airframe_id": airframe_id,
        "icao": icao,
        "registration": registration,
        "flight_number": record.get("flight"),
        "flight_iata": record.get("flightIata"),
        "flight_icao": record.get("flightIcao"),
        "status": record.get("status"),
        "departing_airport": record.get("departingAirport"),
        "destination_airport": record.get("destinationAirport"),
        "departure_time_scheduled": record.get("departureTimeScheduled"),
        "departure_time_actual": record.get("departureTimeActual"),
        "arrival_time_scheduled": record.get("arrivalTimeScheduled"),
        "arrival_time_actual": record.get("arrivalTimeActual"),
        "first_seen": min(tss) if tss else record.get("createdAt"),
        "last_seen": max(tss) if tss else record.get("updatedAt"),
        "message_count": len(msgs),
    }


def _apply_oooi_to_flights(db: Database, icao: str) -> int:
    """Scan acars_messages for this icao and write OOOI times to any
    adsbtrack flight whose [takeoff_time, landing_time] covers the message.

    Returns the number of flights updated. Called at the end of fetch_acars
    so the user's next `trips` command surfaces the OOOI columns.
    """
    # Pull candidate flights once (they're a few hundred at most per icao).
    flights = db.conn.execute(
        """SELECT takeoff_time, landing_time, last_seen_time
           FROM flights WHERE icao = ? AND takeoff_time IS NOT NULL""",
        (icao,),
    ).fetchall()
    if not flights:
        return 0

    # OOOI-bearing labels only, to keep the scan small.
    msgs = db.conn.execute(
        """SELECT timestamp, label, text FROM acars_messages
           WHERE icao = ? AND label IN ('14','44','4T','H1')""",
        (icao,),
    ).fetchall()
    if not msgs:
        return 0

    updated_count = 0
    for f in flights:
        takeoff = _parse_iso(f["takeoff_time"])
        end_time = _parse_iso(f["landing_time"]) or _parse_iso(f["last_seen_time"])
        if takeoff is None or end_time is None:
            continue
        flight_oooi: dict[str, datetime] = {}
        for m in msgs:
            msg_ts = _parse_iso(m["timestamp"])
            if msg_ts is None:
                continue
            if not (takeoff <= msg_ts <= end_time):
                continue
            parsed = parse_oooi(m["label"] or "", m["text"] or "", msg_ts)
            # Merge: earliest value wins for OUT/OFF, latest for ON/IN.
            for key, dt in parsed.items():
                existing = flight_oooi.get(key)
                if existing is None:
                    flight_oooi[key] = dt
                elif key in ("out", "off"):
                    if dt < existing:
                        flight_oooi[key] = dt
                else:  # "on", "in_"
                    if dt > existing:
                        flight_oooi[key] = dt
        if flight_oooi:
            db.update_flight_oooi(
                icao,
                f["takeoff_time"],
                out=flight_oooi.get("out").isoformat() if flight_oooi.get("out") else None,
                off=flight_oooi.get("off").isoformat() if flight_oooi.get("off") else None,
                on=flight_oooi.get("on").isoformat() if flight_oooi.get("on") else None,
                in_=flight_oooi.get("in_").isoformat() if flight_oooi.get("in_") else None,
            )
            updated_count += 1
    db.commit()
    return updated_count


def fetch_acars(
    db: Database,
    client: AirframesClient,
    icao: str,
    *,
    start_date: date,
    end_date: date,
    progress_callback=None,
) -> dict:
    """Fetch ACARS/VDL2/HFDL messages from airframes.io for a single ICAO.

    Flow:
      1. Resolve hex -> airframes.io numeric airframe id (cached on
         aircraft_registry.airframes_id after first lookup).
      2. Fetch /airframes/{id} which returns the aircraft's flight list.
      3. Keep flights whose createdAt falls in [start_date, end_date] AND
         whose flight_id is not already in acars_flights (skip-if-fetched).
      4. For each kept flight, fetch /flights/{id}, store metadata + all
         messages. UNIQUE(airframes_id) dedups messages across re-runs.
      5. Post-pass: scan stored messages and populate acars_out/off/on/in
         on any adsbtrack flight whose window overlaps.

    Returns a stats dict: {flights_fetched, messages_inserted, flights_skipped, flights_with_oooi}.
    progress_callback, when given, is called as progress_callback(done, total)
    after each flight for UI integration.
    """
    icao_upper = icao.upper()
    icao_lower = icao.lower()
    stats = {
        "flights_fetched": 0,
        "flights_skipped": 0,
        "messages_inserted": 0,
        "flights_with_oooi": 0,
    }

    # Step 1: resolve airframe id (use cached value when present)
    airframes_id = db.get_registry_airframes_id(icao_lower)
    registration: str | None = None
    if airframes_id is None:
        airframe = client.get_airframe_by_icao(icao_upper)
        if not airframe or not airframe.get("id"):
            return stats
        airframes_id = int(airframe["id"])
        registration = airframe.get("tail")
        db.update_registry_airframes_id(icao_lower, airframes_id)
        db.commit()
    else:
        # Pull tail from the registry so inserts stamp the right registration
        row = db.conn.execute("SELECT registration FROM aircraft_registry WHERE icao = ?", (icao_lower,)).fetchone()
        registration = row["registration"] if row else None

    # Step 2: fetch airframe with its flights list
    airframe_full = client.get_airframe_by_id(airframes_id)
    if not airframe_full:
        return stats
    if not registration:
        registration = airframe_full.get("tail")
    flights = airframe_full.get("flights") or []

    # Step 3: filter by date and skip flights we've already fetched
    already = db.get_acars_flight_ids_fetched(icao_lower)
    candidates = []
    for f in flights:
        fid = f.get("id")
        if fid is None:
            continue
        if fid in already:
            stats["flights_skipped"] += 1
            continue
        # Flight record's createdAt is when airframes.io first saw it. Use
        # that (or updatedAt as fallback) for the date-range filter.
        ts = f.get("createdAt") or f.get("updatedAt")
        if not _in_range(ts, start_date, end_date):
            continue
        candidates.append(f)

    # Step 4: per-flight message fetch
    total = len(candidates)
    for idx, f in enumerate(candidates, start=1):
        fid = int(f["id"])
        detail = client.get_flight(fid)
        if not detail:
            continue
        flight_row = _flatten_flight(detail, airframe_id=airframes_id, icao=icao_lower, registration=registration)
        db.upsert_acars_flight(flight_row)
        for m in detail.get("messages") or []:
            if m.get("id") is None or not m.get("timestamp"):
                continue
            db.insert_acars_message(_flatten_message(m, icao=icao_lower, flight_id=fid, registration=registration))
            stats["messages_inserted"] += 1
        stats["flights_fetched"] += 1
        db.commit()
        if progress_callback is not None:
            progress_callback(idx, total)

    # Step 5: propagate OOOI to adsbtrack flights
    stats["flights_with_oooi"] = _apply_oooi_to_flights(db, icao_lower)
    return stats
