"""METAR/SPECI history client for aviationweather.gov (issue #26).

Diversion and go-around forensics almost always need the destination
weather around the event (OMAA CAVOK during a divert, KTYS microburst
during a go-around). This module fetches METAR/SPECI observations from
the free aviationweather.gov data API and stores them in the ``metars``
table, keyed by station + observation time so repeated fetches dedupe.

The API serves at most the last ~30 days of history (``date`` anchor +
``hours`` lookback). Older windows cannot be backfilled, which is why the
local table is the permanent archive: run ``adsbtrack wx`` or
``trips --fetch-wx`` while the window is still open and the observations
are kept forever.

Leaf module: imports config and (for typing) db, never adsbtrack.cli.
"""

from __future__ import annotations

import dataclasses
import math
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx

from .config import Config

if TYPE_CHECKING:
    from .db import Database

_DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "adsbtrack/0.1 (+https://github.com/afranke/adsbtrack)",
}


class MetarError(RuntimeError):
    """HTTP or protocol error from the aviationweather.gov data API."""


@dataclass(frozen=True)
class Metar:
    """One METAR or SPECI observation, normalized for the metars table.

    ``raw_text`` is the authority for forensics; the parsed columns exist
    so SQL can filter without re-parsing raw METAR syntax. ``obs_time`` is
    ISO 8601 UTC in ``datetime.isoformat()`` form (``+00:00`` suffix, the
    same convention flights.takeoff_time uses) so lexicographic BETWEEN
    against flight timestamps works.
    """

    station: str
    obs_time: str
    metar_type: str | None  # METAR or SPECI
    raw_text: str
    temp_c: float | None = None
    dewpoint_c: float | None = None
    wind_dir_deg: int | None = None  # NULL for VRB / missing (raw_text keeps it)
    wind_speed_kt: int | None = None
    wind_gust_kt: int | None = None
    visibility_mi: float | None = None  # "10+" style caps parsed to the number
    altim_hpa: float | None = None
    flight_category: str | None = None  # VFR / MVFR / IFR / LIFR


def _num(value: Any) -> float | None:
    """Parse the API's numeric-ish fields: plain numbers pass through,
    "10+" / "6+" style capped values keep the number, anything else
    (VRB, None, garbage) comes back None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.rstrip("+"))
        except ValueError:
            return None
    return None


def _int(value: Any) -> int | None:
    parsed = _num(value)
    return int(parsed) if parsed is not None else None


def parse_metar_json(payload: Any) -> list[Metar]:
    """Turn the API's JSON array into Metar rows.

    Tolerant by design: an entry missing its station id, observation time,
    or raw text is skipped rather than raised, so one malformed element
    can't sink an otherwise-good response. Anything non-list yields [].
    """
    if not isinstance(payload, list):
        return []
    out: list[Metar] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        station = entry.get("icaoId")
        obs_epoch = entry.get("obsTime")
        raw = entry.get("rawOb")
        if not station or not raw or not isinstance(obs_epoch, int | float):
            continue
        out.append(
            Metar(
                station=str(station).upper(),
                obs_time=datetime.fromtimestamp(obs_epoch, UTC).isoformat(),
                metar_type=entry.get("metarType"),
                raw_text=str(raw),
                temp_c=_num(entry.get("temp")),
                dewpoint_c=_num(entry.get("dewp")),
                wind_dir_deg=_int(entry.get("wdir")) if entry.get("wdir") != "VRB" else None,
                wind_speed_kt=_int(entry.get("wspd")),
                wind_gust_kt=_int(entry.get("wgst")),
                visibility_mi=_num(entry.get("visib")),
                altim_hpa=_num(entry.get("altim")),
                flight_category=entry.get("fltCat"),
            )
        )
    return out


def fetch_metars(
    stations: list[str] | tuple[str, ...],
    *,
    hours: float,
    date: datetime | None = None,
    config: Config | None = None,
    client: httpx.Client | None = None,
) -> list[Metar]:
    """Fetch METAR/SPECI history for one or more stations.

    ``hours`` looks back from ``date`` (default: now). The API caps history
    at ~30 days; a caller asking beyond that gets a MetarError carrying the
    server's message. ``client`` is injectable for tests.
    """
    config = config or Config()
    params: dict[str, str] = {
        "ids": ",".join(s.strip().upper() for s in stations if s.strip()),
        "format": "json",
        "hours": str(max(1, math.ceil(hours))),
    }
    if not params["ids"]:
        raise MetarError("no stations given")
    if date is not None:
        anchor = date.astimezone(UTC) if date.tzinfo else date.replace(tzinfo=UTC)
        params["date"] = anchor.strftime("%Y-%m-%dT%H:%M:%SZ")

    owns_client = client is None
    http = client or httpx.Client(headers=_DEFAULT_HEADERS, timeout=config.metar_timeout_secs)
    try:
        response = http.get(config.metar_api_url, params=params)
    except httpx.HTTPError as exc:
        raise MetarError(f"aviationweather.gov request failed: {exc}") from exc
    finally:
        if owns_client:
            http.close()
    if response.status_code != 200:
        raise MetarError(f"aviationweather.gov returned HTTP {response.status_code}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise MetarError("aviationweather.gov returned unparseable JSON") from exc
    if isinstance(payload, dict) and payload.get("status") == "error":
        raise MetarError(f"aviationweather.gov: {payload.get('error', 'unknown error')}")
    return parse_metar_json(payload)


def metar_window(event_time: datetime, window_hours: float, *, now: datetime | None = None) -> tuple[datetime, float]:
    """API request covering ``window_hours`` centered on ``event_time``.

    Returns (anchor, hours) for fetch_metars: the anchor sits half a
    window after the event (so observations both sides of a takeoff or
    landing are captured), clamped to ``now`` for very recent events --
    the lookback then widens so the window's start still reaches
    event - window/2.
    """
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=UTC)
    now = now or datetime.now(UTC)
    half = timedelta(hours=window_hours / 2)
    anchor = min(event_time + half, now)
    hours = (anchor - (event_time - half)).total_seconds() / 3600.0
    return anchor, hours


def _parse_flight_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def stored_metars_near(db: Database, station: str, event_iso: str, *, window_hours: float) -> list:
    """Stored observations for ``station`` within window_hours centered on
    ``event_iso``, oldest first. Read-only; never touches the network."""
    event = _parse_flight_time(event_iso)
    if event is None:
        return []
    half = timedelta(hours=window_hours / 2)
    return db.get_metars(station.upper(), (event - half).isoformat(), (event + half).isoformat())


def fetch_flight_endpoint_metars(
    db: Database,
    config: Config,
    flight: Any,
    *,
    client: httpx.Client | None = None,
    now: datetime | None = None,
) -> tuple[int, list[str]]:
    """Fetch and store METARs around one flight's endpoints.

    ``flight`` is any mapping with origin_icao / takeoff_time /
    destination_icao / landing_time keys (a flights-table row qualifies).
    Each endpoint with a matched airport and a timestamp gets a
    config.metar_window_hours window centered on its event. Endpoints
    whose window already holds >= metar_min_cached_obs stored observations
    are skipped (METAR history is immutable, so cached rows never go
    stale), as are events older than the API's metar_history_max_days
    retention. Returns (stored_row_count, warnings).
    """
    now = now or datetime.now(UTC)
    stored = 0
    warnings: list[str] = []
    endpoints = [
        ("origin", flight["origin_icao"], flight["takeoff_time"]),
        ("destination", flight["destination_icao"], flight["landing_time"]),
    ]
    fetched_any = False
    for label, station, event_iso in endpoints:
        event = _parse_flight_time(event_iso)
        if not station or event is None:
            continue
        if (now - event) > timedelta(days=config.metar_history_max_days):
            warnings.append(
                f"{label} {station} {event_iso}: outside the aviationweather.gov "
                f"{config.metar_history_max_days}-day history window"
            )
            continue
        cached = stored_metars_near(db, station, event_iso, window_hours=config.metar_window_hours)
        if len(cached) >= config.metar_min_cached_obs:
            continue
        if fetched_any and config.metar_rate_limit_secs > 0:
            time.sleep(config.metar_rate_limit_secs)
        anchor, hours = metar_window(event, config.metar_window_hours, now=now)
        try:
            metars = fetch_metars([station], hours=hours, date=anchor, config=config, client=client)
        except MetarError as exc:
            warnings.append(f"{label} {station} {event_iso}: {exc}")
            continue
        fetched_any = True
        stored += db.upsert_metars(dataclasses.asdict(m) for m in metars)
    return stored, warnings
