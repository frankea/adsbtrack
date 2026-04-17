"""OurAirports navaids.csv ingestion + bounding-box query helper.

Attribution: the per-flight alignment algorithm that consumes this table
(see adsbtrack/navaid_alignment.py) is inspired by xoolive/traffic's
BeaconTrackBearingAlignment (MIT-licensed). No code is copied from
traffic; this module only handles I/O and table storage.
"""

from __future__ import annotations

import contextlib
import csv
import io
import math
import sqlite3
from collections.abc import Iterable
from pathlib import Path

from .classifier import _PointSample
from .config import Config
from .db import Database

NavaidRow = tuple[str, str | None, str | None, float, float, int | None, int | None, str | None]


def _parse_row(row: dict) -> NavaidRow | None:
    try:
        lat = float(row["latitude_deg"])
        lon = float(row["longitude_deg"])
    except (ValueError, KeyError):
        return None
    ident = (row.get("ident") or "").strip()
    if not ident:
        return None
    elev: int | None = None
    freq: int | None = None
    if row.get("elevation_ft"):
        with contextlib.suppress(ValueError):
            elev = int(float(row["elevation_ft"]))
    if row.get("frequency_khz"):
        with contextlib.suppress(ValueError):
            freq = int(float(row["frequency_khz"]))
    return (
        ident,
        (row.get("name") or "").strip() or None,
        (row.get("type") or "").strip() or None,
        lat,
        lon,
        elev,
        freq,
        (row.get("iso_country") or "").strip() or None,
    )


def _read_csv(text: str) -> list[NavaidRow]:
    reader = csv.DictReader(io.StringIO(text))
    rows: list[NavaidRow] = []
    for raw in reader:
        parsed = _parse_row(raw)
        if parsed is not None:
            rows.append(parsed)
    return rows


def refresh_navaids(
    db: Database,
    config: Config,
    *,
    local_csv: Path | None = None,
) -> int:
    """Download (or load local) OurAirports navaids.csv and upsert into navaids.

    Returns the number of rows written. Idempotent: re-running replaces
    rows with identical primary key (ident, latitude_deg, longitude_deg).
    """
    if local_csv is not None:
        text = Path(local_csv).read_text(encoding="utf-8")
    else:
        from .airports import fetch_ourairports_csv

        text = fetch_ourairports_csv(config.navaids_csv_url, label="navaids")

    rows = _read_csv(text)
    db.conn.executemany(
        "INSERT OR REPLACE INTO navaids"
        " (ident, name, type, latitude_deg, longitude_deg,"
        "  elevation_ft, frequency_khz, iso_country)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    db.conn.commit()
    return len(rows)


def query_navaids_in_bbox(
    conn: sqlite3.Connection,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
) -> list[sqlite3.Row]:
    """Return navaid rows inside the [min_lat, max_lat] x [min_lon, max_lon] box.

    Uses the (latitude_deg, longitude_deg) indexes. The caller is responsible
    for buffering the box (see navaid_bbox_buffer_nm in Config).
    """
    return list(
        conn.execute(
            "SELECT ident, name, type, latitude_deg, longitude_deg,"
            "       elevation_ft, frequency_khz, iso_country"
            "  FROM navaids"
            " WHERE latitude_deg BETWEEN ? AND ?"
            "   AND longitude_deg BETWEEN ? AND ?",
            (min_lat, max_lat, min_lon, max_lon),
        ).fetchall()
    )


def flight_bbox_from_points(
    points: Iterable[_PointSample],
    *,
    buffer_nm: float,
) -> tuple[float, float, float, float] | None:
    """Compute (min_lat, max_lat, min_lon, max_lon) of a flight, expanded by
    buffer_nm in every direction. Returns None if the flight has no samples
    with lat/lon or crosses the antimeridian (|lon_span| > 180 deg)."""
    lats: list[float] = []
    lons: list[float] = []
    for s in points:
        if s.lat is not None and s.lon is not None:
            lats.append(s.lat)
            lons.append(s.lon)
    if not lats:
        return None
    min_lat, max_lat = min(lats), max(lats)
    min_lon, max_lon = min(lons), max(lons)
    if max_lon - min_lon > 180.0:
        return None
    # 1 deg lat ~ 60 nm. Longitude scales with cos(lat); use the higher-abs
    # latitude to be safe so the box is always wide enough.
    buffer_lat = buffer_nm / 60.0
    worst_lat_rad = math.radians(max(abs(min_lat), abs(max_lat)))
    cos_lat = max(0.01, math.cos(worst_lat_rad))  # clamp near the poles
    buffer_lon = buffer_lat / cos_lat
    return (
        min_lat - buffer_lat,
        max_lat + buffer_lat,
        min_lon - buffer_lon,
        max_lon + buffer_lon,
    )
