"""OurAirports runway ingestion.

Downloads `runways.csv` from OurAirports, parses rows into one tuple per
runway end (so "09" and "27" become two rows), and upserts into the local
`runways` table. Idempotent on re-run - repeated refreshes of the same
airport overwrite existing rows without duplicating.

OurAirports uses two representations we filter out at parse time:
  * Heliport-shape rows where `le_ident="H1"` and every coordinate is blank.
  * Rows where an endpoint's `*_latitude_deg` / `*_longitude_deg` is blank -
    we emit only the endpoints that have coordinates (or zero rows if both
    are missing).

`airport_ident` is preserved exactly as given (can be ICAO like "KATL" or
a FAA local code like "67FL"); our airport-matching code already tolerates
both.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

import httpx
from rich.progress import Progress

from .config import Config
from .db import Database

# Order must match db.Database.insert_runway_ends INSERT column order.
RunwayEnd = tuple[
    str,  # airport_ident
    str,  # runway_name
    float,  # latitude_deg
    float,  # longitude_deg
    int | None,  # elevation_ft
    float | None,  # heading_deg_true
    int | None,  # length_ft
    int | None,  # width_ft
    str | None,  # surface
    int,  # closed
    int | None,  # displaced_threshold_ft
]


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return int(float(stripped))
    except ValueError:
        return None


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return float(stripped)
    except ValueError:
        return None


def _parse_str(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _build_end(
    *,
    airport_ident: str,
    runway_name: str,
    lat: float,
    lon: float,
    elev: int | None,
    heading: float | None,
    length_ft: int | None,
    width_ft: int | None,
    surface: str | None,
    closed: int,
    displaced_threshold: int | None,
) -> RunwayEnd:
    return (
        airport_ident,
        runway_name,
        lat,
        lon,
        elev,
        heading,
        length_ft,
        width_ft,
        surface,
        closed,
        displaced_threshold,
    )


def parse_runway_row(row: dict[str, str]) -> list[RunwayEnd]:
    """Turn one OurAirports runways.csv DictReader row into 0-2 tuples.

    Returns an empty list when:
      * airport_ident is blank (malformed row)
      * both endpoints are missing lat/lon or ident
    """
    airport_ident = _parse_str(row.get("airport_ident"))
    if airport_ident is None:
        return []

    length_ft = _parse_int(row.get("length_ft"))
    width_ft = _parse_int(row.get("width_ft"))
    surface = _parse_str(row.get("surface"))
    closed_raw = _parse_int(row.get("closed"))
    closed = 1 if closed_raw == 1 else 0

    ends: list[RunwayEnd] = []
    for prefix in ("le_", "he_"):
        runway_name = _parse_str(row.get(f"{prefix}ident"))
        lat = _parse_float(row.get(f"{prefix}latitude_deg"))
        lon = _parse_float(row.get(f"{prefix}longitude_deg"))
        # Skip endpoints we can't uniquely key or locate. The spec's
        # "airport centroid fallback" is intentionally deferred - we skip
        # when uncertain rather than guess a direction.
        if runway_name is None or lat is None or lon is None:
            continue
        ends.append(
            _build_end(
                airport_ident=airport_ident,
                runway_name=runway_name,
                lat=lat,
                lon=lon,
                elev=_parse_int(row.get(f"{prefix}elevation_ft")),
                heading=_parse_float(row.get(f"{prefix}heading_degT")),
                length_ft=length_ft,
                width_ft=width_ft,
                surface=surface,
                closed=closed,
                displaced_threshold=_parse_int(row.get(f"{prefix}displaced_threshold_ft")),
            )
        )
    return ends


def _import_runways_from_reader(db: Database, reader: csv.DictReader) -> int:
    """Core loop: iterate CSV rows, parse, bulk-insert. Returns count inserted."""
    ends: list[RunwayEnd] = []
    for row in reader:
        ends.extend(parse_runway_row(row))
    db.insert_runway_ends(ends)
    return len(ends)


def import_runways_from_path(db: Database, path: Path) -> int:
    """Parse a local runways.csv at `path`, upsert every valid end, return
    the count of ends inserted.

    Idempotent: repeated calls upsert via (airport_ident, runway_name) so
    the total row count is bounded by the unique-key space, not the call
    count.
    """
    with path.open("r", encoding="utf-8", newline="") as fh:
        return _import_runways_from_reader(db, csv.DictReader(fh))


def refresh_runways(
    db: Database,
    cfg: Config,
    *,
    local_csv: Path | None = None,
    timeout: float = 120.0,
) -> int:
    """Refresh the runways table from the upstream OurAirports CSV.

    When `local_csv` is provided, the HTTP fetch is skipped and the file is
    parsed directly. Otherwise, the CSV is streamed into memory via httpx
    (file is ~5MB), written nowhere - we parse the text straight from the
    response body.

    Raises httpx.HTTPError variants unchanged; the CLI wrapper surfaces
    them as click.ClickException. Returns the number of runway ends
    inserted.
    """
    if local_csv is not None:
        return import_runways_from_path(db, local_csv)

    with Progress() as progress:
        task = progress.add_task("Downloading OurAirports runways.csv...", total=None)
        resp = httpx.get(cfg.runways_csv_url, follow_redirects=True, timeout=timeout)
        resp.raise_for_status()
        progress.update(task, completed=50)

        reader = csv.DictReader(io.StringIO(resp.text))
        count = _import_runways_from_reader(db, reader)
        progress.update(task, completed=100)
    return count
