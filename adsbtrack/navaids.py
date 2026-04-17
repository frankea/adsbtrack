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
from pathlib import Path

import httpx
from rich.progress import Progress

from .config import Config
from .db import Database

# Row shape matches the INSERT OR REPLACE column order below:
# (ident, name, type, latitude_deg, longitude_deg, elevation_ft, frequency_khz, iso_country).
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
        with Progress() as progress:
            task = progress.add_task("Downloading navaids...", total=None)
            resp = httpx.get(config.navaids_csv_url, follow_redirects=True, timeout=60)
            resp.raise_for_status()
            progress.update(task, completed=100)
            text = resp.text

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
