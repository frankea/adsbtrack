import contextlib
import csv
import io
import time
from pathlib import Path

import httpx
from rich.progress import Progress

from .config import Config
from .db import Database
from .geo import haversine_km
from .models import AirportMatch

__all__ = ["haversine_km", "fetch_ourairports_csv", "download_airports", "enrich_helipad_names", "find_nearest_airport"]


def fetch_ourairports_csv(
    url: str,
    *,
    label: str,
    timeout: float = 60.0,
    cache_dir: Path | None = None,
    cache_max_age_hours: float = 168.0,
) -> str:
    """Download an OurAirports CSV, return its body text.

    Shared by every OurAirports importer (airports / runways / navaids) so
    the httpx + rich-Progress boilerplate has one home. Raises
    httpx.HTTPError variants unchanged; callers wrap as ClickException.

    When ``cache_dir`` is provided, a copy of the CSV is read from (or
    written to) ``cache_dir / <basename-of-url>``. A cache file older than
    ``cache_max_age_hours`` is ignored and re-fetched. Set ``cache_dir`` to
    ``None`` to bypass the cache entirely.
    """
    cache_path: Path | None = None
    if cache_dir is not None:
        cache_path = Path(cache_dir) / url.rsplit("/", 1)[-1]
        if cache_path.exists():
            age_hours = (time.time() - cache_path.stat().st_mtime) / 3600.0
            if age_hours < cache_max_age_hours:
                return cache_path.read_text(encoding="utf-8")

    with Progress() as progress:
        task = progress.add_task(f"Downloading {label}...", total=None)
        resp = httpx.get(url, follow_redirects=True, timeout=timeout)
        resp.raise_for_status()
        progress.update(task, completed=100)
        text = resp.text

    if cache_path is not None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(text, encoding="utf-8")
    return text


def download_airports(db: Database, config: Config):
    text = fetch_ourairports_csv(
        config.airports_csv_url,
        label="airport database",
        cache_dir=config.ourairports_cache_dir,
        cache_max_age_hours=config.ourairports_cache_max_age_hours,
    )
    reader = csv.DictReader(io.StringIO(text))
    airports = []
    for row in reader:
        if row["type"] not in config.airport_types:
            continue
        try:
            lat = float(row["latitude_deg"])
            lon = float(row["longitude_deg"])
        except (ValueError, KeyError):
            continue
        elev = None
        if row.get("elevation_ft"):
            with contextlib.suppress(ValueError):
                elev = int(row["elevation_ft"])
        airports.append(
            (
                row["ident"],
                row["type"],
                row["name"],
                lat,
                lon,
                elev,
                row.get("iso_country", ""),
                row.get("iso_region", ""),
                row.get("municipality", ""),
                row.get("iata_code", ""),
            )
        )

    db.insert_airports(airports)
    return len(airports)


def enrich_helipad_names(db: Database, config: Config, *, max_distance_km: float = 0.5) -> int:
    """Match helipads against OurAirports heliport entries by proximity.

    Downloads the same airports.csv, filters for type='heliport', and for
    each helipad in the DB finds the nearest heliport within max_distance_km.
    Updates the helipad's name_hint with the heliport name.

    Returns the number of helipads enriched.
    """
    # Fetch all helipads that still have a generic name or no name
    helipads = db.conn.execute("SELECT helipad_id, centroid_lat, centroid_lon, name_hint FROM helipads").fetchall()
    if not helipads:
        return 0

    # Download heliport entries from OurAirports CSV (cached on disk)
    text = fetch_ourairports_csv(
        config.airports_csv_url,
        label="airport database",
        cache_dir=config.ourairports_cache_dir,
        cache_max_age_hours=config.ourairports_cache_max_age_hours,
    )
    reader = csv.DictReader(io.StringIO(text))
    heliports: list[tuple[str, str, float, float]] = []  # (ident, name, lat, lon)
    for row in reader:
        if row.get("type") != "heliport":
            continue
        try:
            lat = float(row["latitude_deg"])
            lon = float(row["longitude_deg"])
        except (ValueError, KeyError):
            continue
        name = row.get("name", "")
        ident = row.get("ident", "")
        if name:
            heliports.append((ident, name, lat, lon))

    if not heliports:
        return 0

    # Manual overrides for known facilities not in OurAirports.
    # (lat, lon, name) tuples; matched within 1 km of helipad centroid.
    _MANUAL_OVERRIDES: list[tuple[float, float, str]] = [
        (35.202, -101.919, "Northwest Texas Healthcare System Heliport"),
        (34.439, -100.229, "Childress Regional Medical Center Heliport"),
        (34.444, -100.236, "Childress Regional Medical Center Heliport"),
        (34.438, -100.240, "Childress Regional Medical Center Heliport"),
        (34.447, -100.225, "Childress Regional Medical Center Heliport"),
        (45.583, -122.565, "Legacy Emanuel Medical Center Heliport"),
        (34.231, -84.462, "Northside Hospital Cherokee Heliport"),
        (27.409, -82.570, "Sarasota Memorial Hospital Heliport"),
        (38.517, -92.512, "Capital Region Medical Center Heliport"),
        (38.566, -92.490, "SSM Health St. Mary's Hospital Heliport"),
        (38.862, -99.308, "Hays Medical Center Heliport"),
        (38.862, -99.298, "Hays Medical Center Heliport"),
        (25.241, 51.574, "Hamad Medical Corporation Heliport"),
        (49.252, 0.582, "Deauville Heliport"),
        (35.036, -85.268, "Erlanger East Hospital Heliport"),
        (35.062, -84.718, "Starr Regional Medical Center Heliport"),
        (38.562, -92.492, "Jefferson City MO Medical Heliport"),
        (37.962, -92.665, "Phelps Health Hospital Heliport"),
        (37.723, -97.263, "Ascension Via Christi St. Francis Heliport"),
        (38.842, -99.307, "Hays KS Area Heliport"),
    ]

    enriched = 0
    for hp in helipads:
        hp_lat = hp["centroid_lat"]
        hp_lon = hp["centroid_lon"]
        hp_name = hp["name_hint"] or ""

        # Skip if already named (not generic)
        if hp_name and not hp_name.startswith("helipad_"):
            continue

        # Try OurAirports match first
        best_name: str | None = None
        best_dist = max_distance_km
        for _ident, name, h_lat, h_lon in heliports:
            d = haversine_km(hp_lat, hp_lon, h_lat, h_lon)
            if d < best_dist:
                best_dist = d
                best_name = name

        # Fall back to manual overrides (1 km tolerance)
        if not best_name:
            for m_lat, m_lon, m_name in _MANUAL_OVERRIDES:
                d = haversine_km(hp_lat, hp_lon, m_lat, m_lon)
                if d < 1.0:
                    best_name = m_name
                    break

        if best_name:
            db.conn.execute(
                "UPDATE helipads SET name_hint = ? WHERE helipad_id = ?",
                (best_name, hp["helipad_id"]),
            )
            enriched += 1

    return enriched


def find_nearest_airport(db: Database, lat: float, lon: float, config: Config) -> AirportMatch | None:
    candidates = db.find_nearby_airports(lat, lon, delta=0.15, types=config.airport_types)
    if not candidates:
        # Widen search
        candidates = db.find_nearby_airports(lat, lon, delta=0.5, types=config.airport_types)
    if not candidates:
        return None

    best = None
    best_dist = float("inf")
    for ap in candidates:
        dist = haversine_km(lat, lon, ap["latitude_deg"], ap["longitude_deg"])
        if dist < best_dist:
            best_dist = dist
            best = ap

    if best_dist > config.airport_match_threshold_km:
        return None

    return AirportMatch(
        ident=best["ident"],
        name=best["name"],
        distance_km=round(best_dist, 2),
        municipality=best["municipality"],
        iata_code=best["iata_code"],
    )
