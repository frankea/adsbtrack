import contextlib
import csv
import io
from math import asin, cos, radians, sin, sqrt

import httpx
from rich.progress import Progress

from .config import Config
from .db import Database
from .models import AirportMatch


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * asin(sqrt(a))


def download_airports(db: Database, config: Config):
    with Progress() as progress:
        task = progress.add_task("Downloading airport database...", total=None)
        resp = httpx.get(config.airports_csv_url, follow_redirects=True, timeout=60)
        resp.raise_for_status()
        progress.update(task, completed=50)

        reader = csv.DictReader(io.StringIO(resp.text))
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
        progress.update(task, completed=100)
    return len(airports)


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
