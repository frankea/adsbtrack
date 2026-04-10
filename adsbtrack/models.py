from dataclasses import dataclass
from datetime import datetime


@dataclass
class Flight:
    icao: str
    takeoff_time: datetime
    takeoff_lat: float
    takeoff_lon: float
    takeoff_date: str  # YYYY-MM-DD source file date
    landing_time: datetime | None = None
    landing_lat: float | None = None
    landing_lon: float | None = None
    landing_date: str | None = None
    origin_icao: str | None = None
    origin_name: str | None = None
    origin_distance_km: float | None = None
    destination_icao: str | None = None
    destination_name: str | None = None
    destination_distance_km: float | None = None
    duration_minutes: float | None = None
    callsign: str | None = None
    # Flight quality metadata
    landing_type: str = "unknown"  # confirmed, signal_lost, dropped_on_approach, uncertain, altitude_error
    takeoff_type: str = "unknown"  # observed, found_mid_flight
    takeoff_confidence: float | None = None
    landing_confidence: float | None = None
    data_points: int | None = None
    sources: str | None = None  # comma-separated source names
    max_altitude: int | None = None
    ground_points_at_landing: int | None = None
    ground_points_at_takeoff: int | None = None
    baro_error_points: int | None = None  # count of points where baro said ground but geom disagreed
    # Last observed position regardless of landing outcome. For confirmed
    # landings this equals landing_*; for signal_lost / dropped_on_approach
    # it is the last recorded trace point before coverage dropped.
    last_seen_lat: float | None = None
    last_seen_lon: float | None = None
    last_seen_alt_ft: int | None = None
    last_seen_time: datetime | None = None


@dataclass
class AirportMatch:
    ident: str
    name: str
    distance_km: float
    municipality: str | None = None
    iata_code: str | None = None
