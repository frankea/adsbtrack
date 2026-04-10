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
    landing_type: str = "unknown"  # confirmed, signal_lost, uncertain, altitude_error
    takeoff_confidence: float | None = None
    landing_confidence: float | None = None
    data_points: int | None = None
    sources: str | None = None  # comma-separated source names
    max_altitude: int | None = None
    ground_points_at_landing: int | None = None


@dataclass
class AirportMatch:
    ident: str
    name: str
    distance_km: float
    municipality: str | None = None
    iata_code: str | None = None
