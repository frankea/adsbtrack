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

    # --- v3: squawk and emergency ---
    squawk_first: str | None = None
    squawk_last: str | None = None
    squawk_changes: int | None = None
    emergency_squawk: str | None = None  # most severe 7500/7600/7700 seen, else NULL
    vfr_flight: int | None = None  # 1 iff >=80% of observed squawks == 1200 (SQLite: INTEGER)

    # --- v3: mission / category / autopilot / emergency ---
    mission_type: str | None = None  # ems_hems, offshore, exec_charter, training, survey, pattern, transport, unknown
    category_do260: str | None = None  # A0-B7 DO-260B category, most common across the flight
    autopilot_target_alt_ft: int | None = None  # last nav_altitude_mcp before top-of-descent
    emergency_flag: str | None = None  # detail.emergency (e.g. lifeguard, general_emergency)

    # --- v3: path / loiter ---
    path_length_km: float | None = None  # sum of haversine between consecutive points
    max_distance_km: float | None = None  # max distance from origin
    loiter_ratio: float | None = None  # path_length / (2 * max_distance_from_origin)
    path_efficiency: float | None = None  # great_circle / path_length, only when origin != destination

    # --- v3: hover (rotorcraft only) ---
    max_hover_secs: int | None = None
    hover_episodes: int | None = None

    # --- v3: go-around ---
    go_around_count: int | None = None

    # --- v3: takeoff / landing heading ---
    takeoff_heading_deg: float | None = None
    landing_heading_deg: float | None = None

    # --- v3: phase of flight time budget (integer seconds) ---
    climb_secs: int | None = None
    cruise_secs: int | None = None
    descent_secs: int | None = None
    level_secs: int | None = None
    cruise_alt_ft: int | None = None
    cruise_gs_kt: int | None = None

    # --- v3: peak climb/descent rates (30-s rolling window) ---
    peak_climb_fpm: int | None = None
    peak_descent_fpm: int | None = None

    # --- v3: day / night ---
    takeoff_is_night: int | None = None  # 1 iff sun below -6 deg at takeoff
    landing_is_night: int | None = None
    night_flight: int | None = None  # 1 iff >=50% of sampled points were at night

    # --- v3: callsigns history ---
    callsigns: str | None = None  # JSON array of distinct callsigns seen
    callsign_changes: int | None = None  # transitions count (not distinct), e.g. TWY501<->GS501 churn
    callsign_count: int | None = None  # v4: distinct callsign count = len(set(callsigns))

    # --- v3: destination inference for dropped flights ---
    probable_destination_icao: str | None = None
    probable_destination_distance_km: float | None = None
    probable_destination_confidence: float | None = None


@dataclass
class AirportMatch:
    ident: str
    name: str
    distance_km: float
    municipality: str | None = None
    iata_code: str | None = None
