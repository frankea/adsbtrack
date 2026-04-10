from dataclasses import dataclass, field
from pathlib import Path

SOURCE_URLS = {
    "adsbx": "https://globe.adsbexchange.com/globe_history",
    "adsbfi": "https://globe.adsb.fi/globe_history",
    "airplaneslive": "https://globe.airplanes.live/globe_history",
    "adsblol": "https://adsb.lol/globe_history",
    "theairtraffic": "https://globe.theairtraffic.com/globe_history",
}


# Max endurance in minutes by Mode S type code. Flights longer than this
# are treated as data-gap artifacts rather than real single flights. Values
# are conservative (typical ferry range, not theoretical max).
TYPE_ENDURANCE_MINUTES: dict[str, float] = {
    # Helicopters
    "B407": 180.0,  # Bell 407
    "B429": 180.0,  # Bell 429
    "EC30": 180.0,  # Airbus EC130
    "EC35": 180.0,  # Airbus EC135
    "EC45": 180.0,  # Airbus EC145
    "S76": 210.0,  # Sikorsky S-76
    "S92": 300.0,  # Sikorsky S-92 (oil-rig variant has extended range)
    "H60": 180.0,  # UH-60 Black Hawk
    # Light piston / turboprop
    "C150": 240.0,
    "C172": 240.0,
    "C182": 300.0,
    "C208": 420.0,  # Caravan
    "PC12": 420.0,  # Pilatus PC-12
    "TBM9": 360.0,  # TBM 900
    # Business jets
    "GLF5": 780.0,  # Gulfstream V
    "GLF6": 900.0,  # Gulfstream G650
    "GLF4": 660.0,
    "CL60": 540.0,  # Challenger
    "C56X": 360.0,  # Citation Excel
    "E55P": 360.0,  # Phenom 300
    "FA7X": 720.0,  # Falcon 7X
}


@dataclass
class Config:
    db_path: Path = Path("adsbtrack.db")
    credentials_path: Path = Path("credentials.json")
    airports_csv_url: str = "https://davidmegginson.github.io/ourairports-data/airports.csv"
    rate_limit: float = 0.5  # seconds between requests
    rate_limit_max: float = 30.0  # max backoff after 429s
    rate_limit_recovery: int = 10  # successes before reducing delay
    airport_match_threshold_km: float = 10.0
    airport_types: tuple[str, ...] = ("large_airport", "medium_airport", "small_airport")
    landing_speed_threshold_kts: float = 80.0  # ground speed above which a "ground" alt reading is ignored

    # Flight splitting
    max_point_gap_minutes: float = 30.0  # intra-trace gap that closes a flight
    max_day_gap_days: float = 2.0  # gap between trace days that resets state
    min_flight_minutes: float = 5.0  # minimum duration for a valid flight
    min_flight_distance_km: float = 5.0  # minimum travel for a short flight

    # Landing detection
    post_landing_window_secs: float = 60.0  # keep flight open to collect ground points
    post_landing_max_points: int = 5  # or this many ground points, whichever first
    baro_error_geom_threshold_ft: float = 300.0  # geom > this while baro=ground is a baro error

    # Endurance
    max_endurance_minutes: float = 240.0  # fallback when type_code is unknown
    type_endurance_minutes: dict[str, float] = field(default_factory=lambda: dict(TYPE_ENDURANCE_MINUTES))

    # Trace merging
    dedup_time_secs: float = 1.0  # tighter than before: 2s was dropping legit helicopter hover samples
    dedup_deg: float = 0.001

    # Descent scoring windows (wall-clock seconds)
    descent_window_secs: float = 120.0  # rolling window for signal_lost descent check
    descent_preflare_start_secs: float = 180.0  # lookback start for confirmed-landing descent
    descent_preflare_end_secs: float = 30.0  # lookback end (excludes flare)

    # coord_stab thresholds (meters) for landing cluster stability. Real taxi
    # motion can cover hundreds of meters so spread alone is a bad signal;
    # per-sample jumps above 500m indicate receiver noise and nothing else.
    coord_stab_warn_jump_m: float = 200.0
    coord_stab_noise_jump_m: float = 500.0

    # Fragment stitching: merge signal_lost / dropped_on_approach followed by
    # found_mid_flight fragments that are close enough in time and space to
    # plausibly be the same continuous flight with a coverage hole in the
    # middle.
    stitch_max_gap_minutes: float = 90.0
    stitch_max_alt_delta_ft: float = 3000.0
    stitch_cruise_speed_kts: float = 300.0
    stitch_distance_slack: float = 1.2  # multiply the plausible distance by this for headwind margin

    # dropped_on_approach gating: require sustained descent in the last few
    # baro_rate samples before committing the classification.
    dropped_tail_window: int = 5
    dropped_tail_descent_min_count: int = 3
    dropped_tail_descent_rate_fpm: float = -200.0
    dropped_max_alt_ft: float = 5000.0
