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
    # Military tankers / strategic transports. Mission legs regularly
    # have multi-hour coverage gaps over restricted airspace, so the
    # stitch window needs a generous endurance number to merge what is
    # really one flight.
    "K35R": 720.0,  # KC-135R/T Stratotanker
    "K35E": 720.0,  # KC-135E
    "KC10": 840.0,  # KDC-10 Extender
    "KC30": 900.0,  # KC-30 / A330 MRTT
    "KC46": 780.0,  # KC-46 Pegasus
    "C17": 720.0,  # C-17 Globemaster III
    "C5M": 900.0,  # C-5M Super Galaxy
    "C130": 600.0,  # C-130 Hercules (broad family)
    "P8": 600.0,  # P-8 Poseidon
    "E3TF": 660.0,  # E-3 Sentry
    "E6": 900.0,  # E-6 Mercury
}


# Type codes that are helicopters. Used by hover detection (only emit
# max_hover_secs / hover_episodes on rotorcraft) and mission classification.
HELICOPTER_TYPES: frozenset[str] = frozenset(
    {
        "B407",
        "B429",
        "B429E",
        "EC30",
        "EC35",
        "EC45",
        "EC20",
        "EC25",
        "EC75",
        "S76",
        "S92",
        "H60",
        "UH60",
        "A109",
        "A119",
        "A139",
        "A169",
        "AS50",
        "AS55",
        "AS65",
        "B06",
        "B06T",
        "B212",
        "B412",
    }
)


# Callsign prefix -> mission_type lookup. First match wins during mission
# classification. Order of iteration not stable, so make sure prefixes are
# disjoint.
CALLSIGN_PREFIX_MISSIONS: dict[str, str] = {
    "N911": "ems_hems",
    "PHM": "offshore",
    "PHI": "offshore",
    "ERA": "offshore",
    "BHI": "offshore",  # Bristow
    "TWY": "exec_charter",
    "GLF": "exec_charter",
    "GS5": "exec_charter",  # round-4 §3.6: alt callsign for Solairus aircraft
    "LJ": "exec_charter",
    "NJE": "exec_charter",  # NetJets
    "EJA": "exec_charter",  # NetJets legacy
    "QE7": "exec_charter",  # round-4 §3.6: Qatari amiri 7-prefix
    "A7": "exec_charter",  # round-4 §3.6: Qatari nationality prefix on tail-number callsigns
    "SCH": "training",
    "SIK": "training",
}


# v4 (§3.6): owner_operator substring keywords for the offshore mission.
# Used by classify_mission as a fallback when the callsign prefix doesn't
# match. PHI Aviation, ERA, Bristow, Cougar, CHC are the major offshore
# helicopter operators in the dataset.
OFFSHORE_OPERATOR_KEYWORDS: tuple[str, ...] = (
    "PHI AVIATION",
    "PHI INC",
    "PETROLEUM HELICOPTER",
    "ERA HELICOPTER",
    "ERA AVIATION",
    "BRISTOW",
    "COUGAR HELI",
    "CHC HELI",
    "OMNI HELI",
    "ROTORCRAFT LEASING",
)


# v8 R4: type-specific service ceilings (feet). Used as a backstop after
# persistence filtering to cap max_altitude at physically plausible values.
# If max_altitude exceeds the ceiling by >10%, it is clamped.
TYPE_CEILINGS: dict[str, int] = {
    # Widebody (v14 R4a: reverted to 43,100; light-load ferry never exceeds
    # book ceiling per AP-validated data -- 39,675 ft peak with AP set)
    "B748": 43_100,
    # Helicopters
    "B407": 18_500,  # v16 R4d: lowered from 22,000; service ceiling 18,690 ft
    "S92": 15_000,
    "S76": 15_000,
    "H60": 19_000,
    "UH60": 19_000,
    "EC35": 20_000,
    "EC45": 20_000,
    # Business jets
    "GLF6": 51_000,
    "GLF5": 51_000,
    "GLF4": 45_000,
    "CL60": 41_000,
    "C56X": 45_000,
    "E55P": 45_000,
    "FA7X": 51_000,
    # Turboprop / piston
    "PC12": 30_000,
    "C172": 14_000,
    "C182": 18_100,
    "C208": 25_000,
    "TBM9": 31_000,
    # Military
    "K35R": 50_000,
    "KC46": 40_100,
    "C17": 45_000,
    "C5M": 34_000,
    "C130": 28_000,
    "E3TF": 42_000,
    "E6": 42_000,
    "MIL_FW": 45_000,
}


# v9 R3: type-specific max ground speed (knots). Used as a backstop after
# persistence filtering to cap max_gs_kt at physically plausible values.
# If max_gs_kt exceeds the cap by >10%, it is clamped. Same pattern as
# TYPE_CEILINGS for altitude. Values are Vne/Vmo + margin.
TYPE_MAX_GS: dict[str, int] = {
    # Helicopters
    "B407": 160,  # Vne 140 kt
    "B429": 170,
    "EC30": 155,
    "EC35": 160,
    "EC45": 160,
    "S76": 175,
    "S92": 175,  # Vne 165 kt
    "H60": 200,  # Vne 193 kt
    "UH60": 200,
    # Light piston / turboprop
    "C172": 160,
    "C182": 180,
    "C208": 190,  # Vmo 175 kt
    "PC12": 320,  # Vmo 270 kt; v12 R2a: raised from 290, a66ad3 at 319 kt confirmed legit (tailwind)
    "TBM9": 330,  # Vmo 266 kt but TAS at altitude
    # Business jets
    "GLF6": 590,  # Mmo 0.925, ~530 kt TAS at FL510
    "GLF5": 590,
    "GLF4": 560,
    "CL60": 530,
    "C56X": 480,
    "E55P": 460,
    "FA7X": 590,
    # Widebody
    "B748": 590,  # Mmo 0.92
    # Military
    "K35R": 550,  # Vmo ~490 kt, TAS at altitude ~530
    "KC46": 530,
    "C17": 530,
    "C5M": 500,
    "C130": 360,
    "E3TF": 500,
    "E6": 530,
    "MIL_FW": 600,
}


# Emergency squawk severity. Higher value = more severe. Used to pick the
# most-severe code when a flight sees multiple emergency squawks.
EMERGENCY_SQUAWK_PRIORITY: dict[str, int] = {
    "7500": 3,  # Hijack (most severe)
    "7700": 2,  # General emergency
    "7600": 1,  # Radio/comm failure
}


@dataclass
class Config:
    db_path: Path = Path("adsbtrack.db")
    credentials_path: Path = Path("credentials.json")
    airports_csv_url: str = "https://davidmegginson.github.io/ourairports-data/airports.csv"
    runways_csv_url: str = "https://davidmegginson.github.io/ourairports-data/runways.csv"
    navaids_csv_url: str = "https://davidmegginson.github.io/ourairports-data/navaids.csv"
    faa_registry_url: str = "https://registry.faa.gov/database/ReleasableAircraft.zip"
    faa_registry_cache_path: Path = Path(".cache/ReleasableAircraft.zip")
    mictronics_base_url: str = "https://raw.githubusercontent.com/Mictronics/readsb-protobuf/dev/webapp/src/db"
    mictronics_cache_dir: Path = Path(".cache/mictronics")
    hexdb_base_url: str = "https://hexdb.io"
    hexdb_rate_limit_per_min: int = 60
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

    # v5 (B7) minimum-viable-flight gate for non-confirmed flights. A
    # signal_lost / dropped / uncertain fragment shorter than this is
    # dropped as a sliver. Confirmed landings are never gated out.
    # v8 R5: raised from 2.0 to 3.0 min to tighten the tiny-flight guard.
    min_viable_flight_minutes: float = 3.0
    min_viable_flight_points: int = 10

    # v5 (B8) stationary broadcaster detector. A "flight" where the aircraft
    # never moved, never climbed above ramp height, and never accelerated
    # beyond taxi speed is a parked transponder, not a mission.
    stationary_path_km: float = 0.5
    stationary_max_alt_ft: float = 1000.0  # v6: raised from 500 to catch ramp at higher-elevation airports
    stationary_max_gs_kt: float = 15.0

    # v5 (D1) on-field threshold: origin_icao / destination_icao only get
    # populated when the takeoff/landing fix is within this distance of
    # the matched airport. Farther hits (still within the existing 10 km
    # find_nearest_airport gate) populate the diagnostic nearest_*_icao
    # fields so helicopter and offshore work doesn't get false-attributed
    # to a nearby civil airfield.
    airport_on_field_threshold_km: float = 2.0

    # Landing detection
    post_landing_window_secs: float = 60.0  # keep flight open to collect ground points
    post_landing_max_points: int = 5  # or this many ground points, whichever first
    baro_error_geom_threshold_ft: float = 300.0  # geom > this while baro=ground is a baro error

    # --- landing airport matching anchor ---
    # Final-N-minute window used by adsbtrack.landing_anchor.compute_landing_anchor
    # to find the altitude-minimum point for airport matching. 10 min is a
    # reasonable default for most approach profiles.
    landing_anchor_window_minutes: float = 10.0

    # --- ILS / runway alignment detection ---
    # Drives adsbtrack.ils_alignment: measure how long the aircraft's final
    # approach track was aligned with a runway centerline. An "aligned"
    # sample lies within ils_alignment_max_offset_m metres of the extended
    # centerline, below ils_alignment_max_ft_above_airport AGL, and within
    # the landing-anchor window. Consecutive aligned samples separated by
    # at most ils_alignment_split_gap_secs form an alignment segment; the
    # segment must run at least ils_alignment_min_duration_secs to count.
    # The landing_confidence bonus ladder awards +_bonus_short when the
    # best segment is >= _bonus_short_secs and +_bonus_long when it is
    # >= _bonus_long_secs (bonuses are not additive; long supersedes short).
    ils_alignment_max_offset_m: float = 100.0
    ils_alignment_min_duration_secs: float = 30.0
    ils_alignment_split_gap_secs: float = 20.0
    ils_alignment_max_ft_above_airport: float = 5000.0
    ils_alignment_bonus_short_secs: float = 30.0
    ils_alignment_bonus_long_secs: float = 60.0
    ils_alignment_bonus_short: float = 0.15
    ils_alignment_bonus_long: float = 0.25

    # --- Takeoff runway detector (see adsbtrack/takeoff_runway.py) ---
    # A trapezoid polygon is built per runway end: narrow `little_base_m`
    # base at the threshold, extending `zone_length_m` outward along the
    # departure heading, opening symmetrically by `opening_deg`. A flight's
    # first 600 s of low-altitude climb (below airport_elev +
    # `max_ft_above_airport`) is tested for intersection with each polygon;
    # the runway whose polygon the flight was inside longest wins, subject
    # to a minimum ground-speed peak (`min_gs_kt_default` for commercial,
    # scaled to `min_gs_kt_low` for rotorcraft and type codes listed in
    # `takeoff_low_gs_types`) and a minimum vertical rate.
    takeoff_runway_zone_length_m: float = 6000.0
    takeoff_runway_little_base_m: float = 50.0
    takeoff_runway_opening_deg: float = 5.0
    takeoff_runway_max_ft_above_airport: float = 2000.0
    takeoff_runway_min_gs_kt_default: float = 140.0
    takeoff_runway_min_gs_kt_low: float = 60.0
    takeoff_runway_min_vert_rate_fpm: float = 256.0
    takeoff_low_gs_types: tuple[str, ...] = (
        "C150",
        "C152",
        "C162",
        "C172",
        "C177",
        "C182",
        "DA20",
        "DA40",
        "PA28",
        "PA32",
        "SR20",
        "SR22",
        "BE33",
        "BE35",
        "BE36",
    )

    # --- Navaid alignment (see adsbtrack/navaid_alignment.py) ---
    # For each navaid within navaid_max_distance_nm of any flight point, the
    # algorithm keeps points whose bearing-to-navaid delta from track is under
    # navaid_alignment_tolerance_deg. Consecutive kept points separated by at
    # most navaid_split_gap_secs form a segment. A segment qualifies when it
    # runs at least navaid_min_duration_secs AND the flight's closest approach
    # to that navaid during the segment is under navaid_near_pass_max_nm.
    navaid_alignment_tolerance_deg: float = 1.0
    navaid_max_distance_nm: float = 500.0
    navaid_split_gap_secs: float = 120.0
    navaid_min_duration_secs: float = 30.0
    navaid_near_pass_max_nm: float = 80.0
    # Bounding-box buffer (nm) applied when prefiltering navaids per flight.
    navaid_bbox_buffer_nm: float = 50.0

    # Endurance
    max_endurance_minutes: float = 240.0  # fallback when type_code is unknown
    type_endurance_minutes: dict[str, float] = field(default_factory=lambda: dict(TYPE_ENDURANCE_MINUTES))

    # v5 (B5, B6) persistence-filtered peaks. A candidate max_altitude or
    # max_gs_kt only sets the new peak when it was held for at least
    # `min_samples` points across a rolling wall-clock window of
    # `window_secs`. Guards against single-sample baro or GS spikes
    # pegging the raw max() to garbage (B5 found a B748 at 125,898 ft
    # and a B407 at 197 kt, both from one-point glitches).
    alt_persistence_window_secs: float = 30.0
    alt_persistence_min_samples: int = 3  # v6: lowered from 5 so S92 (15s spacing) activates
    gs_persistence_window_secs: float = 30.0
    gs_persistence_min_samples: int = 3

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
    # Per-type stitch window: effective_gap_minutes =
    #   max(stitch_max_gap_minutes, endurance_for(type) * stitch_endurance_ratio)
    # Long-endurance types (tankers, heavy transports) can legitimately have
    # coverage gaps that exceed the static 90-minute default during one
    # operational mission; this ratio widens the window for those types
    # without changing behavior for light GA.
    stitch_endurance_ratio: float = 0.4

    # dropped_on_approach gating: require sustained descent in the last few
    # baro_rate samples before committing the classification.
    dropped_tail_window: int = 5
    dropped_tail_descent_min_count: int = 3
    dropped_tail_descent_rate_fpm: float = -200.0
    dropped_max_alt_ft: float = 5000.0

    # --- v3 feature thresholds ---

    # Path metrics
    path_max_segment_secs: float = 60.0  # skip segments longer than this (coverage holes)

    # Phase of flight attribution
    phase_climb_fpm: float = 250.0  # >|this| fpm => climb or descent
    phase_cruise_alt_ratio: float = 0.70  # cruise = level AND alt > ratio * max_altitude
    phase_short_flight_min_secs: float = 120.0  # below this, cruise fields return NULL
    phase_short_flight_min_alt: float = 500.0  # below this max altitude, cruise fields NULL

    # Peak rate rolling window. v4 fix (§1.7): bumped from 30s/3pt to 60s/4pt
    # to suppress 1-2 point baro spikes that pegged peaks at impossible values
    # (e.g. PC12 at -21,312 fpm, GLF6 at +16,448 fpm).
    peak_rate_window_secs: float = 60.0
    peak_rate_min_samples: int = 4
    peak_rate_min_span_secs: float = 30.0

    # Hover detection (helicopter only)
    hover_gs_threshold_kts: float = 5.0
    hover_baro_rate_max_fpm: float = 100.0  # reject climb/descent samples pretending to hover
    hover_min_duration_secs: float = 20.0

    # Go-around detection
    go_around_lookback_secs: float = 600.0
    go_around_min_rebound_ft: float = 400.0
    go_around_local_extremum_sep_ft: float = 50.0
    go_around_local_extremum_window_secs: float = 30.0

    # Heading aggregation (takeoff and landing)
    heading_window_secs: float = 60.0
    heading_min_gs_kts: float = 40.0

    # Night detection: civil twilight cutoff in degrees
    night_sun_altitude_deg: float = -6.0
    # Quantization for LRU cache key on solar calls (coarser = more cache hits)
    solar_cache_lat_lon_quant: float = 0.1  # degrees
    solar_cache_ts_bucket_secs: float = 300.0  # 5 minutes
    night_flight_ratio_threshold: float = 0.5

    # Destination inference (for signal_lost / dropped_on_approach)
    prob_dest_max_distance_km: float = 46.3  # 25 nm
    prob_dest_search_delta: float = 0.5  # bbox degrees
    prob_dest_alt_weight: float = 0.4
    prob_dest_prox_weight: float = 0.4
    prob_dest_descent_weight: float = 0.2

    # approach_alts deque cap for go-around detection (samples, ~5s spacing)
    approach_alts_maxlen: int = 240

    # Helicopter types (mirror of HELICOPTER_TYPES for runtime access)
    helicopter_types: frozenset[str] = field(default_factory=lambda: HELICOPTER_TYPES)
    callsign_prefix_missions: dict[str, str] = field(default_factory=lambda: dict(CALLSIGN_PREFIX_MISSIONS))
    emergency_squawk_priority: dict[str, int] = field(default_factory=lambda: dict(EMERGENCY_SQUAWK_PRIORITY))
    offshore_operator_keywords: tuple[str, ...] = field(default_factory=lambda: tuple(OFFSHORE_OPERATOR_KEYWORDS))
