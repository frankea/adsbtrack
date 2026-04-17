import contextlib
import json
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from .models import Flight

SCHEMA = """
CREATE TABLE IF NOT EXISTS trace_days (
    id INTEGER PRIMARY KEY,
    icao TEXT NOT NULL,
    date TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'adsbx',
    registration TEXT,
    type_code TEXT,
    description TEXT,
    owner_operator TEXT,
    year TEXT,
    timestamp REAL NOT NULL,
    trace_json TEXT NOT NULL,
    point_count INTEGER NOT NULL,
    fetched_at TEXT NOT NULL,
    UNIQUE(icao, date, source)
);

CREATE TABLE IF NOT EXISTS fetch_log (
    id INTEGER PRIMARY KEY,
    icao TEXT NOT NULL,
    date TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'adsbx',
    status INTEGER NOT NULL,
    fetched_at TEXT NOT NULL,
    UNIQUE(icao, date, source)
);

CREATE TABLE IF NOT EXISTS flights (
    id INTEGER PRIMARY KEY,
    icao TEXT NOT NULL,
    takeoff_time TEXT NOT NULL,
    takeoff_lat REAL NOT NULL,
    takeoff_lon REAL NOT NULL,
    takeoff_date TEXT NOT NULL,
    landing_time TEXT,
    landing_lat REAL,
    landing_lon REAL,
    landing_date TEXT,
    origin_icao TEXT,
    origin_name TEXT,
    origin_distance_km REAL,
    destination_icao TEXT,
    destination_name TEXT,
    destination_distance_km REAL,
    duration_minutes REAL,
    callsign TEXT,
    landing_type TEXT DEFAULT 'unknown',
    takeoff_type TEXT DEFAULT 'unknown',
    takeoff_confidence REAL,
    landing_confidence REAL,
    data_points INTEGER,
    sources TEXT,
    max_altitude INTEGER,
    ground_points_at_landing INTEGER,
    ground_points_at_takeoff INTEGER,
    baro_error_points INTEGER,
    last_seen_lat REAL,
    last_seen_lon REAL,
    last_seen_alt_ft INTEGER,
    last_seen_time TEXT,
    squawk_first TEXT,
    squawk_last TEXT,
    squawk_changes INTEGER,
    emergency_squawk TEXT,
    vfr_flight INTEGER,
    mission_type TEXT,
    category_do260 TEXT,
    autopilot_target_alt_ft INTEGER,
    emergency_flag TEXT,
    path_length_km REAL,
    max_distance_km REAL,
    loiter_ratio REAL,
    path_efficiency REAL,
    max_hover_secs INTEGER,
    hover_episodes INTEGER,
    go_around_count INTEGER,
    takeoff_heading_deg REAL,
    landing_heading_deg REAL,
    climb_secs INTEGER,
    cruise_secs INTEGER,
    descent_secs INTEGER,
    level_secs INTEGER,
    cruise_alt_ft INTEGER,
    cruise_gs_kt INTEGER,
    cruise_detected INTEGER,
    heavy_signal_gap INTEGER,
    peak_climb_fpm INTEGER,
    peak_descent_fpm INTEGER,
    takeoff_is_night INTEGER,
    landing_is_night INTEGER,
    night_flight INTEGER,
    callsigns TEXT,
    callsign_changes INTEGER,
    callsign_count INTEGER,
    probable_destination_icao TEXT,
    probable_destination_distance_km REAL,
    probable_destination_confidence REAL,
    active_minutes REAL,
    signal_gap_secs INTEGER,
    signal_gap_count INTEGER,
    fragments_stitched INTEGER,
    nearest_origin_icao TEXT,
    nearest_origin_distance_km REAL,
    nearest_destination_icao TEXT,
    nearest_destination_distance_km REAL,
    max_gs_kt INTEGER,
    turnaround_minutes REAL,
    origin_helipad_id INTEGER,
    destination_helipad_id INTEGER,
    type_override TEXT,
    turnaround_category TEXT,
    is_first_observed_flight INTEGER,
    is_last_observed_flight INTEGER,
    mlat_pct REAL,
    tisb_pct REAL,
    adsb_pct REAL,
    landing_anchor_method TEXT,
    aligned_runway TEXT,
    aligned_seconds REAL,
    aligned_min_offset_m REAL,
    takeoff_runway TEXT,
    had_go_around INTEGER,
    pattern_cycles INTEGER,
    squawks_observed TEXT,
    had_emergency INTEGER,
    primary_squawk TEXT,
    UNIQUE(icao, takeoff_time)
);

CREATE TABLE IF NOT EXISTS aircraft_registry (
    icao TEXT PRIMARY KEY,
    registration TEXT,
    type_code TEXT,
    description TEXT,
    owner_operator TEXT,
    year TEXT,
    last_updated TEXT,
    metadata_drift_count INTEGER DEFAULT 0,
    metadata_drift_values TEXT,
    confirmation_rate REAL,
    signal_quality_tier TEXT
);

CREATE TABLE IF NOT EXISTS aircraft_stats (
    icao TEXT PRIMARY KEY,
    registration TEXT,
    type_code TEXT,
    first_seen TEXT,
    last_seen TEXT,
    total_flights INTEGER,
    confirmed_flights INTEGER,
    total_hours REAL,
    total_cycles INTEGER,
    distinct_airports INTEGER,
    distinct_callsigns INTEGER,
    avg_flight_minutes REAL,
    busiest_day_date TEXT,
    busiest_day_count INTEGER,
    home_base_icao TEXT,
    home_base_share REAL,
    home_base_uncertain INTEGER,
    second_base_icao TEXT,
    second_base_share REAL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS airports (
    ident TEXT PRIMARY KEY,
    type TEXT,
    name TEXT NOT NULL,
    latitude_deg REAL NOT NULL,
    longitude_deg REAL NOT NULL,
    elevation_ft INTEGER,
    iso_country TEXT,
    iso_region TEXT,
    municipality TEXT,
    iata_code TEXT
);

CREATE TABLE IF NOT EXISTS helipads (
    helipad_id INTEGER PRIMARY KEY,
    centroid_lat REAL NOT NULL,
    centroid_lon REAL NOT NULL,
    landing_count INTEGER DEFAULT 0,
    first_seen TEXT,
    last_seen TEXT,
    name_hint TEXT
);

CREATE TABLE IF NOT EXISTS runways (
    airport_ident TEXT NOT NULL,
    runway_name TEXT NOT NULL,
    latitude_deg REAL NOT NULL,
    longitude_deg REAL NOT NULL,
    elevation_ft INTEGER,
    heading_deg_true REAL,
    length_ft INTEGER,
    width_ft INTEGER,
    surface TEXT,
    closed INTEGER DEFAULT 0,
    displaced_threshold_ft INTEGER,
    PRIMARY KEY (airport_ident, runway_name)
);

CREATE TABLE IF NOT EXISTS navaids (
    ident TEXT NOT NULL,
    name TEXT,
    type TEXT,
    latitude_deg REAL NOT NULL,
    longitude_deg REAL NOT NULL,
    elevation_ft INTEGER,
    frequency_khz INTEGER,
    iso_country TEXT,
    PRIMARY KEY (ident, latitude_deg, longitude_deg)
);

CREATE TABLE IF NOT EXISTS faa_registry (
    mode_s_code_hex TEXT PRIMARY KEY,
    n_number TEXT,
    serial_number TEXT,
    mfr_mdl_code TEXT,
    eng_mfr_mdl TEXT,
    year_mfr TEXT,
    type_registrant TEXT,
    name TEXT,
    street TEXT,
    street2 TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    region TEXT,
    county TEXT,
    country TEXT,
    last_action_date TEXT,
    cert_issue_date TEXT,
    certification TEXT,
    type_aircraft TEXT,
    type_engine TEXT,
    status_code TEXT,
    mode_s_code TEXT,
    fract_owner TEXT,
    air_worth_date TEXT,
    expiration_date TEXT,
    unique_id TEXT,
    kit_mfr TEXT,
    kit_model TEXT
);

CREATE TABLE IF NOT EXISTS faa_deregistered (
    mode_s_code_hex TEXT PRIMARY KEY,
    n_number TEXT,
    serial_number TEXT,
    mfr_mdl_code TEXT,
    eng_mfr_mdl TEXT,
    year_mfr TEXT,
    type_registrant TEXT,
    name TEXT,
    street TEXT,
    street2 TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    region TEXT,
    county TEXT,
    country TEXT,
    last_action_date TEXT,
    cert_issue_date TEXT,
    certification TEXT,
    type_aircraft TEXT,
    type_engine TEXT,
    status_code TEXT,
    mode_s_code TEXT,
    fract_owner TEXT,
    air_worth_date TEXT,
    expiration_date TEXT,
    unique_id TEXT,
    kit_mfr TEXT,
    kit_model TEXT
);

CREATE TABLE IF NOT EXISTS faa_aircraft_ref (
    code TEXT PRIMARY KEY,
    mfr TEXT,
    model TEXT,
    type_acft TEXT,
    type_eng TEXT
);

CREATE TABLE IF NOT EXISTS acars_flights (
    flight_id INTEGER PRIMARY KEY,
    airframe_id INTEGER NOT NULL,
    icao TEXT NOT NULL,
    registration TEXT,
    flight_number TEXT,
    flight_iata TEXT,
    flight_icao TEXT,
    status TEXT,
    departing_airport TEXT,
    destination_airport TEXT,
    departure_time_scheduled TEXT,
    departure_time_actual TEXT,
    arrival_time_scheduled TEXT,
    arrival_time_actual TEXT,
    first_seen TEXT,
    last_seen TEXT,
    message_count INTEGER,
    fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS acars_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    airframes_id INTEGER NOT NULL UNIQUE,
    uuid TEXT,
    flight_id INTEGER,
    icao TEXT NOT NULL,
    registration TEXT,
    timestamp TEXT NOT NULL,
    source_type TEXT,
    link_direction TEXT,
    from_hex TEXT,
    to_hex TEXT,
    frequency REAL,
    level REAL,
    channel TEXT,
    mode TEXT,
    label TEXT,
    block_id TEXT,
    message_number TEXT,
    ack TEXT,
    flight_number TEXT,
    text TEXT,
    data TEXT,
    latitude REAL,
    longitude REAL,
    altitude REAL,
    departing_airport TEXT,
    destination_airport TEXT,
    fetched_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_airports_lat ON airports(latitude_deg);
CREATE INDEX IF NOT EXISTS idx_airports_lon ON airports(longitude_deg);
CREATE INDEX IF NOT EXISTS idx_runways_airport_ident ON runways(airport_ident);
CREATE INDEX IF NOT EXISTS idx_runways_latlon ON runways(latitude_deg, longitude_deg);
CREATE INDEX IF NOT EXISTS idx_navaids_latlon ON navaids(latitude_deg, longitude_deg);
CREATE INDEX IF NOT EXISTS idx_navaids_ident ON navaids(ident);
CREATE INDEX IF NOT EXISTS idx_flights_icao_time ON flights(icao, takeoff_time);
CREATE INDEX IF NOT EXISTS idx_trace_days_icao_date ON trace_days(icao, date);
CREATE INDEX IF NOT EXISTS idx_faa_registry_n_number ON faa_registry(n_number);
CREATE INDEX IF NOT EXISTS idx_faa_registry_name ON faa_registry(name);
CREATE INDEX IF NOT EXISTS idx_faa_registry_city_state ON faa_registry(city, state);
CREATE INDEX IF NOT EXISTS idx_faa_registry_street ON faa_registry(street);
CREATE INDEX IF NOT EXISTS idx_faa_deregistered_n_number ON faa_deregistered(n_number);
CREATE INDEX IF NOT EXISTS idx_acars_messages_icao_ts ON acars_messages(icao, timestamp);
CREATE INDEX IF NOT EXISTS idx_acars_messages_flight ON acars_messages(flight_id);
CREATE INDEX IF NOT EXISTS idx_acars_flights_icao ON acars_flights(icao);

-- Hex cross-reference: unified icao -> identity lookup merged from FAA
-- registry, Mictronics, and hexdb.io. The source column records which
-- provider supplied the row (so users can see where data came from and
-- re-enrich preferentially from richer sources later).
CREATE TABLE IF NOT EXISTS hex_crossref (
    icao TEXT PRIMARY KEY,
    registration TEXT,
    type_code TEXT,
    type_description TEXT,
    operator TEXT,
    source TEXT,
    is_military INTEGER DEFAULT 0,
    mil_country TEXT,
    mil_branch TEXT,
    last_updated TEXT
);

-- Static military hex allocation ranges. Seed rows are inserted on init
-- from adsbtrack.mil_hex._SEED_RANGES. Users can extend the table with
-- their own rows (INSERT OR REPLACE composes cleanly with the seeder).
CREATE TABLE IF NOT EXISTS mil_hex_ranges (
    range_start TEXT NOT NULL,
    range_end TEXT NOT NULL,
    country TEXT,
    branch TEXT,
    notes TEXT,
    PRIMARY KEY (range_start, range_end)
);

CREATE INDEX IF NOT EXISTS idx_hex_crossref_registration ON hex_crossref(registration);
CREATE INDEX IF NOT EXISTS idx_hex_crossref_military ON hex_crossref(is_military);
CREATE INDEX IF NOT EXISTS idx_mil_hex_ranges_start ON mil_hex_ranges(range_start);

CREATE VIEW IF NOT EXISTS flights_with_type AS
  SELECT f.*, COALESCE(f.type_override, ar.type_code) AS effective_type
  FROM flights f
  LEFT JOIN aircraft_registry ar ON f.icao = ar.icao;
"""

# Individual CREATE statements for safe execution (no implicit commit)
_SCHEMA_STATEMENTS = [stmt.strip() for stmt in SCHEMA.split(";") if stmt.strip()]


def _needs_source_migration(conn: sqlite3.Connection) -> bool:
    """Check if the source column is missing from trace_days."""
    cols = conn.execute("PRAGMA table_info(trace_days)").fetchall()
    if not cols:
        # Table doesn't exist yet (fresh DB) -- no migration needed
        return False
    col_names = {row[1] for row in cols}
    return "source" not in col_names


def _migrate_add_source(conn: sqlite3.Connection, db_path: Path):
    """Add source column via rename-copy-drop with backup and explicit transaction."""
    print("Migrating database schema...")

    # Back up the database before migrating
    backup_path = db_path.with_suffix(".db.bak")
    shutil.copy2(db_path, backup_path)
    print(f"  Backup saved to {backup_path}")

    try:
        conn.execute("BEGIN")

        # trace_days migration
        conn.execute("ALTER TABLE trace_days RENAME TO trace_days_old")
        conn.execute("""
            CREATE TABLE trace_days (
                id INTEGER PRIMARY KEY,
                icao TEXT NOT NULL,
                date TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'adsbx',
                registration TEXT,
                type_code TEXT,
                description TEXT,
                owner_operator TEXT,
                year TEXT,
                timestamp REAL NOT NULL,
                trace_json TEXT NOT NULL,
                point_count INTEGER NOT NULL,
                fetched_at TEXT NOT NULL,
                UNIQUE(icao, date, source)
            )
        """)
        conn.execute("""
            INSERT INTO trace_days
                (icao, date, source, registration, type_code, description,
                 owner_operator, year, timestamp, trace_json, point_count, fetched_at)
            SELECT icao, date, 'adsbx', registration, type_code, description,
                   owner_operator, year, timestamp, trace_json, point_count, fetched_at
            FROM trace_days_old
        """)
        conn.execute("DROP TABLE trace_days_old")

        # fetch_log migration
        conn.execute("ALTER TABLE fetch_log RENAME TO fetch_log_old")
        conn.execute("""
            CREATE TABLE fetch_log (
                id INTEGER PRIMARY KEY,
                icao TEXT NOT NULL,
                date TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'adsbx',
                status INTEGER NOT NULL,
                fetched_at TEXT NOT NULL,
                UNIQUE(icao, date, source)
            )
        """)
        conn.execute("""
            INSERT INTO fetch_log
                (icao, date, source, status, fetched_at)
            SELECT icao, date, 'adsbx', status, fetched_at
            FROM fetch_log_old
        """)
        conn.execute("DROP TABLE fetch_log_old")

        # Recreate indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trace_days_icao_date ON trace_days(icao, date)")

        conn.execute("COMMIT")
        print("  Migration complete.")
    except Exception:
        conn.execute("ROLLBACK")
        print("  Migration failed, rolling back. Backup is at:", backup_path)
        raise


def _migrate_add_flight_columns(conn: sqlite3.Connection):
    """Add all flight metadata columns. Idempotent - safe to run every
    startup. Every ALTER TABLE ADD COLUMN is wrapped in suppress so the
    "duplicate column name" error is swallowed cheaply. The list below is
    the full history of column additions through v3."""
    new_columns = [
        # v2 quality scoring
        ("landing_type", "TEXT DEFAULT 'unknown'"),
        ("takeoff_confidence", "REAL"),
        ("landing_confidence", "REAL"),
        ("data_points", "INTEGER"),
        ("sources", "TEXT"),
        ("max_altitude", "INTEGER"),
        ("ground_points_at_landing", "INTEGER"),
        ("takeoff_type", "TEXT DEFAULT 'unknown'"),
        ("ground_points_at_takeoff", "INTEGER"),
        ("baro_error_points", "INTEGER"),
        ("last_seen_lat", "REAL"),
        ("last_seen_lon", "REAL"),
        ("last_seen_alt_ft", "INTEGER"),
        ("last_seen_time", "TEXT"),
        # v3 feature expansion
        ("squawk_first", "TEXT"),
        ("squawk_last", "TEXT"),
        ("squawk_changes", "INTEGER"),
        ("emergency_squawk", "TEXT"),
        ("vfr_flight", "INTEGER"),
        ("mission_type", "TEXT"),
        ("category_do260", "TEXT"),
        ("autopilot_target_alt_ft", "INTEGER"),
        ("emergency_flag", "TEXT"),
        ("path_length_km", "REAL"),
        ("max_distance_km", "REAL"),
        ("loiter_ratio", "REAL"),
        ("path_efficiency", "REAL"),
        ("max_hover_secs", "INTEGER"),
        ("hover_episodes", "INTEGER"),
        ("go_around_count", "INTEGER"),
        ("takeoff_heading_deg", "REAL"),
        ("landing_heading_deg", "REAL"),
        ("climb_secs", "INTEGER"),
        ("cruise_secs", "INTEGER"),
        ("descent_secs", "INTEGER"),
        ("level_secs", "INTEGER"),
        ("cruise_alt_ft", "INTEGER"),
        ("cruise_gs_kt", "INTEGER"),
        ("peak_climb_fpm", "INTEGER"),
        ("peak_descent_fpm", "INTEGER"),
        ("takeoff_is_night", "INTEGER"),
        ("landing_is_night", "INTEGER"),
        ("night_flight", "INTEGER"),
        ("callsigns", "TEXT"),
        ("callsign_changes", "INTEGER"),
        ("callsign_count", "INTEGER"),
        ("probable_destination_icao", "TEXT"),
        ("probable_destination_distance_km", "REAL"),
        ("probable_destination_confidence", "REAL"),
        # v5 round 6: signal budget, fragments, on-field airport split,
        # persistence-filtered peak ground speed
        ("active_minutes", "REAL"),
        ("signal_gap_secs", "INTEGER"),
        ("signal_gap_count", "INTEGER"),
        ("fragments_stitched", "INTEGER"),
        ("nearest_origin_icao", "TEXT"),
        ("nearest_origin_distance_km", "REAL"),
        ("nearest_destination_icao", "TEXT"),
        ("nearest_destination_distance_km", "REAL"),
        ("max_gs_kt", "INTEGER"),
        ("turnaround_minutes", "REAL"),
        ("origin_helipad_id", "INTEGER"),
        ("destination_helipad_id", "INTEGER"),
        ("type_override", "TEXT"),
        ("turnaround_category", "TEXT"),
        ("is_first_observed_flight", "INTEGER"),
        ("is_last_observed_flight", "INTEGER"),
        ("cruise_detected", "INTEGER"),
        ("heavy_signal_gap", "INTEGER"),
        # Position source mix (readsb type/src field)
        ("mlat_pct", "REAL"),
        ("tisb_pct", "REAL"),
        ("adsb_pct", "REAL"),
        # ACARS OOOI timestamps (ISO 8601, UTC) populated from airframes.io
        # messages with labels 14 / 44 / 4T that fall in the flight window.
        ("acars_out", "TEXT"),
        ("acars_off", "TEXT"),
        ("acars_on", "TEXT"),
        ("acars_in", "TEXT"),
        # Landing airport-matching anchor: "alt_min" or "last_point".
        # Populated by parser.py using adsbtrack.landing_anchor.compute_landing_anchor.
        ("landing_anchor_method", "TEXT"),
        # ILS / runway alignment fields, populated by adsbtrack.ils_alignment.
        ("aligned_runway", "TEXT"),
        ("aligned_seconds", "REAL"),
        ("aligned_min_offset_m", "REAL"),
        ("takeoff_runway", "TEXT"),
        # Go-around + pattern-work counts (see docs/features.md)
        ("had_go_around", "INTEGER"),
        ("pattern_cycles", "INTEGER"),
        # Squawk signals (docs/features.md)
        ("squawks_observed", "TEXT"),
        ("had_emergency", "INTEGER"),
        ("primary_squawk", "TEXT"),
    ]
    for col_name, col_type in new_columns:
        # "column already exists" is expected when re-running the migration.
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(f"ALTER TABLE flights ADD COLUMN {col_name} {col_type}")


def _flights_table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flights'").fetchone()
    return row is not None


def _migrate_add_v4_columns(conn: sqlite3.Connection):
    """Add v4 columns to aircraft_registry and aircraft_stats. Idempotent.

    Safe to run on every startup; duplicate-column errors are suppressed.
    """
    registry_columns = [
        ("confirmation_rate", "REAL"),
        ("signal_quality_tier", "TEXT"),
        # airframes.io numeric airframe id, cached so we skip the icao lookup
        # call on subsequent ACARS fetches for the same aircraft.
        ("airframes_id", "INTEGER"),
    ]
    for col_name, col_type in registry_columns:
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(f"ALTER TABLE aircraft_registry ADD COLUMN {col_name} {col_type}")

    stats_columns = [
        ("home_base_icao", "TEXT"),
        ("home_base_share", "REAL"),
        ("home_base_uncertain", "INTEGER"),
        ("second_base_icao", "TEXT"),
        ("second_base_share", "REAL"),
    ]
    for col_name, col_type in stats_columns:
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(f"ALTER TABLE aircraft_stats ADD COLUMN {col_name} {col_type}")


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        # timeout is the builtin sqlite3 driver's lock wait; we also set
        # PRAGMA busy_timeout below for safety. WAL mode allows multiple
        # concurrent readers plus a single writer without blocking, which
        # makes running multiple `adsbtrack fetch` sessions in parallel
        # terminals work without "database is locked" errors.
        self.conn = sqlite3.connect(db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.conn.execute("PRAGMA busy_timeout = 30000")
        # Migrate existing databases that lack the source column
        if _needs_source_migration(self.conn):
            _migrate_add_source(self.conn, db_path)
        # Run the full column migration every startup. Each ALTER TABLE is
        # wrapped in suppress so duplicate-column errors are ignored cheaply.
        # This is much simpler than maintaining per-column sentinel checks.
        if _flights_table_exists(self.conn):
            _migrate_add_flight_columns(self.conn)
        for stmt in _SCHEMA_STATEMENTS:
            self.conn.execute(stmt)
        # Also run the flight-column migration after CREATE TABLE so brand-new
        # databases pick up any columns that might have been added since the
        # schema string was last written (cheap, idempotent).
        _migrate_add_flight_columns(self.conn)
        # v4: aircraft_registry / aircraft_stats column additions
        _migrate_add_v4_columns(self.conn)
        # Seed the curated military-hex allocations. Idempotent -- repeated
        # init calls just upsert the same rows. Lazy import avoids a
        # module-level cycle (mil_hex needs Database for TYPE_CHECKING).
        from .mil_hex import seed_mil_hex_ranges

        seed_mil_hex_ranges(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        self.conn.close()

    def close(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    # -- trace_days --

    def insert_trace_day(self, icao: str, date: str, data: dict, source: str = "adsbx"):
        self.conn.execute(
            """INSERT OR REPLACE INTO trace_days
               (icao, date, source, registration, type_code, description, owner_operator,
                year, timestamp, trace_json, point_count, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                icao,
                date,
                source,
                data.get("r"),
                data.get("t"),
                data.get("desc"),
                data.get("ownOp"),
                data.get("year"),
                data["timestamp"],
                json.dumps(data["trace"]),
                len(data["trace"]),
                datetime.now(UTC).isoformat(),
            ),
        )

    def get_trace_days(self, icao: str) -> list[sqlite3.Row]:
        """Return ALL source rows -- parser merges them by date."""
        return self.conn.execute("SELECT * FROM trace_days WHERE icao = ? ORDER BY date", (icao,)).fetchall()

    # -- fetch_log --

    def insert_fetch_log(self, icao: str, date: str, status: int, source: str = "adsbx"):
        self.conn.execute(
            """INSERT OR REPLACE INTO fetch_log (icao, date, source, status, fetched_at)
               VALUES (?, ?, ?, ?, ?)""",
            (icao, date, source, status, datetime.now(UTC).isoformat()),
        )

    def get_fetched_dates(self, icao: str, source: str = "adsbx") -> set[str]:
        rows = self.conn.execute(
            """SELECT date FROM fetch_log WHERE icao = ? AND source = ?
               UNION
               SELECT date FROM trace_days WHERE icao = ? AND source = ?""",
            (icao, source, icao, source),
        ).fetchall()
        return {row["date"] for row in rows}

    # -- flights --

    def clear_flights(self, icao: str):
        self.conn.execute("DELETE FROM flights WHERE icao = ?", (icao,))

    def insert_flight(self, flight: Flight):
        # B3 guard: refuse to persist a flight whose landing_time predates
        # its takeoff_time. Cross-midnight stitch bugs and clock glitches
        # were producing these; better to drop and log than to poison
        # downstream rollups. The parser already guards before calling
        # insert_flight, but this is the last-line defense at the boundary.
        if flight.landing_time is not None and flight.landing_time <= flight.takeoff_time:
            print(
                f"  [dim yellow]skipping flight {flight.icao} {flight.takeoff_time.isoformat()}: "
                f"landing_time {flight.landing_time.isoformat()} <= takeoff_time[/]"
            )
            return
        self.conn.execute(
            """INSERT OR REPLACE INTO flights
               (icao, takeoff_time, takeoff_lat, takeoff_lon, takeoff_date,
                landing_time, landing_lat, landing_lon, landing_date,
                origin_icao, origin_name, origin_distance_km,
                destination_icao, destination_name, destination_distance_km,
                duration_minutes, callsign,
                landing_type, takeoff_type, takeoff_confidence, landing_confidence,
                data_points, sources, max_altitude,
                ground_points_at_landing, ground_points_at_takeoff, baro_error_points,
                last_seen_lat, last_seen_lon, last_seen_alt_ft, last_seen_time,
                squawk_first, squawk_last, squawk_changes, emergency_squawk, vfr_flight,
                mission_type, category_do260, autopilot_target_alt_ft, emergency_flag,
                path_length_km, max_distance_km, loiter_ratio, path_efficiency,
                max_hover_secs, hover_episodes, go_around_count,
                takeoff_heading_deg, landing_heading_deg,
                climb_secs, cruise_secs, descent_secs, level_secs,
                cruise_alt_ft, cruise_gs_kt, cruise_detected, heavy_signal_gap,
                peak_climb_fpm, peak_descent_fpm,
                takeoff_is_night, landing_is_night, night_flight,
                callsigns, callsign_changes, callsign_count,
                probable_destination_icao, probable_destination_distance_km, probable_destination_confidence,
                active_minutes, signal_gap_secs, signal_gap_count, fragments_stitched,
                nearest_origin_icao, nearest_origin_distance_km,
                nearest_destination_icao, nearest_destination_distance_km,
                max_gs_kt, turnaround_minutes,
                origin_helipad_id, destination_helipad_id,
                type_override,
                turnaround_category, is_first_observed_flight, is_last_observed_flight,
                mlat_pct, tisb_pct, adsb_pct,
                acars_out, acars_off, acars_on, acars_in, landing_anchor_method,
                aligned_runway, aligned_seconds, aligned_min_offset_m, takeoff_runway,
                had_go_around, pattern_cycles, squawks_observed, had_emergency, primary_squawk)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?,
                       ?, ?, ?, ?,
                       ?, ?, ?, ?,
                       ?, ?, ?, ?,
                       ?, ?,
                       ?, ?, ?, ?,
                       ?, ?, ?,
                       ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?, ?,
                       ?, ?,
                       ?, ?,
                       ?, ?, ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                flight.icao,
                flight.takeoff_time.isoformat(),
                flight.takeoff_lat,
                flight.takeoff_lon,
                flight.takeoff_date,
                flight.landing_time.isoformat() if flight.landing_time else None,
                flight.landing_lat,
                flight.landing_lon,
                flight.landing_date,
                flight.origin_icao,
                flight.origin_name,
                flight.origin_distance_km,
                flight.destination_icao,
                flight.destination_name,
                flight.destination_distance_km,
                flight.duration_minutes,
                flight.callsign,
                flight.landing_type,
                flight.takeoff_type,
                flight.takeoff_confidence,
                flight.landing_confidence,
                flight.data_points,
                flight.sources,
                flight.max_altitude,
                flight.ground_points_at_landing,
                flight.ground_points_at_takeoff,
                flight.baro_error_points,
                flight.last_seen_lat,
                flight.last_seen_lon,
                flight.last_seen_alt_ft,
                flight.last_seen_time.isoformat() if flight.last_seen_time else None,
                flight.squawk_first,
                flight.squawk_last,
                flight.squawk_changes,
                flight.emergency_squawk,
                flight.vfr_flight,
                flight.mission_type,
                flight.category_do260,
                flight.autopilot_target_alt_ft,
                flight.emergency_flag,
                flight.path_length_km,
                flight.max_distance_km,
                flight.loiter_ratio,
                flight.path_efficiency,
                flight.max_hover_secs,
                flight.hover_episodes,
                flight.go_around_count,
                flight.takeoff_heading_deg,
                flight.landing_heading_deg,
                flight.climb_secs,
                flight.cruise_secs,
                flight.descent_secs,
                flight.level_secs,
                flight.cruise_alt_ft,
                flight.cruise_gs_kt,
                flight.cruise_detected,
                flight.heavy_signal_gap,
                flight.peak_climb_fpm,
                flight.peak_descent_fpm,
                flight.takeoff_is_night,
                flight.landing_is_night,
                flight.night_flight,
                flight.callsigns,
                flight.callsign_changes,
                flight.callsign_count,
                flight.probable_destination_icao,
                flight.probable_destination_distance_km,
                flight.probable_destination_confidence,
                flight.active_minutes,
                flight.signal_gap_secs,
                flight.signal_gap_count,
                flight.fragments_stitched,
                flight.nearest_origin_icao,
                flight.nearest_origin_distance_km,
                flight.nearest_destination_icao,
                flight.nearest_destination_distance_km,
                flight.max_gs_kt,
                flight.turnaround_minutes,
                flight.origin_helipad_id,
                flight.destination_helipad_id,
                flight.type_override,
                flight.turnaround_category,
                flight.is_first_observed_flight,
                flight.is_last_observed_flight,
                flight.mlat_pct,
                flight.tisb_pct,
                flight.adsb_pct,
                flight.acars_out,
                flight.acars_off,
                flight.acars_on,
                flight.acars_in,
                flight.landing_anchor_method,
                flight.aligned_runway,
                flight.aligned_seconds,
                flight.aligned_min_offset_m,
                flight.takeoff_runway,
                flight.had_go_around,
                flight.pattern_cycles,
                flight.squawks_observed,
                flight.had_emergency,
                flight.primary_squawk,
            ),
        )

    def get_flights(
        self, icao: str, from_date: str | None = None, to_date: str | None = None, airport: str | None = None
    ) -> list[sqlite3.Row]:
        query = "SELECT * FROM flights WHERE icao = ?"
        params: list = [icao]
        if from_date:
            query += " AND takeoff_time >= ?"
            params.append(from_date)
        if to_date:
            query += " AND takeoff_time <= ?"
            params.append(to_date + "T23:59:59")
        if airport:
            query += " AND (origin_icao = ? OR destination_icao = ?)"
            params.extend([airport, airport])
        query += " ORDER BY takeoff_time"
        return self.conn.execute(query, params).fetchall()

    def get_flight_count(self, icao: str) -> int:
        row = self.conn.execute("SELECT COUNT(*) as cnt FROM flights WHERE icao = ?", (icao,)).fetchone()
        return row["cnt"]

    def get_flight_quality_summary(self, icao: str) -> dict:
        """Return landing type counts and average confidence for an aircraft."""
        rows = self.conn.execute(
            """SELECT landing_type, COUNT(*) as cnt,
                      AVG(landing_confidence) as avg_conf
               FROM flights WHERE icao = ?
               GROUP BY landing_type""",
            (icao,),
        ).fetchall()
        summary = {}
        for row in rows:
            lt = row["landing_type"] or "unknown"
            summary[lt] = {"count": row["cnt"], "avg_confidence": round(row["avg_conf"] or 0, 2)}
        return summary

    def get_top_airports(self, icao: str, limit: int = 10) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT airport, name, COUNT(*) as visits FROM (
                   SELECT origin_icao as airport, origin_name as name FROM flights
                   WHERE icao = ? AND origin_icao IS NOT NULL
                   UNION ALL
                   SELECT destination_icao, destination_name FROM flights
                   WHERE icao = ? AND destination_icao IS NOT NULL
               ) GROUP BY airport ORDER BY visits DESC LIMIT ?""",
            (icao, icao, limit),
        ).fetchall()

    def get_date_range(self, icao: str) -> tuple[str | None, str | None]:
        row = self.conn.execute(
            """SELECT MIN(date) as first_date, MAX(date) as last_date
               FROM fetch_log WHERE icao = ?""",
            (icao,),
        ).fetchone()
        return (row["first_date"], row["last_date"]) if row else (None, None)

    def get_days_with_data(self, icao: str, source: str | None = None) -> int:
        if source:
            row = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM trace_days WHERE icao = ? AND source = ?",
                (icao, source),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT COUNT(DISTINCT date) as cnt FROM trace_days WHERE icao = ?",
                (icao,),
            ).fetchone()
        return row["cnt"]

    def get_total_days_fetched(self, icao: str, source: str | None = None) -> int:
        if source:
            row = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM fetch_log WHERE icao = ? AND source = ?",
                (icao, source),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT COUNT(DISTINCT date) as cnt FROM fetch_log WHERE icao = ?",
                (icao,),
            ).fetchone()
        return row["cnt"]

    # -- airports --

    def airport_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) as cnt FROM airports").fetchone()
        return row["cnt"]

    def insert_airports(self, airports: list[tuple]):
        self.conn.executemany(
            """INSERT OR REPLACE INTO airports
               (ident, type, name, latitude_deg, longitude_deg, elevation_ft,
                iso_country, iso_region, municipality, iata_code)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            airports,
        )
        self.conn.commit()

    def find_nearby_airports(
        self,
        lat: float,
        lon: float,
        delta: float = 0.15,
        types: tuple[str, ...] = ("large_airport", "medium_airport", "small_airport"),
    ) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in types)
        return self.conn.execute(
            f"""SELECT * FROM airports
                WHERE latitude_deg BETWEEN ? AND ?
                AND longitude_deg BETWEEN ? AND ?
                AND type IN ({placeholders})""",
            (lat - delta, lat + delta, lon - delta, lon + delta, *types),
        ).fetchall()

    # -- runways --

    def insert_runway_ends(self, rows: list[tuple]) -> None:
        """Bulk upsert runway ends. Each tuple must match the column order of
        the runways table (see SCHEMA). Uses INSERT OR REPLACE keyed on
        (airport_ident, runway_name) so repeated refreshes are idempotent."""
        self.conn.executemany(
            """INSERT OR REPLACE INTO runways
               (airport_ident, runway_name, latitude_deg, longitude_deg,
                elevation_ft, heading_deg_true, length_ft, width_ft,
                surface, closed, displaced_threshold_ft)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self.conn.commit()

    def clear_runways_for_airport(self, airport_ident: str) -> None:
        """Delete all runway rows for one airport. Used by the refresh pipeline
        to drop ends that disappeared from the upstream CSV."""
        self.conn.execute("DELETE FROM runways WHERE airport_ident = ?", (airport_ident,))
        self.conn.commit()

    def runway_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS cnt FROM runways").fetchone()
        return int(row["cnt"])

    def get_runways_for_airport(self, airport_ident: str) -> list[sqlite3.Row]:
        """Return all runway ends for `airport_ident`, ordered by `runway_name`."""
        return self.conn.execute(
            "SELECT * FROM runways WHERE airport_ident = ? ORDER BY runway_name",
            (airport_ident,),
        ).fetchall()

    def get_airport_elevation(self, airport_ident: str) -> int | None:
        """Return elevation_ft for the given airport, or None if not found."""
        row = self.conn.execute(
            "SELECT elevation_ft FROM airports WHERE ident = ?",
            (airport_ident,),
        ).fetchone()
        if row is None:
            return None
        return row["elevation_ft"]

    # -- aircraft_registry (authoritative per-ICAO identity, v3) --

    def upsert_aircraft_registry(self, icao: str, trace_rows: list[sqlite3.Row]) -> dict | None:
        """Resolve authoritative metadata for an ICAO from trace_days rows.

        v4 fix (§1.9): pick the type_code / description that appears most
        often across all trace_days rows ("majority vote"), breaking ties by
        the most recent fetch. Previously this was "most recent fetch wins"
        which let a single new FAA registration silently overwrite hundreds
        of historical observations (adf64f flipped from GLF6 to GA8C with
        453 votes ignored). Registration / owner / year still come from the
        latest row since those track ownership changes correctly.
        """
        if not trace_rows:
            return None
        # Most recently fetched row - still used for ownership-tracked fields
        latest = max(trace_rows, key=lambda r: r["fetched_at"] or "")

        # Tally (type_code, description) pairs across all rows; remember
        # the latest fetch_at per pair so we can break ties by recency.
        seen_types: dict[tuple[str | None, str | None], dict] = {}
        for row in trace_rows:
            tc = row["type_code"]
            desc = row["description"]
            key = (tc, desc)
            entry = seen_types.setdefault(key, {"count": 0, "latest_ts": ""})
            entry["count"] += 1
            ts = row["fetched_at"] or ""
            if ts > entry["latest_ts"]:
                entry["latest_ts"] = ts

        # Pick the winning (type_code, description) by count desc, then by
        # latest_ts desc. Skip the (None, None) bucket as a candidate -
        # it's a "no metadata" row, not a real type vote.
        candidates = [(k, v) for k, v in seen_types.items() if k != (None, None)]
        winner_type: str | None = None
        winner_desc: str | None = None
        if candidates:
            # Stable: sort with the recency tie-break first, then count.
            candidates.sort(key=lambda kv: kv[1]["latest_ts"], reverse=True)
            candidates.sort(key=lambda kv: kv[1]["count"], reverse=True)
            winner_type, winner_desc = candidates[0][0]
        else:
            winner_type = latest["type_code"]
            winner_desc = latest["description"]

        # v6 D3: military hex blocks have no FAA registration metadata.
        # Fall back to a category-based inference when majority vote yields
        # nothing. US military blocks: ae0000-afffff (USAF), a00000-afffff
        # overlap with civil but ae69xx is distinctly military.
        if winner_type is None:
            # Hard-code known military ICAO blocks
            _military_types: dict[str, tuple[str, str]] = {
                # USAF rotorcraft (DO-260B A7) in the ae69xx block
                "ae69": ("H60", "Sikorsky UH-60 Black Hawk"),
            }
            for prefix, (tc, desc) in _military_types.items():
                if icao.startswith(prefix):
                    winner_type = tc
                    winner_desc = desc
                    break

        # Drift = everything that wasn't the winner (and wasn't the empty bucket)
        drift_values: list[dict] = []
        drift_count = 0
        for (tc, desc), entry in seen_types.items():
            if (tc, desc) == (winner_type, winner_desc):
                continue
            if tc is None and desc is None:
                continue
            drift_count += entry["count"]
            drift_values.append({"type_code": tc, "description": desc, "count": entry["count"]})

        self.conn.execute(
            """INSERT OR REPLACE INTO aircraft_registry
               (icao, registration, type_code, description, owner_operator, year,
                last_updated, metadata_drift_count, metadata_drift_values)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                icao,
                latest["registration"],
                winner_type,
                winner_desc,
                latest["owner_operator"],
                latest["year"],
                datetime.now(UTC).isoformat(),
                drift_count,
                json.dumps(drift_values) if drift_values else None,
            ),
        )
        return {
            "icao": icao,
            "registration": latest["registration"],
            "type_code": winner_type,
            "description": winner_desc,
            "owner_operator": latest["owner_operator"],
            "year": latest["year"],
            "metadata_drift_count": drift_count,
        }

    def get_aircraft_registry(self, icao: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM aircraft_registry WHERE icao = ?", (icao,)).fetchone()

    # -- aircraft_stats (materialized rollup, v3) --

    def refresh_aircraft_stats(self, icao: str | None = None) -> int:
        """Rebuild aircraft_stats rows from the current flights table.

        If ``icao`` is given, only that aircraft is refreshed. Otherwise all
        aircraft in the flights table get rolled up. Returns the number of
        rows written.
        """
        where_clause = ""
        params: list = []
        if icao:
            where_clause = "WHERE icao = ?"
            params.append(icao)

        # Aggregate core metrics in one sweep. Guard the duration-derived
        # rollups (total_hours, avg_flight_minutes) against non-positive
        # values so a single corrupted flight with a negative or zero
        # duration_minutes doesn't drag the totals negative. COUNT(*) and
        # confirmed_flights still count every row so total_flights stays
        # truthful.
        core_rows = self.conn.execute(
            f"""
            SELECT icao,
                   MIN(takeoff_date) AS first_seen,
                   MAX(takeoff_date) AS last_seen,
                   COUNT(*) AS total_flights,
                   SUM(CASE WHEN landing_type = 'confirmed' THEN 1 ELSE 0 END) AS confirmed_flights,
                   SUM(CASE WHEN duration_minutes > 0 THEN duration_minutes ELSE 0 END) / 60.0 AS total_hours,
                   AVG(CASE WHEN duration_minutes > 0 THEN duration_minutes END) AS avg_flight_minutes
            FROM flights
            {where_clause}
            GROUP BY icao
            """,
            params,
        ).fetchall()

        if not core_rows:
            # v6 N6: if filtering removed all flights for this ICAO, delete
            # the stale aircraft_stats row so it doesn't show ghost data.
            if icao:
                self.conn.execute("DELETE FROM aircraft_stats WHERE icao = ?", (icao,))
            return 0

        written = 0
        now_iso = datetime.now(UTC).isoformat()
        for row in core_rows:
            this_icao = row["icao"]

            distinct_airports_row = self.conn.execute(
                """
                SELECT COUNT(DISTINCT airport) AS cnt FROM (
                    SELECT origin_icao AS airport FROM flights WHERE icao = ? AND origin_icao IS NOT NULL
                    UNION
                    SELECT destination_icao FROM flights WHERE icao = ? AND destination_icao IS NOT NULL
                )
                """,
                (this_icao, this_icao),
            ).fetchone()

            distinct_callsigns_row = self.conn.execute(
                "SELECT COUNT(DISTINCT callsign) AS cnt FROM flights WHERE icao = ? AND callsign IS NOT NULL",
                (this_icao,),
            ).fetchone()

            busiest_row = self.conn.execute(
                """
                SELECT takeoff_date, COUNT(*) AS cnt
                FROM flights
                WHERE icao = ?
                GROUP BY takeoff_date
                ORDER BY cnt DESC, takeoff_date DESC
                LIMIT 1
                """,
                (this_icao,),
            ).fetchone()

            # v4 (§3.2): home base = airport with the most takeoffs.
            # Compute the top two by takeoff count and their share of total
            # takeoffs that have an origin assigned.
            base_rows = self.conn.execute(
                """
                SELECT origin_icao, COUNT(*) AS cnt
                FROM flights
                WHERE icao = ? AND origin_icao IS NOT NULL
                GROUP BY origin_icao
                ORDER BY cnt DESC
                LIMIT 2
                """,
                (this_icao,),
            ).fetchall()
            origin_total_row = self.conn.execute(
                "SELECT COUNT(*) AS cnt FROM flights WHERE icao = ? AND origin_icao IS NOT NULL",
                (this_icao,),
            ).fetchone()
            origin_total = origin_total_row["cnt"] if origin_total_row else 0
            home_base_icao = base_rows[0]["origin_icao"] if base_rows else None
            home_base_share = round(base_rows[0]["cnt"] / origin_total, 3) if base_rows and origin_total else None
            # v11 N22: flag aircraft where home_base_share < 0.40 as uncertain.
            # These nomadic aircraft operate from multiple bases and don't have
            # a single "home" in the operational sense.
            home_base_uncertain = 1 if home_base_share is not None and home_base_share < 0.40 else 0
            second_base_icao = base_rows[1]["origin_icao"] if len(base_rows) > 1 else None
            second_base_share = (
                round(base_rows[1]["cnt"] / origin_total, 3) if len(base_rows) > 1 and origin_total else None
            )

            registry = self.get_aircraft_registry(this_icao)

            self.conn.execute(
                """INSERT OR REPLACE INTO aircraft_stats
                   (icao, registration, type_code, first_seen, last_seen,
                    total_flights, confirmed_flights, total_hours, total_cycles,
                    distinct_airports, distinct_callsigns, avg_flight_minutes,
                    busiest_day_date, busiest_day_count,
                    home_base_icao, home_base_share, home_base_uncertain,
                    second_base_icao, second_base_share,
                    updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    this_icao,
                    registry["registration"] if registry else None,
                    registry["type_code"] if registry else None,
                    row["first_seen"],
                    row["last_seen"],
                    row["total_flights"],
                    row["confirmed_flights"],
                    round(row["total_hours"] or 0.0, 2),
                    row["confirmed_flights"],  # cycles = confirmed landings
                    distinct_airports_row["cnt"] if distinct_airports_row else 0,
                    distinct_callsigns_row["cnt"] if distinct_callsigns_row else 0,
                    round(row["avg_flight_minutes"] or 0.0, 1),
                    busiest_row["takeoff_date"] if busiest_row else None,
                    busiest_row["cnt"] if busiest_row else 0,
                    home_base_icao,
                    home_base_share,
                    home_base_uncertain,
                    second_base_icao,
                    second_base_share,
                    now_iso,
                ),
            )

            # v4 (§3.1): write confirmation_rate and signal_quality_tier
            # back to aircraft_registry. Tier thresholds per round-4 spec:
            # excellent (>50%), good (30-50%), poor (10-30%), very_poor (<10%).
            total_flights = row["total_flights"] or 0
            confirmed_flights = row["confirmed_flights"] or 0
            confirmation_rate: float | None = None
            signal_quality_tier: str | None = None
            if total_flights > 0:
                confirmation_rate = round(confirmed_flights / total_flights, 3)
                if confirmation_rate >= 0.50:
                    signal_quality_tier = "excellent"
                elif confirmation_rate >= 0.30:
                    signal_quality_tier = "good"
                elif confirmation_rate >= 0.10:
                    signal_quality_tier = "poor"
                else:
                    signal_quality_tier = "very_poor"
            self.conn.execute(
                """UPDATE aircraft_registry
                   SET confirmation_rate = ?, signal_quality_tier = ?
                   WHERE icao = ?""",
                (confirmation_rate, signal_quality_tier, this_icao),
            )
            written += 1
        return written

    # -- helipad back-fill (v8 H5) --

    def backfill_helipad_ids(self, icao: str, *, eps_km: float = 0.2) -> int:
        """Match flights for ``icao`` against the helipads table and populate
        origin_helipad_id / destination_helipad_id.

        For each flight, if the takeoff coordinates are within ``eps_km``
        of a helipad centroid, set origin_helipad_id to that helipad.
        Similarly for landing coordinates and destination_helipad_id.
        When multiple helipads match, the closest wins.

        Returns the number of flight-helipad links written.
        """
        helipads = self.conn.execute("SELECT helipad_id, centroid_lat, centroid_lon FROM helipads").fetchall()
        if not helipads:
            return 0

        flights = self.conn.execute(
            "SELECT id, takeoff_lat, takeoff_lon, landing_lat, landing_lon FROM flights WHERE icao = ?",
            (icao,),
        ).fetchall()
        if not flights:
            return 0

        # Import haversine locally to avoid circular imports at module level
        from .airports import haversine_km

        links = 0
        for f in flights:
            fid = f["id"]
            to_lat, to_lon = f["takeoff_lat"], f["takeoff_lon"]
            ld_lat, ld_lon = f["landing_lat"], f["landing_lon"]

            # Origin helipad: nearest centroid within eps_km of takeoff
            best_origin_id = None
            best_origin_dist = eps_km
            for h in helipads:
                d = haversine_km(to_lat, to_lon, h["centroid_lat"], h["centroid_lon"])
                if d <= best_origin_dist:
                    best_origin_dist = d
                    best_origin_id = h["helipad_id"]

            # Destination helipad: nearest centroid within eps_km of landing
            best_dest_id = None
            if ld_lat is not None and ld_lon is not None:
                best_dest_dist = eps_km
                for h in helipads:
                    d = haversine_km(ld_lat, ld_lon, h["centroid_lat"], h["centroid_lon"])
                    if d <= best_dest_dist:
                        best_dest_dist = d
                        best_dest_id = h["helipad_id"]

            if best_origin_id is not None or best_dest_id is not None:
                self.conn.execute(
                    "UPDATE flights SET origin_helipad_id = ?, destination_helipad_id = ? WHERE id = ?",
                    (best_origin_id, best_dest_id, fid),
                )
                links += 1

        return links

    # -- registry hygiene (v8 N9) --

    def promote_registry_type(self, icao: str, new_type: str) -> None:
        """Update aircraft_registry.type_code for an ICAO."""
        self.conn.execute(
            "UPDATE aircraft_registry SET type_code = ? WHERE icao = ?",
            (new_type, icao),
        )

    def update_flight_type_override(self, flight) -> None:
        """Update type_override, max_altitude, max_gs_kt, cruise_gs_kt on an existing flight."""
        self.conn.execute(
            """UPDATE flights
               SET type_override = ?, max_altitude = ?, max_gs_kt = ?, cruise_gs_kt = ?
               WHERE icao = ? AND takeoff_time = ?""",
            (
                flight.type_override,
                flight.max_altitude,
                flight.max_gs_kt,
                flight.cruise_gs_kt,
                flight.icao,
                flight.takeoff_time.isoformat() if flight.takeoff_time else None,
            ),
        )

    def update_last_observed_flag(self, flight) -> None:
        """Update is_last_observed_flight and turnaround_category on an existing flight."""
        self.conn.execute(
            """UPDATE flights
               SET is_last_observed_flight = ?, turnaround_category = ?
               WHERE icao = ? AND takeoff_time = ?""",
            (
                flight.is_last_observed_flight,
                flight.turnaround_category,
                flight.icao,
                flight.takeoff_time.isoformat() if flight.takeoff_time else None,
            ),
        )

    def purge_zero_flight_registry(self, icao: str | None = None) -> int:
        """Delete aircraft_registry (and aircraft_stats) entries with zero flights.

        If ``icao`` is given, only check that one ICAO. Otherwise purge
        all zero-flight entries globally. After extraction, some registry
        entries may refer to ICAOs that were retired, never flew during
        the collection window, or had their ICAO changed. Cleaning these
        prevents stale rows from polluting fleet summaries.

        Returns the number of registry rows deleted.
        """
        if icao:
            count = self.conn.execute("SELECT COUNT(*) AS cnt FROM flights WHERE icao = ?", (icao,)).fetchone()["cnt"]
            if count == 0:
                self.conn.execute("DELETE FROM aircraft_registry WHERE icao = ?", (icao,))
                self.conn.execute("DELETE FROM aircraft_stats WHERE icao = ?", (icao,))
                return 1
            return 0
        else:
            result = self.conn.execute(
                """DELETE FROM aircraft_registry
                   WHERE icao NOT IN (SELECT DISTINCT icao FROM flights)"""
            )
            deleted = result.rowcount
            self.conn.execute(
                """DELETE FROM aircraft_stats
                   WHERE icao NOT IN (SELECT DISTINCT icao FROM flights)"""
            )
            return deleted

    # -- FAA registry (faa_registry / faa_deregistered / faa_aircraft_ref) --

    # Column order must match adsbtrack.registry.MASTER_COLUMNS: the tuples
    # passed to executemany are built in that order, and the PRIMARY KEY
    # (mode_s_code_hex) sits at the end.
    _FAA_REGISTRY_COLUMNS = (
        "n_number",
        "serial_number",
        "mfr_mdl_code",
        "eng_mfr_mdl",
        "year_mfr",
        "type_registrant",
        "name",
        "street",
        "street2",
        "city",
        "state",
        "zip_code",
        "region",
        "county",
        "country",
        "last_action_date",
        "cert_issue_date",
        "certification",
        "type_aircraft",
        "type_engine",
        "status_code",
        "mode_s_code",
        "fract_owner",
        "air_worth_date",
        "expiration_date",
        "unique_id",
        "kit_mfr",
        "kit_model",
        "mode_s_code_hex",
    )

    def _faa_insert_sql(self, table: str) -> str:
        cols = ", ".join(self._FAA_REGISTRY_COLUMNS)
        placeholders = ", ".join("?" for _ in self._FAA_REGISTRY_COLUMNS)
        return f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"

    def insert_faa_registry(self, rows: list[tuple]) -> None:
        """Bulk insert/replace rows into faa_registry.

        rows is a list of tuples in MASTER_COLUMNS order (mode_s_code_hex
        last). Safe to call with thousands of rows; the caller controls
        transaction boundaries for best performance.
        """
        self.conn.executemany(self._faa_insert_sql("faa_registry"), rows)

    def insert_faa_deregistered(self, rows: list[tuple]) -> None:
        self.conn.executemany(self._faa_insert_sql("faa_deregistered"), rows)

    def insert_faa_aircraft_ref(self, rows: list[tuple]) -> None:
        self.conn.executemany(
            "INSERT OR REPLACE INTO faa_aircraft_ref (code, mfr, model, type_acft, type_eng) VALUES (?, ?, ?, ?, ?)",
            rows,
        )

    def truncate_faa_tables(self) -> None:
        """Clear all three FAA tables. Used by the update flow before re-import."""
        self.conn.execute("DELETE FROM faa_registry")
        self.conn.execute("DELETE FROM faa_deregistered")
        self.conn.execute("DELETE FROM faa_aircraft_ref")

    def get_faa_registry_by_hex(self, hex_code: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM faa_registry WHERE mode_s_code_hex = ?",
            (hex_code.lower(),),
        ).fetchone()

    def get_faa_registry_by_n_number(self, n_number: str) -> sqlite3.Row | None:
        # N-numbers are stored without the leading 'N'. Accept either form.
        normalized = n_number.upper().lstrip("N").strip()
        return self.conn.execute(
            "SELECT * FROM faa_registry WHERE n_number = ?",
            (normalized,),
        ).fetchone()

    def get_faa_deregistered_by_hex(self, hex_code: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM faa_deregistered WHERE mode_s_code_hex = ?",
            (hex_code.lower(),),
        ).fetchone()

    def get_faa_deregistered_by_n_number(self, n_number: str) -> sqlite3.Row | None:
        normalized = n_number.upper().lstrip("N").strip()
        return self.conn.execute(
            "SELECT * FROM faa_deregistered WHERE n_number = ?",
            (normalized,),
        ).fetchone()

    def get_faa_aircraft_ref(self, code: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM faa_aircraft_ref WHERE code = ?",
            (code,),
        ).fetchone()

    def search_faa_registry_by_name(self, query: str, limit: int = 500) -> list[sqlite3.Row]:
        """Case-insensitive LIKE match over the name column."""
        pattern = f"%{query.upper()}%"
        return self.conn.execute(
            "SELECT * FROM faa_registry WHERE UPPER(name) LIKE ? ORDER BY name, n_number LIMIT ?",
            (pattern, limit),
        ).fetchall()

    def search_faa_registry_by_address(
        self,
        *,
        street: str | None = None,
        city: str | None = None,
        state: str | None = None,
        limit: int = 500,
    ) -> list[sqlite3.Row]:
        """Case-insensitive address search. Combines filters with AND.

        Raises ValueError if no filters are provided, to prevent an
        accidental full-table scan.
        """
        clauses: list[str] = []
        params: list = []
        if street:
            clauses.append("UPPER(street) LIKE ?")
            params.append(f"%{street.upper()}%")
        if city:
            clauses.append("UPPER(city) = ?")
            params.append(city.upper())
        if state:
            clauses.append("UPPER(state) = ?")
            params.append(state.upper())
        if not clauses:
            raise ValueError("at least one of street, city, or state must be provided")
        sql = (
            "SELECT * FROM faa_registry WHERE " + " AND ".join(clauses) + " ORDER BY state, city, street, name LIMIT ?"
        )
        params.append(limit)
        return self.conn.execute(sql, params).fetchall()

    # -- ACARS / airframes.io --

    def update_registry_airframes_id(self, icao: str, airframes_id: int) -> None:
        """Cache the airframes.io numeric airframe id for an ICAO so the
        ACARS fetcher can skip the /airframes/icao/{hex} lookup next time."""
        self.conn.execute(
            "UPDATE aircraft_registry SET airframes_id = ? WHERE icao = ?",
            (airframes_id, icao),
        )

    def get_registry_airframes_id(self, icao: str) -> int | None:
        row = self.conn.execute(
            "SELECT airframes_id FROM aircraft_registry WHERE icao = ?",
            (icao,),
        ).fetchone()
        return row["airframes_id"] if row and row["airframes_id"] is not None else None

    def upsert_acars_flight(self, f: dict) -> None:
        """INSERT OR REPLACE on flight_id PK so re-fetches refresh metadata
        (status, message_count, etc.) without losing the row."""
        self.conn.execute(
            """INSERT OR REPLACE INTO acars_flights
               (flight_id, airframe_id, icao, registration, flight_number,
                flight_iata, flight_icao, status,
                departing_airport, destination_airport,
                departure_time_scheduled, departure_time_actual,
                arrival_time_scheduled, arrival_time_actual,
                first_seen, last_seen, message_count, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                f["flight_id"],
                f["airframe_id"],
                f["icao"],
                f.get("registration"),
                f.get("flight_number"),
                f.get("flight_iata"),
                f.get("flight_icao"),
                f.get("status"),
                f.get("departing_airport"),
                f.get("destination_airport"),
                f.get("departure_time_scheduled"),
                f.get("departure_time_actual"),
                f.get("arrival_time_scheduled"),
                f.get("arrival_time_actual"),
                f.get("first_seen"),
                f.get("last_seen"),
                f.get("message_count"),
                datetime.now(UTC).isoformat(),
            ),
        )

    def get_acars_flight_ids_fetched(self, icao: str) -> set[int]:
        """Return the set of airframes.io flight_ids we have already fetched
        messages for. Used as a skip-list to avoid re-fetching the same
        flight when the user runs `acars` repeatedly for the same range."""
        rows = self.conn.execute("SELECT flight_id FROM acars_flights WHERE icao = ?", (icao,)).fetchall()
        return {row["flight_id"] for row in rows}

    def insert_acars_message(self, m: dict) -> None:
        """INSERT OR IGNORE keyed on UNIQUE(airframes_id) so re-fetches stay
        idempotent. Caller is responsible for stamping fetched_at via this
        method (filled with now() if absent)."""
        self.conn.execute(
            """INSERT OR IGNORE INTO acars_messages
               (airframes_id, uuid, flight_id, icao, registration, timestamp,
                source_type, link_direction, from_hex, to_hex,
                frequency, level, channel, mode,
                label, block_id, message_number, ack, flight_number,
                text, data, latitude, longitude, altitude,
                departing_airport, destination_airport, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                m["airframes_id"],
                m.get("uuid"),
                m.get("flight_id"),
                m["icao"],
                m.get("registration"),
                m["timestamp"],
                m.get("source_type"),
                m.get("link_direction"),
                m.get("from_hex"),
                m.get("to_hex"),
                m.get("frequency"),
                m.get("level"),
                m.get("channel"),
                m.get("mode"),
                m.get("label"),
                m.get("block_id"),
                m.get("message_number"),
                m.get("ack"),
                m.get("flight_number"),
                m.get("text"),
                m.get("data"),
                m.get("latitude"),
                m.get("longitude"),
                m.get("altitude"),
                m.get("departing_airport"),
                m.get("destination_airport"),
                m.get("fetched_at") or datetime.now(UTC).isoformat(),
            ),
        )

    def update_flight_oooi(
        self,
        icao: str,
        takeoff_time_iso: str,
        *,
        out: str | None = None,
        off: str | None = None,
        on: str | None = None,
        in_: str | None = None,
    ) -> None:
        """Set acars_out/off/on/in on a single flight by (icao, takeoff_time).

        Each kwarg only updates its column when the new value is non-NULL,
        so a partial OOOI message (e.g. just OUT seen) does not overwrite
        previously-stored values. The trailing underscore on `in_` avoids
        the Python keyword.
        """
        self.conn.execute(
            """UPDATE flights
               SET acars_out = COALESCE(?, acars_out),
                   acars_off = COALESCE(?, acars_off),
                   acars_on  = COALESCE(?, acars_on),
                   acars_in  = COALESCE(?, acars_in)
               WHERE icao = ? AND takeoff_time = ?""",
            (out, off, on, in_, icao, takeoff_time_iso),
        )

    # -- Hex cross-reference (hex_crossref / mil_hex_ranges) --

    def upsert_hex_crossref(self, row: dict) -> None:
        """Insert or replace a hex_crossref row. Expected keys:
        icao, registration, type_code, type_description, operator, source,
        is_military, mil_country, mil_branch, last_updated. Missing keys
        default to NULL / 0."""
        self.conn.execute(
            """INSERT OR REPLACE INTO hex_crossref
               (icao, registration, type_code, type_description, operator,
                source, is_military, mil_country, mil_branch, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                row["icao"].lower(),
                row.get("registration"),
                row.get("type_code"),
                row.get("type_description"),
                row.get("operator"),
                row.get("source"),
                1 if row.get("is_military") else 0,
                row.get("mil_country"),
                row.get("mil_branch"),
                row.get("last_updated"),
            ),
        )

    def get_hex_crossref(self, hex_code: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM hex_crossref WHERE icao = ?",
            (hex_code.lower(),),
        ).fetchone()

    def get_icaos_missing_crossref(self) -> list[str]:
        """Return icao codes present in trace_days or flights but absent
        from hex_crossref. Used by `enrich --all` to drive the backfill."""
        rows = self.conn.execute(
            """SELECT DISTINCT icao FROM (
                   SELECT icao FROM trace_days
                   UNION
                   SELECT icao FROM flights
               ) WHERE icao NOT IN (SELECT icao FROM hex_crossref)
               ORDER BY icao"""
        ).fetchall()
        return [r["icao"] for r in rows]

    def insert_mil_hex_range(self, row: dict) -> None:
        """Upsert a single military hex allocation range."""
        self.conn.execute(
            """INSERT OR REPLACE INTO mil_hex_ranges
               (range_start, range_end, country, branch, notes)
               VALUES (?, ?, ?, ?, ?)""",
            (
                row["range_start"].lower(),
                row["range_end"].lower(),
                row.get("country"),
                row.get("branch"),
                row.get("notes"),
            ),
        )

    def lookup_mil_hex_range(self, hex_code: str) -> sqlite3.Row | None:
        """Return the mil_hex_ranges row that contains hex_code, or None.

        Ranges are compared lexicographically on the 6-char lowercase hex.
        Since all hex codes are 6 characters, lexicographic and numeric
        comparison agree, which lets us use a plain WHERE BETWEEN.
        """
        key = hex_code.lower().zfill(6)
        return self.conn.execute(
            """SELECT * FROM mil_hex_ranges
               WHERE ? BETWEEN range_start AND range_end
               ORDER BY (range_end < range_start), LENGTH(range_end) - LENGTH(range_start)
               LIMIT 1""",
            (key,),
        ).fetchone()

    def all_mil_hex_ranges(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM mil_hex_ranges ORDER BY range_start").fetchall()

    def get_all_icaos(self) -> list[str]:
        """Return every icao present in trace_days or flights."""
        rows = self.conn.execute(
            """SELECT DISTINCT icao FROM (
                   SELECT icao FROM trace_days
                   UNION
                   SELECT icao FROM flights
               ) ORDER BY icao"""
        ).fetchall()
        return [r["icao"] for r in rows]
