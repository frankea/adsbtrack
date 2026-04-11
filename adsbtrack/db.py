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
    peak_climb_fpm INTEGER,
    peak_descent_fpm INTEGER,
    takeoff_is_night INTEGER,
    landing_is_night INTEGER,
    night_flight INTEGER,
    callsigns TEXT,
    callsign_changes INTEGER,
    probable_destination_icao TEXT,
    probable_destination_distance_km REAL,
    probable_destination_confidence REAL,
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
    metadata_drift_values TEXT
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

CREATE INDEX IF NOT EXISTS idx_airports_lat ON airports(latitude_deg);
CREATE INDEX IF NOT EXISTS idx_airports_lon ON airports(longitude_deg);
CREATE INDEX IF NOT EXISTS idx_flights_icao_time ON flights(icao, takeoff_time);
CREATE INDEX IF NOT EXISTS idx_trace_days_icao_date ON trace_days(icao, date);
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
        ("probable_destination_icao", "TEXT"),
        ("probable_destination_distance_km", "REAL"),
        ("probable_destination_confidence", "REAL"),
    ]
    for col_name, col_type in new_columns:
        # "column already exists" is expected when re-running the migration.
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(f"ALTER TABLE flights ADD COLUMN {col_name} {col_type}")


def _flights_table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flights'").fetchone()
    return row is not None


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
                cruise_alt_ft, cruise_gs_kt,
                peak_climb_fpm, peak_descent_fpm,
                takeoff_is_night, landing_is_night, night_flight,
                callsigns, callsign_changes,
                probable_destination_icao, probable_destination_distance_km, probable_destination_confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?,
                       ?, ?, ?, ?,
                       ?, ?, ?, ?,
                       ?, ?, ?,
                       ?, ?,
                       ?, ?, ?, ?,
                       ?, ?,
                       ?, ?,
                       ?, ?, ?,
                       ?, ?,
                       ?, ?, ?)""",
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
                flight.peak_climb_fpm,
                flight.peak_descent_fpm,
                flight.takeoff_is_night,
                flight.landing_is_night,
                flight.night_flight,
                flight.callsigns,
                flight.callsign_changes,
                flight.probable_destination_icao,
                flight.probable_destination_distance_km,
                flight.probable_destination_confidence,
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

    # -- aircraft_registry (authoritative per-ICAO identity, v3) --

    def upsert_aircraft_registry(self, icao: str, trace_rows: list[sqlite3.Row]) -> dict | None:
        """Resolve authoritative metadata for an ICAO from trace_days rows.

        Picks the most recently fetched row as the source of truth, and flags
        metadata drift when any other row disagrees on type_code / description.
        Returns the resolved row as a dict, or None if no rows were provided.
        """
        if not trace_rows:
            return None
        # Most recently fetched row wins
        latest = max(trace_rows, key=lambda r: r["fetched_at"] or "")

        # Detect drift: different non-null type_code/description/registration
        drift_values: list[dict] = []
        latest_type = latest["type_code"]
        latest_desc = latest["description"]
        seen_types: dict[tuple[str | None, str | None], int] = {}
        for row in trace_rows:
            tc = row["type_code"]
            desc = row["description"]
            key = (tc, desc)
            seen_types[key] = seen_types.get(key, 0) + 1
        drift_count = 0
        for (tc, desc), count in seen_types.items():
            if tc == latest_type and desc == latest_desc:
                continue
            if tc is None and desc is None:
                continue
            drift_count += count
            drift_values.append({"type_code": tc, "description": desc, "count": count})

        self.conn.execute(
            """INSERT OR REPLACE INTO aircraft_registry
               (icao, registration, type_code, description, owner_operator, year,
                last_updated, metadata_drift_count, metadata_drift_values)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                icao,
                latest["registration"],
                latest["type_code"],
                latest["description"],
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
            "type_code": latest["type_code"],
            "description": latest["description"],
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

        # Aggregate core metrics in one sweep
        core_rows = self.conn.execute(
            f"""
            SELECT icao,
                   MIN(takeoff_date) AS first_seen,
                   MAX(takeoff_date) AS last_seen,
                   COUNT(*) AS total_flights,
                   SUM(CASE WHEN landing_type = 'confirmed' THEN 1 ELSE 0 END) AS confirmed_flights,
                   SUM(COALESCE(duration_minutes, 0)) / 60.0 AS total_hours,
                   AVG(duration_minutes) AS avg_flight_minutes
            FROM flights
            {where_clause}
            GROUP BY icao
            """,
            params,
        ).fetchall()

        if not core_rows:
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

            registry = self.get_aircraft_registry(this_icao)

            self.conn.execute(
                """INSERT OR REPLACE INTO aircraft_stats
                   (icao, registration, type_code, first_seen, last_seen,
                    total_flights, confirmed_flights, total_hours, total_cycles,
                    distinct_airports, distinct_callsigns, avg_flight_minutes,
                    busiest_day_date, busiest_day_count, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    now_iso,
                ),
            )
            written += 1
        return written
