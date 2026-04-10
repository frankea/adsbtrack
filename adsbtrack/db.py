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
    takeoff_confidence REAL,
    landing_confidence REAL,
    data_points INTEGER,
    sources TEXT,
    max_altitude INTEGER,
    ground_points_at_landing INTEGER,
    UNIQUE(icao, takeoff_time)
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


def _needs_quality_migration(conn: sqlite3.Connection) -> bool:
    """Check if flight quality columns are missing from flights."""
    cols = conn.execute("PRAGMA table_info(flights)").fetchall()
    if not cols:
        return False
    col_names = {row[1] for row in cols}
    return "landing_type" not in col_names


def _migrate_add_quality_columns(conn: sqlite3.Connection):
    """Add flight quality metadata columns."""
    new_columns = [
        ("landing_type", "TEXT DEFAULT 'unknown'"),
        ("takeoff_confidence", "REAL"),
        ("landing_confidence", "REAL"),
        ("data_points", "INTEGER"),
        ("sources", "TEXT"),
        ("max_altitude", "INTEGER"),
        ("ground_points_at_landing", "INTEGER"),
    ]
    for col_name, col_type in new_columns:
        try:
            conn.execute(f"ALTER TABLE flights ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass  # column already exists


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
        # Migrate to add flight quality columns
        if _needs_quality_migration(self.conn):
            _migrate_add_quality_columns(self.conn)
        for stmt in _SCHEMA_STATEMENTS:
            self.conn.execute(stmt)

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
                landing_type, takeoff_confidence, landing_confidence,
                data_points, sources, max_altitude, ground_points_at_landing)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                flight.takeoff_confidence,
                flight.landing_confidence,
                flight.data_points,
                flight.sources,
                flight.max_altitude,
                flight.ground_points_at_landing,
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
