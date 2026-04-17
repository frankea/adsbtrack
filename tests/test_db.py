"""Tests for adsbtrack.db -- database layer."""

import json
from datetime import UTC, datetime
from sqlite3 import ProgrammingError

import pytest

from adsbtrack.db import Database
from adsbtrack.models import Flight

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path):
    """Return a temporary database file path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path):
    """Create and return a Database instance backed by a temp file."""
    database = Database(db_path)
    yield database
    database.close()


# ---------------------------------------------------------------------------
# Database creation and context manager
# ---------------------------------------------------------------------------


def test_database_creation(db_path):
    """Database should create the file and initialize schema."""
    database = Database(db_path)
    assert db_path.exists()
    # Verify tables were created
    tables = database.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_names = {row["name"] for row in tables}
    assert "trace_days" in table_names
    assert "fetch_log" in table_names
    assert "flights" in table_names
    assert "airports" in table_names
    database.close()


def test_context_manager(db_path):
    """Database should work with 'with' statements."""
    with Database(db_path) as database:
        database.conn.execute("SELECT 1")
    # After exiting, the connection should be closed
    # Trying to use it should fail
    with pytest.raises(ProgrammingError):
        database.conn.execute("SELECT 1")


def test_context_manager_commits_on_success(db_path):
    """Successful context manager exit should commit changes."""
    with Database(db_path) as database:
        database.insert_fetch_log("abc123", "2024-01-01", 200, source="adsbx")

    # Re-open and verify data persisted
    with Database(db_path) as database:
        dates = database.get_fetched_dates("abc123", source="adsbx")
        assert "2024-01-01" in dates


# ---------------------------------------------------------------------------
# trace_days
# ---------------------------------------------------------------------------


def test_insert_and_get_trace_days(db):
    data = {
        "timestamp": 1700000000.0,
        "trace": [[0, 40.0, -74.0, 5000, 200, None, None, None, {}]],
        "r": "N12345",
        "t": "C172",
        "desc": "CESSNA 172",
        "ownOp": "Test Owner",
        "year": "2020",
    }
    db.insert_trace_day("abc123", "2024-01-15", data, source="adsbx")
    db.commit()

    rows = db.get_trace_days("abc123")
    assert len(rows) == 1
    row = rows[0]
    assert row["icao"] == "abc123"
    assert row["date"] == "2024-01-15"
    assert row["source"] == "adsbx"
    assert row["registration"] == "N12345"
    assert row["type_code"] == "C172"
    assert row["point_count"] == 1
    assert json.loads(row["trace_json"]) == data["trace"]


def test_insert_trace_day_upsert(db):
    """Inserting the same icao/date/source should replace."""
    data1 = {
        "timestamp": 1700000000.0,
        "trace": [[0, 40.0, -74.0, 5000, 200, None, None, None, {}]],
    }
    data2 = {
        "timestamp": 1700000000.0,
        "trace": [[0, 41.0, -75.0, 6000, 250, None, None, None, {}]],
    }
    db.insert_trace_day("abc123", "2024-01-15", data1)
    db.insert_trace_day("abc123", "2024-01-15", data2)
    db.commit()

    rows = db.get_trace_days("abc123")
    assert len(rows) == 1
    trace = json.loads(rows[0]["trace_json"])
    assert trace[0][1] == 41.0  # Should be the updated data


def test_multiple_sources_same_date(db):
    """Different sources for the same icao/date should be stored separately."""
    data_adsbx = {
        "timestamp": 1700000000.0,
        "trace": [[0, 40.0, -74.0, 5000, 200, None, None, None, {}]],
    }
    data_adsbfi = {
        "timestamp": 1700000000.0,
        "trace": [[0, 40.001, -74.001, 5001, 201, None, None, None, {}]],
    }
    db.insert_trace_day("abc123", "2024-01-15", data_adsbx, source="adsbx")
    db.insert_trace_day("abc123", "2024-01-15", data_adsbfi, source="adsbfi")
    db.commit()

    rows = db.get_trace_days("abc123")
    assert len(rows) == 2
    sources = {row["source"] for row in rows}
    assert sources == {"adsbx", "adsbfi"}


def test_get_trace_days_empty(db):
    rows = db.get_trace_days("nonexistent")
    assert rows == []


def test_get_trace_days_ordered_by_date(db):
    """Results should be ordered by date."""
    for date_str in ["2024-01-20", "2024-01-10", "2024-01-15"]:
        data = {"timestamp": 1700000000.0, "trace": []}
        db.insert_trace_day("abc123", date_str, data)
    db.commit()

    rows = db.get_trace_days("abc123")
    dates = [row["date"] for row in rows]
    assert dates == ["2024-01-10", "2024-01-15", "2024-01-20"]


# ---------------------------------------------------------------------------
# fetch_log
# ---------------------------------------------------------------------------


def test_insert_and_get_fetched_dates(db):
    db.insert_fetch_log("abc123", "2024-01-15", 200, source="adsbx")
    db.insert_fetch_log("abc123", "2024-01-16", 404, source="adsbx")
    db.commit()

    dates = db.get_fetched_dates("abc123", source="adsbx")
    assert "2024-01-15" in dates
    assert "2024-01-16" in dates


def test_fetched_dates_includes_trace_days(db):
    """get_fetched_dates should include dates from both fetch_log and trace_days."""
    db.insert_fetch_log("abc123", "2024-01-15", 200, source="adsbx")
    data = {"timestamp": 1700000000.0, "trace": []}
    db.insert_trace_day("abc123", "2024-01-16", data, source="adsbx")
    db.commit()

    dates = db.get_fetched_dates("abc123", source="adsbx")
    assert "2024-01-15" in dates
    assert "2024-01-16" in dates


def test_fetched_dates_scoped_to_source(db):
    """get_fetched_dates should be scoped by source."""
    db.insert_fetch_log("abc123", "2024-01-15", 200, source="adsbx")
    db.insert_fetch_log("abc123", "2024-01-16", 200, source="opensky")
    db.commit()

    adsbx_dates = db.get_fetched_dates("abc123", source="adsbx")
    opensky_dates = db.get_fetched_dates("abc123", source="opensky")
    assert "2024-01-15" in adsbx_dates
    assert "2024-01-16" not in adsbx_dates
    assert "2024-01-16" in opensky_dates
    assert "2024-01-15" not in opensky_dates


def test_fetched_dates_empty(db):
    dates = db.get_fetched_dates("nonexistent")
    assert dates == set()


# ---------------------------------------------------------------------------
# flights
# ---------------------------------------------------------------------------


def test_insert_and_get_flights(db):
    flight = Flight(
        icao="abc123",
        takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        takeoff_date="2024-06-15",
        landing_time=datetime(2024, 6, 15, 14, 0, 0, tzinfo=UTC),
        landing_lat=42.0,
        landing_lon=-76.0,
        landing_date="2024-06-15",
        duration_minutes=120.0,
        callsign="UAL123",
    )
    db.insert_flight(flight)
    db.commit()

    rows = db.get_flights("abc123")
    assert len(rows) == 1
    assert rows[0]["icao"] == "abc123"
    assert rows[0]["takeoff_lat"] == 40.0
    assert rows[0]["landing_lat"] == 42.0
    assert rows[0]["callsign"] == "UAL123"
    assert rows[0]["duration_minutes"] == 120.0


def test_get_flights_with_date_filter(db):
    for i, date_str in enumerate(["2024-06-10", "2024-06-15", "2024-06-20"]):
        flight = Flight(
            icao="abc123",
            takeoff_time=datetime.fromisoformat(f"{date_str}T12:00:00+00:00"),
            takeoff_lat=40.0 + i,
            takeoff_lon=-74.0,
            takeoff_date=date_str,
        )
        db.insert_flight(flight)
    db.commit()

    # From date filter
    rows = db.get_flights("abc123", from_date="2024-06-14")
    assert len(rows) == 2

    # To date filter
    rows = db.get_flights("abc123", to_date="2024-06-15")
    assert len(rows) == 2

    # Both filters
    rows = db.get_flights("abc123", from_date="2024-06-14", to_date="2024-06-16")
    assert len(rows) == 1


def test_get_flights_with_airport_filter(db):
    flight1 = Flight(
        icao="abc123",
        takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        takeoff_date="2024-06-15",
        origin_icao="KJFK",
        destination_icao="KLAX",
    )
    flight2 = Flight(
        icao="abc123",
        takeoff_time=datetime(2024, 6, 16, 12, 0, 0, tzinfo=UTC),
        takeoff_lat=34.0,
        takeoff_lon=-118.0,
        takeoff_date="2024-06-16",
        origin_icao="KLAX",
        destination_icao="KORD",
    )
    db.insert_flight(flight1)
    db.insert_flight(flight2)
    db.commit()

    # Filter by KLAX should find both (destination of flight1, origin of flight2)
    rows = db.get_flights("abc123", airport="KLAX")
    assert len(rows) == 2

    # Filter by KJFK should find only flight1
    rows = db.get_flights("abc123", airport="KJFK")
    assert len(rows) == 1


def test_get_flights_empty(db):
    rows = db.get_flights("nonexistent")
    assert rows == []


def test_clear_flights(db):
    flight = Flight(
        icao="abc123",
        takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        takeoff_date="2024-06-15",
    )
    db.insert_flight(flight)
    db.commit()

    db.clear_flights("abc123")
    db.commit()

    rows = db.get_flights("abc123")
    assert rows == []


def test_insert_flight_upsert(db):
    """Inserting a flight with the same icao+takeoff_time should replace."""
    flight1 = Flight(
        icao="abc123",
        takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        takeoff_date="2024-06-15",
        callsign="OLD",
    )
    flight2 = Flight(
        icao="abc123",
        takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        takeoff_date="2024-06-15",
        callsign="NEW",
    )
    db.insert_flight(flight1)
    db.insert_flight(flight2)
    db.commit()

    rows = db.get_flights("abc123")
    assert len(rows) == 1
    assert rows[0]["callsign"] == "NEW"


def test_get_flight_count(db):
    for i in range(3):
        flight = Flight(
            icao="abc123",
            takeoff_time=datetime(2024, 6, 15, 12 + i, 0, 0, tzinfo=UTC),
            takeoff_lat=40.0,
            takeoff_lon=-74.0,
            takeoff_date="2024-06-15",
        )
        db.insert_flight(flight)
    db.commit()

    assert db.get_flight_count("abc123") == 3
    assert db.get_flight_count("nonexistent") == 0


# ---------------------------------------------------------------------------
# airports
# ---------------------------------------------------------------------------


def test_insert_and_find_airports(db):
    airports = [
        ("KJFK", "large_airport", "John F Kennedy Intl", 40.6398, -73.7789, 13, "US", "US-NY", "New York", "JFK"),
    ]
    db.insert_airports(airports)

    results = db.find_nearby_airports(40.64, -73.78)
    assert len(results) == 1
    assert results[0]["ident"] == "KJFK"


def test_find_nearby_airports_type_filter(db):
    airports = [
        ("KJFK", "large_airport", "John F Kennedy Intl", 40.6398, -73.7789, 13, "US", "US-NY", "New York", "JFK"),
        ("NK01", "heliport", "Some Heliport", 40.64, -73.78, 10, "US", "US-NY", "New York", None),
    ]
    db.insert_airports(airports)

    # Default types exclude heliports
    results = db.find_nearby_airports(40.64, -73.78)
    assert len(results) == 1
    assert results[0]["ident"] == "KJFK"


def test_airport_count(db):
    assert db.airport_count() == 0
    airports = [
        ("KJFK", "large_airport", "JFK", 40.64, -73.78, 13, "US", "US-NY", "NY", "JFK"),
        ("KLGA", "large_airport", "LGA", 40.77, -73.87, 21, "US", "US-NY", "NY", "LGA"),
    ]
    db.insert_airports(airports)
    assert db.airport_count() == 2


# ---------------------------------------------------------------------------
# Additional queries
# ---------------------------------------------------------------------------


def test_get_date_range(db):
    db.insert_fetch_log("abc123", "2024-01-10", 200)
    db.insert_fetch_log("abc123", "2024-06-20", 200)
    db.commit()

    first, last = db.get_date_range("abc123")
    assert first == "2024-01-10"
    assert last == "2024-06-20"


def test_get_date_range_empty(db):
    first, last = db.get_date_range("nonexistent")
    assert first is None
    assert last is None


def test_get_days_with_data(db):
    for date_str in ["2024-01-10", "2024-01-15", "2024-01-20"]:
        data = {"timestamp": 1700000000.0, "trace": []}
        db.insert_trace_day("abc123", date_str, data, source="adsbx")
    data2 = {"timestamp": 1700000000.0, "trace": []}
    db.insert_trace_day("abc123", "2024-01-10", data2, source="adsbfi")
    db.commit()

    # All sources
    assert db.get_days_with_data("abc123") == 3  # 3 distinct dates
    # Specific source
    assert db.get_days_with_data("abc123", source="adsbx") == 3
    assert db.get_days_with_data("abc123", source="adsbfi") == 1


def test_get_top_airports(db):
    flights_data = [
        ("KJFK", "KLAX"),
        ("KLAX", "KJFK"),
        ("KJFK", "KORD"),
    ]
    for i, (origin, dest) in enumerate(flights_data):
        flight = Flight(
            icao="abc123",
            takeoff_time=datetime(2024, 6, 15, 12 + i, 0, 0, tzinfo=UTC),
            takeoff_lat=40.0,
            takeoff_lon=-74.0,
            takeoff_date="2024-06-15",
            origin_icao=origin,
            origin_name=origin,
            destination_icao=dest,
            destination_name=dest,
        )
        db.insert_flight(flight)
    db.commit()

    top = db.get_top_airports("abc123", limit=10)
    assert len(top) >= 2
    # KJFK should be the most visited (3 times: 2 as origin, 1 as dest)
    assert top[0]["airport"] == "KJFK"
    assert top[0]["visits"] == 3


# ---------------------------------------------------------------------------
# Bug: aircraft_stats rollup guard against bad durations
# ---------------------------------------------------------------------------


def test_insert_flight_rejects_landing_before_takeoff(db):
    """B3: a flight whose landing_time predates takeoff_time is physically
    impossible. db.insert_flight should refuse it rather than persisting
    a row that will poison downstream rollups."""
    bad = Flight(
        icao="abc123",
        takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        takeoff_date="2024-06-15",
        landing_time=datetime(2024, 6, 15, 11, 55, 0, tzinfo=UTC),
        landing_lat=40.5,
        landing_lon=-74.5,
        landing_date="2024-06-15",
    )
    db.insert_flight(bad)
    db.commit()
    rows = db.get_flights("abc123")
    assert rows == [], "db.insert_flight accepted an invalid flight"


def test_refresh_aircraft_stats_ignores_negative_duration_flights(db):
    """A flight whose duration_minutes is negative (e.g. from a previous
    parser run that hit a phantom trace point) must not drag total_hours
    or avg_flight_minutes negative in the materialized aircraft_stats
    rollup.
    """
    # One real flight plus one corrupted flight with a negative duration.
    good = Flight(
        icao="abc123",
        takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        takeoff_date="2024-06-15",
        landing_time=datetime(2024, 6, 15, 14, 0, 0, tzinfo=UTC),
        landing_lat=42.0,
        landing_lon=-76.0,
        landing_date="2024-06-15",
        duration_minutes=120.0,
        landing_type="confirmed",
    )
    corrupted = Flight(
        icao="abc123",
        takeoff_time=datetime(2024, 6, 16, 12, 0, 0, tzinfo=UTC),
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        takeoff_date="2024-06-16",
        duration_minutes=-1122.9,
        landing_type="signal_lost",
    )
    db.insert_flight(good)
    db.insert_flight(corrupted)
    db.commit()

    db.refresh_aircraft_stats("abc123")
    db.commit()

    row = db.conn.execute(
        "SELECT total_hours, avg_flight_minutes FROM aircraft_stats WHERE icao = ?",
        ("abc123",),
    ).fetchone()
    assert row is not None
    assert row["total_hours"] is not None
    assert row["total_hours"] >= 0, f"total_hours went negative: {row['total_hours']}"
    assert row["avg_flight_minutes"] is not None
    assert row["avg_flight_minutes"] >= 0, f"avg_flight_minutes went negative: {row['avg_flight_minutes']}"


def test_faa_registry_tables_exist(tmp_path):
    """After Database construction, the three FAA tables must exist."""
    from adsbtrack.db import Database

    with Database(tmp_path / "t.db") as db:
        tables = {
            row["name"] for row in db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "faa_registry" in tables
        assert "faa_deregistered" in tables
        assert "faa_aircraft_ref" in tables


def test_insert_and_get_faa_registry(tmp_path):
    """Round-trip a registry row by hex."""
    from adsbtrack.db import Database
    from adsbtrack.registry import MASTER_COLUMNS

    with Database(tmp_path / "t.db") as db:
        # Build a row tuple matching MASTER_COLUMNS length (29 columns).
        # Fill with placeholder values but set n_number, name, hex.
        row = ["X"] * len(MASTER_COLUMNS)
        row[0] = "512WB"  # n_number
        row[6] = "EXAMPLE LLC"  # name
        row[-1] = "a66ad3"  # mode_s_code_hex
        db.insert_faa_registry([tuple(row)])
        fetched = db.get_faa_registry_by_hex("a66ad3")
        assert fetched is not None
        assert fetched["n_number"] == "512WB"
        assert fetched["name"] == "EXAMPLE LLC"


def test_insert_faa_registry_replaces(tmp_path):
    """Re-inserting the same hex key replaces the prior row (idempotent update)."""
    from adsbtrack.db import Database
    from adsbtrack.registry import MASTER_COLUMNS

    with Database(tmp_path / "t.db") as db:
        row = ["X"] * len(MASTER_COLUMNS)
        row[0] = "512WB"
        row[6] = "OLD OWNER"
        row[-1] = "a66ad3"
        db.insert_faa_registry([tuple(row)])
        row[6] = "NEW OWNER"
        db.insert_faa_registry([tuple(row)])
        fetched = db.get_faa_registry_by_hex("a66ad3")
        assert fetched["name"] == "NEW OWNER"


def test_get_faa_registry_by_n_number(tmp_path):
    from adsbtrack.db import Database
    from adsbtrack.registry import MASTER_COLUMNS

    with Database(tmp_path / "t.db") as db:
        row = ["X"] * len(MASTER_COLUMNS)
        row[0] = "512WB"
        row[6] = "EXAMPLE"
        row[-1] = "a66ad3"
        db.insert_faa_registry([tuple(row)])
        fetched = db.get_faa_registry_by_n_number("512WB")
        assert fetched is not None
        assert fetched["mode_s_code_hex"] == "a66ad3"


def test_search_faa_registry_by_name(tmp_path):
    """LIKE search over the name column, case-insensitive."""
    from adsbtrack.db import Database
    from adsbtrack.registry import MASTER_COLUMNS

    with Database(tmp_path / "t.db") as db:
        for tail, name, hex_ in [
            ("1A", "ACME CORP", "a00001"),
            ("2A", "acme holdings llc", "a00002"),
            ("3A", "OTHER LLC", "a00003"),
        ]:
            row = ["X"] * len(MASTER_COLUMNS)
            row[0] = tail
            row[6] = name
            row[-1] = hex_
            db.insert_faa_registry([tuple(row)])
        matches = db.search_faa_registry_by_name("acme")
        tails = {m["n_number"] for m in matches}
        assert tails == {"1A", "2A"}


def test_search_faa_registry_by_address(tmp_path):
    from adsbtrack.db import Database
    from adsbtrack.registry import MASTER_COLUMNS

    with Database(tmp_path / "t.db") as db:
        for tail, street, city, state, hex_ in [
            ("1A", "100 MAIN ST", "AUSTIN", "TX", "a00001"),
            ("2A", "200 OAK AVE", "AUSTIN", "TX", "a00002"),
            ("3A", "100 MAIN ST", "DALLAS", "TX", "a00003"),
        ]:
            row = ["X"] * len(MASTER_COLUMNS)
            row[0] = tail
            row[6] = "OWNER"
            row[7] = street
            row[9] = city
            row[10] = state
            row[-1] = hex_
            db.insert_faa_registry([tuple(row)])
        # Street match hits both buildings at 100 MAIN ST.
        matches = db.search_faa_registry_by_address(street="100 MAIN")
        assert {m["n_number"] for m in matches} == {"1A", "3A"}
        # City+state narrows to AUSTIN, TX.
        matches = db.search_faa_registry_by_address(city="AUSTIN", state="TX")
        assert {m["n_number"] for m in matches} == {"1A", "2A"}


def test_search_faa_registry_by_address_requires_filter(tmp_path):
    """Calling with no filters should raise, not silently full-scan."""
    import pytest

    from adsbtrack.db import Database

    with Database(tmp_path / "t.db") as db, pytest.raises(ValueError, match="at least one"):
        db.search_faa_registry_by_address()


def test_insert_and_check_faa_deregistered(tmp_path):
    from adsbtrack.db import Database
    from adsbtrack.registry import MASTER_COLUMNS

    with Database(tmp_path / "t.db") as db:
        row = ["X"] * len(MASTER_COLUMNS)
        row[0] = "99SK"
        row[6] = "GHOST HELI LLC"
        row[-1] = "abc123"
        db.insert_faa_deregistered([tuple(row)])
        fetched = db.get_faa_deregistered_by_hex("abc123")
        assert fetched is not None
        assert fetched["n_number"] == "99SK"


def test_insert_faa_aircraft_ref(tmp_path):
    from adsbtrack.db import Database

    with Database(tmp_path / "t.db") as db:
        db.insert_faa_aircraft_ref([("1152015", "CESSNA", "172", "4", "1")])
        ref = db.get_faa_aircraft_ref("1152015")
        assert ref is not None
        assert ref["mfr"] == "CESSNA"
        assert ref["model"] == "172"


def test_truncate_faa_tables(tmp_path):
    """The bulk-import update flow DELETEs all rows before reinserting."""
    from adsbtrack.db import Database
    from adsbtrack.registry import MASTER_COLUMNS

    with Database(tmp_path / "t.db") as db:
        row = ["X"] * len(MASTER_COLUMNS)
        row[0] = "1A"
        row[6] = "OWNER"
        row[-1] = "a00001"
        db.insert_faa_registry([tuple(row)])
        db.truncate_faa_tables()
        assert db.get_faa_registry_by_hex("a00001") is None


# ---------------------------------------------------------------------------
# ACARS schema: tables, columns, indexes
# ---------------------------------------------------------------------------


def _columns(db, table):
    return {r["name"] for r in db.conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _indexes(db, table):
    return {r["name"] for r in db.conn.execute(f"PRAGMA index_list({table})").fetchall()}


def test_acars_flights_table_created(db):
    cols = _columns(db, "acars_flights")
    # Core identity + metadata
    assert cols >= {
        "flight_id",
        "airframe_id",
        "icao",
        "registration",
        "flight_number",
        "flight_iata",
        "flight_icao",
        "status",
        "departing_airport",
        "destination_airport",
        "departure_time_scheduled",
        "departure_time_actual",
        "arrival_time_scheduled",
        "arrival_time_actual",
        "first_seen",
        "last_seen",
        "message_count",
        "fetched_at",
    }


def test_acars_messages_table_created(db):
    cols = _columns(db, "acars_messages")
    assert cols >= {
        "id",
        "airframes_id",
        "uuid",
        "flight_id",
        "icao",
        "registration",
        "timestamp",
        "source_type",
        "link_direction",
        "from_hex",
        "to_hex",
        "frequency",
        "level",
        "channel",
        "mode",
        "label",
        "block_id",
        "message_number",
        "ack",
        "flight_number",
        "text",
        "data",
        "latitude",
        "longitude",
        "altitude",
        "departing_airport",
        "destination_airport",
        "fetched_at",
    }
    # UNIQUE(airframes_id) for dedup
    unique_idxs = [r["name"] for r in db.conn.execute("PRAGMA index_list(acars_messages)").fetchall() if r["unique"]]
    # The unique constraint creates an auto index; verify it covers airframes_id
    found = False
    for ix in unique_idxs:
        info = db.conn.execute(f"PRAGMA index_info({ix})").fetchall()
        if len(info) == 1 and info[0]["name"] == "airframes_id":
            found = True
            break
    assert found, f"Expected UNIQUE index on acars_messages(airframes_id), got {unique_idxs}"


def test_acars_messages_icao_timestamp_index(db):
    idx_names = _indexes(db, "acars_messages")
    assert "idx_acars_messages_icao_ts" in idx_names


def test_aircraft_registry_has_airframes_id_column(db):
    assert "airframes_id" in _columns(db, "aircraft_registry")


def test_flights_table_has_acars_oooi_columns(db):
    cols = _columns(db, "flights")
    assert {"acars_out", "acars_off", "acars_on", "acars_in"} <= cols


def test_acars_migration_is_idempotent(db_path):
    """Opening the same DB twice must not error or duplicate columns."""
    Database(db_path).close()
    # Second open should find existing tables and columns, suppress duplicate errors
    d = Database(db_path)
    cols = {r["name"] for r in d.conn.execute("PRAGMA table_info(acars_messages)").fetchall()}
    assert "airframes_id" in cols
    d.close()


def test_insert_acars_message_dedup_by_airframes_id(db):
    """Inserting the same airframes_id twice should be a no-op (UNIQUE)."""
    m = {
        "airframes_id": 6503832431,
        "uuid": "abc-123",
        "flight_id": 5538326232,
        "icao": "06A0A5",
        "registration": "A7-BCA",
        "timestamp": "2026-03-29T13:45:35.138Z",
        "source_type": "aero-acars",
        "link_direction": "uplink",
        "from_hex": "90",
        "to_hex": "06A0A5",
        "frequency": None,
        "level": None,
        "channel": None,
        "mode": "2",
        "label": "H1",
        "block_id": "P",
        "message_number": None,
        "ack": "!",
        "flight_number": None,
        "text": "- #EIEM13R0",
        "data": None,
        "latitude": None,
        "longitude": None,
        "altitude": None,
        "departing_airport": None,
        "destination_airport": None,
    }
    db.insert_acars_message(m)
    db.insert_acars_message(m)  # second insert must dedup silently
    db.commit()
    count = db.conn.execute(
        "SELECT COUNT(*) AS c FROM acars_messages WHERE airframes_id = ?", (m["airframes_id"],)
    ).fetchone()["c"]
    assert count == 1


def test_upsert_acars_flight_updates_message_count(db):
    f = {
        "flight_id": 5538326232,
        "airframe_id": 14166,
        "icao": "06A0A5",
        "registration": "A7-BCA",
        "flight_number": "QR3255",
        "flight_iata": None,
        "flight_icao": None,
        "status": "radio-silence",
        "departing_airport": None,
        "destination_airport": None,
        "departure_time_scheduled": None,
        "departure_time_actual": None,
        "arrival_time_scheduled": None,
        "arrival_time_actual": None,
        "first_seen": "2026-03-29T08:50:24Z",
        "last_seen": "2026-03-29T13:45:35Z",
        "message_count": 200,
    }
    db.upsert_acars_flight(f)
    db.upsert_acars_flight({**f, "message_count": 250, "status": "arrived"})
    db.commit()
    row = db.conn.execute(
        "SELECT message_count, status FROM acars_flights WHERE flight_id = ?",
        (f["flight_id"],),
    ).fetchone()
    assert row["message_count"] == 250
    assert row["status"] == "arrived"


def test_get_acars_flight_ids_fetched_returns_set(db):
    db.upsert_acars_flight(
        {
            "flight_id": 1,
            "airframe_id": 14166,
            "icao": "06A0A5",
            "registration": "A7-BCA",
            "flight_number": None,
            "flight_iata": None,
            "flight_icao": None,
            "status": None,
            "departing_airport": None,
            "destination_airport": None,
            "departure_time_scheduled": None,
            "departure_time_actual": None,
            "arrival_time_scheduled": None,
            "arrival_time_actual": None,
            "first_seen": None,
            "last_seen": None,
            "message_count": 0,
        }
    )
    db.upsert_acars_flight(
        {
            "flight_id": 2,
            "airframe_id": 14166,
            "icao": "06A0A5",
            "registration": "A7-BCA",
            "flight_number": None,
            "flight_iata": None,
            "flight_icao": None,
            "status": None,
            "departing_airport": None,
            "destination_airport": None,
            "departure_time_scheduled": None,
            "departure_time_actual": None,
            "arrival_time_scheduled": None,
            "arrival_time_actual": None,
            "first_seen": None,
            "last_seen": None,
            "message_count": 0,
        }
    )
    db.commit()
    fetched = db.get_acars_flight_ids_fetched("06A0A5")
    assert fetched == {1, 2}


def test_update_registry_airframes_id(db):
    # Seed a registry row via upsert_aircraft_registry (takes sqlite3.Row list)
    db.conn.execute(
        "INSERT INTO aircraft_registry (icao, registration, last_updated) VALUES (?, ?, ?)",
        ("06A0A5", "A7-BCA", "2026-04-16T00:00:00Z"),
    )
    db.update_registry_airframes_id("06A0A5", 14166)
    db.commit()
    row = db.conn.execute("SELECT airframes_id FROM aircraft_registry WHERE icao = ?", ("06A0A5",)).fetchone()
    assert row["airframes_id"] == 14166


# ---------------------------------------------------------------------------
# hex_crossref / mil_hex_ranges
# ---------------------------------------------------------------------------


def test_mil_hex_ranges_seeded_on_init(db):
    """Fresh Database should have the curated military ranges loaded."""
    count = db.conn.execute("SELECT COUNT(*) FROM mil_hex_ranges").fetchone()[0]
    assert count >= 20, f"expected >=20 seed rows, got {count}"


def test_lookup_mil_hex_range_matches_us_dod(db):
    """US DoD range (AE*) should hit for a generic AE hex."""
    row = db.lookup_mil_hex_range("AE1234")
    assert row is not None
    assert row["country"] == "United States"
    assert "DoD" in row["branch"]


def test_lookup_mil_hex_range_civilian_returns_none(db):
    """A typical civilian hex must not match any military range."""
    # N512WB (FAA registered Pilatus PC-12) = a66ad3
    assert db.lookup_mil_hex_range("a66ad3") is None


def test_lookup_mil_hex_range_is_case_insensitive(db):
    assert db.lookup_mil_hex_range("AE0100") is not None
    assert db.lookup_mil_hex_range("ae0100") is not None


def test_upsert_and_get_hex_crossref(db):
    db.upsert_hex_crossref(
        {
            "icao": "A66AD3",
            "registration": "N512WB",
            "type_code": "PC12",
            "type_description": "PC-12 45",
            "operator": "Air Pilatus LLC",
            "source": "faa",
            "is_military": False,
            "last_updated": "2026-04-16T10:00:00Z",
        }
    )
    row = db.get_hex_crossref("a66ad3")
    assert row is not None
    assert row["registration"] == "N512WB"
    assert row["type_code"] == "PC12"
    assert row["is_military"] == 0
    assert row["source"] == "faa"


def test_get_icaos_missing_crossref(db):
    """Pulls icao codes from trace_days + flights minus those already
    present in hex_crossref."""
    from datetime import UTC, datetime

    from adsbtrack.models import Flight

    db.insert_flight(
        Flight(
            icao="aaaaaa",
            takeoff_time=datetime(2022, 6, 15, 12, 0, 0, tzinfo=UTC),
            takeoff_lat=35.0,
            takeoff_lon=-118.0,
            takeoff_date="2022-06-15",
        )
    )
    db.insert_flight(
        Flight(
            icao="bbbbbb",
            takeoff_time=datetime(2022, 6, 15, 13, 0, 0, tzinfo=UTC),
            takeoff_lat=35.0,
            takeoff_lon=-118.0,
            takeoff_date="2022-06-15",
        )
    )
    # aaaaaa already has a crossref entry
    db.upsert_hex_crossref({"icao": "aaaaaa", "registration": "N-A", "source": "faa"})

    missing = db.get_icaos_missing_crossref()
    assert "bbbbbb" in missing
    assert "aaaaaa" not in missing


def test_runways_table_created(db_path):
    """The runways table should exist on a fresh DB with the expected columns."""
    database = Database(db_path)
    cols = {row[1] for row in database.conn.execute("PRAGMA table_info(runways)").fetchall()}
    expected = {
        "airport_ident",
        "runway_name",
        "latitude_deg",
        "longitude_deg",
        "elevation_ft",
        "heading_deg_true",
        "length_ft",
        "width_ft",
        "surface",
        "closed",
        "displaced_threshold_ft",
    }
    assert expected.issubset(cols), f"missing columns: {expected - cols}"
    # Primary key enforces one row per (ident, end).
    pk_rows = database.conn.execute("PRAGMA index_list(runways)").fetchall()
    assert any(r["unique"] for r in pk_rows), "expected a unique index / PK on runways"
    database.close()


def test_runways_index_on_airport_ident(db_path):
    """Lookups by airport_ident must be indexed."""
    database = Database(db_path)
    indexes = database.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='runways'"
    ).fetchall()
    names = {r["name"] for r in indexes}
    assert "idx_runways_airport_ident" in names
    database.close()


def test_insert_runway_ends_roundtrip(db):
    """Insert two ends for one airport, assert we can read both back."""
    rows = [
        # (airport_ident, runway_name, lat, lon, elev, heading, length_ft,
        #  width_ft, surface, closed, displaced_threshold_ft)
        ("KSPG", "18", 27.7656, -82.6271, 7, 180.0, 2864, 75, "ASPH", 0, 0),
        ("KSPG", "36", 27.7735, -82.6271, 7, 360.0, 2864, 75, "ASPH", 0, 0),
    ]
    db.insert_runway_ends(rows)
    fetched = db.conn.execute(
        "SELECT runway_name FROM runways WHERE airport_ident = ? ORDER BY runway_name",
        ("KSPG",),
    ).fetchall()
    assert [r["runway_name"] for r in fetched] == ["18", "36"]


def test_insert_runway_ends_is_idempotent(db):
    """Re-inserting the same (airport_ident, runway_name) should overwrite, not duplicate."""
    row = ("KSPG", "18", 27.7656, -82.6271, 7, 180.0, 2864, 75, "ASPH", 0, 0)
    db.insert_runway_ends([row])
    # Re-run with a changed surface - row count stays at 1, surface updates.
    updated = ("KSPG", "18", 27.7656, -82.6271, 7, 180.0, 2864, 75, "CONC", 0, 0)
    db.insert_runway_ends([updated])
    rows = db.conn.execute("SELECT surface FROM runways WHERE airport_ident = ?", ("KSPG",)).fetchall()
    assert len(rows) == 1
    assert rows[0]["surface"] == "CONC"


def test_clear_runways_for_airport(db):
    """clear_runways_for_airport removes only that airport's rows."""
    db.insert_runway_ends(
        [
            ("KSPG", "18", 27.77, -82.63, 7, 180.0, 2864, 75, "ASPH", 0, 0),
            ("KSPG", "36", 27.77, -82.63, 7, 360.0, 2864, 75, "ASPH", 0, 0),
            ("KATL", "09L", 33.63, -84.44, 1026, 94.0, 9000, 150, "CONC", 0, 0),
        ]
    )
    db.clear_runways_for_airport("KSPG")
    remaining = db.conn.execute("SELECT airport_ident FROM runways").fetchall()
    assert {r["airport_ident"] for r in remaining} == {"KATL"}


def test_flights_landing_anchor_method_column_exists(db_path):
    """Schema should include landing_anchor_method on the flights table."""
    database = Database(db_path)
    cols = {row[1] for row in database.conn.execute("PRAGMA table_info(flights)").fetchall()}
    assert "landing_anchor_method" in cols
    database.close()


def test_insert_flight_persists_landing_anchor_method(db, db_path):
    """insert_flight should roundtrip the landing_anchor_method value."""
    from datetime import UTC, datetime

    f = Flight(
        icao="aaaaaa",
        takeoff_time=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
        takeoff_lat=30.0,
        takeoff_lon=-90.0,
        takeoff_date="2026-04-16",
        landing_anchor_method="alt_min",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute("SELECT landing_anchor_method FROM flights WHERE icao = ?", ("aaaaaa",)).fetchone()
    assert row["landing_anchor_method"] == "alt_min"


def test_insert_flight_landing_anchor_method_defaults_to_null(db):
    """Flights without an explicit method should store NULL."""
    from datetime import UTC, datetime

    f = Flight(
        icao="bbbbbb",
        takeoff_time=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
        takeoff_lat=30.0,
        takeoff_lon=-90.0,
        takeoff_date="2026-04-16",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute("SELECT landing_anchor_method FROM flights WHERE icao = ?", ("bbbbbb",)).fetchone()
    assert row["landing_anchor_method"] is None


def test_config_has_landing_anchor_window_minutes_default():
    from adsbtrack.config import Config

    cfg = Config()
    assert cfg.landing_anchor_window_minutes == 10.0


# ---------------------------------------------------------------------------
# ILS alignment: schema + runway query (Task 1)
# ---------------------------------------------------------------------------


def test_flights_table_has_alignment_columns(tmp_path):
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        cols = {row[1] for row in db.conn.execute("PRAGMA table_info(flights)").fetchall()}
    assert {"aligned_runway", "aligned_seconds", "aligned_min_offset_m"}.issubset(cols)


def test_get_runways_for_airport_returns_ordered_rows(tmp_path):
    db_path = tmp_path / "a.db"
    with Database(db_path) as db:
        db.insert_runway_ends(
            [
                ("KFAKE", "08R", 33.64, -84.43, 1026, 82.7, 9000, 150, "ASP", 0, 0),
                ("KFAKE", "26L", 33.64, -84.44, 1026, 262.7, 9000, 150, "ASP", 0, 0),
                ("KOTHR", "18", 33.00, -84.00, 800, 180.0, 5000, 100, "ASP", 0, 0),
            ]
        )
        rows = db.get_runways_for_airport("KFAKE")
    names = [r["runway_name"] for r in rows]
    assert names == ["08R", "26L"]


def test_insert_flight_persists_alignment_columns(db):
    """insert_flight should roundtrip the three alignment columns.

    Guards against the column list / placeholder group / value tuple drifting
    out of sync for aligned_runway, aligned_seconds, aligned_min_offset_m.
    A schema-only test won't notice a reordered value tuple; this will.
    """
    f = Flight(
        icao="cccccc",
        takeoff_time=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
        takeoff_lat=30.0,
        takeoff_lon=-90.0,
        takeoff_date="2026-04-16",
        aligned_runway="08R",
        aligned_seconds=45.0,
        aligned_min_offset_m=23.5,
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute(
        "SELECT aligned_runway, aligned_seconds, aligned_min_offset_m FROM flights WHERE icao = ?",
        ("cccccc",),
    ).fetchone()
    assert row["aligned_runway"] == "08R"
    assert row["aligned_seconds"] == 45.0
    assert row["aligned_min_offset_m"] == 23.5


def test_insert_flight_alignment_columns_default_to_null(db):
    """Flights constructed without alignment kwargs should store NULL for all three."""
    f = Flight(
        icao="dddddd",
        takeoff_time=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
        takeoff_lat=30.0,
        takeoff_lon=-90.0,
        takeoff_date="2026-04-16",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute(
        "SELECT aligned_runway, aligned_seconds, aligned_min_offset_m FROM flights WHERE icao = ?",
        ("dddddd",),
    ).fetchone()
    assert row["aligned_runway"] is None
    assert row["aligned_seconds"] is None
    assert row["aligned_min_offset_m"] is None


def test_flights_table_has_takeoff_runway_column(db) -> None:
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(flights)").fetchall()}
    assert "takeoff_runway" in cols


def test_insert_flight_persists_takeoff_runway(db) -> None:
    f = Flight(
        icao="ffffff",
        takeoff_time=datetime(2024, 6, 1, 10, 0),
        takeoff_lat=27.77,
        takeoff_lon=-82.67,
        takeoff_date="2024-06-01",
        takeoff_runway="24",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute("SELECT takeoff_runway FROM flights WHERE icao = ?", ("ffffff",)).fetchone()
    assert row["takeoff_runway"] == "24"


def test_insert_flight_takeoff_runway_defaults_to_null(db) -> None:
    f = Flight(
        icao="eeeeee",
        takeoff_time=datetime(2024, 6, 1, 10, 0),
        takeoff_lat=27.77,
        takeoff_lon=-82.67,
        takeoff_date="2024-06-01",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute("SELECT takeoff_runway FROM flights WHERE icao = ?", ("eeeeee",)).fetchone()
    assert row["takeoff_runway"] is None


def test_flights_table_has_go_around_and_pattern_cycles(db) -> None:
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(flights)").fetchall()}
    assert {"had_go_around", "pattern_cycles"}.issubset(cols)


def test_insert_flight_persists_go_around_and_pattern_cycles(db) -> None:
    f = Flight(
        icao="dddddd",
        takeoff_time=datetime(2024, 7, 1, 10, 0),
        takeoff_lat=27.77,
        takeoff_lon=-82.67,
        takeoff_date="2024-07-01",
        had_go_around=1,
        pattern_cycles=3,
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute(
        "SELECT had_go_around, pattern_cycles FROM flights WHERE icao = ?",
        ("dddddd",),
    ).fetchone()
    assert row["had_go_around"] == 1
    assert row["pattern_cycles"] == 3


def test_insert_flight_go_around_pattern_cycles_default_null(db) -> None:
    f = Flight(
        icao="cccccc",
        takeoff_time=datetime(2024, 7, 1, 10, 0),
        takeoff_lat=27.77,
        takeoff_lon=-82.67,
        takeoff_date="2024-07-01",
    )
    db.insert_flight(f)
    db.commit()
    row = db.conn.execute(
        "SELECT had_go_around, pattern_cycles FROM flights WHERE icao = ?",
        ("cccccc",),
    ).fetchone()
    assert row["had_go_around"] is None
    assert row["pattern_cycles"] is None
