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
