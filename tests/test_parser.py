"""Tests for adsbtrack.parser -- flight extraction state machine."""

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

from adsbtrack.config import Config
from adsbtrack.models import Flight
from adsbtrack.parser import extract_flights

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config() -> Config:
    # Tests use synthetic traces with sparse points (often hours apart)
    # that would trip the intra-trace gap splitter. Disable gap splitting
    # by setting a huge threshold so these tests can exercise the state
    # machine without also re-testing gap handling.
    return Config(
        landing_speed_threshold_kts=80.0,
        airport_match_threshold_km=10.0,
        airport_types=("large_airport", "medium_airport", "small_airport"),
        max_point_gap_minutes=10_000.0,
    )


def _make_trace_point(time_offset, lat, lon, alt, gs=None, detail=None):
    """Build a single trace point in the standard format.

    Format: [time_offset, lat, lon, alt, gs, None, None, None, detail_dict]
    alt can be "ground" or a number (feet).
    gs is ground speed in knots (or None for OpenSky data).
    """
    return [time_offset, lat, lon, alt, gs, None, None, None, detail or {}]


def _make_trace_row(date_str, timestamp, trace, source="adsbx", hex_code="aaaaaa"):
    """Build a mock Row-like object matching what db.get_trace_days returns."""
    row = {
        "date": date_str,
        "timestamp": timestamp,
        "trace_json": json.dumps(trace),
        "source": source,
        "registration": "N12345",
        "type_code": "C172",
        "description": "CESSNA 172",
        "owner_operator": "Test Owner",
        "year": "2020",
        "point_count": len(trace),
        "fetched_at": "2024-01-01T00:00:00",
        "icao": hex_code,
    }
    # Make it behave like sqlite3.Row (supports both dict-style and key access)
    mock_row = MagicMock()
    mock_row.__getitem__ = lambda self, key: row[key]
    mock_row.keys = lambda: row.keys()
    return mock_row


def _make_db_mock(trace_rows=None):
    """Create a mock Database that returns the given trace rows."""
    db = MagicMock()
    db.get_trace_days.return_value = trace_rows or []
    db.find_nearby_airports.return_value = []
    return db


def _ts(date_str, hour=0, minute=0, second=0):
    """Convert a date string to a Unix timestamp at the given time (UTC)."""
    dt = datetime.fromisoformat(f"{date_str}T{hour:02d}:{minute:02d}:{second:02d}+00:00")
    return dt.timestamp()


# ---------------------------------------------------------------------------
# Basic takeoff/landing detection
# ---------------------------------------------------------------------------


def test_basic_takeoff_and_landing():
    """Ground -> airborne -> ground should produce one flight."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        # On ground at start
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(60, 40.0, -74.0, "ground", gs=5),
        # Takeoff
        _make_trace_point(120, 40.001, -74.0, 1000, gs=120),
        _make_trace_point(600, 40.5, -74.5, 5000, gs=200),
        _make_trace_point(3600, 41.0, -75.0, 5000, gs=200),
        # Landing
        _make_trace_point(7200, 41.5, -75.5, "ground", gs=10),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    # Verify insert_flight was called once
    assert db.insert_flight.call_count == 1
    flight = db.insert_flight.call_args[0][0]
    assert isinstance(flight, Flight)
    assert flight.icao == "aaaaaa"
    assert flight.landing_lat is not None


def test_airborne_at_start():
    """If first point is airborne, a flight should start from there."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        _make_trace_point(0, 40.0, -74.0, 5000, gs=200),
        _make_trace_point(3600, 41.0, -75.0, 5000, gs=200),
        _make_trace_point(7200, 42.0, -76.0, "ground", gs=10),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.takeoff_lat == 40.0


def test_multiple_flights_in_one_day():
    """Two complete ground-air-ground cycles should produce two flights."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        # Flight 1
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(60, 40.001, -74.0, 2000, gs=130),
        _make_trace_point(3600, 41.0, -75.0, "ground", gs=10),
        # Flight 2
        _make_trace_point(7200, 41.0, -75.0, "ground", gs=0),
        _make_trace_point(7260, 41.001, -75.0, 2000, gs=130),
        _make_trace_point(10800, 42.0, -76.0, "ground", gs=10),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 2


# ---------------------------------------------------------------------------
# Ground speed hysteresis
# ---------------------------------------------------------------------------


def test_high_gs_ground_point_does_not_trigger_landing():
    """A ground-alt point with gs > 80 kts should NOT be treated as a landing.

    This can happen from false altitude readings during high-speed flight.
    """
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        # Takeoff
        _make_trace_point(60, 40.001, -74.0, 2000, gs=130),
        _make_trace_point(600, 40.5, -74.5, 5000, gs=200),
        # False "ground" point at high speed - should be ignored
        _make_trace_point(1200, 40.8, -74.8, "ground", gs=250),
        # Still airborne
        _make_trace_point(1800, 41.0, -75.0, 5000, gs=200),
        # Actual landing at low speed
        _make_trace_point(7200, 42.0, -76.0, "ground", gs=10),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    # Should be one flight (the false ground point should not split it)
    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.landing_lat == 42.0


def test_gs_exactly_at_threshold_triggers_landing():
    """Ground speed exactly at the threshold (80 kts) should trigger landing.

    The check is gs > threshold, so gs == threshold does NOT skip.
    """
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(60, 40.001, -74.0, 2000, gs=130),
        _make_trace_point(3600, 41.0, -75.0, 5000, gs=200),
        # Landing at exactly the threshold speed
        _make_trace_point(7200, 42.0, -76.0, "ground", gs=80),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.landing_lat == 42.0


# ---------------------------------------------------------------------------
# OpenSky gs=None path (requires 2 consecutive ground points)
# ---------------------------------------------------------------------------


def test_opensky_single_ground_point_not_landing():
    """With gs=None, a single ground point should NOT trigger landing.

    OpenSky data lacks ground speed, so the parser requires 2 consecutive
    ground points to confirm a landing (avoids false positives from
    momentary altitude glitches).
    """
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=None),
        # Takeoff
        _make_trace_point(60, 40.001, -74.0, 2000, gs=None),
        _make_trace_point(600, 40.5, -74.5, 5000, gs=None),
        # Single ground glitch - should NOT be a landing
        _make_trace_point(1200, 40.8, -74.8, "ground", gs=None),
        # Back airborne
        _make_trace_point(1800, 41.0, -75.0, 5000, gs=None),
        # Actual landing: two consecutive ground points
        _make_trace_point(7200, 42.0, -76.0, "ground", gs=None),
        _make_trace_point(7260, 42.0, -76.0, "ground", gs=None),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    # Single ground glitch should not split the flight
    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.landing_lat == 42.0


def test_opensky_two_consecutive_ground_points_trigger_landing():
    """With gs=None, two consecutive ground points should confirm a landing."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=None),
        _make_trace_point(60, 40.0, -74.0, "ground", gs=None),
        # Takeoff
        _make_trace_point(120, 40.001, -74.0, 2000, gs=None),
        _make_trace_point(3600, 41.0, -75.0, 5000, gs=None),
        # Landing: two consecutive ground points
        _make_trace_point(7200, 42.0, -76.0, "ground", gs=None),
        _make_trace_point(7260, 42.0, -76.0, "ground", gs=None),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.landing_lat == 42.0


# ---------------------------------------------------------------------------
# Day-gap reset
# ---------------------------------------------------------------------------


def test_day_gap_resets_state():
    """A gap of more than 2 days between trace days should reset state.

    If a pending flight spans a large gap, it should be saved as-is
    (no landing info), and state should reset.
    """
    config = _make_config()

    # Day 1: ground then takeoff, ends airborne
    trace1 = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(60, 40.001, -74.0, 2000, gs=130),
        _make_trace_point(3600, 41.0, -75.0, 5000, gs=200),
    ]
    row1 = _make_trace_row("2024-06-15", _ts("2024-06-15"), trace1)

    # Day 2: 5 days later (gap > 2), new ground -> takeoff -> landing
    trace2 = [
        _make_trace_point(0, 35.0, -80.0, "ground", gs=0),
        _make_trace_point(60, 35.001, -80.0, 2000, gs=130),
        _make_trace_point(7200, 36.0, -81.0, "ground", gs=10),
    ]
    row2 = _make_trace_row("2024-06-20", _ts("2024-06-20"), trace2)

    db = _make_db_mock([row1, row2])

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    # Flight 1: airborne from day 1, no landing (saved due to gap reset)
    # Flight 2: complete flight from day 2
    assert count == 2


def test_consecutive_days_no_reset():
    """Consecutive days (gap <= 2) should NOT reset state."""
    config = _make_config()

    # Day 1: takeoff, ends airborne
    trace1 = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(60, 40.001, -74.0, 2000, gs=130),
        _make_trace_point(3600, 41.0, -75.0, 5000, gs=200),
    ]
    row1 = _make_trace_row("2024-06-15", _ts("2024-06-15"), trace1)

    # Day 2: next day, still airborne then lands
    trace2 = [
        _make_trace_point(0, 42.0, -76.0, 5000, gs=200),
        _make_trace_point(3600, 43.0, -77.0, "ground", gs=10),
    ]
    row2 = _make_trace_row("2024-06-16", _ts("2024-06-16"), trace2)

    db = _make_db_mock([row1, row2])

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    # One continuous flight across two days
    assert count == 1


# ---------------------------------------------------------------------------
# Taxi filtering
# ---------------------------------------------------------------------------


def test_short_taxi_filtered():
    """A movement <5 min AND <5 km should be filtered as taxi."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        # Very short "flight" - just taxiing
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(30, 40.0001, -74.0, 1000, gs=50),  # 30 sec airborne
        _make_trace_point(60, 40.0002, -74.0, "ground", gs=0),  # 1 min total, ~20m traveled
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 0


def test_short_but_long_distance_not_filtered():
    """A movement <5 min but >5 km should NOT be filtered."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    # ~100 km distance (1 degree latitude ~ 111 km)
    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(30, 40.5, -74.0, 2000, gs=300),  # 30 sec, ~55 km north
        _make_trace_point(180, 41.0, -74.0, "ground", gs=0),  # 3 min total, ~111 km
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1


def test_long_duration_not_filtered():
    """A movement >=5 min should NOT be filtered regardless of distance."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(60, 40.0001, -74.0, 1000, gs=50),
        # 6 minutes total
        _make_trace_point(360, 40.0002, -74.0, "ground", gs=0),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1


# ---------------------------------------------------------------------------
# Empty / no data
# ---------------------------------------------------------------------------


def test_no_trace_days():
    config = _make_config()
    db = _make_db_mock([])

    count = extract_flights(db, config, "aaaaaa")
    assert count == 0
    assert db.insert_flight.call_count == 0


def test_all_ground_points():
    """All ground points with no takeoff should produce no flights."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(60, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(120, 40.0, -74.0, "ground", gs=0),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 0


# ---------------------------------------------------------------------------
# Reprocess flag
# ---------------------------------------------------------------------------


def test_reprocess_clears_flights():
    """reprocess=True should call db.clear_flights."""
    config = _make_config()
    db = _make_db_mock([])

    extract_flights(db, config, "aaaaaa", reprocess=True)
    db.clear_flights.assert_called_once_with("aaaaaa")


def test_no_reprocess_does_not_clear():
    """reprocess=False (default) should NOT call db.clear_flights."""
    config = _make_config()
    db = _make_db_mock([])

    extract_flights(db, config, "aaaaaa", reprocess=False)
    db.clear_flights.assert_not_called()


# ---------------------------------------------------------------------------
# Callsign extraction from detail object
# ---------------------------------------------------------------------------


def test_callsign_from_detail():
    """Callsign should be extracted from the detail dict in trace points."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0, detail={"flight": "UAL123  "}),
        _make_trace_point(60, 40.001, -74.0, 2000, gs=130),
        _make_trace_point(7200, 42.0, -76.0, "ground", gs=10),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        extract_flights(db, config, "aaaaaa", reprocess=True)

    flight = db.insert_flight.call_args[0][0]
    assert flight.callsign == "UAL123"


# ---------------------------------------------------------------------------
# In-progress flight at end of data
# ---------------------------------------------------------------------------


def test_in_progress_flight_saved():
    """A flight still airborne at end of data should be saved (no landing)."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(60, 40.001, -74.0, 2000, gs=130),
        _make_trace_point(3600, 41.0, -75.0, 5000, gs=200),
        # No landing
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.landing_lat is None
    assert flight.landing_time is None
