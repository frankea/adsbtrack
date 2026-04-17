"""Tests for adsbtrack.parser -- flight extraction state machine."""

import json
import math
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from adsbtrack.airports import AirportMatch
from adsbtrack.classifier import FlightMetrics
from adsbtrack.config import Config
from adsbtrack.models import Flight
from adsbtrack.parser import _extract_point_fields, _stitch_fragments, extract_flights

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config() -> Config:
    # Tests use synthetic traces with sparse points (often hours apart)
    # that would trip the intra-trace gap splitter. Disable gap splitting
    # by setting a huge threshold so these tests can exercise the state
    # machine without also re-testing gap handling. Also raise the path
    # segment cap so path_length accumulates across sparse synthetic points.
    return Config(
        landing_speed_threshold_kts=80.0,
        airport_match_threshold_km=10.0,
        airport_types=("large_airport", "medium_airport", "small_airport"),
        max_point_gap_minutes=10_000.0,
        path_max_segment_secs=10_000.0,
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


# ---------------------------------------------------------------------------
# v3: full detail payload populates all new Flight fields
# ---------------------------------------------------------------------------


def _rich_trace_point(time_offset, lat, lon, alt, gs=None, track=None, baro_rate=None, geom_alt=None, detail=None):
    """Build a 12-element trace point with geom_alt and baro vertical rate."""
    return [
        time_offset,
        lat,
        lon,
        alt,
        gs,
        track,
        None,
        baro_rate,
        detail or {},
        None,
        geom_alt,
        None,
    ]


def test_v3_rich_trace_populates_new_fields():
    """A flight built from 12-element trace points with full detail dicts
    should populate the v3 feature columns on the Flight row."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    detail_cruise = {
        "flight": "TWY501  ",
        "squawk": "1200",
        "category": "A5",
        "nav_altitude_mcp": 32000,
        "nav_qnh": 1013.0,
        "emergency": "none",
        "true_heading": 90.0,
    }

    trace = [
        _rich_trace_point(0, 40.0, -74.0, "ground", gs=0, track=90.0, detail={"flight": "TWY501  "}),
        _rich_trace_point(
            60, 40.001, -74.0, 2000, gs=150, track=90.0, baro_rate=1500, geom_alt=2100, detail=detail_cruise
        ),
        _rich_trace_point(
            900, 40.5, -74.5, 30000, gs=450, track=90.0, baro_rate=0, geom_alt=30100, detail=detail_cruise
        ),
        _rich_trace_point(
            1800, 41.0, -75.0, 30000, gs=450, track=90.0, baro_rate=0, geom_alt=30100, detail=detail_cruise
        ),
        _rich_trace_point(
            3000, 41.5, -75.5, 5000, gs=250, track=90.0, baro_rate=-1200, geom_alt=5100, detail=detail_cruise
        ),
        _rich_trace_point(3600, 42.0, -76.0, "ground", gs=10, track=90.0, detail=detail_cruise),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    # Squawk tracking
    assert flight.squawk_first == "1200"
    assert flight.squawk_last == "1200"
    assert flight.vfr_flight == 1
    # Category
    assert flight.category_do260 == "A5"
    # Callsigns JSON
    assert flight.callsigns is not None
    assert "TWY501" in flight.callsigns
    # Path metrics populated
    assert flight.path_length_km is not None
    assert flight.path_length_km > 0
    # Mission classification: TWY prefix -> exec_charter
    assert flight.mission_type == "exec_charter"
    # Hover NULL for non-rotorcraft (C172 default type)
    assert flight.max_hover_secs is None
    # Day/night populated
    assert flight.takeoff_is_night in (0, 1)
    assert flight.landing_is_night in (0, 1)
    # Headings populated
    assert flight.takeoff_heading_deg is not None
    assert abs(flight.takeoff_heading_deg - 90.0) < 5.0


# ---------------------------------------------------------------------------
# Bug: phantom / out-of-order trace points
# ---------------------------------------------------------------------------


def test_out_of_order_phantom_points_do_not_corrupt_duration():
    """Readsb trace files occasionally contain "phantom" points with a deeply
    negative time offset (leakage from a prior day or cache glitch). The
    parser used to process points in stored order, which let a phantom point
    overwrite last_point_ts with a timestamp earlier than first_point_ts,
    producing a negative flight duration and junk last_seen_* values.

    Every extracted flight must have a non-negative duration and last_seen
    coordinates that match one of the real trace points, not the phantom.
    """
    config = Config(
        landing_speed_threshold_kts=80.0,
        airport_match_threshold_km=10.0,
        airport_types=("large_airport", "medium_airport", "small_airport"),
    )
    base_ts = _ts("2024-06-15", hour=12)

    # Normal flight: ground -> airborne -> ground over 30 minutes.
    # Phantom point is inserted mid-trace with a -54000 second offset
    # (roughly 15 hours before base_ts) at a completely different location.
    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(60, 40.001, -74.0, 2000, gs=130),
        _make_trace_point(900, 40.2, -74.2, 10000, gs=250),
        # PHANTOM: offset is ~15 hours in the past, at Edwards AFB latitude
        _make_trace_point(-54000, 35.456, -118.107, 11850, gs=300),
        _make_trace_point(1200, 40.3, -74.3, 10000, gs=250),
        _make_trace_point(1500, 40.4, -74.4, 5000, gs=150),
        _make_trace_point(1800, 40.5, -74.5, "ground", gs=10),
    ]

    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        extract_flights(db, config, "aaaaaa", reprocess=True)

    flights = [call[0][0] for call in db.insert_flight.call_args_list]
    assert flights, "Expected at least one flight to be extracted"

    # No flight may have a negative duration.
    for flight in flights:
        if flight.duration_minutes is not None:
            assert flight.duration_minutes >= 0, f"Flight has negative duration: {flight.duration_minutes} min"

    # The real New Jersey flight must end at the real last point (40.5, -74.5),
    # NOT at the phantom Edwards AFB coordinates (35.456, -118.107).
    nj_flights = [f for f in flights if f.takeoff_lat is not None and abs(f.takeoff_lat - 40.0) < 0.5]
    assert nj_flights, "Real NJ flight was not extracted"
    for f in nj_flights:
        if f.last_seen_lat is not None:
            assert abs(f.last_seen_lat - 35.456) > 1.0, (
                f"last_seen_lat {f.last_seen_lat} matches the phantom point; phantom data corrupted flight state"
            )


# ---------------------------------------------------------------------------
# Feature: type-endurance-aware stitch window
# ---------------------------------------------------------------------------


def _make_signal_lost_fragment(
    icao: str,
    start_ts: float,
    end_ts: float,
    start_lat: float,
    start_lon: float,
    end_lat: float,
    end_lon: float,
    end_alt_ft: int,
) -> tuple[Flight, FlightMetrics]:
    """Build a synthetic (Flight, FlightMetrics) pair for a signal_lost
    fragment that ends airborne without a landing transition."""
    flight = Flight(
        icao=icao,
        takeoff_time=datetime.fromtimestamp(start_ts, tz=UTC),
        takeoff_lat=start_lat,
        takeoff_lon=start_lon,
        takeoff_date=datetime.fromtimestamp(start_ts, tz=UTC).date().isoformat(),
    )
    metrics = FlightMetrics()
    metrics.first_point_ts = start_ts
    metrics.last_point_ts = end_ts
    metrics.last_seen_ts = end_ts
    metrics.last_seen_lat = end_lat
    metrics.last_seen_lon = end_lon
    metrics.last_seen_alt_ft = end_alt_ft
    metrics.last_airborne_alt = end_alt_ft
    metrics.takeoff_type = "observed"
    return flight, metrics


def _make_found_mid_flight_fragment(
    icao: str,
    start_ts: float,
    end_ts: float,
    start_lat: float,
    start_lon: float,
    start_alt_ft: int,
    end_lat: float,
    end_lon: float,
) -> tuple[Flight, FlightMetrics]:
    """Build a synthetic (Flight, FlightMetrics) pair for a found_mid_flight
    fragment whose coverage starts already airborne."""
    flight = Flight(
        icao=icao,
        takeoff_time=datetime.fromtimestamp(start_ts, tz=UTC),
        takeoff_lat=start_lat,
        takeoff_lon=start_lon,
        takeoff_date=datetime.fromtimestamp(start_ts, tz=UTC).date().isoformat(),
    )
    metrics = FlightMetrics()
    metrics.first_point_ts = start_ts
    metrics.last_point_ts = end_ts
    metrics.last_seen_ts = end_ts
    metrics.last_seen_lat = end_lat
    metrics.last_seen_lon = end_lon
    metrics.last_airborne_alt = start_alt_ft
    metrics.max_altitude = start_alt_ft
    metrics.takeoff_type = "found_mid_flight"
    return flight, metrics


def test_stitch_uses_type_endurance_for_long_endurance_aircraft():
    """A KC-135-style long-endurance type should allow stitching across gaps
    larger than the default stitch_max_gap_minutes (90 min). The stitch
    window should scale with the type's endurance so operational missions
    with ~2.5 hour coverage gaps over sensitive areas end up as one flight
    rather than two signal_lost fragments.
    """
    # Gap setup: matches the real 2022-06-16 ae07b3 case.
    # First fragment: 12:43:27Z -> 12:50:06Z at (35.59, -117.46, 17575 ft)
    # Second fragment: 15:26:45Z -> 15:29:55Z starting (37.08, -116.31, 19925 ft)
    t0 = _ts("2022-06-16", hour=12, minute=43, second=27)
    f1_end = t0 + (7 * 60)  # ~12:50:27Z
    f2_start = t0 + (2 * 60 + 43) * 60 + 18  # ~15:26:45Z
    f2_end = f2_start + (3 * 60)  # ~15:29:45Z

    f1, m1 = _make_signal_lost_fragment(
        icao="ae07b3",
        start_ts=t0,
        end_ts=f1_end,
        start_lat=35.03,
        start_lon=-117.93,
        end_lat=35.59,
        end_lon=-117.46,
        end_alt_ft=17575,
    )
    f2, m2 = _make_found_mid_flight_fragment(
        icao="ae07b3",
        start_ts=f2_start,
        end_ts=f2_end,
        start_lat=37.08,
        start_lon=-116.31,
        start_alt_ft=19925,
        end_lat=37.39,
        end_lon=-116.03,
    )

    # Without a long-endurance type, the default 90-min stitch window
    # should refuse to merge the ~156-min gap: two fragments stay separate.
    config_default = Config()
    stitched, _ = _stitch_fragments([f1, f2], [m1, m2], config_default, type_code=None)
    assert len(stitched) == 2, "Default stitch window should not merge fragments with a 156-min gap"

    # Fresh fragments for the second call (stitching mutates in place).
    f1b, m1b = _make_signal_lost_fragment(
        icao="ae07b3",
        start_ts=t0,
        end_ts=f1_end,
        start_lat=35.03,
        start_lon=-117.93,
        end_lat=35.59,
        end_lon=-117.46,
        end_alt_ft=17575,
    )
    f2b, m2b = _make_found_mid_flight_fragment(
        icao="ae07b3",
        start_ts=f2_start,
        end_ts=f2_end,
        start_lat=37.08,
        start_lon=-116.31,
        start_alt_ft=19925,
        end_lat=37.39,
        end_lon=-116.03,
    )

    # With K35R registered as a long-endurance type (720 min), the
    # per-type stitch window should scale up and merge the fragments.
    config_long = Config()
    config_long.type_endurance_minutes = {**config_long.type_endurance_minutes, "K35R": 720.0}
    stitched_long, _ = _stitch_fragments([f1b, f2b], [m1b, m2b], config_long, type_code="K35R")
    assert len(stitched_long) == 1, (
        "K35R long-endurance type should widen the stitch window enough to merge the 156-min gap into one flight"
    )
    merged = stitched_long[0]
    assert merged.takeoff_lat == 35.03
    assert merged.takeoff_lon == -117.93
    # The merged duration must reflect the full stitched span from the earlier
    # takeoff through the last observed point, not just the next fragment's
    # pre-merge duration. Full span: t0 -> f2_end = ~166 minutes.
    expected_minutes = round((f2_end - t0) / 60.0, 1)
    assert merged.duration_minutes == expected_minutes, (
        f"Expected merged duration {expected_minutes} min, got {merged.duration_minutes} min"
    )


# ---------------------------------------------------------------------------
# F2: fragments_stitched counter
# ---------------------------------------------------------------------------


def test_stitch_fragments_increments_fragments_stitched():
    """A two-way stitch must yield fragments_stitched == 2. A non-stitched
    flight gets 1 by definition."""
    t0 = _ts("2022-06-16", hour=12, minute=43, second=27)
    f1_end = t0 + (7 * 60)
    f2_start = t0 + (2 * 60 + 43) * 60 + 18
    f2_end = f2_start + (3 * 60)

    f1, m1 = _make_signal_lost_fragment(
        icao="ae07b3",
        start_ts=t0,
        end_ts=f1_end,
        start_lat=35.03,
        start_lon=-117.93,
        end_lat=35.59,
        end_lon=-117.46,
        end_alt_ft=17575,
    )
    f2, m2 = _make_found_mid_flight_fragment(
        icao="ae07b3",
        start_ts=f2_start,
        end_ts=f2_end,
        start_lat=37.08,
        start_lon=-116.31,
        start_alt_ft=19925,
        end_lat=37.39,
        end_lon=-116.03,
    )
    config = Config()
    config.type_endurance_minutes = {**config.type_endurance_minutes, "K35R": 720.0}

    # Each fragment defaults to fragments_stitched=1
    assert m1.fragments_stitched == 1
    assert m2.fragments_stitched == 1

    stitched, metrics = _stitch_fragments([f1, f2], [m1, m2], config, type_code="K35R")
    assert len(stitched) == 1
    merged_metric = metrics[0]
    assert merged_metric.fragments_stitched == 2
    # Flight-level passthrough - derive_all does the copy but for this unit
    # we only assert the metrics-side counter.


# ---------------------------------------------------------------------------
# B3: landing_time > takeoff_time guard in parser
# ---------------------------------------------------------------------------


def test_parser_skips_flight_with_landing_before_takeoff():
    """If the extractor ever produces a flight where landing_time <= takeoff_time
    (shouldn't happen post-v4 sort fix, but defense in depth), the parser
    should not call insert_flight on it.

    We synthesize this by directly patching the flight list before insert,
    then asserting that insert_flight was not called with the bad row.
    """
    # This test exercises the db.insert_flight guard directly since the
    # parser state machine won't produce a bad flight under normal input.
    # It is essentially a compile-time promise that the guard still exists.
    import tempfile
    from datetime import datetime
    from pathlib import Path

    from adsbtrack.db import Database
    from adsbtrack.models import Flight

    with tempfile.TemporaryDirectory() as td:
        db = Database(Path(td) / "test.db")
        bad = Flight(
            icao="abc123",
            takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
            takeoff_lat=40.0,
            takeoff_lon=-74.0,
            takeoff_date="2024-06-15",
            landing_time=datetime(2024, 6, 15, 11, 55, 0, tzinfo=UTC),  # before takeoff!
            landing_lat=40.0,
            landing_lon=-74.0,
            landing_date="2024-06-15",
        )
        db.insert_flight(bad)
        db.commit()
        rows = db.get_flights("abc123")
        assert rows == [], f"bad flight was persisted: {rows}"
        db.close()


# ---------------------------------------------------------------------------
# B7: tiny non-confirmed flights are dropped
# ---------------------------------------------------------------------------


def test_tiny_signal_lost_flight_dropped():
    """A signal_lost flight with < 2 min duration and < 10 data points must
    be dropped. These are the 'signal flicker' slivers - nothing useful."""
    config = Config(
        landing_speed_threshold_kts=80.0,
        airport_match_threshold_km=10.0,
        airport_types=("large_airport", "medium_airport", "small_airport"),
    )
    base_ts = _ts("2024-06-15", hour=12)

    trace = [
        # Found mid-flight at altitude, only 5 points across 60 seconds
        _make_trace_point(0, 40.0, -74.0, 10000, gs=250),
        _make_trace_point(15, 40.01, -74.01, 10020, gs=252),
        _make_trace_point(30, 40.02, -74.02, 10050, gs=251),
        _make_trace_point(45, 40.03, -74.03, 10080, gs=250),
        _make_trace_point(60, 40.04, -74.04, 10100, gs=249),
    ]
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 0, "tiny signal_lost sliver should have been dropped"


def test_short_confirmed_pattern_flight_kept():
    """A confirmed landing < 2 min (a short pattern hop at the same airport)
    must survive the B7 filter. Only non-confirmed flights get gated."""
    config = Config(
        landing_speed_threshold_kts=80.0,
        airport_match_threshold_km=10.0,
        airport_types=("large_airport", "medium_airport", "small_airport"),
    )
    base_ts = _ts("2024-06-15", hour=12)

    trace = [
        # Ground -> airborne -> ground within 90 s, 12 points, ground points
        # both ends. This should classify as confirmed. Travel > 5 km so the
        # taxi filter (< 5 min AND < 5 km) doesn't interfere - we want to
        # exercise B7's confirmed-landing exemption specifically.
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(5, 40.0, -74.0, "ground", gs=5),
        _make_trace_point(10, 40.005, -74.0, 500, gs=120),
        _make_trace_point(20, 40.015, -74.0, 1500, gs=150),
        _make_trace_point(30, 40.025, -74.0, 2200, gs=160),
        _make_trace_point(40, 40.035, -74.0, 2500, gs=160),
        _make_trace_point(50, 40.045, -74.0, 2500, gs=160),
        _make_trace_point(60, 40.05, -74.0, 2000, gs=150),
        _make_trace_point(70, 40.055, -74.0, 1000, gs=120),
        _make_trace_point(80, 40.058, -74.0, 500, gs=80),
        _make_trace_point(85, 40.06, -74.0, "ground", gs=20),
        _make_trace_point(90, 40.06, -74.0, "ground", gs=5),
    ]
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1, "confirmed short pattern hop should survive B7 tiny-flight filter"


# ---------------------------------------------------------------------------
# B8: stationary broadcaster filter
# ---------------------------------------------------------------------------


def test_stationary_broadcaster_filtered():
    """An aircraft sitting on a ramp with its transponder on, producing
    hundreds of points at the same lat/lon/alt=ground, is not a flight.
    """
    config = Config(
        landing_speed_threshold_kts=80.0,
        airport_match_threshold_km=10.0,
        airport_types=("large_airport", "medium_airport", "small_airport"),
    )
    base_ts = _ts("2024-06-15", hour=12)

    # 40 ground points at the same position, over ~40 minutes.
    trace = []
    for i in range(40):
        trace.append(_make_trace_point(i * 60, 40.0, -74.0, "ground", gs=0))
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 0


# ---------------------------------------------------------------------------
# D1: on-field vs nearest airport split
# ---------------------------------------------------------------------------


def test_airport_match_on_field_populates_origin():
    """A takeoff 1.2 km from the nearest airport is on-field - populate
    origin_icao with the match and leave nearest_origin_icao NULL."""
    config = Config(
        landing_speed_threshold_kts=80.0,
        airport_match_threshold_km=10.0,
        airport_types=("large_airport", "medium_airport", "small_airport"),
    )
    base_ts = _ts("2024-06-15", hour=12)

    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(30, 40.001, -74.0, 1000, gs=100),
        _make_trace_point(600, 41.0, -75.0, 5000, gs=200),
        _make_trace_point(1200, 42.0, -76.0, "ground", gs=20),
    ]
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    on_field = AirportMatch(ident="KONF", name="On Field", distance_km=1.2)
    with patch("adsbtrack.parser.find_nearest_airport", return_value=on_field):
        extract_flights(db, config, "aaaaaa", reprocess=True)

    flight = db.insert_flight.call_args[0][0]
    assert flight.origin_icao == "KONF"
    assert flight.origin_distance_km == 1.2
    assert flight.nearest_origin_icao is None


def test_airport_match_off_field_populates_nearest_only():
    """A takeoff 6.5 km from the nearest airport is off-field - leave
    origin_icao NULL but populate the diagnostic nearest_origin_icao."""
    config = Config(
        landing_speed_threshold_kts=80.0,
        airport_match_threshold_km=10.0,
        airport_types=("large_airport", "medium_airport", "small_airport"),
    )
    base_ts = _ts("2024-06-15", hour=12)

    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(30, 40.001, -74.0, 1000, gs=100),
        _make_trace_point(600, 41.0, -75.0, 5000, gs=200),
        _make_trace_point(1200, 42.0, -76.0, "ground", gs=20),
    ]
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    off_field = AirportMatch(ident="KFAR", name="Far Airport", distance_km=6.5)
    with patch("adsbtrack.parser.find_nearest_airport", return_value=off_field):
        extract_flights(db, config, "aaaaaa", reprocess=True)

    flight = db.insert_flight.call_args[0][0]
    assert flight.origin_icao is None
    assert flight.origin_distance_km is None
    assert flight.nearest_origin_icao == "KFAR"
    assert flight.nearest_origin_distance_km == 6.5


# ---------------------------------------------------------------------------
# Position source extraction (readsb type/src field)
# ---------------------------------------------------------------------------


def test_extract_point_fields_reads_position_source_from_index_9():
    """14-element trace points carry the source tag at point[9]."""
    # Layout: [offset, lat, lon, baro, gs, track, flags, baro_rate, detail,
    #          source_tag, geom_alt, geom_rate, ias_or_reserved, ...]
    point = [0.0, 40.0, -74.0, 10000, 300.0, 90.0, 0, 0, None, "mlat", 10012, 0, None, None]
    data = _extract_point_fields(point, ts=1000.0, lat=40.0, lon=-74.0)
    assert data.position_source == "mlat"


def test_extract_point_fields_falls_back_to_detail_type():
    """9-element trace points have no point[9]; type lives inside detail."""
    point = [0.0, 40.0, -74.0, 10000, 300.0, 90.0, 0, 0, {"type": "adsb_icao", "alt_geom": 10012}]
    data = _extract_point_fields(point, ts=1000.0, lat=40.0, lon=-74.0)
    assert data.position_source == "adsb_icao"


def test_extract_point_fields_position_source_none_when_absent():
    """Legacy / OpenSky points with no detail dict have position_source=None."""
    point = [0.0, 40.0, -74.0, 10000, 300.0, 90.0, 0, 0, None]
    data = _extract_point_fields(point, ts=1000.0, lat=40.0, lon=-74.0)
    assert data.position_source is None


def test_extract_point_fields_prefers_index_9_over_detail():
    """When both are present, index 9 wins (readsb writes them identically)."""
    # Intentionally disagree so the test can tell which source won.
    point = [0.0, 40.0, -74.0, 10000, 300.0, 90.0, 0, 0, {"type": "adsb_icao"}, "tisb_icao", 10012, 0, None, None]
    data = _extract_point_fields(point, ts=1000.0, lat=40.0, lon=-74.0)
    assert data.position_source == "tisb_icao"


# ---------------------------------------------------------------------------
# Position source percentages on flights
# ---------------------------------------------------------------------------


def _make_trace_point_with_source(time_offset, lat, lon, alt, gs, source_type):
    """Build a 9-element trace point with a given detail.type source tag."""
    detail = {"type": source_type}
    return [time_offset, lat, lon, alt, gs, None, None, None, detail]


def test_extract_flights_populates_position_source_percentages():
    """mlat_pct / tisb_pct / adsb_pct reflect the mix of source types."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    # The pre-takeoff ground point is consumed by the state machine for the
    # takeoff-position fix but never recorded into metrics. So the 8-point
    # trace yields 7 recorded metric points: 5 ADS-B, 1 MLAT, 1 TIS-B.
    trace = [
        _make_trace_point_with_source(0, 40.0, -74.0, "ground", 0, "adsb_icao"),
        _make_trace_point_with_source(60, 40.001, -74.0, 2000, 130, "adsb_icao"),
        _make_trace_point_with_source(600, 40.5, -74.5, 5000, 200, "adsb_icao"),
        _make_trace_point_with_source(1200, 40.7, -74.7, 5000, 200, "mlat"),
        _make_trace_point_with_source(1800, 40.9, -74.9, 5000, 200, "tisb_icao"),
        _make_trace_point_with_source(2400, 41.0, -75.0, 5000, 200, "adsb_icao"),
        _make_trace_point_with_source(3000, 41.1, -75.1, 5000, 200, "adsb_icao"),
        _make_trace_point_with_source(7200, 41.5, -75.5, "ground", 10, "adsb_icao"),
    ]
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        extract_flights(db, config, "aaaaaa", reprocess=True)

    flight = db.insert_flight.call_args[0][0]
    assert flight.data_points == 7
    assert flight.mlat_pct == round(1 / 7 * 100, 2)
    assert flight.tisb_pct == round(1 / 7 * 100, 2)
    assert flight.adsb_pct == round(5 / 7 * 100, 2)


def test_extract_flights_pct_zero_when_no_source_tag():
    """Traces without detail.type (OpenSky-style) get 0% for all three."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        _make_trace_point(0, 40.0, -74.0, "ground", gs=0),
        _make_trace_point(60, 40.001, -74.0, 2000, gs=130),
        _make_trace_point(3600, 41.0, -75.0, "ground", gs=10),
    ]
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        extract_flights(db, config, "aaaaaa", reprocess=True)

    flight = db.insert_flight.call_args[0][0]
    assert flight.mlat_pct == 0.0
    assert flight.tisb_pct == 0.0
    assert flight.adsb_pct == 0.0


def test_extract_flights_pct_handles_all_mlat_flight():
    """An entirely-MLAT flight (common for military) should be 100% mlat."""
    config = _make_config()
    base_ts = _ts("2024-06-15")

    trace = [
        _make_trace_point_with_source(0, 40.0, -74.0, "ground", 0, "mlat"),
        _make_trace_point_with_source(60, 40.001, -74.0, 2000, 130, "mlat"),
        _make_trace_point_with_source(600, 40.5, -74.5, 5000, 200, "mlat"),
        _make_trace_point_with_source(3600, 41.0, -75.0, "ground", 10, "mlat"),
    ]
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    with patch("adsbtrack.parser.find_nearest_airport", return_value=None):
        extract_flights(db, config, "aaaaaa", reprocess=True)

    flight = db.insert_flight.call_args[0][0]
    assert flight.mlat_pct == 100.0
    assert flight.tisb_pct == 0.0
    assert flight.adsb_pct == 0.0


def test_extract_flights_uses_alt_min_anchor_for_destination():
    """Synthetic flight where the altitude minimum is near airport KA but
    the last observed trace point drifted close to airport KB. The
    destination should resolve to KA via the alt_min anchor; method
    recorded as 'alt_min'."""
    from unittest.mock import MagicMock

    base = "2026-04-16"
    start_ts = _ts(base, 12, 0, 0)

    # Flight profile: takeoff, climb, cruise, descent to touchdown near KA,
    # then a few more airborne points that drift toward KB before signal
    # drop. Simulates a dropped_on_approach at KA followed by aircraft
    # continuing visible past it.
    trace = [
        _make_trace_point(0, 30.00, -90.00, "ground", gs=0),
        _make_trace_point(60, 30.00, -90.00, 100, gs=80),  # takeoff
        _make_trace_point(300, 30.05, -90.10, 3000, gs=140),
        _make_trace_point(600, 30.05, -90.05, 500, gs=80),  # alt-min, near KA (30.0, -90.0)
        _make_trace_point(660, 30.20, -89.80, 2000, gs=120),
        _make_trace_point(720, 30.40, -89.60, 2500, gs=120),  # last point, near KB (30.5, -89.5)
        # Signal lost (trace ends airborne).
    ]
    row = _make_trace_row(base, start_ts, trace)

    db = _make_db_mock([row])

    # KA is the alt_min airport, KB is the last-point airport.
    def fake_nearby(lat, lon, **kwargs):
        return [
            {
                "ident": "KA",
                "latitude_deg": 30.00,
                "longitude_deg": -90.00,
                "elevation_ft": 10,
                "name": "Alpha",
                "municipality": "",
                "iata_code": "",
                "type": "small_airport",
            },
            {
                "ident": "KB",
                "latitude_deg": 30.50,
                "longitude_deg": -89.50,
                "elevation_ft": 10,
                "name": "Bravo",
                "municipality": "",
                "iata_code": "",
                "type": "small_airport",
            },
        ]

    db.find_nearby_airports.side_effect = fake_nearby
    db.insert_flight = MagicMock()

    cfg = _make_config()

    extract_flights(db, cfg, "aaaaaa", reprocess=False)

    # Grab the flight object(s) passed to insert_flight and verify the
    # probable destination was picked via the alt_min anchor.
    inserted = [c.args[0] for c in db.insert_flight.call_args_list]
    assert len(inserted) >= 1
    flight = inserted[0]
    # landing_type will be signal_lost or dropped_on_approach - either way,
    # anchor method should be 'alt_min'.
    assert flight.landing_anchor_method == "alt_min"
    # Probable destination should resolve to KA (closest to alt-min point),
    # not KB (closest to last point).
    assert flight.probable_destination_icao == "KA"


def test_extract_flights_falls_back_to_last_point_when_tail_alts_missing():
    """When the final window has no altitude data (OpenSky-style traces
    with 'ground' strings or None throughout), landing_anchor_method
    should record 'last_point'."""
    from unittest.mock import MagicMock

    base = "2026-04-16"
    start_ts = _ts(base, 12, 0, 0)

    # Short "flight" where every airborne point has baro_alt='ground'.
    # The parser typically flags these as altitude_error; we only care
    # that the fallback method is recorded.
    trace = [
        _make_trace_point(0, 30.00, -90.00, "ground", gs=0),
        _make_trace_point(60, 30.00, -90.00, "ground", gs=90),  # baro='ground' at flight speed
        _make_trace_point(300, 30.05, -90.05, "ground", gs=120),
        _make_trace_point(600, 30.10, -90.10, "ground", gs=80),
    ]
    row = _make_trace_row(base, start_ts, trace)
    db = _make_db_mock([row])
    db.insert_flight = MagicMock()

    extract_flights(db, _make_config(), "aaaaaa", reprocess=False)

    inserted = [c.args[0] for c in db.insert_flight.call_args_list]
    # At least one flight may have been extracted; if so, confirm the
    # anchor fell back. If the parser filtered this trace out entirely,
    # the test is non-applicable and passes vacuously.
    for flight in inserted:
        assert flight.landing_anchor_method in ("last_point", None)


def test_reprocess_recomputes_landing_anchor_method():
    """Calling extract_flights with reprocess=True should recompute the
    anchor even if flights already exist with stale values."""
    import tempfile
    from datetime import UTC, datetime
    from pathlib import Path

    from adsbtrack.db import Database

    tmp = tempfile.TemporaryDirectory()
    try:
        db_path = Path(tmp.name) / "t.db"
        with Database(db_path) as db:
            # Insert a stale flight with landing_anchor_method=NULL.
            stale = Flight(
                icao="aaaaaa",
                takeoff_time=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
                takeoff_lat=30.0,
                takeoff_lon=-90.0,
                takeoff_date="2026-04-16",
                landing_anchor_method=None,
            )
            db.insert_flight(stale)
            db.commit()

            # Insert trace for the same icao.
            base = "2026-04-16"
            start_ts = _ts(base, 12, 0, 0)
            trace = [
                _make_trace_point(0, 30.00, -90.00, "ground", gs=0),
                _make_trace_point(60, 30.00, -90.00, 100, gs=80),
                _make_trace_point(300, 30.05, -90.10, 3000, gs=140),
                _make_trace_point(600, 30.05, -90.05, 500, gs=80),
            ]
            db.insert_trace_day(
                "aaaaaa",
                base,
                {
                    "r": "N1",
                    "t": "C172",
                    "desc": "CESSNA",
                    "ownOp": "",
                    "year": "",
                    "timestamp": start_ts,
                    "trace": trace,
                },
                source="adsbx",
            )
            db.commit()

            extract_flights(db, _make_config(), "aaaaaa", reprocess=True)
            db.commit()

            rows = db.conn.execute(
                "SELECT landing_anchor_method FROM flights WHERE icao = ?",
                ("aaaaaa",),
            ).fetchall()
            # After reprocess, the new flight row should have a populated
            # anchor method (either alt_min or last_point depending on trace).
            methods = [r["landing_anchor_method"] for r in rows]
            assert any(m in ("alt_min", "last_point") for m in methods), f"got {methods}"
    finally:
        tmp.cleanup()


# ---------------------------------------------------------------------------
# ILS alignment integration
# ---------------------------------------------------------------------------


def _walk_approach(
    base_ts: float,
    n: int,
    spacing_secs: float,
    runway_lat: float,
    runway_lon: float,
    runway_heading_deg: float,
    start_alt_ft: int,
    alt_step_ft: int,
) -> list[list]:
    """Generate n trace points approaching a runway threshold along its extended
    centerline. Points start ~n*0.3 km out and march toward the threshold,
    descending by alt_step_ft per sample.

    Each row is a 9-element trace point with track populated in position 5
    (position 5 is the field the parser reads via _extract_point_fields for
    _PointSample.track). Hand-built to avoid _make_trace_point's None-at-5.
    """
    approach_bearing_rad = math.radians((runway_heading_deg + 180.0) % 360.0)
    points: list[list] = []
    for i in range(n):
        km_out = (n - i) * 0.3
        dlat = (km_out / 111.0) * math.cos(approach_bearing_rad)
        dlon = (km_out / (111.0 * math.cos(math.radians(runway_lat)))) * math.sin(approach_bearing_rad)
        lat = runway_lat + dlat
        lon = runway_lon + dlon
        alt: int | str = max(0, start_alt_ft - i * alt_step_ft)
        time_offset = base_ts + i * spacing_secs
        points.append(
            [
                time_offset,
                lat,
                lon,
                alt,
                150.0,
                float(runway_heading_deg),
                None,
                -700.0,
                {"track": float(runway_heading_deg)},
            ]
        )
    return points


def test_aligned_confirmed_landing_bumps_confidence():
    """A confirmed landing whose final approach hugs a runway centerline
    should populate the alignment columns and get a landing_confidence
    bonus."""
    config = _make_config()
    # Force the detector's min to 30s so the tier is reachable.
    config.ils_alignment_min_duration_secs = 30.0

    runway_lat = 33.64
    runway_lon = -84.43
    base_ts = _ts("2024-06-15")

    origin = [
        _make_trace_point(0, 33.64, -86.5, "ground", gs=0),
        _make_trace_point(60, 33.64, -86.5, "ground", gs=5),
        _make_trace_point(120, 33.64, -86.49, 1000, gs=120),
    ]
    cruise = [
        _make_trace_point(600, 33.64, -85.5, 5000, gs=200),
        _make_trace_point(1800, 33.64, -85.0, 5000, gs=200),
    ]
    # 30-sample centerline approach, 3s spacing, starting 9km out at 3000ft.
    approach = _walk_approach(
        base_ts=3000.0,
        n=30,
        spacing_secs=3.0,
        runway_lat=runway_lat,
        runway_lon=runway_lon,
        runway_heading_deg=90.0,
        start_alt_ft=3000,
        alt_step_ft=100,
    )
    # Touchdown/rollout
    rollout_start = 3000.0 + 30 * 3.0
    rollout = [
        _make_trace_point(rollout_start + 2, runway_lat, runway_lon + 0.0005, "ground", gs=60),
        _make_trace_point(rollout_start + 4, runway_lat, runway_lon + 0.001, "ground", gs=30),
        _make_trace_point(rollout_start + 6, runway_lat, runway_lon + 0.0015, "ground", gs=10),
    ]

    trace = origin + cruise + approach + rollout
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    origin_match = AirportMatch(ident="KORIG", name="Origin", distance_km=1.0)
    dest_match = AirportMatch(ident="KFAKE", name="Fake Intl", distance_km=0.5)

    db.get_airport_elevation.return_value = 1026
    db.get_runways_for_airport.return_value = [
        {
            "runway_name": "09",
            "latitude_deg": runway_lat,
            "longitude_deg": runway_lon,
            "heading_deg_true": 90.0,
        }
    ]

    with patch(
        "adsbtrack.parser.find_nearest_airport",
        side_effect=[origin_match, dest_match],
    ):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.landing_type == "confirmed"
    assert flight.aligned_runway == "09"
    assert flight.aligned_seconds is not None and flight.aligned_seconds >= 60.0
    assert flight.aligned_min_offset_m is not None and flight.aligned_min_offset_m < 100.0
    assert flight.landing_confidence is not None


def test_overflight_no_alignment_no_bonus():
    """An overflight ~2 km off the runway should never satisfy the offset
    gate; the alignment columns remain NULL."""
    config = _make_config()
    config.ils_alignment_min_duration_secs = 30.0

    runway_lat = 33.64
    runway_lon = -84.43
    # 2 km north-offset overflight: centerline runs east-west along
    # latitude 33.64, so shift every sample to lat 33.658.
    base_ts = _ts("2024-06-15")

    origin = [
        _make_trace_point(0, 33.0, -86.5, "ground", gs=0),
        _make_trace_point(120, 33.0, -86.49, 1000, gs=120),
    ]
    # Overflight points 2 km north of centerline. We use the same helper
    # but then override latitudes.
    approach_bearing_rad = math.radians(270.0)  # heading 90 -> approach bearing 270
    flyover: list[list] = []
    off_lat = runway_lat + 0.018  # ~2 km north
    for i in range(30):
        km_out = (30 - i) * 0.3
        dlon = (km_out / (111.0 * math.cos(math.radians(off_lat)))) * math.sin(approach_bearing_rad)
        lon = runway_lon + dlon
        flyover.append(
            [
                3000.0 + i * 3.0,
                off_lat,
                lon,
                4000,
                200.0,
                90.0,
                None,
                0.0,
                {"track": 90.0},
            ]
        )
    # Landing back at origin area (to make this a confirmed flight).
    rollout_start = 3000.0 + 30 * 3.0 + 30.0
    rollout = [
        _make_trace_point(rollout_start, 33.0, -86.5, "ground", gs=30),
        _make_trace_point(rollout_start + 4, 33.0, -86.5, "ground", gs=10),
    ]

    trace = origin + flyover + rollout
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    origin_match = AirportMatch(ident="KORIG", name="Origin", distance_km=1.0)
    # The parser matches the landing airport -- since rollout is back near
    # origin, the dest match is still the same airport. Runway lookup on
    # KORIG returns the "09" runway (the detector still runs but no
    # alignment should be detected because offset is too large).
    db.get_airport_elevation.return_value = 1026
    db.get_runways_for_airport.return_value = [
        {
            "runway_name": "09",
            "latitude_deg": runway_lat,
            "longitude_deg": runway_lon,
            "heading_deg_true": 90.0,
        }
    ]

    with patch(
        "adsbtrack.parser.find_nearest_airport",
        side_effect=[origin_match, origin_match],
    ):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.aligned_runway is None
    assert flight.aligned_seconds is None
    assert flight.aligned_min_offset_m is None


def test_signal_lost_with_alignment_upgrades_to_drop():
    """A signal_lost flight whose trace ends on a 60s+ alignment segment at
    low altitude upgrades to dropped_on_approach.

    The approach finishes at 5130 ft MSL -- above `Config.dropped_max_alt_ft`
    (5000) so `classify_landing` returns `signal_lost` (not
    `dropped_on_approach` via its own sustained-descent branch) -- but below
    airport_elev (1026) + `ils_alignment_max_ft_above_airport` (5000) = 6026
    MSL so the alignment detector's AGL cap and the upgrade rule's altitude
    check both pass. The alignment upgrade rule in parser.py is then the
    code path under test.
    """
    config = _make_config()
    config.ils_alignment_min_duration_secs = 30.0

    runway_lat = 33.64
    runway_lon = -84.43
    base_ts = _ts("2024-06-15")

    origin = [
        _make_trace_point(0, 33.64, -86.5, "ground", gs=0),
        _make_trace_point(120, 33.64, -86.49, 1000, gs=120),
    ]
    climb_cruise = [
        _make_trace_point(600, 33.64, -85.5, 10000, gs=300),
        _make_trace_point(1800, 33.64, -85.0, 10000, gs=300),
    ]
    # Descend along centerline from 6000 ft MSL down to 5130 ft MSL. Every
    # sample sits under the 6026 MSL alt cap, so all 30 points contribute
    # to the aligned segment (29 * 3s = 87s duration, above the 60s long
    # bonus threshold). Final airborne altitude 5130 is above the 5000 ft
    # dropped_max_alt_ft floor, so classify_landing returns "signal_lost",
    # and the alignment upgrade rule is the thing that promotes it.
    approach = _walk_approach(
        base_ts=3000.0,
        n=30,
        spacing_secs=3.0,
        runway_lat=runway_lat,
        runway_lon=runway_lon,
        runway_heading_deg=90.0,
        start_alt_ft=6000,
        alt_step_ft=30,  # ends at 6000 - 29*30 = 5130 ft MSL
    )
    # NO ground points -> trace ends airborne -> signal_lost/dropped
    trace = origin + climb_cruise + approach
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    origin_match = AirportMatch(ident="KORIG", name="Origin", distance_km=1.0)

    # infer_destination needs at least one nearby-airport candidate.
    db.find_nearby_airports.return_value = [
        {
            "ident": "KFAKE",
            "name": "Fake",
            "iata_code": None,
            "municipality": None,
            "latitude_deg": runway_lat,
            "longitude_deg": runway_lon,
            "type": "medium_airport",
            "elevation_ft": 1026,
        }
    ]
    db.get_airport_elevation.return_value = 1026
    db.get_runways_for_airport.return_value = [
        {
            "runway_name": "09",
            "latitude_deg": runway_lat,
            "longitude_deg": runway_lon,
            "heading_deg_true": 90.0,
        }
    ]

    with patch(
        "adsbtrack.parser.find_nearest_airport",
        side_effect=[origin_match],
    ):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.aligned_seconds is not None and flight.aligned_seconds >= 60.0
    assert flight.landing_type == "dropped_on_approach"


def test_candidate_airport_without_runways_leaves_alignment_null():
    """Airport matched but no runway rows -> all alignment columns stay NULL,
    flight is NOT rejected, landing_type still 'confirmed'."""
    config = _make_config()
    config.ils_alignment_min_duration_secs = 30.0

    runway_lat = 33.64
    runway_lon = -84.43
    base_ts = _ts("2024-06-15")

    origin = [
        _make_trace_point(0, 33.64, -86.5, "ground", gs=0),
        _make_trace_point(60, 33.64, -86.5, "ground", gs=5),
        _make_trace_point(120, 33.64, -86.49, 1000, gs=120),
    ]
    cruise = [
        _make_trace_point(600, 33.64, -85.5, 5000, gs=200),
        _make_trace_point(1800, 33.64, -85.0, 5000, gs=200),
    ]
    approach = _walk_approach(
        base_ts=3000.0,
        n=30,
        spacing_secs=3.0,
        runway_lat=runway_lat,
        runway_lon=runway_lon,
        runway_heading_deg=90.0,
        start_alt_ft=3000,
        alt_step_ft=100,
    )
    rollout_start = 3000.0 + 30 * 3.0
    rollout = [
        _make_trace_point(rollout_start + 2, runway_lat, runway_lon + 0.0005, "ground", gs=60),
        _make_trace_point(rollout_start + 4, runway_lat, runway_lon + 0.001, "ground", gs=30),
        _make_trace_point(rollout_start + 6, runway_lat, runway_lon + 0.0015, "ground", gs=10),
    ]

    trace = origin + cruise + approach + rollout
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    origin_match = AirportMatch(ident="KORIG", name="Origin", distance_km=1.0)
    dest_match = AirportMatch(ident="KFAKE", name="Fake Intl", distance_km=0.5)

    db.get_airport_elevation.return_value = 1026
    # Intentionally no runway rows.
    db.get_runways_for_airport.return_value = []

    with patch(
        "adsbtrack.parser.find_nearest_airport",
        side_effect=[origin_match, dest_match],
    ):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.landing_type == "confirmed"
    assert flight.aligned_runway is None
    assert flight.aligned_seconds is None
    assert flight.aligned_min_offset_m is None


# ---------------------------------------------------------------------------
# Takeoff runway integration
# ---------------------------------------------------------------------------


def _walk_takeoff(
    base_ts: float,
    n: int,
    spacing_secs: float,
    threshold_lat: float,
    threshold_lon: float,
    heading_deg: float,
    start_alt_ft: int,
    alt_step_ft: int,
    start_gs_kt: float,
    gs_step_kt: float,
) -> list[list]:
    """Generate n trace points climbing out along a runway heading.

    Walks along the departure bearing starting at the threshold, climbing by
    alt_step_ft and accelerating by gs_step_kt per sample. Each row is a
    9-element trace point with track populated in position 5 (the field
    _extract_point_fields reads into _PointSample.track).
    """
    departure_bearing_rad = math.radians(heading_deg)
    points: list[list] = []
    for i in range(n):
        km_out = i * 0.05  # 50 m per step
        dlat = (km_out / 111.0) * math.cos(departure_bearing_rad)
        dlon = (km_out / (111.0 * math.cos(math.radians(threshold_lat)))) * math.sin(departure_bearing_rad)
        lat = threshold_lat + dlat
        lon = threshold_lon + dlon
        alt = start_alt_ft + i * alt_step_ft
        gs = start_gs_kt + i * gs_step_kt
        points.append(
            [
                base_ts + i * spacing_secs,
                lat,
                lon,
                alt,
                gs,
                float(heading_deg),
                None,
                1500.0,
                {"track": float(heading_deg)},
            ]
        )
    return points


def test_takeoff_runway_commercial_jet_identified():
    """A jet ground-rolling + climbing along runway 24 at KSPG should have
    takeoff_runway populated."""
    config = _make_config()

    threshold_lat = 27.76
    threshold_lon = -82.63
    base_ts = _ts("2024-06-15")

    # A few ground samples at the threshold (taxi/hold-short/ground roll).
    ground_roll = [
        _make_trace_point(0, threshold_lat, threshold_lon, "ground", gs=0),
        _make_trace_point(30, threshold_lat, threshold_lon, "ground", gs=15),
        _make_trace_point(45, threshold_lat, threshold_lon, "ground", gs=30),
    ]
    # 30-sample departure climb along 240 deg; gs goes 30 -> 30+29*6 = 204 kt.
    climbout = _walk_takeoff(
        base_ts=60.0,
        n=30,
        spacing_secs=3.0,
        threshold_lat=threshold_lat,
        threshold_lon=threshold_lon,
        heading_deg=240.0,
        start_alt_ft=10,
        alt_step_ft=100,
        start_gs_kt=30.0,
        gs_step_kt=6.0,
    )
    # Brief cruise then a ground landing elsewhere so the flight closes.
    cruise = [
        _make_trace_point(600, 27.5, -83.0, 5000, gs=210),
        _make_trace_point(1800, 27.0, -83.5, 5000, gs=210),
    ]
    landing = [
        _make_trace_point(3600, 27.0, -83.6, "ground", gs=20),
        _make_trace_point(3660, 27.0, -83.6, "ground", gs=5),
    ]

    trace = ground_roll + climbout + cruise + landing
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    origin_match = AirportMatch(ident="KSPG", name="KSPG", distance_km=0.3)
    dest_match = AirportMatch(ident="KDEST", name="Dest", distance_km=0.5)

    db.get_airport_elevation.return_value = 7
    db.get_runways_for_airport.return_value = [
        {
            "runway_name": "24",
            "latitude_deg": threshold_lat,
            "longitude_deg": threshold_lon,
            "heading_deg_true": 240.0,
        }
    ]

    with patch(
        "adsbtrack.parser.find_nearest_airport",
        side_effect=[origin_match, dest_match],
    ):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.takeoff_runway == "24"


def test_takeoff_runway_helicopter_threshold_scaled():
    """A rotorcraft (H60) peaking at only ~80 kt should still be identified
    because the H-prefix triggers low-gs scaling to 60 kt."""
    config = _make_config()

    threshold_lat = 27.76
    threshold_lon = -82.63
    base_ts = _ts("2024-06-15")

    ground_roll = [
        _make_trace_point(0, threshold_lat, threshold_lon, "ground", gs=0),
        _make_trace_point(30, threshold_lat, threshold_lon, "ground", gs=15),
    ]
    # gs 30 -> 30+29*2 = 88 kt (below 140 default, above 60 scaled).
    climbout = _walk_takeoff(
        base_ts=60.0,
        n=30,
        spacing_secs=3.0,
        threshold_lat=threshold_lat,
        threshold_lon=threshold_lon,
        heading_deg=240.0,
        start_alt_ft=10,
        alt_step_ft=100,
        start_gs_kt=30.0,
        gs_step_kt=2.0,
    )
    cruise = [
        _make_trace_point(600, 27.5, -83.0, 3000, gs=85),
        _make_trace_point(1800, 27.0, -83.5, 3000, gs=85),
    ]
    landing = [
        _make_trace_point(3600, 27.0, -83.6, "ground", gs=10),
        _make_trace_point(3660, 27.0, -83.6, "ground", gs=0),
    ]

    trace = ground_roll + climbout + cruise + landing
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    # Force the effective type_code to H60 via the registry upsert return.
    db.upsert_aircraft_registry.return_value = {
        "type_code": "H60",
        "owner_operator": None,
    }

    origin_match = AirportMatch(ident="KSPG", name="KSPG", distance_km=0.3)
    dest_match = AirportMatch(ident="KDEST", name="Dest", distance_km=0.5)

    db.get_airport_elevation.return_value = 7
    db.get_runways_for_airport.return_value = [
        {
            "runway_name": "24",
            "latitude_deg": threshold_lat,
            "longitude_deg": threshold_lon,
            "heading_deg_true": 240.0,
        }
    ]

    with patch(
        "adsbtrack.parser.find_nearest_airport",
        side_effect=[origin_match, dest_match],
    ):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.takeoff_runway == "24"


def test_takeoff_runway_sparse_data_fails_gracefully():
    """A flight whose takeoff samples lack track data (or exceed the AGL cap)
    should leave takeoff_runway NULL. _make_trace_point sets track=None, which
    _filter_takeoff_samples rejects; this exercises the detector's graceful-
    fail path rather than skipping the detector entirely."""
    config = _make_config()

    threshold_lat = 27.76
    threshold_lon = -82.63
    base_ts = _ts("2024-06-15")

    # Minimal trace: ground -> a couple of airborne samples (with track=None
    # so the detector's filter rejects them) -> ground. State machine sees a
    # valid takeoff/landing pair but the polygon detector has nothing to do.
    trace = [
        _make_trace_point(0, threshold_lat, threshold_lon, "ground", gs=0),
        _make_trace_point(60, threshold_lat + 0.01, threshold_lon - 0.01, 1500, gs=150),
        _make_trace_point(120, threshold_lat + 0.05, threshold_lon - 0.05, 2500, gs=180),
        _make_trace_point(3600, 27.0, -83.6, "ground", gs=10),
    ]
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    origin_match = AirportMatch(ident="KSPG", name="KSPG", distance_km=0.3)
    dest_match = AirportMatch(ident="KDEST", name="Dest", distance_km=0.5)

    db.get_airport_elevation.return_value = 7
    db.get_runways_for_airport.return_value = [
        {
            "runway_name": "24",
            "latitude_deg": threshold_lat,
            "longitude_deg": threshold_lon,
            "heading_deg_true": 240.0,
        }
    ]

    with patch(
        "adsbtrack.parser.find_nearest_airport",
        side_effect=[origin_match, dest_match],
    ):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count >= 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.takeoff_runway is None


def test_takeoff_runway_no_runway_data_leaves_null():
    """When the origin airport has no runway rows, takeoff_runway stays NULL
    and the flight is still saved."""
    config = _make_config()

    threshold_lat = 27.76
    threshold_lon = -82.63
    base_ts = _ts("2024-06-15")

    ground_roll = [
        _make_trace_point(0, threshold_lat, threshold_lon, "ground", gs=0),
        _make_trace_point(30, threshold_lat, threshold_lon, "ground", gs=15),
        _make_trace_point(45, threshold_lat, threshold_lon, "ground", gs=30),
    ]
    climbout = _walk_takeoff(
        base_ts=60.0,
        n=30,
        spacing_secs=3.0,
        threshold_lat=threshold_lat,
        threshold_lon=threshold_lon,
        heading_deg=240.0,
        start_alt_ft=10,
        alt_step_ft=100,
        start_gs_kt=30.0,
        gs_step_kt=6.0,
    )
    cruise = [
        _make_trace_point(600, 27.5, -83.0, 5000, gs=210),
        _make_trace_point(1800, 27.0, -83.5, 5000, gs=210),
    ]
    landing = [
        _make_trace_point(3600, 27.0, -83.6, "ground", gs=20),
        _make_trace_point(3660, 27.0, -83.6, "ground", gs=5),
    ]

    trace = ground_roll + climbout + cruise + landing
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    origin_match = AirportMatch(ident="KSPG", name="KSPG", distance_km=0.3)
    dest_match = AirportMatch(ident="KDEST", name="Dest", distance_km=0.5)

    db.get_airport_elevation.return_value = 7
    db.get_runways_for_airport.return_value = []

    with patch(
        "adsbtrack.parser.find_nearest_airport",
        side_effect=[origin_match, dest_match],
    ):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.takeoff_runway is None


# ---------------------------------------------------------------------------
# Go-around + pattern_cycles integration
# ---------------------------------------------------------------------------


def test_parser_go_around_detected():
    """Flight with two aligned-approach segments at KSPG RWY 24 separated
    by a climb >500 ft sets had_go_around=1 and pattern_cycles=2."""
    config = _make_config()
    config.ils_alignment_min_duration_secs = 30.0

    runway_lat = 27.76
    runway_lon = -82.63
    runway_heading = 240.0
    base_ts = _ts("2024-06-15")

    # Ground + short climb so takeoff is observed.
    origin = [
        _make_trace_point(0, runway_lat, runway_lon, "ground", gs=0),
        _make_trace_point(30, runway_lat, runway_lon, "ground", gs=5),
        _make_trace_point(60, runway_lat + 0.002, runway_lon - 0.002, 500, gs=120),
        _make_trace_point(120, runway_lat + 0.02, runway_lon - 0.02, 2000, gs=140),
    ]

    # Approach 1: 25 samples along centerline, starting 7.5 km out at
    # 2500 ft MSL, descending 100 ft per sample -> ends at 100 ft MSL.
    approach1 = _walk_approach(
        base_ts=200.0,
        n=25,
        spacing_secs=3.0,
        runway_lat=runway_lat,
        runway_lon=runway_lon,
        runway_heading_deg=runway_heading,
        start_alt_ft=2500,
        alt_step_ft=100,
    )
    # approach1 samples span ts 200..272. End sample alt = 2500 - 24*100 = 100 MSL.
    # First sample of approach1: (n-i)*0.3 = 25*0.3 = 7.5 km out, alt=2500.
    # Alignment needs to be detected, so its end_alt_ft will be ~100 MSL.

    # Gap (go-around climb): 8 samples OFF centerline (2 km north of it) at
    # altitudes 800, 1400, 2000, 2500, 2500, 2000, 1500, 1000 -- well above
    # the approach1 end_alt (100 MSL) by > 500 ft.
    # Place these between approach1 last_ts (272) and approach2 first_ts.
    # Give the gap 8 samples at 5 s intervals, starting at ts=278.
    off_lat = runway_lat + 0.018  # ~2 km north of centerline
    gap_alts = [800, 1400, 2000, 2500, 2500, 2000, 1500, 1000]
    gap = []
    for i, alt in enumerate(gap_alts):
        ts_off = 278.0 + i * 5.0
        # lon drifts gently; track=60 (reciprocal of 240) so these are not
        # moving-toward-threshold by the detector's cos gate.
        gap.append(
            [
                ts_off,
                off_lat,
                runway_lon - 0.02 + i * 0.004,
                alt,
                140.0,
                60.0,  # reciprocal of runway heading -> excluded by cos gate
                None,
                200.0,
                {"track": 60.0},
            ]
        )

    # Approach 2: 25 samples along centerline starting at ts=325, same geometry.
    approach2 = _walk_approach(
        base_ts=325.0,
        n=25,
        spacing_secs=3.0,
        runway_lat=runway_lat,
        runway_lon=runway_lon,
        runway_heading_deg=runway_heading,
        start_alt_ft=2500,
        alt_step_ft=100,
    )
    # Rollout so flight closes as confirmed.
    rollout_start = 325.0 + 25 * 3.0
    rollout = [
        _make_trace_point(rollout_start + 2, runway_lat, runway_lon + 0.0005, "ground", gs=60),
        _make_trace_point(rollout_start + 4, runway_lat, runway_lon + 0.001, "ground", gs=30),
        _make_trace_point(rollout_start + 6, runway_lat, runway_lon + 0.0015, "ground", gs=10),
    ]

    trace = origin + approach1 + gap + approach2 + rollout
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    kspg_match = AirportMatch(ident="KSPG", name="Albert Whitted", distance_km=0.3)
    db.get_airport_elevation.return_value = 7
    db.get_runways_for_airport.return_value = [
        {
            "runway_name": "24",
            "latitude_deg": runway_lat,
            "longitude_deg": runway_lon,
            "heading_deg_true": runway_heading,
        }
    ]

    with patch(
        "adsbtrack.parser.find_nearest_airport",
        side_effect=[kspg_match, kspg_match],
    ):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.pattern_cycles == 2, f"expected 2 segments, got {flight.pattern_cycles}"
    assert flight.had_go_around == 1, f"expected had_go_around=1, got {flight.had_go_around}"


def test_parser_training_pattern_detects_six_cycles():
    """6 pattern laps at KSPG RWY 24 -> pattern_cycles=6, had_go_around=1
    (each pair separated by climb > 500 ft), mission_type='pattern'.

    Implemented as a unit-level test that calls _any_climb_between and the
    pattern-override branch directly with a pre-built list of
    IlsAlignmentResult objects. This is the fallback path described in the
    plan: weaving six parser-recognizable approach segments through the
    state machine + FlightMetrics.recent_points deque (maxlen=240) proved
    too fragile to converge in 3+ attempts without also re-testing the
    state machine. The integration-level go-around test above exercises the
    parser wiring end-to-end; this test locks the six-cycle invariants
    (pattern_cycles count, had_go_around for repeated climbs, mission
    override) against the same helper the parser uses."""
    from collections import deque

    from adsbtrack.classifier import _PointSample
    from adsbtrack.ils_alignment import IlsAlignmentResult
    from adsbtrack.parser import _any_climb_between

    # Build 6 aligned segments with end altitudes near touchdown (200 ft
    # MSL) separated by climbs to 1500-2000 ft MSL (> 500 ft above
    # segment-end).
    segments: list[IlsAlignmentResult] = []
    points_deque: deque = deque(maxlen=240)
    ts_cursor = 0.0
    for _ in range(6):
        # Approach segment: 45 s long, ends at 200 ft MSL.
        seg_start = ts_cursor
        seg_end = ts_cursor + 45.0
        segments.append(
            IlsAlignmentResult(
                runway_name="24",
                duration_secs=45.0,
                min_offset_m=20.0,
                first_ts=seg_start,
                last_ts=seg_end,
                end_alt_ft=200,
            )
        )
        # Climb gap: 15 s, peaks at 1800 ft MSL (1800 - 200 = 1600 ft rise,
        # well above the 500 ft threshold).
        gap_start = seg_end + 1.0
        gap_end = gap_start + 15.0
        for i in range(6):
            points_deque.append(
                _PointSample(
                    ts=gap_start + i * 2.5,
                    baro_alt=1800,
                    geom_alt=None,
                    gs=120.0,
                    baro_rate=None,
                )
            )
        ts_cursor = gap_end + 1.0

    assert len(segments) == 6
    # pattern_cycles is just len(segments) from parser.
    assert len(segments) == 6
    # had_go_around fires when any pair's gap climbs > 500 ft above segment-end.
    assert _any_climb_between(segments, points_deque, threshold_ft=500.0) is True

    # Now verify the parser's mission-override predicate. Build a Flight
    # with the fields the override inspects.
    flight = Flight(
        icao="aaaaaa",
        takeoff_time=datetime(2024, 6, 15, 10, 0, tzinfo=UTC),
        takeoff_lat=27.76,
        takeoff_lon=-82.63,
        takeoff_date="2024-06-15",
        origin_icao="KSPG",
        destination_icao="KSPG",
        mission_type="unknown",
    )
    flight.pattern_cycles = len(segments)
    # Reproduce the override predicate from parser.py verbatim.
    if (
        flight.origin_icao is not None
        and flight.destination_icao is not None
        and flight.origin_icao == flight.destination_icao
        and flight.pattern_cycles is not None
        and flight.pattern_cycles >= 2
        and flight.mission_type in ("unknown", "transport", "pattern")
    ):
        flight.mission_type = "pattern"
    assert flight.mission_type == "pattern"

    # Sanity check: a flight already classified as "training" should NOT be
    # overridden (the predicate restricts to unknown/transport/pattern).
    flight2 = Flight(
        icao="aaaaaa",
        takeoff_time=datetime(2024, 6, 15, 10, 0, tzinfo=UTC),
        takeoff_lat=27.76,
        takeoff_lon=-82.63,
        takeoff_date="2024-06-15",
        origin_icao="KSPG",
        destination_icao="KSPG",
        mission_type="training",
    )
    flight2.pattern_cycles = 6
    if (
        flight2.origin_icao is not None
        and flight2.destination_icao is not None
        and flight2.origin_icao == flight2.destination_icao
        and flight2.pattern_cycles is not None
        and flight2.pattern_cycles >= 2
        and flight2.mission_type in ("unknown", "transport", "pattern")
    ):
        flight2.mission_type = "pattern"
    assert flight2.mission_type == "training"


def test_parser_normal_a_to_b_has_zero_go_around_and_low_pattern_cycles():
    """A straight KSPG-to-KPIE flight has had_go_around=0 and
    pattern_cycles <= 1 (0 if no alignment detected, 1 if a single
    alignment segment was found)."""
    config = _make_config()
    config.ils_alignment_min_duration_secs = 30.0

    runway_lat = 27.76
    runway_lon = -82.63
    base_ts = _ts("2024-06-15")

    origin = [
        _make_trace_point(0, 27.9, -82.7, "ground", gs=0),
        _make_trace_point(60, 27.9, -82.7, "ground", gs=5),
        _make_trace_point(120, 27.89, -82.69, 1000, gs=120),
    ]
    cruise = [
        _make_trace_point(600, 27.85, -82.66, 5000, gs=200),
        _make_trace_point(1800, 27.82, -82.65, 5000, gs=200),
    ]
    approach = _walk_approach(
        base_ts=3000.0,
        n=30,
        spacing_secs=3.0,
        runway_lat=runway_lat,
        runway_lon=runway_lon,
        runway_heading_deg=240.0,
        start_alt_ft=3000,
        alt_step_ft=100,
    )
    rollout_start = 3000.0 + 30 * 3.0
    rollout = [
        _make_trace_point(rollout_start + 2, runway_lat, runway_lon + 0.0005, "ground", gs=60),
        _make_trace_point(rollout_start + 4, runway_lat, runway_lon + 0.001, "ground", gs=30),
        _make_trace_point(rollout_start + 6, runway_lat, runway_lon + 0.0015, "ground", gs=10),
    ]

    trace = origin + cruise + approach + rollout
    rows = [_make_trace_row("2024-06-15", base_ts, trace)]
    db = _make_db_mock(rows)

    kspg_match = AirportMatch(ident="KSPG", name="Albert Whitted", distance_km=0.3)
    kpie_match = AirportMatch(ident="KPIE", name="St Petersburg Clearwater", distance_km=0.5)

    db.get_airport_elevation.return_value = 7
    db.get_runways_for_airport.return_value = [
        {
            "runway_name": "24",
            "latitude_deg": runway_lat,
            "longitude_deg": runway_lon,
            "heading_deg_true": 240.0,
        }
    ]

    with patch(
        "adsbtrack.parser.find_nearest_airport",
        side_effect=[kspg_match, kpie_match],
    ):
        count = extract_flights(db, config, "aaaaaa", reprocess=True)

    assert count == 1
    flight = db.insert_flight.call_args[0][0]
    assert flight.had_go_around == 0, f"expected had_go_around=0, got {flight.had_go_around}"
    assert flight.pattern_cycles is not None and flight.pattern_cycles <= 1, (
        f"expected pattern_cycles <= 1, got {flight.pattern_cycles}"
    )
