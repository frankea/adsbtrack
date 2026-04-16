"""Tests for adsbtrack.parser -- flight extraction state machine."""

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

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
    from adsbtrack.airports import AirportMatch

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
    from adsbtrack.airports import AirportMatch

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
