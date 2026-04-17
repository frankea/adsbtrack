"""Tests for adsbtrack.events -- per-flight event timeline.

Events are a rendering layer over flight-level columns already populated
by the extractor. No heuristics here beyond "is this flight notable"
predicates; the signals themselves (emergency_squawk, emergency_flag,
go_around_count, max_hover_secs, destination_icao/helipad_id) are
already validated by classifier/features tests.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from adsbtrack.db import Database
from adsbtrack.events import collect_events
from adsbtrack.models import Flight

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_with_events(tmp_path):
    """DB populated with flights that each trigger one event type."""
    db_path = tmp_path / "events.db"
    with Database(db_path) as db:
        # 1. Emergency squawk (7700)
        db.insert_flight(
            Flight(
                icao="aaa001",
                takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-06-15",
                landing_time=datetime(2024, 6, 15, 13, 30, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL100",
                emergency_squawk="7700",
                destination_icao="KBOS",
            )
        )
        # 2. Emergency flag (nordo)
        db.insert_flight(
            Flight(
                icao="aaa001",
                takeoff_time=datetime(2024, 7, 1, 10, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-07-01",
                landing_time=datetime(2024, 7, 1, 11, 0, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL101",
                emergency_flag="nordo",
                destination_icao="KBOS",
            )
        )
        # 3. Off-airport landing
        db.insert_flight(
            Flight(
                icao="aaa001",
                takeoff_time=datetime(2024, 8, 1, 14, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-08-01",
                landing_time=datetime(2024, 8, 1, 15, 0, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL102",
                destination_icao=None,
                destination_helipad_id=None,
            )
        )
        # 4. Long hover (600s = 10 min)
        db.insert_flight(
            Flight(
                icao="aaa001",
                takeoff_time=datetime(2024, 9, 1, 9, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-09-01",
                landing_time=datetime(2024, 9, 1, 10, 0, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL103",
                max_hover_secs=600,
                destination_icao="KBOS",
            )
        )
        # 5. Multiple go-arounds (3)
        db.insert_flight(
            Flight(
                icao="aaa001",
                takeoff_time=datetime(2024, 10, 1, 16, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-10-01",
                landing_time=datetime(2024, 10, 1, 17, 30, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL104",
                go_around_count=3,
                destination_icao="KBOS",
            )
        )
        # 6. Un-noteworthy baseline flight (should emit NO events)
        db.insert_flight(
            Flight(
                icao="aaa001",
                takeoff_time=datetime(2024, 11, 1, 8, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-11-01",
                landing_time=datetime(2024, 11, 1, 9, 0, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL105",
                go_around_count=1,  # single go-around is noise, not event
                max_hover_secs=60,  # short hover is noise
                destination_icao="KBOS",
            )
        )
        db.commit()
    return db_path


# ---------------------------------------------------------------------------
# collect_events: happy paths
# ---------------------------------------------------------------------------


def test_collect_events_finds_all_five_types(db_with_events):
    """Each of the five noteworthy flights produces exactly one event;
    the plain flight produces none. Sort order: newest first (by ts)."""
    with Database(db_with_events) as db:
        events = collect_events(db, "aaa001")

    assert len(events) == 5
    types = {e.event_type for e in events}
    assert types == {
        "emergency_squawk",
        "emergency_flag",
        "off_airport_landing",
        "long_hover",
        "multiple_go_arounds",
    }
    # Newest first
    timestamps = [e.ts for e in events]
    assert timestamps == sorted(timestamps, reverse=True)


def test_collect_events_severity_labels(db_with_events):
    with Database(db_with_events) as db:
        events = collect_events(db, "aaa001")
    by_type = {e.event_type: e for e in events}
    assert by_type["emergency_squawk"].severity == "emergency"
    assert by_type["emergency_flag"].severity == "emergency"
    assert by_type["off_airport_landing"].severity == "unusual"
    assert by_type["long_hover"].severity == "unusual"
    assert by_type["multiple_go_arounds"].severity == "unusual"


def test_collect_events_emergency_filter(db_with_events):
    with Database(db_with_events) as db:
        events = collect_events(db, "aaa001", severity="emergency")
    assert len(events) == 2
    assert {e.event_type for e in events} == {"emergency_squawk", "emergency_flag"}


def test_collect_events_unusual_filter(db_with_events):
    with Database(db_with_events) as db:
        events = collect_events(db, "aaa001", severity="unusual")
    assert len(events) == 3
    assert {e.event_type for e in events} == {
        "off_airport_landing",
        "long_hover",
        "multiple_go_arounds",
    }


def test_collect_events_since_filter(db_with_events):
    """--since 2024-09-01 should exclude flights from June / July / August."""
    with Database(db_with_events) as db:
        events = collect_events(db, "aaa001", since=datetime(2024, 9, 1, tzinfo=UTC))
    types = {e.event_type for e in events}
    assert types == {"long_hover", "multiple_go_arounds"}


def test_collect_events_returns_empty_for_unknown_icao(db_with_events):
    with Database(db_with_events) as db:
        events = collect_events(db, "ffffff")
    assert events == []


# ---------------------------------------------------------------------------
# Noise-rejection boundary cases
# ---------------------------------------------------------------------------


def test_go_around_count_1_is_not_event(tmp_path):
    """Single go-around = noise (one missed approach happens routinely).
    Event requires >= 2."""
    db_path = tmp_path / "ga.db"
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="bbb001",
                takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-06-15",
                go_around_count=1,
                destination_icao="KBOS",
            )
        )
        db.commit()
        events = collect_events(db, "bbb001")
    assert [e for e in events if e.event_type == "multiple_go_arounds"] == []


def test_short_hover_is_not_event(tmp_path):
    """Hover < 300s (5 min) doesn't qualify; many helicopters hover
    briefly at approach and that's not an event."""
    db_path = tmp_path / "hov.db"
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="bbb002",
                takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-06-15",
                max_hover_secs=120,  # 2 min
                destination_icao="KBOS",
            )
        )
        db.commit()
        events = collect_events(db, "bbb002")
    assert [e for e in events if e.event_type == "long_hover"] == []


def test_airport_match_is_not_off_airport(tmp_path):
    """Confirmed landing with an airport match is NOT an off-airport
    event, even if no helipad match."""
    db_path = tmp_path / "apt.db"
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="bbb003",
                takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-06-15",
                landing_type="confirmed",
                destination_icao="KBOS",
                destination_helipad_id=None,
            )
        )
        db.commit()
        events = collect_events(db, "bbb003")
    assert events == []


def test_non_confirmed_landing_is_not_off_airport(tmp_path):
    """landing_type='signal_lost' with no airport match isn't a confirmed
    off-airport landing; we don't know where it landed. Skip."""
    db_path = tmp_path / "sig.db"
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="bbb004",
                takeoff_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2024-06-15",
                landing_type="signal_lost",
                destination_icao=None,
                destination_helipad_id=None,
            )
        )
        db.commit()
        events = collect_events(db, "bbb004")
    assert events == []
