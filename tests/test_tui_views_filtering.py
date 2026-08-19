"""Tests for the pure needle-match functions used by the TUI's filter bars.

Task 14 split each filterable view's data fetch from its filtering: refresh_data
queries and caches rows, on_input_changed re-filters the cache via one of these
module-level functions. These tests exercise the filter functions directly --
no Textual app, no DB -- matching the exact field set and case-insensitive
substring semantics the pre-split in-view ``_matches`` helpers used.
"""

from __future__ import annotations

from datetime import UTC, datetime

from adsbtrack.events import Event
from adsbtrack.tui.queries import AircraftRow, FlightRow, JumpMatch
from adsbtrack.tui.views.aircraft import filter_aircraft
from adsbtrack.tui.views.events import filter_events
from adsbtrack.tui.views.flights import filter_flights
from adsbtrack.tui.views.jump import filter_jump_matches

# ---------------------------------------------------------------------------
# events
# ---------------------------------------------------------------------------


def _event(**overrides) -> Event:
    fields = dict(
        ts=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
        icao="a1b2c3",
        callsign="UAL1",
        event_type="emergency_squawk",
        severity="emergency",
        summary="squawked 7700 over KEWR",
    )
    fields.update(overrides)
    return Event(**fields)


def test_filter_events_empty_needle_returns_all():
    rows = [_event(), _event(icao="d4e5f6", callsign="DAL2")]
    assert filter_events(rows, "") == rows


def test_filter_events_matches_event_type():
    rows = [_event(event_type="emergency_squawk"), _event(event_type="go_around")]
    result = filter_events(rows, "go_around")
    assert [e.event_type for e in result] == ["go_around"]


def test_filter_events_matches_icao_case_insensitive():
    rows = [_event(icao="a1b2c3"), _event(icao="ffffff")]
    result = filter_events(rows, "A1B2")
    assert [e.icao for e in result] == ["a1b2c3"]


def test_filter_events_matches_callsign():
    rows = [_event(callsign="UAL1"), _event(callsign="DAL2")]
    result = filter_events(rows, "dal")
    assert [e.callsign for e in result] == ["DAL2"]


def test_filter_events_matches_summary():
    rows = [_event(summary="squawked 7700"), _event(summary="long hover 12m")]
    result = filter_events(rows, "hover")
    assert [e.summary for e in result] == ["long hover 12m"]


def test_filter_events_skips_none_callsign():
    rows = [_event(callsign=None, summary="no callsign event")]
    result = filter_events(rows, "callsign")
    assert len(result) == 1


def test_filter_events_no_match_returns_empty():
    rows = [_event()]
    assert filter_events(rows, "zzzzzz") == []


# ---------------------------------------------------------------------------
# flights
# ---------------------------------------------------------------------------


def _flight(**overrides) -> FlightRow:
    fields = dict(
        takeoff_time="2026-03-01T12:00:00+00:00",
        takeoff_date="2026-03-01",
        origin_icao="KEWR",
        destination_icao="KBOS",
        duration_minutes=120.0,
        callsign="UAL1",
        mission_type="transport",
        max_altitude=35000,
        cruise_gs_kt=430,
        landing_type="confirmed",
        landing_confidence=0.9,
        emergency_squawk=None,
        had_go_around=None,
        max_hover_secs=None,
    )
    fields.update(overrides)
    return FlightRow(**fields)


def test_filter_flights_empty_needle_returns_all():
    rows = [_flight(), _flight(callsign="DAL2")]
    assert filter_flights(rows, "") == rows


def test_filter_flights_matches_origin():
    rows = [_flight(origin_icao="KEWR"), _flight(origin_icao="KJFK")]
    result = filter_flights(rows, "jfk")
    assert [r.origin_icao for r in result] == ["KJFK"]


def test_filter_flights_matches_destination():
    rows = [_flight(destination_icao="KBOS"), _flight(destination_icao="KLAX")]
    result = filter_flights(rows, "lax")
    assert [r.destination_icao for r in result] == ["KLAX"]


def test_filter_flights_matches_callsign_case_insensitive():
    rows = [_flight(callsign="UAL1"), _flight(callsign="DAL2")]
    result = filter_flights(rows, "ual")
    assert [r.callsign for r in result] == ["UAL1"]


def test_filter_flights_matches_takeoff_date():
    rows = [_flight(takeoff_date="2026-03-01"), _flight(takeoff_date="2026-04-15")]
    result = filter_flights(rows, "04-15")
    assert [r.takeoff_date for r in result] == ["2026-04-15"]


def test_filter_flights_matches_mission_type():
    rows = [_flight(mission_type="transport"), _flight(mission_type="training")]
    result = filter_flights(rows, "train")
    assert [r.mission_type for r in result] == ["training"]


def test_filter_flights_skips_none_fields():
    rows = [_flight(origin_icao=None, destination_icao=None, callsign=None, mission_type=None)]
    result = filter_flights(rows, "kewr")
    assert result == []


def test_filter_flights_no_match_returns_empty():
    rows = [_flight()]
    assert filter_flights(rows, "zzzzzz") == []


# ---------------------------------------------------------------------------
# aircraft
# ---------------------------------------------------------------------------


def _aircraft(**overrides) -> AircraftRow:
    fields = dict(
        icao="aaa111",
        registration="N111AA",
        type_code="B738",
        description="BOEING 737-800",
        total_flights=5,
        total_hours=12.5,
        home_base_icao="KEWR",
        last_seen="2026-03-01",
        spoof_count=0,
        is_military=0,
        flags="",
    )
    fields.update(overrides)
    return AircraftRow(**fields)


def test_filter_aircraft_empty_needle_returns_all():
    rows = [_aircraft(), _aircraft(icao="bbb222")]
    assert filter_aircraft(rows, "") == rows


def test_filter_aircraft_matches_icao():
    rows = [_aircraft(icao="aaa111"), _aircraft(icao="bbb222")]
    result = filter_aircraft(rows, "bbb")
    assert [r.icao for r in result] == ["bbb222"]


def test_filter_aircraft_matches_registration_case_insensitive():
    rows = [_aircraft(registration="N111AA"), _aircraft(registration="A6-EEN")]
    result = filter_aircraft(rows, "111aa")
    assert [r.registration for r in result] == ["N111AA"]


def test_filter_aircraft_matches_type_code():
    rows = [_aircraft(type_code="B738"), _aircraft(type_code="A388")]
    result = filter_aircraft(rows, "a388")
    assert [r.type_code for r in result] == ["A388"]


def test_filter_aircraft_matches_home_base():
    rows = [_aircraft(home_base_icao="KEWR"), _aircraft(home_base_icao="KJFK")]
    result = filter_aircraft(rows, "jfk")
    assert [r.home_base_icao for r in result] == ["KJFK"]


def test_filter_aircraft_does_not_match_description():
    """Mirrors the SQL LIKE clause: description is not one of the matched columns."""
    rows = [_aircraft(description="BOEING 737-800", type_code="B738")]
    result = filter_aircraft(rows, "boeing")
    assert result == []


def test_filter_aircraft_skips_none_home_base():
    rows = [_aircraft(home_base_icao=None)]
    result = filter_aircraft(rows, "kewr")
    assert result == []


def test_filter_aircraft_no_match_returns_empty():
    rows = [_aircraft()]
    assert filter_aircraft(rows, "zzzzzz") == []


# ---------------------------------------------------------------------------
# jump-to-hex palette
# ---------------------------------------------------------------------------


def _jump_match(**overrides) -> JumpMatch:
    fields = dict(
        icao="aaa111",
        registration="N111AA",
        type_code="B738",
        description="BOEING 737-800",
    )
    fields.update(overrides)
    return JumpMatch(**fields)


def test_filter_jump_matches_empty_needle_returns_all():
    rows = [_jump_match(), _jump_match(icao="bbb222")]
    assert filter_jump_matches(rows, "") == rows


def test_filter_jump_matches_matches_icao():
    rows = [_jump_match(icao="aaa111"), _jump_match(icao="bbb222")]
    result = filter_jump_matches(rows, "bbb")
    assert [r.icao for r in result] == ["bbb222"]


def test_filter_jump_matches_matches_registration_case_insensitive():
    rows = [_jump_match(registration="N111AA"), _jump_match(registration="A6-EEN")]
    result = filter_jump_matches(rows, "111aa")
    assert [r.registration for r in result] == ["N111AA"]


def test_filter_jump_matches_matches_type_code():
    rows = [_jump_match(type_code="B738"), _jump_match(type_code="A388")]
    result = filter_jump_matches(rows, "a388")
    assert [r.type_code for r in result] == ["A388"]


def test_filter_jump_matches_matches_description():
    """Unlike filter_aircraft, the jump palette matches on description --
    mirrors queries.search_aircraft's SQL match set (icao, registration,
    type_code, description), which does not include home_base_icao."""
    rows = [_jump_match(description="BOEING 737-800"), _jump_match(description="AIRBUS A-380-800")]
    result = filter_jump_matches(rows, "airbus")
    assert [r.description for r in result] == ["AIRBUS A-380-800"]


def test_filter_jump_matches_skips_none_fields():
    rows = [_jump_match(registration=None, type_code=None, description=None)]
    result = filter_jump_matches(rows, "n111aa")
    assert result == []


def test_filter_jump_matches_no_match_returns_empty():
    rows = [_jump_match()]
    assert filter_jump_matches(rows, "zzzzzz") == []
