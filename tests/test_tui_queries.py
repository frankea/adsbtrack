"""Tests for the TUI's read-only query layer.

The TUI renders the output of these functions; the functions themselves
are plain dataclass returns so we can test them without a running
Textual app. Every test builds a throwaway Database fixture so the
migrations and schema stay in the loop.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from adsbtrack.db import Database
from adsbtrack.models import Flight
from adsbtrack.tui.queries import (
    count_aircraft,
    count_flights,
    count_trace_bytes,
    distinct_dates_for_icao,
    list_aircraft,
    list_flights,
    list_spoofed_broadcasts,
    load_trace_points,
    search_aircraft,
    status_snapshot,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def seeded_db(tmp_path):
    """DB with two aircraft: one clean, one with a rejected spoof broadcast."""
    db_path = tmp_path / "tui.db"
    with Database(db_path) as db:
        # Clean aircraft
        db.insert_flight(
            Flight(
                icao="aaa111",
                takeoff_time=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
                takeoff_lat=40.0,
                takeoff_lon=-74.0,
                takeoff_date="2026-03-01",
                landing_time=datetime(2026, 3, 1, 14, 0, tzinfo=UTC),
                landing_type="confirmed",
                callsign="UAL1",
                destination_icao="KBOS",
                origin_icao="KEWR",
                duration_minutes=120.0,
                max_altitude=35000,
                cruise_gs_kt=430,
                landing_confidence=0.9,
                mission_type="transport",
            )
        )
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("aaa111", "N111AA", "B738", "BOEING 737-800"),
        )
        # Rejected-as-spoofed broadcast
        db.insert_spoofed_broadcast(
            icao="bbb222",
            takeoff_time="2026-04-21T00:49:47.580000+00:00",
            landing_time="2026-04-21T01:41:52.140000+00:00",
            takeoff_date="2026-04-21",
            callsign="EK01",
            takeoff_lat=25.25,
            takeoff_lon=55.38,
            landing_lat=27.14,
            landing_lon=55.55,
            max_altitude=250,
            data_points=350,
            sources="adsbfi,adsbx",
            origin_icao=None,
            destination_icao=None,
            reason="bimodal_integrity",
            reason_detail=json.dumps(
                {
                    "date": "2026-04-21",
                    "v2_samples": 350,
                    "v2_sil0_pct": 25.14,
                    "v2_nic0_pct": 27.14,
                    "sources": ["adsbfi", "adsbx"],
                    "source_rates": [["adsbfi", 26.04], ["adsbx", 24.31]],
                }
            ),
        )
        db.conn.execute(
            "INSERT INTO aircraft_registry (icao, registration, type_code, description) VALUES (?, ?, ?, ?)",
            ("bbb222", "A6-EEN", "A388", "AIRBUS A-380-800"),
        )
        db.refresh_aircraft_stats("aaa111")
        db.commit()
    return db_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_count_helpers(seeded_db):
    with Database(seeded_db) as db:
        assert count_flights(db) == 1
        assert count_aircraft(db) >= 1


def test_list_aircraft_returns_rows(seeded_db):
    with Database(seeded_db) as db:
        rows = list_aircraft(db)
    assert any(r.icao == "aaa111" for r in rows)
    row = next(r for r in rows if r.icao == "aaa111")
    assert row.registration == "N111AA"
    assert row.total_flights >= 1


def test_list_aircraft_filter_matches_registration(seeded_db):
    with Database(seeded_db) as db:
        rows = list_aircraft(db, filter_substr="111aa")
    assert [r.icao for r in rows] == ["aaa111"]


def test_list_aircraft_filter_non_match(seeded_db):
    with Database(seeded_db) as db:
        rows = list_aircraft(db, filter_substr="zzzzzz")
    assert rows == []


def test_list_flights_per_icao(seeded_db):
    with Database(seeded_db) as db:
        flights = list_flights(db, "aaa111")
    assert len(flights) == 1
    assert flights[0].callsign == "UAL1"
    assert flights[0].origin_icao == "KEWR"


def test_list_flights_unknown_icao(seeded_db):
    with Database(seeded_db) as db:
        assert list_flights(db, "ffffff") == []


def test_list_spoofed_broadcasts(seeded_db):
    with Database(seeded_db) as db:
        rows = list_spoofed_broadcasts(db)
    assert len(rows) == 1
    row = rows[0]
    assert row.icao == "bbb222"
    assert row.reason == "bimodal_integrity"
    assert isinstance(row.reason_detail, dict)
    assert row.reason_detail["v2_samples"] == 350


def test_list_spoofed_broadcasts_filtered_by_icao(seeded_db):
    with Database(seeded_db) as db:
        assert list_spoofed_broadcasts(db, icao="aaa111") == []
        bbb = list_spoofed_broadcasts(db, icao="bbb222")
    assert len(bbb) == 1


def test_status_snapshot_has_registry_and_stats(seeded_db):
    with Database(seeded_db) as db:
        snap = status_snapshot(db, "aaa111")
    assert snap["icao"] == "aaa111"
    assert snap["registry"]["registration"] == "N111AA"
    assert snap["stats"]["total_flights"] == 1


def test_status_snapshot_reports_spoof_count(seeded_db):
    with Database(seeded_db) as db:
        snap = status_snapshot(db, "bbb222")
    assert snap["spoof_count"] == 1


def test_load_trace_points_empty_when_no_trace(seeded_db):
    with Database(seeded_db) as db:
        pts = load_trace_points(db, "aaa111", "2099-01-01")
    assert pts == []


def test_distinct_dates_for_icao(seeded_db):
    with Database(seeded_db) as db:
        assert distinct_dates_for_icao(db, "aaa111") == []


def test_count_trace_bytes_empty(seeded_db):
    with Database(seeded_db) as db:
        assert count_trace_bytes(db) == 0


def test_count_trace_bytes_counts_stored_json(seeded_db):
    with Database(seeded_db) as db:
        db.conn.execute(
            "INSERT INTO trace_days (icao, date, source, timestamp, trace_json, point_count, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("aaa111", "2026-04-20", "adsbx", 1_700_000_000.0, "[[0,40,-74,1000]]", 1, "2026-04-21T00:00:00Z"),
        )
        db.commit()
        assert count_trace_bytes(db) == len("[[0,40,-74,1000]]")


def test_search_aircraft_by_icao(seeded_db):
    with Database(seeded_db) as db:
        hits = search_aircraft(db, "aaa")
    assert [h.icao for h in hits] == ["aaa111"]


def test_search_aircraft_by_description(seeded_db):
    with Database(seeded_db) as db:
        hits = search_aircraft(db, "737")
    assert any(h.icao == "aaa111" for h in hits)


def test_search_aircraft_empty_query_returns_list(seeded_db):
    with Database(seeded_db) as db:
        hits = search_aircraft(db, "")
    assert hits  # at least the seeded aircraft


def test_status_snapshot_includes_indicators(seeded_db):
    with Database(seeded_db) as db:
        snap = status_snapshot(db, "aaa111")
    stats = snap["stats"]
    # Indicators: seeded flight is a clean confirmed landing, no emergencies.
    assert stats["confirmed_landings"] == 1
    assert stats["emergency_flights"] == 0
    assert stats["signal_lost_landings"] == 0
    assert stats["off_airport_landings"] == 0
    assert stats["days_with_data"] == 0
