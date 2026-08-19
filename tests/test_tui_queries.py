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
    distinct_dates_for_icao,
    list_aircraft,
    list_events,
    list_flights,
    list_spoofed_broadcasts,
    load_trace_points,
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


def test_load_trace_points_reads_legacy_and_compressed_rows(seeded_db):
    """load_trace_points must pool a normally-inserted (compressed) row
    with a hand-inserted legacy raw-JSON TEXT row for the same icao/date --
    Task 11's sniff has to work through this reader too."""
    compressed_trace = [[0, 40.1, -74.1, 5500, 210, None, None, None, {}, "adsb_icao"]]
    legacy_trace = [[10, 41.2, -75.2, 6000, 220, None, None, None, {}, "mlat"]]
    with Database(seeded_db) as db:
        db.insert_trace_day(
            "aaa111",
            "2026-05-01",
            {"timestamp": 1700000000.0, "trace": compressed_trace},
            source="adsbx",
        )
        db.conn.execute(
            """INSERT INTO trace_days
               (icao, date, source, registration, type_code, description, owner_operator,
                year, timestamp, trace_json, point_count, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "aaa111",
                "2026-05-01",
                "adsbfi",
                None,
                None,
                None,
                None,
                None,
                1700000000.0,
                json.dumps(legacy_trace),
                len(legacy_trace),
                datetime.now(UTC).isoformat(),
            ),
        )
        db.commit()
        pts = load_trace_points(db, "aaa111", "2026-05-01")

    assert {p.lat for p in pts} == {40.1, 41.2}
    assert {p.source for p in pts} == {"adsb_icao", "mlat"}


def test_distinct_dates_for_icao(seeded_db):
    with Database(seeded_db) as db:
        assert distinct_dates_for_icao(db, "aaa111") == []


def _spoof_sample(sil):
    ac = {"version": 2, "nic": 8, "sil": sil, "flight": "UAL1"}
    return [0.0, 25.25, 55.38, "ground", 0.5, 30.9, 0, None, ac, "adsb_icao", None, None, None, None]


def test_list_events_all_aircraft_finds_spoof_events_via_stat_columns(tmp_path):
    """The icao=None (all-aircraft) events view must surface a spoof event
    for a fully-optimized aircraft through the Task 12 grouped-query path,
    not just the per-aircraft decode loop."""
    db_path = tmp_path / "all_events.db"
    spoofy = [_spoof_sample(3) for _ in range(40)] + [_spoof_sample(0) for _ in range(20)]
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="111111",
                takeoff_time=datetime(2026, 4, 21, 1, 0, tzinfo=UTC),
                takeoff_lat=25.25,
                takeoff_lon=55.38,
                takeoff_date="2026-04-21",
            )
        )
        db.insert_trace_day("111111", "2026-04-21", {"timestamp": 1776600000.0, "trace": spoofy})
        db.commit()
        events = list_events(db)
    spoof = [e for e in events if e.event_type == "spoof_bimodal_integrity"]
    assert len(spoof) == 1
    assert spoof[0].icao == "111111"


def test_list_events_all_aircraft_finds_spoof_events_via_decode_fallback(tmp_path):
    """Same icao=None view, but for an aircraft whose trace_days row predates
    Task 12's materialized stat columns (v2_samples/v2_sil0/v2_nic0/v2_callsigns
    all NULL, as a raw legacy insert leaves them). bulk_detect_spoof_events must
    route this icao through the per-aircraft decode fallback instead of dropping
    it from the grouped-stats query."""
    db_path = tmp_path / "all_events_fallback.db"
    spoofy = [_spoof_sample(3) for _ in range(40)] + [_spoof_sample(0) for _ in range(20)]
    with Database(db_path) as db:
        db.insert_flight(
            Flight(
                icao="222222",
                takeoff_time=datetime(2026, 4, 21, 1, 0, tzinfo=UTC),
                takeoff_lat=25.25,
                takeoff_lon=55.38,
                takeoff_date="2026-04-21",
            )
        )
        # Bypasses insert_trace_day's count_v2_integrity computation, so the
        # v2_* stat columns stay NULL -- the condition _trace_days_needs_fallback
        # checks for.
        db.conn.execute(
            """INSERT INTO trace_days
               (icao, date, source, registration, type_code, description, owner_operator,
                year, timestamp, trace_json, point_count, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "222222",
                "2026-04-21",
                "adsbx",
                None,
                None,
                None,
                None,
                None,
                1776600000.0,
                json.dumps(spoofy),
                len(spoofy),
                datetime.now(UTC).isoformat(),
            ),
        )
        db.commit()
        events = list_events(db)
    spoof = [e for e in events if e.event_type == "spoof_bimodal_integrity"]
    assert len(spoof) == 1
    assert spoof[0].icao == "222222"


def test_list_aircraft_respects_row_limit(tmp_path):
    """list_aircraft's LIMIT clause caps the returned row count even when
    aircraft_stats holds more rows than the limit. Seeded directly via SQL
    (skipping insert_flight/refresh_aircraft_stats) since only the row count
    matters here."""
    db_path = tmp_path / "many_aircraft.db"
    with Database(db_path) as db:
        for i in range(8):
            db.conn.execute(
                "INSERT INTO aircraft_stats (icao, total_flights, total_hours, last_seen) VALUES (?, ?, ?, ?)",
                (f"{i:06x}", 1, 1.0, f"2026-01-{i + 1:02d}"),
            )
        db.commit()
        rows = list_aircraft(db, limit=5)
    assert len(rows) == 5
