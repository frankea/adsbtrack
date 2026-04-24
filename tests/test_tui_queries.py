"""Tests for the TUI's read-only query layer.

The TUI renders the output of these functions; the functions themselves
are plain dataclass returns so we can test them without a running
Textual app. Every test builds a throwaway Database fixture so the
migrations and schema stay in the loop.
"""

from __future__ import annotations

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

# Fixtures live in tests/conftest.py so the TUI app smoke tests can reuse them.


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


def _flight(icao: str, hour: int, **overrides):
    base = dict(
        icao=icao,
        takeoff_time=datetime(2026, 3, 2, hour, 0, tzinfo=UTC),
        takeoff_lat=40.0,
        takeoff_lon=-74.0,
        takeoff_date="2026-03-02",
        landing_time=datetime(2026, 3, 2, hour + 1, 0, tzinfo=UTC),
        landing_type="confirmed",
        callsign="TEST",
        destination_icao="KBOS",
        origin_icao="KEWR",
        duration_minutes=60.0,
        max_altitude=35000,
        cruise_gs_kt=430,
        landing_confidence=0.9,
        mission_type="transport",
    )
    base.update(overrides)
    return Flight(**base)


def test_status_snapshot_indicator_branches_each_hit_one(tmp_path):
    db_path = tmp_path / "indicators.db"
    with Database(db_path) as db:
        icao = "ccc333"
        db.insert_flight(_flight(icao, hour=1, emergency_squawk="7700"))
        db.insert_flight(_flight(icao, hour=3, had_go_around=1))
        db.insert_flight(_flight(icao, hour=5, max_hover_secs=600))
        db.insert_flight(_flight(icao, hour=7, landing_type="signal_lost"))
        db.insert_flight(_flight(icao, hour=9, landing_type="confirmed", destination_icao=None))
        db.refresh_aircraft_stats(icao)
        db.commit()
        snap = status_snapshot(db, icao)
    stats = snap["stats"]
    assert stats["emergency_flights"] == 1
    assert stats["go_around_flights"] == 1
    assert stats["long_hover_flights"] == 1
    assert stats["signal_lost_landings"] == 1
    assert stats["off_airport_landings"] == 1
    # off_airport is a subset of confirmed (same landing_type='confirmed'
    # filter plus a null-destination clause), so the hour=9 row counts in
    # both - do not "fix" 4 to 3.
    assert stats["confirmed_landings"] == 4


def test_status_snapshot_unknown_icao(seeded_db):
    with Database(seeded_db) as db:
        snap = status_snapshot(db, "ffffff")
    assert snap["icao"] == "ffffff"
    assert snap["stats"] is None
    assert snap["registry"] is None
    assert snap["sources"] is None
    assert snap["missions"] == []
    assert snap["spoof_count"] == 0


def test_status_snapshot_days_with_data_counts_trace_days(tmp_path):
    db_path = tmp_path / "days.db"
    with Database(db_path) as db:
        icao = "ddd444"
        other = "ddd445"
        db.insert_flight(_flight(icao, hour=2))
        db.insert_flight(_flight(other, hour=2))
        db.refresh_aircraft_stats(icao)
        for date in ("2026-03-02", "2026-03-03", "2026-03-03"):
            db.insert_trace_day(
                icao,
                date,
                {"timestamp": 1_700_000_000.0, "trace": [[0, 40.0, -74.0, 1000]]},
                source="adsbx" if date != "2026-03-03" else "airplaneslive",
            )
        # Seed a trace_day for the other ICAO on a date not shared with icao.
        # A regression that drops the WHERE icao = ? predicate in queries.py
        # would incorrectly count this row and push days_with_data to 3.
        db.insert_trace_day(
            other,
            "2026-03-04",
            {"timestamp": 1_700_000_000.0, "trace": [[0, 40.0, -74.0, 1000]]},
            source="adsbx",
        )
        db.commit()
        snap = status_snapshot(db, icao)
    # 2026-03-02 and 2026-03-03; the second date is inserted twice across
    # different sources but COUNT(DISTINCT date) collapses them.
    assert snap["stats"]["days_with_data"] == 2


def test_status_snapshot_sources_weighted_average(tmp_path):
    db_path = tmp_path / "sources.db"
    with Database(db_path) as db:
        icao = "eee555"
        # Two flights, one ADS-B-heavy with many points, one MLAT-heavy
        # with few. The weighted ADS-B pct should favour the first flight.
        db.insert_flight(_flight(icao, hour=1, adsb_pct=90.0, mlat_pct=10.0, tisb_pct=0.0, data_points=900))
        db.insert_flight(_flight(icao, hour=3, adsb_pct=10.0, mlat_pct=90.0, tisb_pct=0.0, data_points=100))
        db.refresh_aircraft_stats(icao)
        db.commit()
        snap = status_snapshot(db, icao)
    src = snap["sources"]
    # (90 * 900 + 10 * 100) / (900 + 100) = 82.0
    assert src["adsb"] == pytest.approx(82.0)
    assert src["mlat"] == pytest.approx(18.0)
    assert src["total_points"] == 1000


def test_status_snapshot_missions_filters_nulls_and_limits_to_six(tmp_path):
    db_path = tmp_path / "missions.db"
    with Database(db_path) as db:
        icao = "fff666"
        # Null mission_type is dropped by the queries layer. Seed one null
        # plus seven distinct missions (to exercise LIMIT 6); seed the
        # "training" mission twice so its count is 2, pinning the ORDER BY
        # n DESC head of the list and avoiding unspecified tie-break
        # ordering when every bucket has n=1.
        db.insert_flight(_flight(icao, hour=1, mission_type=None))
        db.insert_flight(_flight(icao, hour=2, mission_type="training"))
        for hour, name in enumerate(
            ["training", "transport", "cargo", "medical", "survey", "patrol", "sightseeing"], start=3
        ):
            db.insert_flight(_flight(icao, hour=hour, mission_type=name))
        db.refresh_aircraft_stats(icao)
        db.commit()
        snap = status_snapshot(db, icao)
    names = [m[0] for m in snap["missions"]]
    assert None not in names, "null mission_type should be filtered out"
    assert len(snap["missions"]) == 6, "LIMIT 6 not enforced"
    assert snap["missions"][0] == ("training", 2), "ORDER BY n DESC head not pinned"


def test_status_snapshot_sources_returns_none_when_all_points_zero(tmp_path):
    db_path = tmp_path / "zero_points.db"
    with Database(db_path) as db:
        icao = "ggg777"
        # Every flight has data_points=0 so the `WHERE data_points > 0`
        # predicate in queries.py filters everything out; the status_snapshot
        # sources key should collapse to None rather than crashing or
        # returning a zero-denominator result.
        db.insert_flight(_flight(icao, hour=1, adsb_pct=50.0, mlat_pct=50.0, data_points=0))
        db.insert_flight(_flight(icao, hour=3, adsb_pct=50.0, mlat_pct=50.0, data_points=0))
        db.refresh_aircraft_stats(icao)
        db.commit()
        snap = status_snapshot(db, icao)
    assert snap["sources"] is None
