"""Tests for the TUI's read-only query layer.

The TUI renders the output of these functions; the functions themselves
are plain dataclass returns so we can test them without a running
Textual app. Every test builds a throwaway Database fixture so the
migrations and schema stay in the loop.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

from adsbtrack.db import Database
from adsbtrack.models import Flight
from adsbtrack.tui.queries import (
    count_aircraft,
    count_flights,
    count_trace_bytes,
    daily_activity,
    distinct_dates_for_icao,
    list_aircraft,
    list_events,
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


def test_count_trace_bytes_measures_compressed_blob_not_raw_json(seeded_db):
    """count_trace_bytes reports what is stored, not the JSON it decodes to.

    insert_trace_day writes trace_json as a zlib BLOB, so the strip's
    `traces` figure must equal the compressed byte length -- SQLite's
    length() returns bytes for a BLOB. A regression that decoded the
    payload (or that stopped compressing) would report the much larger
    raw-JSON size instead.
    """
    trace = [[i, 40.0 + i / 1000, -74.0, 1000 + i, 200, None, None, None, {}, "adsb_icao"] for i in range(200)]
    with Database(seeded_db) as db:
        db.insert_trace_day("aaa111", "2026-04-22", {"timestamp": 1_700_000_000.0, "trace": trace}, source="adsbx")
        db.commit()
        stored = db.conn.execute("SELECT trace_json FROM trace_days WHERE date = '2026-04-22'").fetchone()[0]
        total = count_trace_bytes(db)

    assert isinstance(stored, bytes), "insert_trace_day should store a compressed BLOB"
    assert total == len(stored)
    # The whole point of the compression change: well under the raw JSON.
    assert total < len(json.dumps(trace)) / 2


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


# ---------------------------------------------------------------------------
# daily_activity (Task A1: real per-day activity strip data)
# ---------------------------------------------------------------------------


def test_daily_activity_returns_one_row_per_day_oldest_first(tmp_path):
    db_path = tmp_path / "activity_window.db"
    today = date(2026, 6, 30)
    with Database(db_path) as db:
        db.commit()
        rows = daily_activity(db, "hhh888", days=52, today=today)
    assert len(rows) == 52
    assert rows[0].date == "2026-05-10"  # today - 51 days
    assert rows[-1].date == "2026-06-30"  # today
    assert all(r.flight_count == 0 for r in rows)
    assert all(r.flagged is False for r in rows)


def test_daily_activity_counts_real_flights_per_day(tmp_path):
    db_path = tmp_path / "activity_counts.db"
    icao = "iii999"
    today = date(2026, 6, 30)
    with Database(db_path) as db:
        db.insert_flight(_flight(icao, hour=1, takeoff_date="2026-06-30"))
        db.insert_flight(_flight(icao, hour=3, takeoff_date="2026-06-30"))
        db.insert_flight(_flight(icao, hour=5, takeoff_date="2026-06-29"))
        db.commit()
        rows = daily_activity(db, icao, days=52, today=today)
    by_date = {r.date: r.flight_count for r in rows}
    assert by_date["2026-06-30"] == 2
    assert by_date["2026-06-29"] == 1
    assert by_date["2026-06-28"] == 0


def test_daily_activity_ignores_flights_outside_the_window(tmp_path):
    db_path = tmp_path / "activity_window_edge.db"
    icao = "jjj000"
    today = date(2026, 6, 30)
    with Database(db_path) as db:
        # 53 days before today: just outside a 52-day window ending today.
        db.insert_flight(_flight(icao, hour=1, takeoff_date="2026-05-08"))
        db.commit()
        rows = daily_activity(db, icao, days=52, today=today)
    assert all(r.flight_count == 0 for r in rows)


def test_daily_activity_flags_day_with_emergency_squawk(tmp_path):
    db_path = tmp_path / "activity_emergency_squawk.db"
    icao = "kkk111"
    today = date(2026, 6, 30)
    with Database(db_path) as db:
        db.insert_flight(_flight(icao, hour=1, takeoff_date="2026-06-30", emergency_squawk="7700"))
        db.commit()
        rows = daily_activity(db, icao, days=52, today=today)
    by_date = {r.date: r.flagged for r in rows}
    assert by_date["2026-06-30"] is True
    assert by_date["2026-06-29"] is False


def test_daily_activity_flags_day_with_emergency_flag(tmp_path):
    db_path = tmp_path / "activity_emergency_flag.db"
    icao = "lll222"
    today = date(2026, 6, 30)
    with Database(db_path) as db:
        db.insert_flight(_flight(icao, hour=1, takeoff_date="2026-06-30", emergency_flag="7700"))
        db.commit()
        rows = daily_activity(db, icao, days=52, today=today)
    by_date = {r.date: r.flagged for r in rows}
    assert by_date["2026-06-30"] is True


def test_daily_activity_flags_day_with_spoofed_broadcast_even_with_zero_flights(tmp_path):
    """A rejected broadcast is diverted out of `flights` entirely (see
    parser.py's spoof-reject gate), so a flagged day from a spoof rejection
    can have flight_count == 0. That's the normal case, not a bug."""
    db_path = tmp_path / "activity_spoof.db"
    icao = "mmm333"
    today = date(2026, 6, 30)
    with Database(db_path) as db:
        db.insert_spoofed_broadcast(
            icao=icao,
            takeoff_time="2026-06-30T00:49:47.580000+00:00",
            landing_time="2026-06-30T01:41:52.140000+00:00",
            takeoff_date="2026-06-30",
            callsign="TEST1",
            takeoff_lat=25.25,
            takeoff_lon=55.38,
            landing_lat=27.14,
            landing_lon=55.55,
            max_altitude=250,
            data_points=350,
            sources="adsbfi",
            origin_icao=None,
            destination_icao=None,
            reason="bimodal_integrity",
            reason_detail=json.dumps({}),
        )
        db.commit()
        rows = daily_activity(db, icao, days=52, today=today)
    row = next(r for r in rows if r.date == "2026-06-30")
    assert row.flight_count == 0
    assert row.flagged is True


def test_daily_activity_default_days_is_52(tmp_path):
    db_path = tmp_path / "activity_default.db"
    with Database(db_path) as db:
        db.commit()
        rows = daily_activity(db, "nnn444", today=date(2026, 6, 30))
    assert len(rows) == 52


def test_daily_activity_scoped_to_icao(tmp_path):
    db_path = tmp_path / "activity_scoped.db"
    today = date(2026, 6, 30)
    with Database(db_path) as db:
        db.insert_flight(_flight("ooo555", hour=1, takeoff_date="2026-06-30"))
        db.insert_flight(_flight("ppp666", hour=1, takeoff_date="2026-06-30"))
        db.commit()
        rows = daily_activity(db, "ooo555", days=52, today=today)
    by_date = {r.date: r.flight_count for r in rows}
    assert by_date["2026-06-30"] == 1
