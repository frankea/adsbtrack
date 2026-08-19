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

from adsbtrack.config import Config
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


# ---------------------------------------------------------------------------
# Bimodal-integrity spoof detector (opt-in)
# ---------------------------------------------------------------------------


def _make_sample(version, nic, sil, *, t=0.0, lat=25.25, lon=55.38, alt="ground"):
    """Construct a 14-element readsb trace sample for tests."""
    ac = {"version": version, "nic": nic, "sil": sil, "flight": "EK01    ", "category": "A5"}
    return [t, lat, lon, alt, 0.5, 30.9, 0, None, ac, "adsb_icao", None, None, None, None]


def _insert_trace_day(db, icao, date, samples, source="adsbx"):
    """Direct-insert a trace_day with synthetic readsb samples."""
    import json

    db.conn.execute(
        """INSERT INTO trace_days
           (icao, date, source, timestamp, trace_json, point_count, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            icao,
            date,
            source,
            1776600000.0,
            json.dumps(samples),
            len(samples),
            datetime(2026, 4, 21, tzinfo=UTC).isoformat(),
        ),
    )


def test_spoof_detector_flags_bimodal_integrity(tmp_path):
    """A trace_day where >= 10% of v2 samples carry sil=0 and the day has
    >= 25 v2 samples must produce a spoof_bimodal_integrity event."""
    db_path = tmp_path / "spoof.db"
    samples = (
        [_make_sample(2, 8, 3) for _ in range(40)]  # 40 realistic v2
        + [_make_sample(2, 0, 0) for _ in range(20)]  # 20 garbage v2 (33%)
    )
    with Database(db_path) as db:
        _insert_trace_day(db, "89618d", "2026-04-21", samples)
        db.commit()
        events = collect_events(db, "89618d", include_spoof_checks=True)
    spoof = [e for e in events if e.event_type == "spoof_bimodal_integrity"]
    assert len(spoof) == 1
    assert spoof[0].context["v2_samples"] == 60
    assert spoof[0].context["v2_sil0_pct"] > 30.0
    assert spoof[0].callsign == "EK01"


def test_spoof_detector_opt_in_default_off(tmp_path):
    """Without include_spoof_checks, the detector never runs. Guards
    against retroactively tagging trace_days on unrelated queries."""
    db_path = tmp_path / "spoof_off.db"
    samples = [_make_sample(2, 0, 0) for _ in range(60)]  # all garbage, very spoofy
    with Database(db_path) as db:
        _insert_trace_day(db, "89618d", "2026-04-21", samples)
        db.commit()
        events = collect_events(db, "89618d")
    assert [e for e in events if e.event_type == "spoof_bimodal_integrity"] == []


def test_spoof_detector_ignores_clean_day(tmp_path):
    """A realistic trace_day (sil0 = 0%) emits no event."""
    db_path = tmp_path / "clean.db"
    samples = [_make_sample(2, 8, 3) for _ in range(100)]
    with Database(db_path) as db:
        _insert_trace_day(db, "89618d", "2025-12-01", samples)
        db.commit()
        events = collect_events(db, "89618d", include_spoof_checks=True)
    assert [e for e in events if e.event_type == "spoof_bimodal_integrity"] == []


def test_spoof_detector_skips_sparse_day(tmp_path):
    """Fewer than 25 v2 samples on a day -> detector bails out even if
    the ratio looks bad; one-off ground-handling bursts should not flag."""
    db_path = tmp_path / "sparse.db"
    samples = [_make_sample(2, 0, 0) for _ in range(10)]  # all garbage but sparse
    with Database(db_path) as db:
        _insert_trace_day(db, "89618d", "2026-04-08", samples)
        db.commit()
        events = collect_events(db, "89618d", include_spoof_checks=True)
    assert [e for e in events if e.event_type == "spoof_bimodal_integrity"] == []


def test_spoof_detector_dedupes_across_aggregators(tmp_path):
    """Five aggregators that all received the same spoofed broadcast must
    produce exactly one event for that date, not five."""
    db_path = tmp_path / "dedup.db"
    spoofy_samples = [_make_sample(2, 8, 3) for _ in range(40)] + [_make_sample(2, 0, 0) for _ in range(20)]
    with Database(db_path) as db:
        for src in ("adsbx", "adsbfi", "adsblol", "airplaneslive", "theairtraffic"):
            _insert_trace_day(db, "89618d", "2026-04-21", spoofy_samples, source=src)
        db.commit()
        events = collect_events(db, "89618d", include_spoof_checks=True)
    spoof = [e for e in events if e.event_type == "spoof_bimodal_integrity"]
    assert len(spoof) == 1


def test_spoof_detector_stats_path_matches_parse_path(tmp_path):
    """A fully-stat-filled aircraft (Task 12 materialized v2_samples/
    v2_sil0/v2_nic0/v2_callsigns columns) must produce byte-identical
    spoof events to the decode-every-row parse path -- same fixture, both
    routes."""
    db_path = tmp_path / "stats_parity.db"
    trace = [_make_sample(2, 8, 3) for _ in range(40)] + [_make_sample(2, 0, 0) for _ in range(20)]
    data = {"timestamp": 1776600000.0, "trace": trace}
    with Database(db_path) as db:
        db.insert_trace_day("89618d", "2026-04-21", data)  # fills stats -- fast path eligible
        db.commit()

        stats_path_events = collect_events(db, "89618d", include_spoof_checks=True)

        # Simulate a pre-Task-12 (never-optimized) row by nulling the
        # materialized columns back out, forcing the decode-based fallback.
        db.conn.execute(
            "UPDATE trace_days SET v2_samples = NULL, v2_sil0 = NULL, v2_nic0 = NULL, v2_callsigns = NULL "
            "WHERE icao = ?",
            ("89618d",),
        )
        db.commit()
        parse_path_events = collect_events(db, "89618d", include_spoof_checks=True)

    stats_spoof = [e for e in stats_path_events if e.event_type == "spoof_bimodal_integrity"]
    parse_spoof = [e for e in parse_path_events if e.event_type == "spoof_bimodal_integrity"]
    assert len(stats_spoof) == 1
    assert stats_spoof == parse_spoof


def test_pool_spoof_scores_stats_and_parse_paths_agree_with_mixed_source_rates(tmp_path):
    """Two sources on the same date with different sil0 rates (33% vs 75%)
    must pool to field-identical output whether computed via the
    materialized stat columns (_pool_spoof_scores_from_stats) or by
    decoding trace_json (pool_spoof_scores) -- v2_samples, sil/nic
    percentages, sources, source_rates ordering, timestamp, and callsigns
    all match. Guards against the two pooling routes drifting apart on a
    multi-aggregator, non-uniform-rate day (test_spoof_detector_stats_path_
    matches_parse_path only exercises a single source)."""
    from adsbtrack.db import iter_parsed_trace_days
    from adsbtrack.events import _pool_spoof_scores_from_stats
    from adsbtrack.parser import pool_spoof_scores

    db_path = tmp_path / "multi_source.db"
    icao = "89618d"
    date = "2026-04-21"
    # adsbx: 20 clean + 10 garbage = 30 v2, sil0 rate 33.33%
    source_adsbx = [_make_sample(2, 8, 3) for _ in range(20)] + [_make_sample(2, 0, 0) for _ in range(10)]
    # adsbfi: 5 clean + 15 garbage = 20 v2, sil0 rate 75%
    source_adsbfi = [_make_sample(2, 8, 3) for _ in range(5)] + [_make_sample(2, 0, 0) for _ in range(15)]

    config = Config()
    with Database(db_path) as db:
        db.insert_trace_day(icao, date, {"timestamp": 1776600000.0, "trace": source_adsbx}, source="adsbx")
        db.insert_trace_day(icao, date, {"timestamp": 1776600100.0, "trace": source_adsbfi}, source="adsbfi")
        db.commit()

        stats_result = _pool_spoof_scores_from_stats(db, [icao], None, config)

        rows = db.conn.execute(
            "SELECT date, source, trace_json, timestamp FROM trace_days WHERE icao = ? ORDER BY date, source",
            (icao,),
        ).fetchall()
        parse_result = pool_spoof_scores(iter_parsed_trace_days(rows, icao), config)

    stats_agg = stats_result[(icao, date)]
    parse_agg = parse_result[date]

    assert stats_agg["v2_samples"] == parse_agg["v2_samples"] == 50
    assert stats_agg["v2_sil0_pct"] == pytest.approx(parse_agg["v2_sil0_pct"])
    assert stats_agg["v2_nic0_pct"] == pytest.approx(parse_agg["v2_nic0_pct"])
    assert stats_agg["sources"] == parse_agg["sources"] == ["adsbfi", "adsbx"]
    assert stats_agg["source_rates"] == parse_agg["source_rates"]
    assert stats_agg["timestamp"] == parse_agg["timestamp"]
    assert stats_agg["callsigns"] == parse_agg["callsigns"] == ["EK01"]


def test_spoof_detector_mixed_null_rows_falls_back_for_correctness(tmp_path):
    """If ANY trace_days row for the aircraft is missing the materialized
    stat columns (e.g. one date optimized, one still legacy), the whole
    aircraft must fall back to the decode-based parse path rather than
    silently under-counting from a partially-filled stats table."""
    db_path = tmp_path / "mixed.db"
    spoofy = [_make_sample(2, 8, 3) for _ in range(40)] + [_make_sample(2, 0, 0) for _ in range(20)]
    with Database(db_path) as db:
        # One date fully stat-filled via insert_trace_day...
        db.insert_trace_day("89618d", "2026-04-21", {"timestamp": 1776600000.0, "trace": spoofy})
        # ...and a second date inserted the legacy way (NULL stat columns).
        _insert_trace_day(db, "89618d", "2026-05-01", spoofy)
        db.commit()
        events = collect_events(db, "89618d", include_spoof_checks=True)
    spoof = [e for e in events if e.event_type == "spoof_bimodal_integrity"]
    assert {e.context["date"] for e in spoof} == {"2026-04-21", "2026-05-01"}


def test_bulk_detect_spoof_events_covers_optimized_and_fallback_aircraft(tmp_path):
    """bulk_detect_spoof_events (the all-aircraft events view's spoof pass)
    must flag a fully-stat-filled aircraft via the grouped-query path and a
    still-legacy aircraft via the decode fallback, in one call."""
    from adsbtrack.events import bulk_detect_spoof_events

    db_path = tmp_path / "bulk.db"
    spoofy = [_make_sample(2, 8, 3) for _ in range(40)] + [_make_sample(2, 0, 0) for _ in range(20)]
    with Database(db_path) as db:
        db.insert_trace_day("111111", "2026-04-21", {"timestamp": 1776600000.0, "trace": spoofy})
        _insert_trace_day(db, "222222", "2026-05-01", spoofy)
        db.commit()
        events = bulk_detect_spoof_events(db, ["111111", "222222"])
    flagged_icaos = {e.icao for e in events if e.event_type == "spoof_bimodal_integrity"}
    assert flagged_icaos == {"111111", "222222"}


def test_spoof_detector_threshold_follows_config_override(tmp_path):
    """A7: the events detector's sil0-rate threshold must move with a
    Config override, not stay pinned to a module-level constant.

    5% sil0 is below the default 10% threshold (no event) but above a
    Config(spoof_v2_sil0_pct=3.0) override (event fires).
    """
    db_path = tmp_path / "cfg_override.db"
    samples = [_make_sample(2, 8, 3) for _ in range(95)] + [_make_sample(2, 0, 0) for _ in range(5)]
    with Database(db_path) as db:
        _insert_trace_day(db, "89618d", "2026-05-01", samples)
        db.commit()
        default_events = collect_events(db, "89618d", include_spoof_checks=True)
        lowered_events = collect_events(db, "89618d", include_spoof_checks=True, config=Config(spoof_v2_sil0_pct=3.0))
    assert [e for e in default_events if e.event_type == "spoof_bimodal_integrity"] == []
    spoof = [e for e in lowered_events if e.event_type == "spoof_bimodal_integrity"]
    assert len(spoof) == 1
