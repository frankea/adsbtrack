"""Tests for adsbtrack.features - pure per-flight derivation functions."""

from collections import deque

from adsbtrack.classifier import FlightMetrics
from adsbtrack.config import Config
from adsbtrack.features import (
    _circular_mean_deg,
    classify_category_do260,
    classify_mission,
    compute_callsigns_summary,
    compute_go_around,
    compute_headings,
    compute_hover,
    compute_path_metrics,
    compute_peak_rates,
    compute_phase_budget,
    compute_squawk_summary,
)
from adsbtrack.models import Flight


def _cfg() -> Config:
    return Config()


def _make_sample_for_descent(ts: float, baro_alt: int | None, baro_rate: float | None):
    """Minimal _PointSample for descent scoring tests."""
    from adsbtrack.classifier import _PointSample

    return _PointSample(
        ts=ts,
        baro_alt=baro_alt,
        geom_alt=None,
        gs=None,
        baro_rate=baro_rate,
    )


# ---------------------------------------------------------------------------
# Mission classification
# ---------------------------------------------------------------------------


def test_mission_n911_prefix_is_ems():
    mt = classify_mission(
        callsign="N911LG",
        origin_icao="KABC",
        destination_icao="KABC",
        max_altitude=2500,
        loiter_ratio=None,
        cruise_gs_kt=None,
        config=_cfg(),
    )
    assert mt == "ems_hems"


def test_mission_mt_suffix_is_ems():
    mt = classify_mission(
        callsign="457MT",
        origin_icao=None,
        destination_icao=None,
        max_altitude=None,
        loiter_ratio=None,
        cruise_gs_kt=None,
        config=_cfg(),
    )
    assert mt == "ems_hems"


def test_mission_lmt_suffix_not_ems():
    """'LMT' ending should NOT be classified as EMS (false positive filter)."""
    mt = classify_mission(
        callsign="FOOLMT",
        origin_icao=None,
        destination_icao=None,
        max_altitude=None,
        loiter_ratio=None,
        cruise_gs_kt=None,
        config=_cfg(),
    )
    assert mt != "ems_hems"


def test_mission_twy_prefix_is_exec_charter():
    mt = classify_mission(
        callsign="TWY501",
        origin_icao="KTEB",
        destination_icao="KSJC",
        max_altitude=45000,
        loiter_ratio=None,
        cruise_gs_kt=None,
        config=_cfg(),
    )
    assert mt == "exec_charter"


def test_mission_pattern_same_airport_low_alt():
    mt = classify_mission(
        callsign=None,
        origin_icao="KTIX",
        destination_icao="KTIX",
        max_altitude=2500,
        loiter_ratio=None,
        cruise_gs_kt=None,
        config=_cfg(),
    )
    assert mt == "pattern"


def test_mission_survey_high_loiter_low_gs():
    mt = classify_mission(
        callsign=None,
        origin_icao="KABC",
        destination_icao="KABC",
        max_altitude=8000,
        loiter_ratio=5.0,
        cruise_gs_kt=90,
        config=_cfg(),
    )
    assert mt == "survey"


def test_mission_transport_distinct_origin_dest():
    mt = classify_mission(
        callsign="N12345",
        origin_icao="KABC",
        destination_icao="KDEF",
        max_altitude=25000,
        loiter_ratio=None,
        cruise_gs_kt=None,
        config=_cfg(),
    )
    assert mt == "transport"


def test_mission_unknown_fallthrough():
    mt = classify_mission(
        callsign=None,
        origin_icao=None,
        destination_icao=None,
        max_altitude=None,
        loiter_ratio=None,
        cruise_gs_kt=None,
        config=_cfg(),
    )
    assert mt == "unknown"


# ---------------------------------------------------------------------------
# Path metrics
# ---------------------------------------------------------------------------


def test_path_metrics_straight_line():
    m = FlightMetrics()
    m.path_length_km = 100.0
    m.max_distance_from_origin_km = 50.0  # halfway point is the farthest
    result = compute_path_metrics(
        m,
        origin_icao="KA",
        destination_icao="KB",
        takeoff_lat=0.0,
        takeoff_lon=0.0,
        landing_lat=0.9,
        landing_lon=0.0,
    )
    assert result["path_length_km"] == 100.0
    # loiter = 100 / (2*50) = 1.0 - straight there and back
    assert result["loiter_ratio"] == 1.0


def test_path_metrics_high_loiter():
    m = FlightMetrics()
    m.path_length_km = 300.0
    m.max_distance_from_origin_km = 30.0  # orbiting close to origin
    result = compute_path_metrics(
        m,
        origin_icao=None,
        destination_icao=None,
        takeoff_lat=0.0,
        takeoff_lon=0.0,
        landing_lat=None,
        landing_lon=None,
    )
    # loiter = 300 / (2*30) = 5.0
    assert result["loiter_ratio"] == 5.0
    # v7 H2: path_efficiency = max_distance / path_length = 30/300 = 0.1
    # (no longer requires airport IDs)
    assert result["path_efficiency"] == 0.1


def test_path_metrics_same_airport_has_efficiency():
    """v7 H2: path_efficiency is now max_distance / path_length, which works
    for same-airport flights too (displacement ratio, not airport-to-airport)."""
    m = FlightMetrics()
    m.path_length_km = 100.0
    m.max_distance_from_origin_km = 50.0
    result = compute_path_metrics(
        m,
        origin_icao="KA",
        destination_icao="KA",  # same airport
        takeoff_lat=0.0,
        takeoff_lon=0.0,
        landing_lat=0.0,
        landing_lon=0.0,
    )
    assert result["path_efficiency"] == 0.5


# ---------------------------------------------------------------------------
# Phase budget
# ---------------------------------------------------------------------------


def test_phase_budget_short_flight_null_cruise():
    m = FlightMetrics()
    m.climb_secs = 30
    m.descent_secs = 30
    m.level_secs = 0
    m.max_altitude = 1000
    m.first_point_ts = 1000.0
    m.last_point_ts = 1060.0  # 60s total - below threshold
    p = compute_phase_budget(m, config=_cfg())
    assert p["cruise_secs"] is None
    assert p["cruise_alt_ft"] is None


def test_phase_budget_long_flight_with_cruise():
    m = FlightMetrics()
    m.climb_secs = 300
    m.descent_secs = 300
    m.level_secs = 1200
    m.max_altitude = 35000
    m.first_point_ts = 1000.0
    m.last_point_ts = 10_000.0
    # level_buf samples all in the cruise band (> 70% of 35000)
    m.level_buf = [(60.0, 30000, 450.0)] * 20
    p = compute_phase_budget(m, config=_cfg())
    assert p["cruise_secs"] == 1200  # sum of all level_buf dts
    assert p["cruise_alt_ft"] == 30000
    assert p["cruise_gs_kt"] == 450


# ---------------------------------------------------------------------------
# Peak rates
# ---------------------------------------------------------------------------


def test_peak_rates_from_accumulators():
    m = FlightMetrics()
    m.peak_climb_fpm = 2100.0
    m.peak_descent_fpm = -1800.0
    r = compute_peak_rates(m)
    assert r["peak_climb_fpm"] == 2100
    assert r["peak_descent_fpm"] == -1800


def test_peak_rates_none_when_zero():
    m = FlightMetrics()
    r = compute_peak_rates(m)
    assert r["peak_climb_fpm"] is None
    assert r["peak_descent_fpm"] is None


# ---------------------------------------------------------------------------
# Hover
# ---------------------------------------------------------------------------


def test_hover_none_for_non_rotorcraft():
    m = FlightMetrics()
    m.max_hover_secs = 45
    m.hover_episodes = 2
    r = compute_hover(m, type_code="GLF6", config=_cfg())
    assert r["max_hover_secs"] is None
    assert r["hover_episodes"] is None


def test_hover_populated_for_rotorcraft():
    m = FlightMetrics()
    m.max_hover_secs = 45
    m.hover_episodes = 2
    r = compute_hover(m, type_code="B407", config=_cfg())
    assert r["max_hover_secs"] == 45
    assert r["hover_episodes"] == 2


# ---------------------------------------------------------------------------
# Go-around
# ---------------------------------------------------------------------------


def test_go_around_zero_when_not_confirmed():
    m = FlightMetrics()
    m.landing_transition_ts = 1000.0
    m.approach_alts = deque([(0.0, 1000)])
    assert compute_go_around(m, landing_type="signal_lost", config=_cfg()) == 0


def test_go_around_detects_single_rebound():
    """Synthetic dip/climb/dip pattern should yield at least one go-around."""
    m = FlightMetrics()
    landing_ts = 1000.0
    m.landing_transition_ts = landing_ts
    # Build altitudes: descend to 500 -> climb to 1200 -> descend to 0
    pts = []
    # 200s worth of descent
    for i in range(20):
        pts.append((landing_ts - 500 + i * 10, 2000 - i * 75))  # 2000 -> 575
    # rebound climb
    for i in range(10):
        pts.append((landing_ts - 300 + i * 10, 575 + i * 80))  # 575 -> 1295
    # final descent
    for i in range(20):
        pts.append((landing_ts - 200 + i * 10, 1295 - i * 65))  # 1295 -> 0
    m.approach_alts = deque(pts)
    count = compute_go_around(m, landing_type="confirmed", config=_cfg())
    assert count >= 1


# ---------------------------------------------------------------------------
# Headings
# ---------------------------------------------------------------------------


def test_circular_mean_wraparound():
    # 350 and 10 should mean 0, not 180
    result = _circular_mean_deg([350.0, 10.0])
    assert result is not None
    assert result < 5.0 or result > 355.0


def test_circular_mean_empty_returns_none():
    assert _circular_mean_deg([]) is None


def test_compute_headings_filters_by_gs():
    m = FlightMetrics()
    # Taxi samples (low gs) should be dropped; only fast samples counted.
    # Need at least 3 qualifying tracks per the v4 fallback gate.
    m.takeoff_tracks = [(0.0, 90.0, 50.0), (10.0, 90.0, 60.0), (20.0, 90.0, 55.0), (5.0, 90.0, 10.0)]
    m.landing_tracks = [
        (100.0, 270.0, 50.0),
        (105.0, 270.0, 55.0),
        (110.0, 270.0, 60.0),
        (115.0, 270.0, 5.0),
    ]
    m.landing_transition_ts = 120.0
    r = compute_headings(m, config=_cfg())
    assert r["takeoff_heading_deg"] is not None
    assert abs(r["takeoff_heading_deg"] - 90.0) < 1.0
    assert r["landing_heading_deg"] is not None
    assert abs(r["landing_heading_deg"] - 270.0) < 1.0


def test_compute_headings_helicopter_fallback():
    """v4 §1.4: helicopters land with gs ≈ 0; fallback should walk back to
    a window with non-zero gs and use that for the landing heading."""
    m = FlightMetrics()
    m.landing_tracks = [
        # Approach 4 minutes before touchdown (still moving forward)
        (-200.0, 180.0, 25.0),
        (-190.0, 180.0, 22.0),
        (-180.0, 180.0, 18.0),
        # Final hover/vertical descent (gs ≈ 0)
        (-30.0, 0.0, 1.0),
        (-15.0, 0.0, 0.0),
        (0.0, 0.0, 0.0),
    ]
    m.landing_transition_ts = 0.0
    r = compute_headings(m, config=_cfg())
    # Should use the 180° approach heading, not be NULL
    assert r["landing_heading_deg"] is not None
    assert abs(r["landing_heading_deg"] - 180.0) < 1.0


# ---------------------------------------------------------------------------
# Category / squawk / callsigns
# ---------------------------------------------------------------------------


def test_category_do260_most_common():
    m = FlightMetrics()
    m.category_counts = {"A7": 100, "A2": 5, "A5": 30}
    assert classify_category_do260(m) == "A7"


def test_category_do260_none_when_empty():
    m = FlightMetrics()
    assert classify_category_do260(m) is None


def test_squawk_summary_with_vfr():
    m = FlightMetrics()
    m.squawk_first = "1200"
    m.squawk_last = "1200"
    m.squawk_total_count = 10
    m.squawk_1200_count = 9
    m.squawk_changes = 0
    r = compute_squawk_summary(m, config=_cfg())
    assert r["vfr_flight"] == 1
    assert r["emergency_squawk"] is None


def test_squawk_summary_emergency_priority():
    m = FlightMetrics()
    m.squawk_total_count = 3
    m.emergency_squawks_seen = {"7600", "7500", "7700"}
    r = compute_squawk_summary(m, config=_cfg())
    # 7500 has highest priority in config
    assert r["emergency_squawk"] == "7500"


def test_callsigns_summary_distinct_sorted():
    m = FlightMetrics()
    # callsigns_seen is the order-of-first-appearance distinct list per
    # classifier semantics; set it directly for this pure unit.
    m.callsigns_seen = ["TWY501", "GS501"]
    m.callsign_changes = 1
    r = compute_callsigns_summary(m)
    assert r["callsigns"] is not None
    assert "GS501" in r["callsigns"]
    assert "TWY501" in r["callsigns"]
    assert r["callsign_changes"] == 1


def test_callsigns_summary_empty():
    m = FlightMetrics()
    r = compute_callsigns_summary(m)
    assert r["callsigns"] is None
    assert r["callsign_changes"] is None


def test_callsigns_summary_caps_changes_at_distinct_minus_one():
    """B4: if the online counter ever runs ahead of reality (e.g. ping-pong
    flicker in a legacy DB), compute_callsigns_summary must cap the stored
    callsign_changes at max(0, distinct - 1). Two distinct callsigns with
    a wildly inflated changes counter should clamp to 1."""
    m = FlightMetrics()
    m.callsigns_seen = ["TWY501", "GS501"]
    m.callsign_changes = 148  # inflated by ping-pong inflation
    r = compute_callsigns_summary(m)
    assert r["callsign_changes"] == 1, f"expected cap to 1, got {r['callsign_changes']}"


def test_callsigns_summary_caps_changes_single_callsign_to_zero():
    m = FlightMetrics()
    m.callsigns_seen = ["TWY501"]
    m.callsign_changes = 5
    r = compute_callsigns_summary(m)
    assert r["callsign_changes"] == 0


# ---------------------------------------------------------------------------
# B2: phase budget rescale invariant
# ---------------------------------------------------------------------------


def test_phase_budget_rescales_overrun_to_duration():
    """B2: when the raw phase counters overshoot the metric-span duration
    (from double-counting boundary points), the rescale must bring them
    back down so their sum equals min(raw_total, duration_secs).

    This test builds a FlightMetrics where climb+descent+level sums to
    3780 s but the metric span is only 3600 s (5% overshoot). The
    returned dict should rescale proportionally so sum == 3600.
    """
    m = FlightMetrics()
    m.first_point_ts = 1000.0
    m.last_point_ts = 1000.0 + 3600.0  # 1 h metric span
    # climb + descent + cruise(level_buf) = 600 + 600 + 2580 = 3780 (5% over)
    m.climb_secs = 600
    m.descent_secs = 600
    m.level_secs = 2580  # all above cruise threshold, goes to cruise_secs
    m.max_altitude = 35_000
    m.level_buf = [(2580.0, 31_000, 450.0)]  # one big cruise sample
    p = compute_phase_budget(m, config=_cfg())
    phase_sum = (p["climb_secs"] or 0) + (p["descent_secs"] or 0) + (p["level_secs"] or 0) + (p["cruise_secs"] or 0)
    # Must land within 1 s of 3600 (the metric span, since 3780 > 3600)
    assert abs(phase_sum - 3600) <= 1, f"phase_sum={phase_sum}, expected ~3600"


def test_phase_budget_no_inflation_when_underrun():
    """When the raw phase counters sum BELOW the metric-span duration
    (signal gaps), the rescale must NOT inflate them. The difference is
    signal gap time, accounted for separately in F1's signal_gap_secs."""
    m = FlightMetrics()
    m.first_point_ts = 1000.0
    m.last_point_ts = 1000.0 + 7200.0  # 2 h metric span
    # climb + descent + cruise = 300 + 300 + 1680 = 2280 s (< 7200)
    m.climb_secs = 300
    m.descent_secs = 300
    m.level_secs = 1680
    m.max_altitude = 35_000
    m.level_buf = [(1680.0, 31_000, 450.0)]
    p = compute_phase_budget(m, config=_cfg())
    phase_sum = (p["climb_secs"] or 0) + (p["descent_secs"] or 0) + (p["level_secs"] or 0) + (p["cruise_secs"] or 0)
    # Sum should equal the raw total (2280), NOT the duration (7200)
    assert abs(phase_sum - 2280) <= 1, f"phase_sum={phase_sum}, expected ~2280"


# ---------------------------------------------------------------------------
# D5: heading modulo 360
# ---------------------------------------------------------------------------


def test_compute_headings_wraps_360_to_zero():
    """A circular mean that rounds to 360.0 must wrap back to 0.0.

    Picks takeoff tracks spanning 359.6 / 0.2 / 0.3 which average around
    359.9 but round() can nudge them past 360.
    """
    m = FlightMetrics()
    m.takeoff_tracks = [
        (0.0, 359.96, 200.0),
        (10.0, 0.02, 200.0),
        (20.0, 0.04, 200.0),
    ]
    m.landing_tracks = [
        (100.0, 359.95, 200.0),
        (105.0, 0.01, 200.0),
        (110.0, 0.03, 200.0),
    ]
    m.landing_transition_ts = 120.0
    r = compute_headings(m, config=_cfg())
    # Must not be 360.0 - a directed heading in [0, 360).
    assert r["takeoff_heading_deg"] is not None
    assert r["takeoff_heading_deg"] < 360.0
    assert r["landing_heading_deg"] is not None
    assert r["landing_heading_deg"] < 360.0


# ---------------------------------------------------------------------------
# F1: signal budget (active_minutes, signal_gap_secs, signal_gap_count)
# ---------------------------------------------------------------------------


def test_compute_signal_budget_no_gaps():
    """A continuously-tracked flight has active_minutes == duration and
    signal_gap_secs == 0."""
    from adsbtrack.features import compute_signal_budget

    m = FlightMetrics()
    m.first_point_ts = 1000.0
    m.last_point_ts = 1000.0 + 3600.0  # 1 h
    m.climb_secs = 300
    m.descent_secs = 300
    m.level_secs = 3000
    m.signal_gap_count = 0
    r = compute_signal_budget(m, duration_secs=3600.0)
    assert r["active_minutes"] == 60.0
    assert r["signal_gap_secs"] == 0
    assert r["signal_gap_count"] == 0


def test_compute_signal_budget_with_gap():
    """A 2 h flight with only 90 min of signal has signal_gap_secs == 1800."""
    from adsbtrack.features import compute_signal_budget

    m = FlightMetrics()
    m.first_point_ts = 1000.0
    m.last_point_ts = 1000.0 + 7200.0  # 2 h wall clock
    m.climb_secs = 600
    m.descent_secs = 600
    m.level_secs = 4200  # total active = 5400 s = 90 min
    m.signal_gap_count = 1
    r = compute_signal_budget(m, duration_secs=7200.0)
    assert r["active_minutes"] == 90.0
    assert r["signal_gap_secs"] == 1800
    assert r["signal_gap_count"] == 1


def test_infer_destination_uses_supplied_anchor():
    """When anchor_lat / anchor_lon are supplied, proximity is measured
    from the anchor, not from flight.last_seen_*. Pick the airport that
    is closer to the anchor."""
    from datetime import UTC, datetime

    from adsbtrack.config import Config
    from adsbtrack.features import infer_destination

    # Airports: KA at (30.0, -90.0), KB at (31.0, -90.0)
    candidates = [
        {"ident": "KA", "latitude_deg": 30.0, "longitude_deg": -90.0, "elevation_ft": 10},
        {"ident": "KB", "latitude_deg": 31.0, "longitude_deg": -90.0, "elevation_ft": 10},
    ]
    m = FlightMetrics()
    m.recent_points.append(_make_sample_for_descent(ts=1_700_000_000.0, baro_alt=2000, baro_rate=-500))
    flight = Flight(
        icao="aaaaaa",
        takeoff_time=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
        takeoff_lat=30.0,
        takeoff_lon=-90.0,
        takeoff_date="2026-04-16",
        landing_type="dropped_on_approach",
        last_seen_lat=31.0,  # close to KB
        last_seen_lon=-90.0,
        last_seen_alt_ft=2000,
    )
    cfg = Config()

    # Without anchor kwargs -> KB (closest to last_seen_*)
    result_default = infer_destination(flight=flight, metrics=m, candidates=candidates, config=cfg)
    assert result_default["probable_destination_icao"] == "KB"

    # With anchor kwargs pointing near KA -> KA
    result_anchor = infer_destination(
        flight=flight,
        metrics=m,
        candidates=candidates,
        config=cfg,
        anchor_lat=30.0,
        anchor_lon=-90.0,
    )
    assert result_anchor["probable_destination_icao"] == "KA"


# ----------------------------------------------------------------------
# Squawk signals: squawks_observed, had_emergency, primary_squawk
# ----------------------------------------------------------------------


def _metrics_with_squawk_history(squawks_with_ts: list[tuple[float, str | None]]) -> FlightMetrics:
    """Build a FlightMetrics by feeding record_point with (ts, squawk) tuples."""
    import json as _json  # noqa: F401 - keep import local as a reminder tests assert JSON format

    from adsbtrack.classifier import PointData

    m = FlightMetrics()
    for ts, sq in squawks_with_ts:
        p = PointData(
            ts=ts,
            lat=0.0,
            lon=0.0,
            baro_alt=1000,
            gs=150.0,
            track=0.0,
            geom_alt=1000,
            baro_rate=0.0,
            geom_rate=None,
            squawk=sq,
            category=None,
            nav_altitude_mcp=None,
            nav_qnh=None,
            emergency_field=None,
            true_heading=None,
            callsign=None,
        )
        m.record_point(p, ground_state="airborne", ground_reason="ok")
    m.flush_open_squawk()
    return m


def test_compute_squawk_summary_has_emergency() -> None:
    """Flight with 7700 at any point has had_emergency=1 and emergency_squawk='7700'."""
    import json

    assert json  # silence linter; json checked in a later test
    m = _metrics_with_squawk_history(
        [
            (0.0, "1200"),
            (60.0, "1200"),
            (120.0, "7700"),
            (180.0, "7700"),
            (240.0, "1200"),
            (300.0, "1200"),
        ]
    )
    out = compute_squawk_summary(m, config=Config())
    assert out["had_emergency"] == 1
    assert out["emergency_squawk"] == "7700"
    assert out["primary_squawk"] == "1200"  # longer cumulative duration


def test_compute_squawk_summary_three_handoffs() -> None:
    """Three distinct squawks yields squawk_changes=2 (raw transitions) and
    squawks_observed listed sorted."""
    import json

    m = _metrics_with_squawk_history(
        [
            (0.0, "1200"),
            (60.0, "1200"),
            (90.0, "5201"),
            (150.0, "5201"),
            (180.0, "5203"),
            (240.0, "5203"),
        ]
    )
    out = compute_squawk_summary(m, config=Config())
    assert out["squawk_changes"] == 2
    assert out["had_emergency"] == 0
    assert out["squawks_observed"] == json.dumps(["1200", "5201", "5203"])


def test_compute_squawk_summary_no_squawk_data() -> None:
    """Flight with no observed squawks: all squawk-derived fields NULL-ish."""
    m = _metrics_with_squawk_history([(0.0, None), (60.0, None)])
    out = compute_squawk_summary(m, config=Config())
    assert out["squawk_first"] is None
    assert out["squawk_last"] is None
    assert out["squawk_changes"] is None
    assert out["emergency_squawk"] is None
    assert out["squawks_observed"] is None
    assert out["had_emergency"] == 0
    assert out["primary_squawk"] is None
