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


def _cfg() -> Config:
    return Config()


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
    # path_efficiency None (no destination)
    assert result["path_efficiency"] is None


def test_path_metrics_same_airport_efficiency_none():
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
    assert result["path_efficiency"] is None


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
    # Taxi samples (low gs) should be dropped; only fast samples counted
    m.takeoff_tracks = [(0.0, 90.0, 10.0), (10.0, 90.0, 50.0)]
    m.landing_tracks = [(100.0, 270.0, 50.0), (110.0, 270.0, 5.0)]
    m.landing_transition_ts = 120.0
    r = compute_headings(m, config=_cfg())
    assert r["takeoff_heading_deg"] is not None
    assert abs(r["takeoff_heading_deg"] - 90.0) < 1.0
    assert r["landing_heading_deg"] is not None
    assert abs(r["landing_heading_deg"] - 270.0) < 1.0


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
    m.callsigns_seen = ["TWY501", "GS501", "TWY501"]
    m.callsign_changes = 2
    r = compute_callsigns_summary(m)
    assert r["callsigns"] is not None
    assert "GS501" in r["callsigns"]
    assert "TWY501" in r["callsigns"]
    assert r["callsign_changes"] == 2


def test_callsigns_summary_empty():
    m = FlightMetrics()
    r = compute_callsigns_summary(m)
    assert r["callsigns"] is None
    assert r["callsign_changes"] is None
