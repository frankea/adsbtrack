"""Tests for adsbtrack.landing_anchor -- pick the anchor point for
destination airport matching."""

from adsbtrack.classifier import FlightMetrics, _PointSample
from adsbtrack.landing_anchor import LandingAnchor, compute_landing_anchor


def _make_sample(ts: float, alt: int | None, lat: float, lon: float) -> _PointSample:
    return _PointSample(
        ts=ts,
        baro_alt=alt,
        geom_alt=None,
        gs=None,
        baro_rate=None,
        lat=lat,
        lon=lon,
    )


def _metrics_with_samples(samples: list[_PointSample]) -> FlightMetrics:
    m = FlightMetrics()
    # FlightMetrics constructs recent_points with a maxlen; reuse that deque.
    for s in samples:
        m.recent_points.append(s)
    if samples:
        last = samples[-1]
        m.last_seen_ts = last.ts
        m.last_seen_lat = last.lat
        m.last_seen_lon = last.lon
        m.last_seen_alt_ft = last.baro_alt
        m.last_point_ts = last.ts
    return m


def test_clean_descent_alt_min_near_touchdown():
    """A clean descent where alt_min is the last sample (touchdown).
    Method should be 'alt_min' and coordinates should match that sample."""
    base_ts = 1_700_000_000.0
    samples = [
        _make_sample(base_ts + 0, 3000, 33.70, -84.50),
        _make_sample(base_ts + 60, 2000, 33.68, -84.48),
        _make_sample(base_ts + 120, 1000, 33.66, -84.46),
        _make_sample(base_ts + 180, 100, 33.64, -84.44),  # alt-min = touchdown
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    assert anchor == LandingAnchor(lat=33.64, lon=-84.44, method="alt_min")


def test_sig_lost_alt_min_well_before_last_point():
    """Signal lost after a descent-then-climb (approach missed or holding).
    alt_min is in the middle of the window; last point is at altitude."""
    base_ts = 1_700_000_000.0
    samples = [
        _make_sample(base_ts + 0, 5000, 40.00, -100.00),
        _make_sample(base_ts + 60, 1500, 40.05, -100.05),  # alt-min
        _make_sample(base_ts + 120, 3000, 40.10, -100.10),
        _make_sample(base_ts + 180, 4000, 40.15, -100.15),  # last point, at altitude
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    assert anchor == LandingAnchor(lat=40.05, lon=-100.05, method="alt_min")


def test_drop_alt_min_equals_last_point():
    """Dropped on approach: last point is the lowest we saw."""
    base_ts = 1_700_000_000.0
    samples = [
        _make_sample(base_ts + 0, 5000, 25.00, -80.00),
        _make_sample(base_ts + 60, 3000, 25.05, -80.05),
        _make_sample(base_ts + 120, 1200, 25.10, -80.10),  # last point, lowest
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    assert anchor == LandingAnchor(lat=25.10, lon=-80.10, method="alt_min")


def test_missing_altitude_falls_back_to_last_point():
    """When no sample in the window has altitude, fall back to last_seen_*."""
    base_ts = 1_700_000_000.0
    samples = [
        _make_sample(base_ts + 0, None, 47.00, -122.00),
        _make_sample(base_ts + 60, None, 47.05, -122.05),
        _make_sample(base_ts + 120, None, 47.10, -122.10),
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    # Fallback uses last_seen_lat/lon (which _metrics_with_samples sets to the
    # final sample above).
    assert anchor == LandingAnchor(lat=47.10, lon=-122.10, method="last_point")


def test_samples_outside_window_are_excluded():
    """Only samples within the final N minutes should be considered."""
    base_ts = 1_700_000_000.0
    samples = [
        # This one is 15 min before the last point - outside a 10-min window
        _make_sample(base_ts + 0, 100, 10.00, 10.00),
        _make_sample(base_ts + 900, 5000, 20.00, 20.00),  # last point (15 min later)
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    # alt=100 is outside the 10-min window, so the in-window min is 5000.
    assert anchor == LandingAnchor(lat=20.00, lon=20.00, method="alt_min")


def test_tie_break_by_latest_timestamp():
    """If multiple samples share the same min altitude, pick the latest."""
    base_ts = 1_700_000_000.0
    samples = [
        _make_sample(base_ts + 0, 1000, 33.64, -84.40),
        _make_sample(base_ts + 60, 500, 33.65, -84.41),  # tie
        _make_sample(base_ts + 120, 500, 33.66, -84.42),  # tie, later
        _make_sample(base_ts + 180, 800, 33.67, -84.43),
    ]
    metrics = _metrics_with_samples(samples)
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    assert anchor.lat == 33.66 and anchor.lon == -84.42


def test_empty_metrics_returns_none():
    """No samples and no last_seen coords -> None."""
    m = FlightMetrics()
    assert compute_landing_anchor(m, window_minutes=10.0) is None


def test_fallback_when_recent_points_empty_but_last_seen_set():
    """recent_points empty but last_seen coords exist (unusual but possible
    after certain stitches) -> last_point fallback."""
    m = FlightMetrics()
    m.last_seen_lat = 51.47
    m.last_seen_lon = -0.45
    m.last_seen_ts = 1_700_000_000.0
    anchor = compute_landing_anchor(m, window_minutes=10.0)
    assert anchor == LandingAnchor(lat=51.47, lon=-0.45, method="last_point")


def test_uses_geom_alt_when_baro_alt_missing():
    """If baro_alt is None but geom_alt is present, geom_alt should be used."""
    base_ts = 1_700_000_000.0
    sample_a = _PointSample(
        ts=base_ts,
        baro_alt=None,
        geom_alt=3000,
        gs=None,
        baro_rate=None,
        lat=33.70,
        lon=-84.50,
    )
    sample_b = _PointSample(
        ts=base_ts + 60,
        baro_alt=None,
        geom_alt=500,
        gs=None,
        baro_rate=None,
        lat=33.64,
        lon=-84.44,
    )
    metrics = _metrics_with_samples([sample_a, sample_b])
    anchor = compute_landing_anchor(metrics, window_minutes=10.0)
    assert anchor == LandingAnchor(lat=33.64, lon=-84.44, method="alt_min")
