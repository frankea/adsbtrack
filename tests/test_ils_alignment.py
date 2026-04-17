"""Tests for adsbtrack.ils_alignment."""

from __future__ import annotations

from collections import deque

from adsbtrack.classifier import _PointSample
from adsbtrack.ils_alignment import detect_ils_alignment


def _sample(ts: float, lat: float, lon: float, alt: int, track: float) -> _PointSample:
    return _PointSample(
        ts=ts,
        baro_alt=alt,
        geom_alt=None,
        gs=150.0,
        baro_rate=-800.0,
        lat=lat,
        lon=lon,
        track=track,
    )


class _Metrics:
    """Stand-in that exposes the attribute the detector reads."""

    def __init__(self, samples: list[_PointSample]):
        self.recent_points = deque(samples)


def _walk_toward(
    runway_lat: float,
    runway_lon: float,
    runway_heading: float,
    start_ts: float,
    n: int,
    spacing_secs: float,
) -> list[_PointSample]:
    """Generate `n` samples approaching a runway threshold along its extended centerline."""
    import math

    approach_bearing = (runway_heading + 180.0) % 360.0
    samples: list[_PointSample] = []
    for i in range(n):
        km_out = (n - i) * 0.3  # 300m per step toward threshold
        br_rad = math.radians(approach_bearing)
        dlat = (km_out / 111.0) * math.cos(br_rad)
        dlon = (km_out / (111.0 * math.cos(math.radians(runway_lat)))) * math.sin(br_rad)
        lat = runway_lat + dlat
        lon = runway_lon + dlon
        alt = 1500 - i * 30  # gentle descent
        samples.append(_sample(start_ts + i * spacing_secs, lat, lon, alt, runway_heading))
    return samples


def test_no_runways_returns_none() -> None:
    metrics = _Metrics([_sample(0, 33.6, -84.4, 1000, 90)])
    assert detect_ils_alignment(metrics, airport_elev_ft=1000, runway_ends=[]) is None


def test_clean_ils_captures_segment() -> None:
    # Runway 09 (heading 090) at (33.64, -84.43), elevation 1026 ft.
    runway = {
        "runway_name": "09",
        "latitude_deg": 33.64,
        "longitude_deg": -84.43,
        "heading_deg_true": 90.0,
    }
    # Walk 30 samples at 3s intervals = 87s duration toward the threshold on centerline
    samples = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=30, spacing_secs=3.0)
    metrics = _Metrics(samples)
    result = detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert result is not None
    assert result.runway_name == "09"
    assert result.duration_secs >= 60.0
    assert result.min_offset_m < 100.0


def test_overflight_not_on_centerline_returns_none() -> None:
    # 90s of level overflight at 3000 ft AGL, passing abeam the runway
    # (translate the walk 5 km north so perpendicular offset blows past 100m).
    runway = {
        "runway_name": "09",
        "latitude_deg": 33.64,
        "longitude_deg": -84.43,
        "heading_deg_true": 90.0,
    }
    samples = _walk_toward(33.64 + 0.045, -84.43, 90.0, start_ts=0.0, n=30, spacing_secs=3.0)
    metrics = _Metrics(samples)
    result = detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert result is None


def test_too_high_returns_none() -> None:
    # Clean centerline but at 10,000 ft AGL; exceeds max_ft_above_airport (5,000)
    runway = {
        "runway_name": "09",
        "latitude_deg": 33.64,
        "longitude_deg": -84.43,
        "heading_deg_true": 90.0,
    }
    samples = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=30, spacing_secs=3.0)
    # Override altitudes to 10,000 ft
    high = [
        _PointSample(
            ts=s.ts,
            baro_alt=10_000,
            geom_alt=None,
            gs=s.gs,
            baro_rate=s.baro_rate,
            lat=s.lat,
            lon=s.lon,
            track=s.track,
        )
        for s in samples
    ]
    metrics = _Metrics(high)
    result = detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert result is None


def test_split_on_gap_picks_longest_segment() -> None:
    runway = {
        "runway_name": "09",
        "latitude_deg": 33.64,
        "longitude_deg": -84.43,
        "heading_deg_true": 90.0,
    }
    # 15 samples (45s), then a 40s gap, then 25 samples (75s).
    s1 = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=15, spacing_secs=3.0)
    s2 = _walk_toward(33.64, -84.43, 90.0, start_ts=120.0, n=25, spacing_secs=3.0)
    metrics = _Metrics(s1 + s2)
    result = detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert result is not None
    assert result.duration_secs >= 70.0  # the longer segment wins


def test_short_segment_below_min_duration_returns_none() -> None:
    runway = {
        "runway_name": "09",
        "latitude_deg": 33.64,
        "longitude_deg": -84.43,
        "heading_deg_true": 90.0,
    }
    samples = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=8, spacing_secs=3.0)  # 21s
    metrics = _Metrics(samples)
    result = detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway])
    assert result is None


def test_missing_heading_skips_runway() -> None:
    runway = {
        "runway_name": "09",
        "latitude_deg": 33.64,
        "longitude_deg": -84.43,
        "heading_deg_true": None,
    }
    samples = _walk_toward(33.64, -84.43, 90.0, start_ts=0.0, n=30, spacing_secs=3.0)
    metrics = _Metrics(samples)
    assert detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway]) is None


def test_samples_moving_away_from_threshold_are_excluded() -> None:
    # Samples walk AWAY from the threshold (track = runway heading opposite)
    runway = {
        "runway_name": "09",
        "latitude_deg": 33.64,
        "longitude_deg": -84.43,
        "heading_deg_true": 90.0,
    }
    import math

    samples = []
    for i in range(30):
        km_out = 0.5 + i * 0.3
        # East, moving past and away from threshold; sit on the parallel of
        # the threshold so dlat == 0 and only dlon matters.
        dlon = km_out / (111.0 * math.cos(math.radians(33.64)))
        samples.append(_sample(i * 3.0, 33.64, -84.43 + dlon, 1500, 90.0))
    metrics = _Metrics(samples)
    # Aircraft is east of threshold, heading east, so bearing from point to
    # threshold is west (270) but track is 90: cos(bearing - track) = cos(180) = -1
    assert detect_ils_alignment(metrics, airport_elev_ft=1026, runway_ends=[runway]) is None
