"""Tests for adsbtrack.takeoff_runway."""

from __future__ import annotations

import math

from adsbtrack.classifier import _PointSample
from adsbtrack.takeoff_runway import _destination_point, detect_takeoff_runway


def _sample(ts: float, lat: float, lon: float, alt: int, gs: float, baro_rate: float, track: float) -> _PointSample:
    return _PointSample(
        ts=ts,
        baro_alt=alt,
        geom_alt=None,
        gs=gs,
        baro_rate=baro_rate,
        lat=lat,
        lon=lon,
        track=track,
    )


class _Metrics:
    def __init__(self, samples: list[_PointSample]) -> None:
        self.takeoff_points = samples


def _walk_departure(
    threshold_lat: float,
    threshold_lon: float,
    heading: float,
    start_ts: float,
    n: int,
    spacing_secs: float,
    start_alt_ft: int,
    alt_step_ft: int,
    start_gs_kt: float,
    gs_step_kt: float,
) -> list[_PointSample]:
    """Generate n samples departing a runway: starts at the threshold moving
    along the runway heading, climbing and accelerating."""
    samples = []
    departure_bearing_rad = math.radians(heading)
    for i in range(n):
        km_out = i * 0.05  # 50m per step
        dlat = (km_out / 111.0) * math.cos(departure_bearing_rad)
        dlon = (km_out / (111.0 * math.cos(math.radians(threshold_lat)))) * math.sin(departure_bearing_rad)
        lat = threshold_lat + dlat
        lon = threshold_lon + dlon
        alt = start_alt_ft + i * alt_step_ft
        gs = start_gs_kt + i * gs_step_kt
        samples.append(_sample(start_ts + i * spacing_secs, lat, lon, alt, gs, 1500.0, heading))
    return samples


def test_destination_point_roundtrip() -> None:
    """Roundtripping a destination point should return close to the origin."""
    # Equatorial cardinal: 1000m east, then 1000m west, back to start
    lat1, lon1 = _destination_point(0.0, 0.0, 90.0, 1000.0)
    lat2, lon2 = _destination_point(lat1, lon1, 270.0, 1000.0)
    assert abs(lat2 - 0.0) < 1e-6
    assert abs(lon2 - 0.0) < 1e-6

    # Off-equator, non-cardinal: 2000m at bearing 45 from (45, -80), then
    # 2000m at bearing 225 back -- should return within 1m.
    lat3, lon3 = _destination_point(45.0, -80.0, 45.0, 2000.0)
    lat4, lon4 = _destination_point(lat3, lon3, 225.0, 2000.0)
    assert abs(lat4 - 45.0) < 1e-5
    assert abs(lon4 - (-80.0)) < 1e-5


def test_no_runways_returns_none() -> None:
    metrics = _Metrics([_sample(0, 27.77, -82.67, 100, 150.0, 500.0, 240.0)])
    assert (
        detect_takeoff_runway(
            metrics,
            airport_elev_ft=7,
            runway_ends=[],
        )
        is None
    )


def test_empty_takeoff_points_returns_none() -> None:
    metrics = _Metrics([])
    runway = {"runway_name": "24", "latitude_deg": 27.77, "longitude_deg": -82.67, "heading_deg_true": 240.0}
    assert (
        detect_takeoff_runway(
            metrics,
            airport_elev_ft=7,
            runway_ends=[runway],
        )
        is None
    )


def test_clean_commercial_departure_identifies_runway() -> None:
    # KSPG runway 24 has heading ~240° magnetic. Simulate a departure: 40
    # samples at 3s intervals, starting from threshold at 30 kt accelerating
    # to 150+ kt, climbing 100 ft per sample from 10 ft MSL.
    runway = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": 240.0}
    samples = _walk_departure(
        27.76,
        -82.63,
        240.0,
        start_ts=0,
        n=40,
        spacing_secs=3.0,
        start_alt_ft=10,
        alt_step_ft=50,
        start_gs_kt=30,
        gs_step_kt=4.0,
    )
    metrics = _Metrics(samples)
    result = detect_takeoff_runway(
        metrics,
        airport_elev_ft=7,
        runway_ends=[runway],
    )
    assert result is not None
    assert result.runway_name == "24"
    assert result.duration_secs > 0.0
    assert result.max_gs_kt >= 140.0


def test_too_slow_returns_none() -> None:
    # Aircraft on runway 24 centerline but never exceeds 100 kt (sparse data
    # or small piston); default 140 kt min should reject.
    runway = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": 240.0}
    samples = _walk_departure(
        27.76,
        -82.63,
        240.0,
        start_ts=0,
        n=40,
        spacing_secs=3.0,
        start_alt_ft=10,
        alt_step_ft=20,
        start_gs_kt=30,
        gs_step_kt=1.0,
    )  # peaks ~70 kt
    metrics = _Metrics(samples)
    result = detect_takeoff_runway(
        metrics,
        airport_elev_ft=7,
        runway_ends=[runway],
    )
    assert result is None  # below default 140 kt
    # Now retry with low min_gs_kt (helicopter/GA) -- should pass
    result2 = detect_takeoff_runway(
        metrics,
        airport_elev_ft=7,
        runway_ends=[runway],
        min_gs_kt=60.0,
    )
    assert result2 is not None
    assert result2.runway_name == "24"


def test_too_high_returns_none() -> None:
    # Samples over centerline but all above airport_elev + 2000ft
    runway = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": 240.0}
    samples = _walk_departure(
        27.76,
        -82.63,
        240.0,
        start_ts=0,
        n=40,
        spacing_secs=3.0,
        start_alt_ft=3000,
        alt_step_ft=100,
        start_gs_kt=150,
        gs_step_kt=2.0,
    )
    metrics = _Metrics(samples)
    result = detect_takeoff_runway(
        metrics,
        airport_elev_ft=7,
        runway_ends=[runway],
    )
    assert result is None


def test_offset_departure_not_aligned_returns_none() -> None:
    # Departure in wrong direction (away from runway 24 polygon): heading 60°
    # instead of 240°; samples walk east, not southwest.
    runway = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": 240.0}
    samples = _walk_departure(
        27.76,
        -82.63,
        60.0,
        start_ts=0,
        n=40,
        spacing_secs=3.0,
        start_alt_ft=10,
        alt_step_ft=50,
        start_gs_kt=30,
        gs_step_kt=4.0,
    )
    metrics = _Metrics(samples)
    assert (
        detect_takeoff_runway(
            metrics,
            airport_elev_ft=7,
            runway_ends=[runway],
        )
        is None
    )


def test_multi_runway_picks_longest() -> None:
    # Two runways 24 and 06 at same airport. Departure is on 24's polygon
    # (60 s inside) and briefly clips 06's polygon at the start (~6 s). Winner = 24.
    runway_24 = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": 240.0}
    runway_06 = {"runway_name": "06", "latitude_deg": 27.745, "longitude_deg": -82.650, "heading_deg_true": 60.0}
    samples = _walk_departure(
        27.76,
        -82.63,
        240.0,
        start_ts=0,
        n=30,
        spacing_secs=3.0,
        start_alt_ft=10,
        alt_step_ft=50,
        start_gs_kt=30,
        gs_step_kt=5.0,
    )
    metrics = _Metrics(samples)
    result = detect_takeoff_runway(
        metrics,
        airport_elev_ft=7,
        runway_ends=[runway_24, runway_06],
    )
    assert result is not None
    assert result.runway_name == "24"


def test_missing_heading_skips_runway() -> None:
    runway = {"runway_name": "24", "latitude_deg": 27.76, "longitude_deg": -82.63, "heading_deg_true": None}
    samples = _walk_departure(
        27.76,
        -82.63,
        240.0,
        start_ts=0,
        n=40,
        spacing_secs=3.0,
        start_alt_ft=10,
        alt_step_ft=50,
        start_gs_kt=30,
        gs_step_kt=5.0,
    )
    metrics = _Metrics(samples)
    assert (
        detect_takeoff_runway(
            metrics,
            airport_elev_ft=7,
            runway_ends=[runway],
        )
        is None
    )
