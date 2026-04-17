"""Tests for the navaid alignment algorithm.

These tests build synthetic FlightMetrics with recent_points directly so we
can pin down the algorithm's behavior without the parser pipeline.
"""

from __future__ import annotations

from adsbtrack.classifier import FlightMetrics, _PointSample
from adsbtrack.navaid_alignment import detect_navaid_alignments


def _sample(ts: float, lat: float, lon: float, track: float) -> _PointSample:
    """Build a minimal _PointSample for navaid alignment tests."""
    return _PointSample(
        ts=ts,
        baro_alt=5000,
        geom_alt=5000,
        gs=250.0,
        baro_rate=0.0,
        lat=lat,
        lon=lon,
        track=track,
    )


def _metrics_from_points(points: list[_PointSample]) -> FlightMetrics:
    m = FlightMetrics(first_point_ts=points[0].ts)
    for p in points:
        m.recent_points.append(p)
    return m


def test_detect_aligned_segment_near_single_navaid():
    """An aircraft flying due north (track=0) straight toward a navaid
    1 degree north of its position generates a sustained alignment."""
    navaids = [{"ident": "TEST", "latitude_deg": 35.5, "longitude_deg": -80.0, "type": "VOR"}]
    # 40 samples, 2s apart, at lat 34.5 moving toward lat 35.5 (distance shrinks
    # from ~60 nm to ~0 as ts advances). Track stays 0 (due north).
    points = []
    for i in range(40):
        lat = 34.5 + (i * 0.02)  # 0.02 deg ~ 1.2 nm step; 40 steps ~ 48 nm
        points.append(_sample(ts=1000.0 + 2.0 * i, lat=lat, lon=-80.0, track=0.0))
    metrics = _metrics_from_points(points)
    segs = detect_navaid_alignments(
        metrics,
        navaids=navaids,
        tolerance_deg=1.0,
        max_distance_nm=500.0,
        split_gap_secs=120.0,
        min_duration_secs=30.0,
        near_pass_max_nm=80.0,
    )
    assert len(segs) == 1
    assert segs[0].navaid_ident == "TEST"
    assert segs[0].end_ts - segs[0].start_ts >= 30.0
    assert segs[0].min_distance_km < 80.0 * 1.852


def test_no_navaids_returns_empty():
    # Single realistic sample, but no navaids to check against.
    points = [_sample(ts=1000.0, lat=34.5, lon=-80.0, track=0.0)]
    metrics = _metrics_from_points(points)
    assert detect_navaid_alignments(metrics, navaids=[]) == []


def test_track_misaligned_rejects_all_points():
    """Aircraft heading east (track=90) while a navaid sits due north sees
    bearing=0 but track=90: delta=90 >> 1-degree tolerance, nothing kept."""
    navaids = [{"ident": "NORTH", "latitude_deg": 35.5, "longitude_deg": -80.0}]
    points = [_sample(1000.0 + 2.0 * i, 34.5, -80.0 + 0.005 * i, 90.0) for i in range(20)]
    metrics = _metrics_from_points(points)
    assert detect_navaid_alignments(metrics, navaids=navaids) == []


def test_gap_splits_segment():
    """Two chunks of track-aligned samples separated by a >2-min gap split
    into two segments. Both must pass the duration + close-pass filters."""
    navaids = [{"ident": "TEST", "latitude_deg": 35.5, "longitude_deg": -80.0}]
    first = [_sample(1000.0 + 2.0 * i, 34.5 + 0.02 * i, -80.0, 0.0) for i in range(20)]
    # 3-minute gap.
    second = [_sample(2000.0 + 2.0 * i, 34.7 + 0.02 * i, -80.0, 0.0) for i in range(20)]
    metrics = _metrics_from_points(first + second)
    segs = detect_navaid_alignments(metrics, navaids=navaids)
    assert len(segs) == 2
    assert segs[0].end_ts < segs[1].start_ts


def test_far_pass_rejected_by_near_pass_filter():
    """Points can briefly align toward a very distant navaid by coincidence.
    The closest-approach filter (80 nm) rejects those as fingerprints."""
    navaids = [{"ident": "FAR", "latitude_deg": 40.0, "longitude_deg": -80.0}]
    # Aircraft sits at 34.5N (~330 nm south). Track 0 points it roughly
    # toward the navaid so bearing-track delta can be tiny. Kept points exist
    # but min distance is ~330 nm -> far exceeds 80 nm, segment dropped.
    points = [_sample(1000.0 + 2.0 * i, 34.5, -80.0, 0.0) for i in range(40)]
    metrics = _metrics_from_points(points)
    assert detect_navaid_alignments(metrics, navaids=navaids, near_pass_max_nm=80.0) == []


def test_short_segment_filtered_by_min_duration():
    """Only 10 s of aligned flight isn't enough to count."""
    navaids = [{"ident": "TEST", "latitude_deg": 35.5, "longitude_deg": -80.0}]
    # 5 samples, 2s apart = 8s wall-clock -> below 30s floor.
    points = [_sample(1000.0 + 2.0 * i, 34.5 + 0.02 * i, -80.0, 0.0) for i in range(5)]
    metrics = _metrics_from_points(points)
    assert detect_navaid_alignments(metrics, navaids=navaids) == []
