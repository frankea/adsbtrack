"""Tests for the navaid alignment algorithm.

The algorithm is a pure function over a point list, so tests build
synthetic ``_PointSample`` sequences directly without any FlightMetrics
or parser scaffolding.
"""

from __future__ import annotations

import math
import random
import time
from collections.abc import Iterable, Mapping, Sequence

from adsbtrack.classifier import _PointSample
from adsbtrack.geo import bearing_deg, haversine_km, smallest_angle_deg
from adsbtrack.navaid_alignment import NavaidAlignmentSegment, detect_navaid_alignments

_KM_PER_NM = 1.852


def _brute_force_detect(
    points: Sequence[_PointSample],
    navaids: Iterable[Mapping[str, object]],
    *,
    tolerance_deg: float = 1.0,
    max_distance_nm: float = 500.0,
    split_gap_secs: float = 120.0,
    min_duration_secs: float = 30.0,
    near_pass_max_nm: float = 80.0,
) -> list[NavaidAlignmentSegment]:
    """Reference O(points × navaids) implementation for correctness checks.

    This is intentionally simple and does not use any spatial index; it is
    the ground truth the optimized detector must agree with on every
    fixture. Kept inside the test file so perf rewrites can never silently
    drift from the documented algorithm.
    """
    max_distance_km = max_distance_nm * _KM_PER_NM
    near_pass_max_km = near_pass_max_nm * _KM_PER_NM

    out: list[NavaidAlignmentSegment] = []
    for nav in navaids:
        ident = str(nav.get("ident") or "")
        if not ident:
            continue
        n_lat = nav.get("latitude_deg")
        n_lon = nav.get("longitude_deg")
        if n_lat is None or n_lon is None:
            continue
        n_lat_f = float(n_lat)  # type: ignore[arg-type]
        n_lon_f = float(n_lon)  # type: ignore[arg-type]

        kept: list[tuple[float, float]] = []
        for s in points:
            if s.lat is None or s.lon is None or s.track is None:
                continue
            dist_km = haversine_km(s.lat, s.lon, n_lat_f, n_lon_f)
            if dist_km > max_distance_km:
                continue
            b = bearing_deg(s.lat, s.lon, n_lat_f, n_lon_f)
            if smallest_angle_deg(b, float(s.track)) >= tolerance_deg:
                continue
            kept.append((s.ts, dist_km))

        if not kept:
            continue

        segments: list[list[tuple[float, float]]] = [[kept[0]]]
        for prev, cur in zip(kept, kept[1:], strict=False):
            if cur[0] - prev[0] > split_gap_secs:
                segments.append([cur])
            else:
                segments[-1].append(cur)

        for seg in segments:
            duration = seg[-1][0] - seg[0][0]
            if duration < min_duration_secs:
                continue
            min_d = min(d for _, d in seg)
            if min_d >= near_pass_max_km:
                continue
            out.append(
                NavaidAlignmentSegment(
                    navaid_ident=ident,
                    start_ts=seg[0][0],
                    end_ts=seg[-1][0],
                    min_distance_km=round(min_d, 3),
                )
            )
    out.sort(key=lambda s: s.start_ts)
    return out


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
    segs = detect_navaid_alignments(
        points,
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
    points = [_sample(ts=1000.0, lat=34.5, lon=-80.0, track=0.0)]
    assert detect_navaid_alignments(points, navaids=[]) == []


def test_empty_points_returns_empty():
    navaids = [{"ident": "X", "latitude_deg": 35.0, "longitude_deg": -80.0}]
    assert detect_navaid_alignments([], navaids=navaids) == []


def test_track_misaligned_rejects_all_points():
    """Aircraft heading east (track=90) while a navaid sits due north sees
    bearing=0 but track=90: delta=90 >> 1-degree tolerance, nothing kept."""
    navaids = [{"ident": "NORTH", "latitude_deg": 35.5, "longitude_deg": -80.0}]
    points = [_sample(1000.0 + 2.0 * i, 34.5, -80.0 + 0.005 * i, 90.0) for i in range(20)]
    assert detect_navaid_alignments(points, navaids=navaids) == []


def test_gap_splits_segment():
    """Two chunks of track-aligned samples separated by a >2-min gap split
    into two segments. Both must pass the duration + close-pass filters."""
    navaids = [{"ident": "TEST", "latitude_deg": 35.5, "longitude_deg": -80.0}]
    first = [_sample(1000.0 + 2.0 * i, 34.5 + 0.02 * i, -80.0, 0.0) for i in range(20)]
    # 3-minute gap.
    second = [_sample(2000.0 + 2.0 * i, 34.7 + 0.02 * i, -80.0, 0.0) for i in range(20)]
    segs = detect_navaid_alignments(first + second, navaids=navaids)
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
    assert detect_navaid_alignments(points, navaids=navaids, near_pass_max_nm=80.0) == []


def test_short_segment_filtered_by_min_duration():
    """Only 10 s of aligned flight isn't enough to count."""
    navaids = [{"ident": "TEST", "latitude_deg": 35.5, "longitude_deg": -80.0}]
    # 5 samples, 2s apart = 8s wall-clock -> below 30s floor.
    points = [_sample(1000.0 + 2.0 * i, 34.5 + 0.02 * i, -80.0, 0.0) for i in range(5)]
    assert detect_navaid_alignments(points, navaids=navaids) == []


def test_flight_bbox_from_points_basic():
    from adsbtrack.navaids import flight_bbox_from_points

    points = [_sample(1000.0 + 2.0 * i, 34.5 + 0.02 * i, -80.0, 0.0) for i in range(10)]
    bbox = flight_bbox_from_points(points, buffer_nm=50.0)
    assert bbox is not None
    min_lat, max_lat, min_lon, max_lon = bbox
    # Raw lat span was 34.5..34.68, buffer 50 nm ~ 0.83 deg.
    assert min_lat < 34.5 - 0.8
    assert max_lat > 34.68 + 0.8
    # Lon was constant at -80 so buffered box still symmetric.
    assert min_lon < -80.0
    assert max_lon > -80.0


def test_flight_bbox_returns_none_with_no_points():
    from adsbtrack.navaids import flight_bbox_from_points

    assert flight_bbox_from_points([], buffer_nm=50.0) is None


def test_flight_bbox_returns_none_on_antimeridian_span():
    """A flight that straddles the 180 deg meridian would produce a giant
    wrong-wrapping bbox; the helper declines rather than return garbage."""
    from adsbtrack.navaids import flight_bbox_from_points

    points = [
        _sample(1000.0, 0.0, -179.0, 0.0),
        _sample(2000.0, 0.0, 179.0, 0.0),
    ]
    assert flight_bbox_from_points(points, buffer_nm=50.0) is None


def test_detect_matches_brute_force_and_completes_under_budget():
    """Stress test: 5000 points × 500 navaids covering a transcontinental
    flight with a realistic post-bbox-filter candidate set.

    Correctness: optimized detector output must be identical to the
    brute-force reference implementation at the top of this file. The
    reference is O(points × navaids) and is intentionally simple so
    regressions in the optimized path are caught structurally.

    Performance: detector must complete in under 0.7 s. The brute-force
    reference lands around 0.85 s on a developer laptop; the grid index
    should come in comfortably under that by pruning navaids that are
    never within range of any flight point.
    """
    rng = random.Random(20260417)
    track_deg = 75.0

    # Flight: 5000 points along a straight ENE track spanning 50° lon and
    # 10° lat (a transcontinental profile). 2 s spacing.
    points: list[_PointSample] = []
    for i in range(5000):
        lat = 35.0 + 10.0 * (i / 5000)
        lon = -120.0 + 50.0 * (i / 5000)
        points.append(_sample(ts=1000.0 + 2.0 * i, lat=lat, lon=lon, track=track_deg))

    # 400 background navaids spread across a wide bbox around the flight;
    # most are far from any flight point and the grid prunes them cheaply.
    navaids: list[dict] = []
    for i in range(400):
        navaids.append(
            {
                "ident": f"NAV{i:03d}",
                "latitude_deg": rng.uniform(30.0, 50.0),
                "longitude_deg": rng.uniform(-125.0, -65.0),
                "type": "VOR",
            }
        )
    # 100 navaids placed forward along the flight's track so some alignment
    # segments actually qualify. Distance 40-200 nm ahead varies to produce
    # a mix of long and short segments.
    for i in range(100):
        anchor = points[rng.randrange(500, 4500)]
        dist_nm = rng.uniform(40.0, 200.0)
        dist_deg = dist_nm / 60.0
        dlat = dist_deg * math.cos(math.radians(track_deg))
        dlon = dist_deg * math.sin(math.radians(track_deg)) / math.cos(math.radians(anchor.lat))
        navaids.append(
            {
                "ident": f"FWD{i:03d}",
                "latitude_deg": anchor.lat + dlat,
                "longitude_deg": anchor.lon + dlon,
                "type": "VOR",
            }
        )

    expected = _brute_force_detect(points, navaids)
    # Sanity: dataset must actually exercise segment emission.
    assert len(expected) > 0

    start = time.perf_counter()
    actual = detect_navaid_alignments(points, navaids=navaids)
    elapsed = time.perf_counter() - start

    assert actual == expected
    assert elapsed < 0.7, f"detect_navaid_alignments took {elapsed:.2f}s, budget 0.7s"
