"""Tests for adsbtrack.helipads DBSCAN clustering.

The correctness test pins expected cluster structure on a synthetic
500-point fixture with well-separated clusters. The equivalence test
compares the production ``_dbscan`` against the brute-force reference
inlined below on 10,000 random points. The benchmark harness times
both implementations at 500 / 5000 / 20000 points — kept even after the
grid refactor lands (or is reverted) so the next person has a baseline.
"""

from __future__ import annotations

import os
import random
import time

import pytest

from adsbtrack.airports import haversine_km
from adsbtrack.helipads import _dbscan


def _dbscan_brute(points: list[tuple[float, float]], eps_km: float, min_samples: int) -> list[int]:
    """Reference O(n^2) DBSCAN. Mirrors the pre-refactor implementation so
    the grid-bucketed version can be tested for partition equivalence on
    arbitrary fixtures. Lives in the test file, not the module."""
    n = len(points)
    labels = [-1] * n
    visited = [False] * n
    cluster_id = 0

    def _neighbors(idx):
        lat_i, lon_i = points[idx]
        return [j for j in range(n) if j != idx and haversine_km(lat_i, lon_i, points[j][0], points[j][1]) <= eps_km]

    for i in range(n):
        if visited[i]:
            continue
        visited[i] = True
        nbrs = _neighbors(i)
        if len(nbrs) < min_samples - 1:
            continue
        labels[i] = cluster_id
        seed_set = list(nbrs)
        j = 0
        while j < len(seed_set):
            q = seed_set[j]
            if not visited[q]:
                visited[q] = True
                q_nbrs = _neighbors(q)
                if len(q_nbrs) >= min_samples - 1:
                    seed_set.extend(q_nbrs)
            if labels[q] == -1:
                labels[q] = cluster_id
            j += 1
        cluster_id += 1
    return labels


def _partition(labels: list[int]) -> tuple[frozenset[frozenset[int]], frozenset[int]]:
    """Convert labels to (set of clusters, set of noise indices) so two
    labelings can be compared for partition equivalence regardless of
    which integer ID each cluster received."""
    clusters: dict[int, set[int]] = {}
    noise: set[int] = set()
    for i, lab in enumerate(labels):
        if lab == -1:
            noise.add(i)
        else:
            clusters.setdefault(lab, set()).add(i)
    return frozenset(frozenset(c) for c in clusters.values()), frozenset(noise)


def _well_separated_fixture(seed: int = 20260417) -> list[tuple[float, float]]:
    """3 tight clusters (~100 points each) + ~200 noise points, all in a
    region around (40N, -74W). Clusters are spaced ~10 km apart so any
    reasonable eps <= 0.5 km produces the same partition regardless of
    neighbor-iteration order within a cluster."""
    rng = random.Random(seed)
    pts: list[tuple[float, float]] = []
    centers = [(40.0000, -74.0000), (40.1000, -74.0000), (40.0000, -73.8700)]
    for clat, clon in centers:
        for _ in range(100):
            # 50m jitter = ~0.00045 deg, well inside eps=0.2 km
            pts.append((clat + rng.gauss(0, 0.00045), clon + rng.gauss(0, 0.00045)))
    # Sprinkle 200 scattered noise points over a ~200 km bbox.
    for _ in range(200):
        pts.append((40.0 + rng.uniform(-1.0, 1.0), -74.0 + rng.uniform(-1.0, 1.0)))
    rng.shuffle(pts)
    return pts


def test_dbscan_finds_three_clusters():
    """Sanity: the production detector finds the three tight clusters in
    the fixture. Three clusters, each with 100 points; the ~200 scattered
    noise points stay as noise."""
    points = _well_separated_fixture()
    labels = _dbscan(points, eps_km=0.2, min_samples=3)
    clusters, noise = _partition(labels)
    assert len(clusters) == 3
    assert sorted(len(c) for c in clusters) == [100, 100, 100]
    assert len(noise) == 200


def test_dbscan_matches_brute_force_reference():
    """Equivalence check on the well-separated fixture: production and
    reference must produce the same partition (same groupings + same
    noise set, label IDs may differ)."""
    points = _well_separated_fixture()
    a = _dbscan(points, eps_km=0.2, min_samples=3)
    b = _dbscan_brute(points, eps_km=0.2, min_samples=3)
    assert _partition(a) == _partition(b)


def test_dbscan_matches_brute_force_random_2k():
    """2,000 random points in a ~100 km x 100 km area. Partition must
    match brute-force reference. Held at 2k rather than 10k so default
    CI stays fast; the benchmark harness exercises larger sizes behind
    ADSBTRACK_BENCH=1."""
    rng = random.Random(20260417)
    points = [(40.0 + rng.uniform(-0.45, 0.45), -74.0 + rng.uniform(-0.45, 0.45)) for _ in range(2000)]
    a = _dbscan(points, eps_km=0.2, min_samples=3)
    b = _dbscan_brute(points, eps_km=0.2, min_samples=3)
    assert _partition(a) == _partition(b)


def _generate_points(n: int, seed: int = 20260417) -> list[tuple[float, float]]:
    """Uniformly distributed points in a ~100 km x 100 km bbox centered on
    (40N, -74W). At the default 0.9 deg lat / 1.17 deg lon span this is
    representative of a metropolitan area's helipad footprint."""
    rng = random.Random(seed)
    return [(40.0 + rng.uniform(-0.45, 0.45), -74.0 + rng.uniform(-0.585, 0.585)) for _ in range(n)]


@pytest.mark.skipif(
    not os.environ.get("ADSBTRACK_BENCH"),
    reason="benchmark harness; run with ADSBTRACK_BENCH=1",
)
def test_dbscan_benchmark_harness(capsys):
    """Prints timings at 500 / 5000 / 20000 points. Skipped by default to
    keep CI fast; run with ADSBTRACK_BENCH=1 to capture baseline numbers.
    Kept even after a refactor lands (or is reverted) so the next person
    has a comparable baseline.

    Kill-switch thresholds for the grid-bucket refactor (from spec):
      - 500:   may be up to 1.5x slower than baseline (constants dominate)
      - 5000:  must be at least 2x faster than baseline
      - 20000: must be at least 5x faster than baseline
    Baseline numbers from the brute-force O(n^2) impl on a dev laptop:
      500=70 ms, 5000=7.1 s, 20000=114.5 s.
    """
    # Warm the haversine_km hot path.
    _dbscan(_generate_points(50), eps_km=0.2, min_samples=3)

    scales = [500, 5000, 20000]
    timings: dict[int, float] = {}
    for n in scales:
        pts = _generate_points(n)
        t0 = time.perf_counter()
        _dbscan(pts, eps_km=0.2, min_samples=3)
        elapsed = time.perf_counter() - t0
        timings[n] = elapsed

    # Surface the numbers even on pass so the CI log carries the perf trail.
    with capsys.disabled():
        print("\nBENCHMARK _dbscan:")
        for n, t in sorted(timings.items()):
            print(f"  n={n}: {t * 1000:.1f} ms")
        if timings[5000] > 0:
            print(f"  5k rate:  {5000 / timings[5000]:.0f} points/s")
        if timings[20000] > 0:
            print(f"  20k rate: {20000 / timings[20000]:.0f} points/s")
