"""Geometric navaid-alignment detector.

For each candidate navaid (pre-filtered by bbox to keep cost bounded) the
algorithm walks the flight's point stream and keeps every point whose
bearing-to-navaid lies within a degree or so of the ground track, subject to
a maximum range. Kept points are split into segments on long gaps, then
filtered by minimum duration and minimum closest-approach distance.

Callers pass points directly rather than a ``FlightMetrics``: navaid
alignment is enroute by nature, so it needs the full per-flight trajectory
rather than the 240-sample tail deque in ``FlightMetrics.recent_points``.

Attribution: the geometric idea (|bearing-to-beacon - track| under a
threshold, split-on-gap, duration + close-pass filter) mirrors xoolive/
traffic's ``BeaconTrackBearingAlignment`` (MIT-licensed). No code is copied
from traffic.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from .airports import haversine_km
from .classifier import _PointSample
from .ils_alignment import _bearing_deg, _smallest_angle

_KM_PER_NM = 1.852


@dataclass(frozen=True)
class NavaidAlignmentSegment:
    """One qualifying alignment segment between a flight and one navaid."""

    navaid_ident: str
    start_ts: float
    end_ts: float
    min_distance_km: float


class _NavaidGrid:
    """Lat/lon bucket index over navaids.

    Given a cell size in degrees, every navaid falls into exactly one cell
    keyed by (int(lat // cell_size), int(lon // cell_size)). The detector
    walks the neighborhood of each query point using its own loop so the
    inner body stays a single Python frame (generator yields are too
    costly on a per-point hot path).

    The grid merely bounds the candidate set. The per-navaid degree gate,
    haversine, and bearing/track checks still run for each candidate
    exactly as the brute-force walk does, so the optimized detector
    produces segments identical to the reference implementation.
    """

    __slots__ = ("cell_size", "cells")

    def __init__(
        self,
        navaids: Iterable[Mapping[str, object]],
        *,
        cell_size_deg: float = 1.0,
    ) -> None:
        self.cell_size = cell_size_deg
        self.cells: dict[tuple[int, int], list[tuple[str, float, float]]] = defaultdict(list)
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
            key = (int(n_lat_f // cell_size_deg), int(n_lon_f // cell_size_deg))
            self.cells[key].append((ident, n_lat_f, n_lon_f))


def detect_navaid_alignments(
    points: Iterable[_PointSample],
    *,
    navaids: Iterable[Mapping[str, object]],
    tolerance_deg: float = 1.0,
    max_distance_nm: float = 500.0,
    split_gap_secs: float = 120.0,
    min_duration_secs: float = 30.0,
    near_pass_max_nm: float = 80.0,
    cell_size_deg: float = 1.0,
) -> list[NavaidAlignmentSegment]:
    """Return every qualifying alignment segment across all provided navaids,
    chronologically ordered by start_ts. Empty list if no segments qualify.

    ``points`` should be the full chronological per-flight stream. Passing a
    truncated tail (for example ``FlightMetrics.recent_points``) will cause
    the algorithm to miss navaids overflown earlier in the flight.

    ``cell_size_deg`` tunes the internal lat/lon bucket index (default 1°);
    smaller cells cut per-point candidate counts but use more memory.
    """
    samples: Sequence[_PointSample] = points if isinstance(points, Sequence) else list(points)
    if not samples:
        return []
    nav_list = list(navaids)
    if not nav_list:
        return []

    max_distance_km = max_distance_nm * _KM_PER_NM
    near_pass_max_km = near_pass_max_nm * _KM_PER_NM
    max_dlat_deg = max_distance_km / 111.0

    grid = _NavaidGrid(nav_list, cell_size_deg=cell_size_deg)
    cells = grid.cells
    cell_size = grid.cell_size
    r_lat = int(math.ceil(max_dlat_deg / cell_size))

    # Single point-stream sweep. Samples are already in chronological order
    # (parser.FlightMetrics.record_point appends monotonically), so each
    # per-navaid kept list is built in ascending-ts order without needing a
    # post-sort.
    kept_by_ident: dict[str, list[tuple[float, float]]] = defaultdict(list)
    # Inlined neighborhood walk: per-point we scan (2*r_lat+1) * (2*r_lon+1)
    # cells. r_lon scales with 1/cos(lat) to cover max_distance in km at the
    # sample's latitude.
    for s in samples:
        if s.lat is None or s.lon is None or s.track is None:
            continue
        s_lat = s.lat
        s_lon = s.lon
        s_track = float(s.track)
        s_ts = s.ts
        cos_lat = max(0.01, math.cos(math.radians(s_lat)))
        max_dlon_deg = max_dlat_deg / cos_lat
        r_lon = int(math.ceil(max_dlon_deg / cell_size))
        lat_c = int(s_lat // cell_size)
        lon_c = int(s_lon // cell_size)
        for dlat_c in range(-r_lat, r_lat + 1):
            row = lat_c + dlat_c
            for dlon_c in range(-r_lon, r_lon + 1):
                cell = cells.get((row, lon_c + dlon_c))
                if cell is None:
                    continue
                for ident, n_lat, n_lon in cell:
                    # Defensive degree gate. Grid bounds the candidate set
                    # coarsely; the per-axis delta check rejects out-of-range
                    # pairs before haversine when cells are wide relative to
                    # max_distance.
                    if abs(s_lat - n_lat) > max_dlat_deg:
                        continue
                    if abs(s_lon - n_lon) > max_dlon_deg:
                        continue
                    dist_km = haversine_km(s_lat, s_lon, n_lat, n_lon)
                    if dist_km > max_distance_km:
                        continue
                    bearing = _bearing_deg(s_lat, s_lon, n_lat, n_lon)
                    if _smallest_angle(bearing, s_track) >= tolerance_deg:
                        continue
                    kept_by_ident[ident].append((s_ts, dist_km))

    out: list[NavaidAlignmentSegment] = []
    for ident, kept in kept_by_ident.items():
        # kept is in sample-order by construction.
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
