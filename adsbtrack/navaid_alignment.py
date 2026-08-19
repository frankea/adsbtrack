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
from .geo import bearing_deg as _bearing_deg
from .geo import smallest_angle_deg as _smallest_angle
from .geo import split_on_gaps

_KM_PER_NM = 1.852


@dataclass(frozen=True)
class NavaidAlignmentSegment:
    """One qualifying alignment segment between a flight and one navaid."""

    navaid_ident: str
    start_ts: float
    end_ts: float
    min_distance_km: float


def _normalize_navaids(navaids: Iterable[Mapping[str, object]]) -> list[tuple[str, float, float]]:
    """Filter + coerce raw navaid rows into (ident, lat, lon) tuples.

    Shared by the grid index and the below-threshold direct scan so both
    candidate-generation paths see the exact same set (skip missing ident,
    skip missing lat/lon) -- neither path can silently drift from the
    other's filtering.
    """
    out: list[tuple[str, float, float]] = []
    for nav in navaids:
        ident = str(nav.get("ident") or "")
        if not ident:
            continue
        n_lat = nav.get("latitude_deg")
        n_lon = nav.get("longitude_deg")
        if n_lat is None or n_lon is None:
            continue
        out.append((ident, float(n_lat), float(n_lon)))  # type: ignore[arg-type]
    return out


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
        for ident, n_lat_f, n_lon_f in _normalize_navaids(navaids):
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
    grid_min_count: int = 64,
) -> list[NavaidAlignmentSegment]:
    """Return every qualifying alignment segment across all provided navaids,
    chronologically ordered by start_ts. Empty list if no segments qualify.

    ``points`` should be the full chronological per-flight stream. Passing a
    truncated tail (for example ``FlightMetrics.recent_points``) will cause
    the algorithm to miss navaids overflown earlier in the flight.

    ``cell_size_deg`` tunes the internal lat/lon bucket index (default 1°);
    smaller cells cut per-point candidate counts but use more memory.

    ``navaids`` is already bbox-prefiltered to the flight envelope (by the
    caller) before it reaches here, so the occupied set is usually tiny
    relative to the theoretical max_distance_nm radius. Below
    ``grid_min_count`` navaids, building a grid costs more than scanning the
    list directly; at/above it, the grid's neighborhood walk is clamped to
    the loaded navaids' own bounding box rather than the full
    max_distance_nm radius, since cells outside it are guaranteed empty.
    Both paths run the identical per-candidate degree gate, haversine, and
    bearing/track checks, so results are identical either way -- this
    parameter only affects speed.
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

    # Single point-stream sweep. Samples are already in chronological order
    # (parser.FlightMetrics.record_point appends monotonically), so each
    # per-navaid kept list is built in ascending-ts order without needing a
    # post-sort.
    kept_by_ident: dict[str, list[tuple[float, float]]] = defaultdict(list)

    if len(nav_list) < grid_min_count:
        # Below threshold: the grid's bucket/dict-lookup overhead outweighs
        # just scanning the (small) list directly for every point.
        flat = _normalize_navaids(nav_list)
        for s in samples:
            if s.lat is None or s.lon is None or s.track is None:
                continue
            s_lat = s.lat
            s_lon = s.lon
            s_track = float(s.track)
            s_ts = s.ts
            cos_lat = max(0.01, math.cos(math.radians(s_lat)))
            max_dlon_deg = max_dlat_deg / cos_lat
            for ident, n_lat, n_lon in flat:
                # Same per-axis degree gate the grid path uses before
                # haversine/bearing, just applied to every candidate
                # instead of only those in nearby cells.
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
    else:
        grid = _NavaidGrid(nav_list, cell_size_deg=cell_size_deg)
        cells = grid.cells
        if not cells:
            return []
        cell_size = grid.cell_size
        r_lat_cap = int(math.ceil(max_dlat_deg / cell_size))

        # Clamp the walk to the loaded navaids' own bounding box in cell
        # space (derived from the occupied cell keys -- min/max of a
        # floor-division is exactly floor of the min/max input, so this
        # equals the bbox of the filtered navaid set). Cells outside it are
        # guaranteed empty, so widening the theoretical max_distance_nm
        # radius to cover them only adds dict lookups, never candidates.
        rows = [key[0] for key in cells]
        cols = [key[1] for key in cells]
        row_lo, row_hi = min(rows), max(rows)
        col_lo, col_hi = min(cols), max(cols)

        # Inlined neighborhood walk: per-point we scan (2*r_lat+1) * (2*r_lon+1)
        # cells, clamped to [row_lo, row_hi] x [col_lo, col_hi]. r_lon scales
        # with 1/cos(lat) to cover max_distance in km at the sample's latitude.
        for s in samples:
            if s.lat is None or s.lon is None or s.track is None:
                continue
            s_lat = s.lat
            s_lon = s.lon
            s_track = float(s.track)
            s_ts = s.ts
            cos_lat = max(0.01, math.cos(math.radians(s_lat)))
            max_dlon_deg = max_dlat_deg / cos_lat
            r_lon_cap = int(math.ceil(max_dlon_deg / cell_size))
            lat_c = int(s_lat // cell_size)
            lon_c = int(s_lon // cell_size)
            row_start = max(lat_c - r_lat_cap, row_lo)
            row_end = min(lat_c + r_lat_cap, row_hi)
            col_start = max(lon_c - r_lon_cap, col_lo)
            col_end = min(lon_c + r_lon_cap, col_hi)
            for row in range(row_start, row_end + 1):
                for col in range(col_start, col_end + 1):
                    cell = cells.get((row, col))
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
        segments = split_on_gaps(
            kept,
            ts=lambda item: item[0],
            split_gap_secs=split_gap_secs,
            min_duration_secs=min_duration_secs,
            extra_predicate=lambda seg: min(d for _, d in seg) < near_pass_max_km,
        )
        for seg in segments:
            min_d = min(d for _, d in seg)
            out.append(
                NavaidAlignmentSegment(
                    navaid_ident=ident,
                    start_ts=seg[0][0],
                    end_ts=seg[-1][0],
                    min_distance_km=round(min_d, 3),
                )
            )

    # Secondary key on navaid_ident: two navaids can qualify from the exact
    # same first sample (a start_ts tie). Without a tiebreaker, relative
    # order between them falls out of dict-insertion order, which differs
    # between the grid path (cell-traversal order) and the direct-scan path
    # (navaid-list order) -- an observable, path-dependent difference that
    # would violate this function's identical-results-regardless-of-path
    # contract. The ident key makes tie order deterministic and identical
    # across both paths.
    out.sort(key=lambda s: (s.start_ts, s.navaid_ident))
    return out
