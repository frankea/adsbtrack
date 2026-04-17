"""Geometric navaid-alignment detector.

For each candidate navaid (pre-filtered by bbox to keep cost bounded) the
algorithm walks the flight's point stream and keeps every point whose
bearing-to-navaid lies within a degree or so of the ground track, subject to
a maximum range. Kept points are split into segments on long gaps, then
filtered by minimum duration and minimum closest-approach distance. The
surviving list is this flight's navaid track fingerprint.

Callers pass points directly rather than a ``FlightMetrics``: navaid
alignment is enroute by nature, so it needs the full per-flight trajectory
rather than ``FlightMetrics.recent_points`` (a 240-sample tail deque that
only covers the last ~20 minutes).

Attribution: the geometric idea (|bearing-to-beacon - track| under a
threshold, split-on-gap, duration + close-pass filter) mirrors xoolive/
traffic's ``BeaconTrackBearingAlignment`` (MIT-licensed). No code is copied
from traffic; this module reimplements the algorithm in our style against
the ``_PointSample`` layer already present in this codebase.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from .classifier import _PointSample

_KM_PER_NM = 1.852
_EARTH_R_KM = 6371.0


@dataclass(frozen=True)
class NavaidAlignmentSegment:
    """One qualifying alignment segment between a flight and one navaid."""

    navaid_ident: str
    start_ts: float
    end_ts: float
    min_distance_km: float


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(rlat2)
    y = math.cos(rlat1) * math.sin(rlat2) - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = rlat2 - rlat1
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return _EARTH_R_KM * 2 * math.asin(math.sqrt(a))


def _smallest_angle(a_deg: float, b_deg: float) -> float:
    d = (a_deg - b_deg) % 360.0
    return d if d <= 180.0 else 360.0 - d


def _alignments_for_navaid(
    samples: Sequence[_PointSample],
    navaid: Mapping[str, object],
    *,
    tolerance_deg: float,
    max_distance_km: float,
    split_gap_secs: float,
    min_duration_secs: float,
    near_pass_max_km: float,
) -> list[NavaidAlignmentSegment]:
    ident = str(navaid.get("ident") or "")
    if not ident:
        return []
    n_lat = navaid.get("latitude_deg")
    n_lon = navaid.get("longitude_deg")
    if n_lat is None or n_lon is None:
        return []
    n_lat_f = float(n_lat)  # type: ignore[arg-type]
    n_lon_f = float(n_lon)  # type: ignore[arg-type]

    kept: list[tuple[float, float]] = []  # (ts, distance_km)
    for s in samples:
        if s.lat is None or s.lon is None or s.track is None:
            continue
        dist_km = _haversine_km(s.lat, s.lon, n_lat_f, n_lon_f)
        if dist_km > max_distance_km:
            continue
        bearing = _bearing_deg(s.lat, s.lon, n_lat_f, n_lon_f)
        if _smallest_angle(bearing, float(s.track)) >= tolerance_deg:
            continue
        kept.append((s.ts, dist_km))

    if not kept:
        return []

    segments: list[list[tuple[float, float]]] = [[kept[0]]]
    for prev, cur in zip(kept, kept[1:], strict=False):
        if cur[0] - prev[0] > split_gap_secs:
            segments.append([cur])
        else:
            segments[-1].append(cur)

    results: list[NavaidAlignmentSegment] = []
    for seg in segments:
        duration = seg[-1][0] - seg[0][0]
        if duration < min_duration_secs:
            continue
        min_d = min(d for _, d in seg)
        if min_d >= near_pass_max_km:
            continue
        results.append(
            NavaidAlignmentSegment(
                navaid_ident=ident,
                start_ts=seg[0][0],
                end_ts=seg[-1][0],
                min_distance_km=round(min_d, 3),
            )
        )
    return results


def detect_navaid_alignments(
    points: Iterable[_PointSample],
    *,
    navaids: Iterable[Mapping[str, object]],
    tolerance_deg: float = 1.0,
    max_distance_nm: float = 500.0,
    split_gap_secs: float = 120.0,
    min_duration_secs: float = 30.0,
    near_pass_max_nm: float = 80.0,
) -> list[NavaidAlignmentSegment]:
    """Return every qualifying alignment segment across all provided navaids,
    chronologically ordered by start_ts. Empty list if no segments qualify.

    ``points`` should be the full chronological per-flight stream. Passing a
    truncated tail (for example ``FlightMetrics.recent_points``) will cause
    the algorithm to miss navaids overflown earlier in the flight.
    """
    samples = list(points)
    if not samples:
        return []
    max_distance_km = max_distance_nm * _KM_PER_NM
    near_pass_max_km = near_pass_max_nm * _KM_PER_NM

    out: list[NavaidAlignmentSegment] = []
    for nav in navaids:
        out.extend(
            _alignments_for_navaid(
                samples,
                nav,
                tolerance_deg=tolerance_deg,
                max_distance_km=max_distance_km,
                split_gap_secs=split_gap_secs,
                min_duration_secs=min_duration_secs,
                near_pass_max_km=near_pass_max_km,
            )
        )
    out.sort(key=lambda s: s.start_ts)
    return out
