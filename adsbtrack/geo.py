"""Spherical-earth geographic helpers shared across modules.

Consolidates haversine distance, initial bearing, smallest-angle-between-
bearings, and destination-point math so no module has to reimplement
them or reach for a leading-underscore name across module boundaries.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence

EARTH_RADIUS_M = 6_371_000.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters between two lat/lon points."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometers."""
    return haversine_m(lat1, lon1, lat2, lon2) / 1000.0


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing from (lat1, lon1) to (lat2, lon2) in degrees [0, 360)."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(rlat2)
    y = math.cos(rlat1) * math.sin(rlat2) - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def smallest_angle_deg(a_deg: float, b_deg: float) -> float:
    """Smallest unsigned angle between two bearings, in [0, 180]."""
    d = (a_deg - b_deg) % 360.0
    return d if d <= 180.0 else 360.0 - d


def destination_point(lat_deg: float, lon_deg: float, bearing_deg: float, distance_m: float) -> tuple[float, float]:
    """Destination (lat, lon) given a start point, bearing (degrees true), and distance (meters).

    Spherical earth model. Accuracy at runway scale (<10 km) is within a
    meter or two, well under a typical trapezoid's least-sensitive dimension.
    """
    br = math.radians(bearing_deg)
    ang = distance_m / EARTH_RADIUS_M
    phi1 = math.radians(lat_deg)
    lam1 = math.radians(lon_deg)
    sin_phi2 = math.sin(phi1) * math.cos(ang) + math.cos(phi1) * math.sin(ang) * math.cos(br)
    phi2 = math.asin(sin_phi2)
    y = math.sin(br) * math.sin(ang) * math.cos(phi1)
    x = math.cos(ang) - math.sin(phi1) * sin_phi2
    lam2 = lam1 + math.atan2(y, x)
    return math.degrees(phi2), ((math.degrees(lam2) + 540.0) % 360.0) - 180.0


def split_on_gaps[T](
    points: Sequence[T],
    *,
    ts: Callable[[T], float],
    split_gap_secs: float,
    min_duration_secs: float,
    extra_predicate: Callable[[Sequence[T]], bool] | None = None,
) -> list[list[T]]:
    """Split a chronological run of already-qualified points into contiguous
    segments, dropping short ones.

    Shared by the geometric detectors (ILS alignment, navaid alignment):
    each collects the points passing its own per-point predicate first,
    then hands the kept, chronologically-ordered list here to do the
    split-on-time-gap / min-duration-filter step common to all of them.
    ``points`` is assumed already filtered to that per-point predicate --
    this function only knows about gaps and duration.

    A new segment starts whenever two consecutive points are more than
    ``split_gap_secs`` apart. A segment survives only if its span (last
    ``ts`` minus first ``ts``) is at least ``min_duration_secs`` and, when
    given, ``extra_predicate(segment)`` returns True (used for segment-level
    filters beyond duration, e.g. a minimum closest-approach distance).
    """
    if not points:
        return []

    segments: list[list[T]] = [[points[0]]]
    for prev, cur in zip(points, points[1:], strict=False):
        if ts(cur) - ts(prev) > split_gap_secs:
            segments.append([cur])
        else:
            segments[-1].append(cur)

    out: list[list[T]] = []
    for seg in segments:
        if ts(seg[-1]) - ts(seg[0]) < min_duration_secs:
            continue
        if extra_predicate is not None and not extra_predicate(seg):
            continue
        out.append(seg)
    return out
