"""Geometric ILS-alignment detector.

For each runway end at a candidate landing airport, compute per-trace-point
the bearing to the runway threshold and the perpendicular offset from the
extended centerline, then collect contiguous segments of points that are

  * within ``max_offset_m`` of the centerline
  * moving toward the threshold (cos(bearing - track) > 0)
  * below ``airport_elev_ft + max_ft_above_airport``

Segments are split on trace gaps longer than ``split_gap_secs``. Any segment
at least ``min_duration_secs`` long is a candidate. The longest candidate
across all runway ends wins and is returned.

Attribution: the geometric approach (perpendicular offset =
distance * |bearing - runway_heading| in radians, split-on-gap, min-duration
filter, AGL cap) mirrors xoolive/traffic's ``LandingAlignedOnILS``
(MIT-licensed). No code is copied from traffic; this module reimplements
the algorithm in our style against the FlightMetrics / recent_points layer
already present in this codebase.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from .classifier import FlightMetrics, _haversine_m, _PointSample


@dataclass(frozen=True)
class IlsAlignmentResult:
    """Winning alignment segment for a flight."""

    runway_name: str
    duration_secs: float
    min_offset_m: float
    first_ts: float
    last_ts: float
    end_alt_ft: int | None


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing from (lat1, lon1) to (lat2, lon2) in degrees [0, 360)."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(rlat2)
    y = math.cos(rlat1) * math.sin(rlat2) - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def _smallest_angle(a_deg: float, b_deg: float) -> float:
    """Smallest unsigned angle (degrees) between two bearings, in [0, 180]."""
    d = (a_deg - b_deg) % 360.0
    return d if d <= 180.0 else 360.0 - d


def _sample_alt(s: _PointSample) -> int | None:
    if s.baro_alt is not None:
        return s.baro_alt
    return s.geom_alt


def _alignment_for_runway(
    samples: Sequence[_PointSample],
    runway: Mapping[str, object],
    *,
    airport_elev_ft: float,
    max_offset_m: float,
    max_ft_above_airport: float,
    split_gap_secs: float,
    min_duration_secs: float,
) -> list[IlsAlignmentResult]:
    """Return every qualifying alignment segment for a single runway end.

    A segment qualifies when its contiguous duration (in seconds, computed
    from first-kept-sample ts to last-kept-sample ts) is at least
    ``min_duration_secs``. Returns an empty list when nothing qualifies.
    """
    heading = runway.get("heading_deg_true")
    r_lat = runway.get("latitude_deg")
    r_lon = runway.get("longitude_deg")
    if heading is None or r_lat is None or r_lon is None:
        return []

    r_heading_f = float(heading)  # type: ignore[arg-type]
    r_lat_f = float(r_lat)  # type: ignore[arg-type]
    r_lon_f = float(r_lon)  # type: ignore[arg-type]
    alt_cap = airport_elev_ft + max_ft_above_airport

    # Collect (ts, offset_m, sample) for every qualifying sample. Keep the
    # sample reference so we can read end-of-segment altitude without a
    # second walk over the full point stream.
    kept: list[tuple[float, float, _PointSample]] = []
    for s in samples:
        if s.lat is None or s.lon is None or s.track is None:
            continue
        alt = _sample_alt(s)
        if alt is None or alt > alt_cap:
            continue

        distance_m = _haversine_m(s.lat, s.lon, r_lat_f, r_lon_f)
        bearing_to_threshold = _bearing_deg(s.lat, s.lon, r_lat_f, r_lon_f)
        # Moving-toward-threshold gate: cos(bearing - track) > 0.
        delta_track = (bearing_to_threshold - float(s.track) + 540.0) % 360.0 - 180.0
        if math.cos(math.radians(delta_track)) <= 0.0:
            continue

        # Perpendicular offset from extended centerline, small-angle
        # approximation: distance * |smallest_angle(bearing, runway_heading)|
        # in radians. The threshold (100m) is tiny compared to typical
        # distances (km), so the approximation holds well under the
        # alignment cone and blows past the threshold well outside it.
        angle_to_centerline = _smallest_angle(bearing_to_threshold, r_heading_f)
        offset_m = distance_m * math.radians(angle_to_centerline)
        if offset_m >= max_offset_m:
            continue

        kept.append((s.ts, offset_m, s))

    if not kept:
        return []

    # Split on gaps larger than split_gap_secs.
    segments: list[list[tuple[float, float, _PointSample]]] = [[kept[0]]]
    for prev, cur in zip(kept, kept[1:], strict=False):
        if cur[0] - prev[0] > split_gap_secs:
            segments.append([cur])
        else:
            segments[-1].append(cur)

    # Emit every segment meeting the duration floor.
    results: list[IlsAlignmentResult] = []
    runway_name = str(runway.get("runway_name", ""))
    for seg in segments:
        dur = seg[-1][0] - seg[0][0]
        if dur < min_duration_secs:
            continue
        results.append(
            IlsAlignmentResult(
                runway_name=runway_name,
                duration_secs=round(dur, 1),
                min_offset_m=round(min(o for _, o, _ in seg), 1),
                first_ts=seg[0][0],
                last_ts=seg[-1][0],
                end_alt_ft=_sample_alt(seg[-1][2]),
            )
        )
    return results


def _alignments_for_samples(
    samples: Sequence[_PointSample],
    runway_ends: Sequence[Mapping[str, object]],
    *,
    airport_elev_ft: float,
    max_offset_m: float,
    max_ft_above_airport: float,
    split_gap_secs: float,
    min_duration_secs: float,
) -> list[IlsAlignmentResult]:
    """Flatten per-runway alignment segments into one chronological list."""
    all_segments: list[IlsAlignmentResult] = []
    for runway in runway_ends:
        all_segments.extend(
            _alignment_for_runway(
                samples,
                runway,
                airport_elev_ft=airport_elev_ft,
                max_offset_m=max_offset_m,
                max_ft_above_airport=max_ft_above_airport,
                split_gap_secs=split_gap_secs,
                min_duration_secs=min_duration_secs,
            )
        )
    all_segments.sort(key=lambda s: s.first_ts)
    return all_segments


def detect_ils_alignment(
    metrics: FlightMetrics,
    *,
    airport_elev_ft: float,
    runway_ends: Iterable[Mapping[str, object]],
    max_offset_m: float = 100.0,
    max_ft_above_airport: float = 5000.0,
    split_gap_secs: float = 20.0,
    min_duration_secs: float = 30.0,
) -> IlsAlignmentResult | None:
    """Run the alignment check across every provided runway end.

    Returns the longest qualifying segment, or None if no runway end
    produces a segment meeting ``min_duration_secs``.

    Assumes ``metrics.recent_points`` is in chronological order; gap-split
    logic depends on it (which is how ``FlightMetrics.record_point``
    appends them).
    """
    samples = list(metrics.recent_points)
    if not samples:
        return None

    segments = _alignments_for_samples(
        samples,
        list(runway_ends),
        airport_elev_ft=airport_elev_ft,
        max_offset_m=max_offset_m,
        max_ft_above_airport=max_ft_above_airport,
        split_gap_secs=split_gap_secs,
        min_duration_secs=min_duration_secs,
    )
    if not segments:
        return None
    # Longest wins; tie-break on earliest first_ts for determinism.
    return max(segments, key=lambda s: (s.duration_secs, -s.first_ts))


def detect_all_ils_alignments(
    metrics: FlightMetrics,
    *,
    airport_elev_ft: float,
    runway_ends: Iterable[Mapping[str, object]],
    max_offset_m: float = 100.0,
    max_ft_above_airport: float = 5000.0,
    split_gap_secs: float = 20.0,
    min_duration_secs: float = 30.0,
) -> list[IlsAlignmentResult]:
    """Return every qualifying ILS-aligned segment across runway ends,
    chronologically ordered by ``first_ts``. Empty list when none qualified.
    Same kwargs and thresholds as :func:`detect_ils_alignment`."""
    samples = list(metrics.recent_points)
    if not samples:
        return []
    return _alignments_for_samples(
        samples,
        list(runway_ends),
        airport_elev_ft=airport_elev_ft,
        max_offset_m=max_offset_m,
        max_ft_above_airport=max_ft_above_airport,
        split_gap_secs=split_gap_secs,
        min_duration_secs=min_duration_secs,
    )
