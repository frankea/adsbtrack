"""Polygon-based takeoff runway identification.

For each runway end at a known origin airport, build a trapezoid polygon:
a narrow base at the runway threshold, extending ``zone_length_m`` outward
along the departure heading, opening symmetrically by ``opening_deg``.
Filter the flight's first-600-s trace window to points below
``airport_elev_ft + max_ft_above_airport`` that are either climbing at
``min_vert_rate_fpm`` or rolling at ``min_gs_kt``. Test which polygons
those points pass through; the runway whose polygon the flight occupied
the longest wins, subject to reaching the speed floor inside the polygon.

Attribution: the trapezoid geometry and "longest segment wins" selection
match xoolive/traffic's ``PolygonBasedRunwayDetection`` (MIT-licensed).
This module reimplements the algorithm using shapely for polygon
containment and our own spherical destination-point helper; no code is
copied from traffic.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from shapely.geometry import Point, Polygon  # type: ignore[import-untyped]

from .classifier import FlightMetrics, _PointSample
from .geo import destination_point
from .geo import destination_point as _destination_point  # noqa: F401  (kept for tests)


@dataclass(frozen=True)
class TakeoffRunwayResult:
    runway_name: str
    duration_secs: float
    max_gs_kt: float


def _build_polygon(
    *,
    threshold_lat: float,
    threshold_lon: float,
    heading_deg: float,
    zone_length_m: float,
    little_base_m: float,
    opening_deg: float,
) -> Polygon:
    half_near = little_base_m / 2.0
    wide_half = half_near + zone_length_m * math.tan(math.radians(opening_deg))
    # heading_deg is the runway's true heading (direction of travel). For
    # runway "24", this is ~240 degrees; a departing aircraft flies along
    # that bearing away from the threshold, so the trapezoid extends in
    # the same direction.
    departure_heading = heading_deg % 360.0

    near_left = destination_point(threshold_lat, threshold_lon, (heading_deg + 90.0) % 360.0, half_near)
    near_right = destination_point(threshold_lat, threshold_lon, (heading_deg - 90.0) % 360.0, half_near)
    far_center = destination_point(threshold_lat, threshold_lon, departure_heading, zone_length_m)
    far_left = destination_point(far_center[0], far_center[1], (heading_deg + 90.0) % 360.0, wide_half)
    far_right = destination_point(far_center[0], far_center[1], (heading_deg - 90.0) % 360.0, wide_half)

    # Shapely uses (x, y) = (lon, lat).
    return Polygon(
        [
            (near_left[1], near_left[0]),
            (far_left[1], far_left[0]),
            (far_right[1], far_right[0]),
            (near_right[1], near_right[0]),
        ]
    )


def _filter_takeoff_samples(
    samples: Sequence[_PointSample],
    *,
    airport_elev_ft: float,
    max_ft_above_airport: float,
    min_gs_kt: float,
    min_vert_rate_fpm: float,
) -> list[_PointSample]:
    alt_cap = airport_elev_ft + max_ft_above_airport
    kept: list[_PointSample] = []
    for s in samples:
        if s.lat is None or s.lon is None or s.track is None:
            continue
        alt = s.altitude()
        if alt is None or alt > alt_cap:
            continue
        climbing = s.baro_rate is not None and s.baro_rate > min_vert_rate_fpm
        rolling = s.gs is not None and s.gs > min_gs_kt
        if not (climbing or rolling):
            continue
        kept.append(s)
    return kept


def _longest_inside_run(
    samples: Sequence[_PointSample],
    polygon: Polygon,
    *,
    split_gap_secs: float,
    min_gs_kt: float,
) -> tuple[float, float] | None:
    """Walk samples, find the longest contiguous run inside polygon whose
    max gs >= min_gs_kt. Returns (duration_secs, max_gs) or None."""
    best_duration = 0.0
    best_max_gs = 0.0
    run_start: float | None = None
    run_last: float | None = None
    run_max_gs = 0.0
    prev_ts: float | None = None

    def _close_run() -> None:
        nonlocal best_duration, best_max_gs
        if run_start is None or run_last is None:
            return
        dur = run_last - run_start
        if run_max_gs >= min_gs_kt and dur > best_duration:
            best_duration = dur
            best_max_gs = run_max_gs

    for s in samples:
        inside = polygon.contains(Point(s.lon, s.lat))
        gap = prev_ts is not None and (s.ts - prev_ts) > split_gap_secs
        if inside and (run_start is None or gap):
            if run_start is not None and gap:
                _close_run()
            run_start = s.ts
            run_last = s.ts
            run_max_gs = s.gs or 0.0
        elif inside and run_start is not None:
            run_last = s.ts
            if s.gs is not None and s.gs > run_max_gs:
                run_max_gs = s.gs
        elif not inside and run_start is not None:
            _close_run()
            run_start = None
            run_last = None
            run_max_gs = 0.0
        prev_ts = s.ts
    _close_run()

    if best_duration <= 0.0:
        return None
    return best_duration, best_max_gs


def detect_takeoff_runway(
    metrics: FlightMetrics,
    *,
    airport_elev_ft: float,
    runway_ends: Iterable[Mapping[str, object]],
    config=None,
    max_ft_above_airport: float = 2000.0,
    zone_length_m: float = 6000.0,
    little_base_m: float = 50.0,
    opening_deg: float = 5.0,
    min_gs_kt: float = 140.0,
    min_vert_rate_fpm: float = 256.0,
    split_gap_secs: float = 20.0,
) -> TakeoffRunwayResult | None:
    """Identify the runway used on takeoff, or None.

    Assumes ``metrics.takeoff_points`` is in chronological order (it is, by
    ``FlightMetrics.record_point``'s append-only contract).

    When ``config`` is supplied, any threshold kwarg not explicitly passed
    is sourced from the corresponding ``Config.takeoff_runway_*`` field.
    ``min_gs_kt`` is runtime-computed (scales by type), so callers always
    pass it explicitly.
    """
    if config is not None:
        max_ft_above_airport = config.takeoff_runway_max_ft_above_airport
        zone_length_m = config.takeoff_runway_zone_length_m
        little_base_m = config.takeoff_runway_little_base_m
        opening_deg = config.takeoff_runway_opening_deg
        min_vert_rate_fpm = config.takeoff_runway_min_vert_rate_fpm
    filtered = _filter_takeoff_samples(
        metrics.takeoff_points,
        airport_elev_ft=airport_elev_ft,
        max_ft_above_airport=max_ft_above_airport,
        min_gs_kt=min_gs_kt,
        min_vert_rate_fpm=min_vert_rate_fpm,
    )
    if not filtered:
        return None

    best: TakeoffRunwayResult | None = None
    for runway in runway_ends:
        heading = runway.get("heading_deg_true")
        r_lat = runway.get("latitude_deg")
        r_lon = runway.get("longitude_deg")
        if heading is None or r_lat is None or r_lon is None:
            continue
        polygon = _build_polygon(
            threshold_lat=float(r_lat),  # type: ignore[arg-type]
            threshold_lon=float(r_lon),  # type: ignore[arg-type]
            heading_deg=float(heading),  # type: ignore[arg-type]
            zone_length_m=zone_length_m,
            little_base_m=little_base_m,
            opening_deg=opening_deg,
        )
        hit = _longest_inside_run(
            filtered,
            polygon,
            split_gap_secs=split_gap_secs,
            min_gs_kt=min_gs_kt,
        )
        if hit is None:
            continue
        duration, max_gs = hit
        if best is None or duration > best.duration_secs:
            best = TakeoffRunwayResult(
                runway_name=str(runway.get("runway_name", "")),
                duration_secs=round(duration, 1),
                max_gs_kt=round(max_gs, 1),
            )
    return best
